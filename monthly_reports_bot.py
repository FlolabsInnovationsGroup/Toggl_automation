import os, csv, json, base64, requests, time, random, boto3
from io import StringIO
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from botocore.exceptions import ClientError

SESSION = requests.Session()

GOOGLE_SHEET_URL         = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID                 = os.getenv("TEAM_GID", "")
WORKSPACES_GID           = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK     = os.getenv("SYSTEM_ALERT_WEBHOOK", "")
DISCORD_FALLBACK_WEBHOOK = os.getenv("DISCORD_FALLBACK_WEBHOOK", "").strip()
HTTP_TIMEOUT             = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
FILTER_WORKSPACE_IDS_RAW = os.getenv("FILTER_WORKSPACE_IDS", "").strip()
DYNAMODB_TABLE           = os.getenv("DYNAMODB_TABLE", "")

HISTORY_STEP_MONTHS        = int(os.getenv("HISTORY_STEP_MONTHS", "12"))
HISTORY_MAX_EMPTY_WINDOWS  = int(os.getenv("HISTORY_MAX_EMPTY_WINDOWS", "2"))
HISTORY_MAX_YEARS          = int(os.getenv("HISTORY_MAX_YEARS", "15"))

DISCORD_MIN_INTERVAL_MS        = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES            = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

FILTER_WIDS = set()
if FILTER_WORKSPACE_IDS_RAW:
    FILTER_WIDS = {int(x.strip()) for x in FILTER_WORKSPACE_IDS_RAW.split(",") if x.strip().isdigit()}

def _env_bool(name: str, default: bool = False) -> bool:
    val = (os.getenv(name, "") or "").strip().lower()
    if val == "" and default:
        return True
    return val in ("1","true","yes","y","on")

DRY_RUN_NO_SEND    = _env_bool("DRY_RUN_NO_SEND", False)
SEND_NOTIFICATIONS = _env_bool("SEND_NOTIFICATIONS", True)
NOTIFY_ENABLED     = (not DRY_RUN_NO_SEND) and SEND_NOTIFICATIONS

MAX_SEEN_IDS       = int(os.getenv("MAX_SEEN_IDS", "500000"))

BACKFILL_PAGE_SIZE     = int(os.getenv("BACKFILL_PAGE_SIZE", "200"))
BACKFILL_MAX_PAGES     = int(os.getenv("BACKFILL_MAX_PAGES", "200"))
BACKFILL_PAUSE_MS      = int(os.getenv("BACKFILL_PAUSE_MS", "75"))
BACKFILL_RETRY_MAX     = int(os.getenv("BACKFILL_RETRY_MAX", "5"))
BACKFILL_RETRY_BASE_MS = int(os.getenv("BACKFILL_RETRY_BASE_MS", "250"))

# New guardrails / observability
BACKFILL_TIME_BUDGET_MS     = int(os.getenv("BACKFILL_TIME_BUDGET_MS", "60000"))  # stop backfill when < this left
WARN_ITEM_KB_OVER           = int(os.getenv("WARN_ITEM_KB_OVER", "340"))
ABORT_SAVE_IF_ITEM_KB_OVER  = int(os.getenv("ABORT_SAVE_IF_ITEM_KB_OVER", "380"))

dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(DYNAMODB_TABLE)

def _alert(msg: str):
    print(f"[ALERT] {msg}")
    if SYSTEM_ALERT_WEBHOOK:
        try:
            SESSION.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
        except Exception:
            pass

def _estimate_item_kb(item: dict) -> int:
    try:
        return int(len(json.dumps(item, separators=(',',':')).encode('utf-8')) / 1024)
    except Exception:
        return -1

def _ddb_put_with_retry(item: dict, user_key: str) -> tuple[bool, float, str]:
    attempt = 0
    base = 0.15
    start = time.perf_counter()
    while attempt <= BACKFILL_RETRY_MAX:
        try:
            table.put_item(Item=item)
            latency = time.perf_counter() - start
            return True, latency, ""
        except ClientError as ce:
            code = ce.response.get("Error", {}).get("Code", "")
            if code in ("ProvisionedThroughputExceededException", "ThrottlingException") or code.startswith("5"):
                wait_s = base * (2 ** attempt) + random.uniform(0.01, 0.08)
                _alert(f"[WARN] DDB throttled on {user_key} (attempt {attempt+1}/{BACKFILL_RETRY_MAX}); backoff {wait_s:.2f}s ({code})")
                time.sleep(wait_s)
                attempt += 1
                continue
            latency = time.perf_counter() - start
            return False, latency, f"{code}: {str(ce)}"
        except Exception as e:
            latency = time.perf_counter() - start
            return False, latency, f"{type(e).__name__}: {e}"
    latency = time.perf_counter() - start
    return False, latency, "MaxRetriesExceeded"

def _get_user_state(user_key: str) -> dict:
    try:
        resp = table.get_item(Key={"user_name": user_key})
        item = resp.get("Item", {}) or {}
        seen_entries   = item.get("seen_entries", []) or []
        seen_updates   = item.get("seen_updates", {}) or {}
        seen_snapshots = item.get("seen_snapshots", {}) or {}
        seen_updates   = {str(k): str(v) for k, v in seen_updates.items()}
        seen_snapshots = {str(k): v for k, v in seen_snapshots.items()}
        return {"seen_entries": seen_entries, "seen_updates": seen_updates, "seen_snapshots": seen_snapshots}
    except Exception as e:
        _alert(f"[ERROR] DynamoDB get error for {user_key}: {e}")
        return {"seen_entries": [], "seen_updates": {}, "seen_snapshots": {}}

def _save_user_state(user_key: str, seen_entries: list, seen_updates: dict, seen_snapshots: dict, metrics: dict | None = None):
    try:
        if MAX_SEEN_IDS > 0 and len(seen_entries) > MAX_SEEN_IDS:
            capped_ids = list(seen_entries[-MAX_SEEN_IDS:])
        else:
            capped_ids = list(seen_entries)
        keep = set(capped_ids)
        pruned_updates   = {k: v for k, v in seen_updates.items()   if k in keep}
        pruned_snapshots = {k: v for k, v in seen_snapshots.items() if k in keep}
        item = {
            "user_name": user_key,
            "seen_entries": capped_ids,
            "seen_updates": pruned_updates,
            "seen_snapshots": pruned_snapshots
        }
        approx_kb = _estimate_item_kb(item)
        if approx_kb >= WARN_ITEM_KB_OVER:
            _alert(f"[WARN] Large DDB item {user_key}: ~{approx_kb} KB | ids={len(capped_ids)} snaps={len(pruned_snapshots)}")
        if approx_kb >= ABORT_SAVE_IF_ITEM_KB_OVER:
            _alert(f"[ERROR] Aborting save for {user_key}: estimated size {approx_kb} KB ≥ {ABORT_SAVE_IF_ITEM_KB_OVER} KB")
            if metrics is not None:
                metrics["ddb_skipped_oversize"] += 1
            return False

        ok, latency, err = _ddb_put_with_retry(item, user_key)
        if metrics is not None:
            metrics["ddb_put_count"] += 1
            metrics["ddb_put_latency_sum_sec"] += latency
            metrics["ddb_put_max_latency_sec"]  = max(metrics["ddb_put_max_latency_sec"], latency)
        if not ok:
            _alert(f"[ERROR] DynamoDB put failed for {user_key} after {latency:.2f}s: {err}")
        else:
            print(f"[INFO] DDB put {user_key} ok in {latency:.2f}s (~{approx_kb} KB)")
        return ok
    except Exception as e:
        _alert(f"[ERROR] DynamoDB put error for {user_key}: {e}")
        if metrics is not None:
            metrics["ddb_put_errors"] += 1
        return False

def _sheet_export_csv_url(sheet_url: str, gid: str) -> str:
    if not sheet_url or not gid:
        return ""
    try:
        sid = sheet_url.split("/d/")[1].split("/")[0]
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
    except Exception:
        return ""

def _fetch_csv_rows(sheet_url: str, gid: str) -> list[list[str]]:
    url = _sheet_export_csv_url(sheet_url, gid)
    if not url:
        return []
    resp = SESSION.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return list(csv.reader(StringIO(resp.text)))

def _find_header(headers: list[str], *cands) -> int | None:
    h = [x.strip().lower() for x in headers]
    for c in cands:
        if c in h:
            return h.index(c)
    return None

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def load_team_sheet() -> list[dict]:
    rows = _fetch_csv_rows(GOOGLE_SHEET_URL, TEAM_GID)
    if not rows:
        _alert("TEAM tab not reachable or empty")
        return []
    headers = rows[0]
    i_name = _find_header(headers, "name", "full name", "fullname")
    i_web  = _find_header(headers, "discord_webhook_url", "webhook", "discord webhook")
    team = []
    for r in rows[1:]:
        try:
            name = r[i_name].strip() if i_name is not None and i_name < len(r) else ""
            if not name:
                continue
            item = {"name": name}
            if i_web is not None and i_web < len(r) and r[i_web].strip():
                item["discord_webhook_url"] = r[i_web].strip()
            team.append(item)
        except Exception:
            continue
    return team

def load_workspace_tokens() -> list[dict]:
    rows = _fetch_csv_rows(GOOGLE_SHEET_URL, WORKSPACES_GID)
    if not rows:
        _alert("WORKSPACES tab not reachable or empty")
        return []
    headers = rows[0]
    i_name = _find_header(headers, "name")
    i_tok  = _find_header(headers, "api_token", "token", "api token")
    out = []
    for r in rows[1:]:
        try:
            name = r[i_name].strip() if i_name is not None and i_name < len(r) else ""
            tok  = r[i_tok].strip()  if i_tok  is not None and i_tok  < len(r) else ""
            if name and tok:
                out.append({"name": name, "api_token": tok})
        except Exception:
            continue
    return out

def _headers_for_token(api_token: str) -> dict:
    return {
        "Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode(),
        "Content-Type": "application/json"
    }

def list_workspaces(api_token: str) -> list[dict]:
    try:
        r = SESSION.get("https://api.track.toggl.com/api/v9/workspaces",
                        headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"⚠️ v9/workspaces failed …{api_token[-4:]}: {r.status_code} {r.text}")
            return []
        return r.json() or []
    except Exception as e:
        _alert(f"⚠️ v9/workspaces exception …{api_token[-4:]}: {e}")
        return []

def get_workspace_users(api_token: str, workspace_id: int) -> list[dict]:
    try:
        r = SESSION.get(f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/users",
                        headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"⚠️ v9/users wid={workspace_id}: {r.status_code} {r.text}")
            return []
        return r.json() or []
    except Exception as e:
        _alert(f"⚠️ v9/users wid={workspace_id} exception: {e}")
        return []

def _sleep_ms(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)

def fetch_window_totals_v3(api_token: str, workspace_id: int, start_dt: datetime, end_dt: datetime, user_ids: list[int]) -> dict[int,int]:
    """
    Lightweight probe to see if specified users have any seconds in the window.
    Returns dict {user_id: seconds}
    """
    if not user_ids:
        return {}
    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries/totals"
    payload = {
        "start_date": start_dt.strftime("%Y-%m-%d"),
        "end_date":   end_dt.strftime("%Y-%m-%d"),
        "grouping":   "users",
        "grouped":    True,
        "user_ids":   user_ids
    }
    try:
        r = SESSION.post(url, headers=_headers_for_token(api_token), json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"[WARN] v3 totals probe wid={workspace_id}: {r.status_code} {r.text}")
            return {}
        data = r.json() or {}
        totals = {}
        groups = data.get("groups") or data.get("data") or []
        for g in groups:
            uid = g.get("id") or g.get("user_id") or (g.get("group") or {}).get("id")
            secs = g.get("seconds") or g.get("total_seconds") or 0
            try:
                uid = int(uid)
                totals[uid] = int(secs or 0)
            except Exception:
                pass
        return totals
    except Exception as e:
        _alert(f"[WARN] v3 totals probe exception wid={workspace_id}: {e}")
        return {}

def search_time_entries_v3(api_token: str, workspace_id: int,
                           start_dt: datetime, end_dt: datetime,
                           user_ids: list[int] | None = None) -> list[dict]:
    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries"
    params = {"page_size": BACKFILL_PAGE_SIZE, "page": 1}
    body = {
        "start_date": start_dt.strftime("%Y-%m-%d"),
        "end_date":   end_dt.strftime("%Y-%m-%d"),
        "grouped": False,
    }
    if user_ids:
        body["user_ids"] = [int(u) for u in user_ids if isinstance(u, int)]

    out = []
    page = 1
    while page <= BACKFILL_MAX_PAGES:
        params["page"] = page
        attempt = 0
        while attempt < BACKFILL_RETRY_MAX:
            try:
                r = SESSION.post(url, headers=_headers_for_token(api_token), params=params, json=body, timeout=HTTP_TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    break
                if r.status_code == 429 or (500 <= r.status_code < 600):
                    ra = r.headers.get("Retry-After") or r.headers.get("X-RateLimit-Reset-After")
                    try:
                        wait_ms = int(float(ra) * 1000) if ra else BACKFILL_RETRY_BASE_MS * (2 ** attempt)
                    except Exception:
                        wait_ms = BACKFILL_RETRY_BASE_MS * (2 ** attempt)
                    wait_ms += int(random.uniform(10, 40))
                    _sleep_ms(wait_ms)
                    attempt += 1
                    continue
                _alert(f"[WARN] v3 search failed wid={workspace_id} p={page}: {r.status_code} {r.text}")
                data = None
                break
            except Exception as e:
                wait_ms = BACKFILL_RETRY_BASE_MS * (2 ** attempt) + int(random.uniform(10, 40))
                _sleep_ms(wait_ms)
                attempt += 1
                if attempt >= BACKFILL_RETRY_MAX:
                    _alert(f"[WARN] v3 search exception wid={workspace_id} p={page}: {e}")
                    data = None

        if not data:
            break

        if isinstance(data, list):
            rows = data
            total = 0
        elif isinstance(data, dict):
            rows = data.get("data") or data.get("time_entries") or data.get("results") or []
            total = int(data.get("total_count") or data.get("total") or 0)
        else:
            rows, total = [], 0

        if not rows:
            break

        out.extend(rows)
        if total and len(out) >= total:
            break

        page += 1
        _sleep_ms(BACKFILL_PAUSE_MS)

    return out

def _month_bounds_utc(dt: datetime) -> tuple[datetime, datetime]:
    if dt.month == 1:
        first = datetime(dt.year - 1, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    else:
        first = datetime(dt.year, dt.month - 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    last = datetime(dt.year, dt.month, 1, 0, 0, 0, tzinfo=timezone.utc) - timedelta(days=1)
    return first, last

def _shift_months(dt: datetime, months: int) -> datetime:
    y = dt.year + (dt.month - 1 + months) // 12
    m = ((dt.month - 1 + months) % 12) + 1
    return datetime(y, m, 1, 0, 0, 0, tzinfo=timezone.utc)

def _history_windows(end_dt: datetime):
    _, current_end = _month_bounds_utc(end_dt)
    max_steps = max(1, (HISTORY_MAX_YEARS * 12) // max(1, HISTORY_STEP_MONTHS))
    for _ in range(max_steps):
        start_month = _shift_months(current_end, -(HISTORY_STEP_MONTHS - 1))
        window_start = start_month
        window_end   = current_end
        yield (window_start, window_end)
        prev_day = window_start - timedelta(days=1)
        _, current_end = _month_bounds_utc(prev_day)

def _parse_entry_user_id(e: dict) -> int | None:
    uid = e.get("user_id")
    if isinstance(uid, (int, float)):
        return int(uid)
    uo = e.get("user")
    if isinstance(uo, dict):
        u = uo.get("id")
        if isinstance(u, (int, float)):
            return int(u)
    return None

def _parse_entry_project(e: dict) -> str:
    p = e.get("project")
    if isinstance(p, dict):
        n = p.get("name")
        if isinstance(n, str) and n.strip():
            return n.strip()
    if isinstance(p, str) and p.strip():
        return p.strip()
    if isinstance(p, list) and p:
        first = p[0]
        if isinstance(first, dict) and isinstance(first.get("name"), str) and first["name"].strip():
            return first["name"].strip()
        if isinstance(first, str) and first.strip():
            return first.strip()
    pn = e.get("project_name")
    if isinstance(pn, str) and pn.strip():
        return pn.strip()
    return "No project"

def _parse_entry_tags(e: dict) -> list:
    t = e.get("tags")
    if isinstance(t, list):
        return [str(x) for x in t if isinstance(x, (str, int, float))]
    return []

def _parse_entry_seconds(e: dict) -> int:
    if "seconds" in e and isinstance(e["seconds"], (int, float)):
        return max(0, int(e["seconds"]))
    if "dur" in e and isinstance(e["dur"], (int, float)):
        return max(0, int(e["dur"])) // 1000
    if "time" in e and isinstance(e["time"], (int, float)):
        return max(0, int(e["time"])) // 1000
    return 0

def _safe_str(v) -> str:
    return v if isinstance(v, str) else ""

def _write_entries_to_db(primary_key: str, state_keys: list[str], entries: list[dict], user_uid: int | None, metrics: dict):
    if not entries:
        return 0
    st = _get_user_state(primary_key)
    seen_entries   = list(st.get("seen_entries", []))
    seen_updates   = dict(st.get("seen_updates", {}))
    seen_snapshots = dict(st.get("seen_snapshots", {}))

    added = 0
    for e in entries:
        if not isinstance(e, dict):
            continue
        uid = _parse_entry_user_id(e)
        if user_uid is not None and uid is not None and int(uid) != int(user_uid):
            continue

        eid = str(e.get("id") or "")
        if not eid:
            continue
        desc      = _safe_str(e.get("description") or e.get("title") or "")
        project   = _parse_entry_project(e)
        tags      = _parse_entry_tags(e)
        start_s   = _safe_str(e.get("start") or e.get("at") or "")
        updated_s = _safe_str(e.get("updated") or e.get("at") or start_s)
        seconds   = _parse_entry_seconds(e)
        dur_ms    = seconds * 1000

        snap = {
            "dur": dur_ms,
            "desc": desc,
            "project": project or "No project",
            "tags": tags,
            "start": start_s,
            "updated": updated_s if updated_s else start_s,
        }

        if eid not in seen_entries:
            seen_entries.append(eid)
        seen_updates[eid]   = snap["updated"]
        seen_snapshots[eid] = snap
        added += 1

    if added:
        ok = _save_user_state(primary_key, seen_entries, seen_updates, seen_snapshots, metrics)
        if not ok:
            _alert(f"[WARN] Save skipped/failed for primary key {primary_key}; secondary keys not mirrored.")
            return added

    # mirror IDs to secondary keys (snapshots remain only on primary)
    for sk in state_keys[1:]:
        st2 = _get_user_state(sk)
        ids2 = list(st2.get("seen_entries", []))
        ups2 = dict(st2.get("seen_updates", {}))
        snaps2 = dict(st2.get("seen_snapshots", {}))
        changed = False
        for eid in seen_entries:
            if eid not in ids2:
                ids2.append(eid)
                if eid not in ups2 and eid in seen_updates:
                    ups2[eid] = seen_updates[eid]
                changed = True
        if changed:
            _save_user_state(sk, ids2, ups2, snaps2, metrics)

    return added

def _parse_iso_opt(ts: str):
    try:
        ts = ts.replace("Z", "+0000").replace("+00:00", "+0000")
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return None

def aggregate_month_from_state(username: str, keys: list[str], month_start: datetime, month_end: datetime) -> dict:
    total_seconds = 0
    per_project_seconds = defaultdict(int)
    seen_eids = set()
    for k in keys:
        st = _get_user_state(k)
        snaps = st.get("seen_snapshots", {}) or {}
        for eid, snap in snaps.items():
            if eid in seen_eids:
                continue
            start_s = snap.get("start") or ""
            dur_ms  = int(snap.get("dur") or 0)
            project = snap.get("project") or "No project"
            start_dt = _parse_iso_opt(start_s)
            if not start_dt:
                continue
            if not start_dt.tzinfo:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            if month_start <= start_dt <= month_end:
                secs = max(0, dur_ms // 1000)
                total_seconds += secs
                per_project_seconds[project] += secs
                seen_eids.add(eid)
    return {"username": username, "total_seconds": total_seconds, "per_project_seconds": dict(per_project_seconds)}

def aggregate_total_from_state(keys: list[str]) -> int:
    total_seconds = 0
    seen_eids = set()
    for k in keys:
        st = _get_user_state(k)
        snaps = st.get("seen_snapshots", {}) or {}
        for eid, snap in snaps.items():
            if eid in seen_eids:
                continue
            total_seconds += max(0, int(snap.get("dur") or 0) // 1000)
            seen_eids.add(eid)
    return total_seconds

def _sum_seconds_to_hours(seconds: int) -> float:
    return round(max(0, int(seconds)) / 3600.0, 2)

def format_discord_message(name: str, hours_month: float, hours_total: float, m_start: datetime, m_end: datetime) -> str:
    period = f"{m_start.strftime('%b %d')}–{m_end.strftime('%b %d, %Y')}"
    now    = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return "\n".join([
        "# 📆 MONTHLY TIME REPORT",
        "—" * 30,
        f"## 👤 **Employee:** {name}",
        f"🗓️ **Period:** {period}",
        f"⏰ **Hours this month:** {hours_month:.2f} h",
        f"📈 **Total hours (company history):** {hours_total:.2f} h",
        "",
        f"🤖 *Generated on {now} UTC*"
    ])

_last_send_ts_per_webhook = {}
_last_send_ts_global = 0.0

def _respect_min_interval(webhook_url: str):
    now = time.monotonic()
    last = _last_send_ts_per_webhook.get(webhook_url)
    if last is not None:
        elapsed_ms = int((now - last) * 1000)
        if elapsed_ms < DISCORD_MIN_INTERVAL_MS:
            _sleep_ms(DISCORD_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts_per_webhook[webhook_url] = time.monotonic()

def _respect_global_interval():
    global _last_send_ts_global
    if DISCORD_GLOBAL_MIN_INTERVAL_MS <= 0:
        return
    now = time.monotonic()
    if _last_send_ts_global > 0:
        elapsed_ms = int((now - _last_send_ts_global) * 1000)
        if elapsed_ms < DISCORD_GLOBAL_MIN_INTERVAL_MS:
            _sleep_ms(DISCORD_GLOBAL_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts_global = time.monotonic()

def send_discord_with_backoff(webhook_url: str, content: str, timeout: int = 20) -> bool:
    if not NOTIFY_ENABLED:
        return True
    _respect_global_interval()
    _respect_min_interval(webhook_url)
    payload = {"content": content}
    attempt = 0
    base_backoff = 1.0
    while attempt < DISCORD_MAX_RETRIES:
        try:
            resp = SESSION.post(webhook_url, json=payload, timeout=timeout)
            if resp.status_code == 204:
                return True
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After") or resp.headers.get("X-RateLimit-Reset-After")
                try:
                    wait_s = float(ra) if ra else 2.0
                except Exception:
                    wait_s = 2.0
                wait_s += random.uniform(0.05, 0.25)
                time.sleep(wait_s)
                attempt += 1
                continue
            if 500 <= resp.status_code < 600:
                wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
                time.sleep(wait_s)
                attempt += 1
                continue
            _alert(f"⚠️ Discord send failed: {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            if attempt >= DISCORD_MAX_RETRIES:
                _alert(f"⚠️ Discord send exception: {e}")
                return False
    return False

def lambda_handler(event, context):
    team = load_team_sheet()
    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}

    workspaces = load_workspace_tokens()
    if not workspaces:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

    name_to_webhook = { _norm(t["name"]): (t.get("discord_webhook_url", "").strip()) for t in team if t.get("name") }

    name_to_state_keys = defaultdict(list)
    for t in team:
        nm = _norm(t["name"])
        k  = f"name:{nm}"
        if k not in name_to_state_keys[nm]:
            name_to_state_keys[nm].append(k)

    discovered = set()
    wid_to_team_uids = defaultdict(list)  # NEW: map wid -> list of team user_ids for filtering
    for token_entry in workspaces:
        api_token = token_entry.get("api_token")
        if not api_token:
            _alert(f"⚠️ Missing api_token in workspace row: {token_entry}")
            continue
        for ws in list_workspaces(api_token):
            wid = ws.get("id")
            if not wid or (FILTER_WIDS and wid not in FILTER_WIDS):
                continue
            for u in get_workspace_users(api_token, wid):
                uid  = u.get("id")
                full = u.get("fullname") or u.get("name") or ""
                if not uid or not full:
                    continue
                key = (wid, int(uid))
                if key in discovered:
                    continue
                discovered.add(key)
                nm_ws = _norm(full)
                if nm_ws in name_to_state_keys:
                    uid_key = f"uid:{int(uid)}"
                    if uid_key not in name_to_state_keys[nm_ws]:
                        name_to_state_keys[nm_ws].insert(0, uid_key)
                    wid_to_team_uids[wid].append(int(uid))

    now = datetime.now(timezone.utc)
    m_start, m_end = _month_bounds_utc(now)

    metrics = {
        "backfilled_windows": 0,
        "backfilled_entries": 0,
        "rows_fetched": 0,
        "rows_skipped_by_totals": 0,
        "windows_empty_for_team": 0,
        "windows_skipped_timebudget": 0,
        "ddb_put_count": 0,
        "ddb_put_errors": 0,
        "ddb_put_latency_sum_sec": 0.0,
        "ddb_put_max_latency_sec": 0.0,
        "ddb_skipped_oversize": 0,
    }

    # ========= DRY-RUN BACKFILL (with user filtering + totals probe) =========
    if DRY_RUN_NO_SEND:
        for token_entry in workspaces:
            api_token = token_entry.get("api_token")
            if not api_token:
                continue
            for ws in list_workspaces(api_token):
                wid = ws.get("id")
                if not wid or (FILTER_WIDS and wid not in FILTER_WIDS):
                    continue

                team_uids = wid_to_team_uids.get(wid, [])
                if not team_uids:
                    _alert(f"[INFO] Skipping wid={wid}: no team members in this workspace.")
                    continue

                # name -> uid (for primary key hint if missing)
                nm_to_uid = {}
                for u in get_workspace_users(api_token, wid):
                    uid  = u.get("id")
                    full = u.get("fullname") or u.get("name") or ""
                    if uid and full:
                        nm_to_uid[_norm(full)] = int(uid)

                empty_streak = 0
                for (win_start, win_end) in _history_windows(now):
                    remaining = context.get_remaining_time_in_millis() if context else 999999
                    if remaining < BACKFILL_TIME_BUDGET_MS:
                        metrics["windows_skipped_timebudget"] += 1
                        _alert(f"[WARN] Stopping backfill early (wid={wid}): {remaining}ms left < budget {BACKFILL_TIME_BUDGET_MS}ms")
                        break
                    if empty_streak >= HISTORY_MAX_EMPTY_WINDOWS:
                        break

                    # FAST PROBE: do our team users have any time in this window?
                    totals = fetch_window_totals_v3(api_token, wid, win_start, win_end, team_uids)
                    if not totals or sum(totals.values()) == 0:
                        metrics["rows_skipped_by_totals"] += 1
                        empty_streak += 1
                        print(f"[INFO] totals probe: wid={wid} {win_start.date()}→{win_end.date()} team_seconds=0 (skip)")
                        continue

                    t0 = time.perf_counter()
                    rows = search_time_entries_v3(api_token, wid, win_start, win_end, user_ids=team_uids)
                    dt = time.perf_counter() - t0
                    print(f"[INFO] v3 window wid={wid} {win_start.date()}→{win_end.date()} fetched={len(rows)} in {dt:.2f}s (team-filtered)")
                    if not rows:
                        empty_streak += 1
                        continue

                    metrics["backfilled_windows"] += 1
                    metrics["rows_fetched"] += len(rows)

                    # Group by normalized display name
                    rows_by_name = defaultdict(list)
                    for e in rows:
                        disp = ""
                        uo = e.get("user")
                        if isinstance(uo, dict):
                            disp = (uo.get("fullname") or uo.get("name") or "").strip()
                        if not disp:
                            disp = (e.get("user_name") or e.get("username") or "").strip()
                        if not disp:
                            continue
                        rows_by_name[_norm(disp)].append(e)

                    window_added = 0
                    for nm, entries in rows_by_name.items():
                        if nm not in name_to_state_keys:
                            continue
                        state_keys = name_to_state_keys[nm]
                        primary_key = state_keys[0]
                        user_uid = None
                        if primary_key.startswith("uid:"):
                            try:
                                user_uid = int(primary_key.split(":",1)[1])
                            except Exception:
                                user_uid = nm_to_uid.get(nm)
                        added = _write_entries_to_db(primary_key, state_keys, entries, user_uid, metrics)
                        window_added += added
                        metrics["backfilled_entries"] += added
                        if added:
                            print(f"[INFO] upsert nm={nm} key={primary_key} added={added}")

                    if window_added == 0:
                        empty_streak += 1
                        metrics["windows_empty_for_team"] += 1
                    else:
                        empty_streak = 0

                    _sleep_ms(BACKFILL_PAUSE_MS)

    # ========= Compute from DynamoDB only =========
    sent = 0
    processed = 0
    for t in team:
        username = t["name"]
        nm = _norm(username)
        keys = name_to_state_keys.get(nm, [f"name:{nm}"])

        # monthly & lifetime
        # (Optionally, you can add per-project breakdown here if you want to show it monthly.)
        month_agg  = aggregate_month_from_state(username, keys, m_start, m_end)
        total_secs = aggregate_total_from_state(keys)

        msg = format_discord_message(
            username,
            _sum_seconds_to_hours(month_agg["total_seconds"]),
            _sum_seconds_to_hours(total_secs),
            m_start, m_end
        )
        processed += 1

        webhook = name_to_webhook.get(nm) or DISCORD_FALLBACK_WEBHOOK
        if not webhook:
            _alert(f"ℹ️ No webhook for '{username}' and no DISCORD_FALLBACK_WEBHOOK; skipping send.")
            continue
        if send_discord_with_backoff(webhook, msg, timeout=HTTP_TIMEOUT):
            sent += 1

    # DDB perf summary
    avg = (metrics["ddb_put_latency_sum_sec"] / metrics["ddb_put_count"]) if metrics["ddb_put_count"] else 0.0
    print(f"[INFO] DDB puts={metrics['ddb_put_count']} avg={avg:.3f}s max={metrics['ddb_put_max_latency_sec']:.3f}s "
          f"errors={metrics['ddb_put_errors']} oversize_skips={metrics['ddb_skipped_oversize']}")

    return {
        "status": "complete",
        "dry_run_no_send": DRY_RUN_NO_SEND,
        "send_notifications": SEND_NOTIFICATIONS,
        "notify_enabled": NOTIFY_ENABLED,
        "users_processed": processed,
        "sent": sent,
        "month_start": m_start.strftime("%Y-%m-%d"),
        "month_end":   m_end.strftime("%Y-%m-%d"),
        # backfill metrics
        **metrics,
    }
