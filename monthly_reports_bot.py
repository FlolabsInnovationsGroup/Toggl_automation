import os, csv, json, base64, requests, time, random
from io import StringIO
from datetime import datetime, timedelta, timezone
from collections import defaultdict

"""
Monthly Toggl Reports (Lambda) with Discord Rate Limiter

Sends one Discord message per person with:
- Hours this month
- Total hours in the entire history (across all selected workspaces)

It aggregates by NORMALIZED NAME (not Toggl user_id) so a person is not split across workspaces.
It prefers Reports API v3 totals; if empty, falls back to Reports API v2 summary.

ENV VARS
--------
GOOGLE_SHEET_URL          -> Google Sheets URL (Anyone with link: view)
TEAM_GID                  -> gid for TEAM tab
WORKSPACES_GID            -> gid for WORKSPACES tab
SYSTEM_ALERT_WEBHOOK      -> Discord webhook for admin alerts (optional but recommended)
DISCORD_FALLBACK_WEBHOOK  -> Fallback webhook if a user-specific one is missing (optional)
HTTP_TIMEOUT_SECONDS      -> HTTP timeout for API calls (default: 20)
FILTER_WORKSPACE_IDS      -> optional comma-separated wid list to include (e.g. "123,456")

# History scan controls (optional)
HISTORY_STEP_MONTHS       -> months per window when scanning history (default: 12)
HISTORY_MAX_EMPTY_WINDOWS -> stop after N consecutive empty windows (default: 2)
HISTORY_MAX_YEARS         -> hard cap on how far back to go (default: 15)

# Discord rate limiting (optional)
DISCORD_MIN_INTERVAL_MS        -> per-webhook minimum gap (default: 500)
DISCORD_GLOBAL_MIN_INTERVAL_MS -> global minimum gap between ANY sends (default: 0)
DISCORD_MAX_RETRIES            -> max retries on 429/5xx (default: 5)
"""

# ==========================
# ENV / CONFIG
# ==========================
GOOGLE_SHEET_URL         = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID                 = os.getenv("TEAM_GID", "")
WORKSPACES_GID           = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK     = os.getenv("SYSTEM_ALERT_WEBHOOK", "")
DISCORD_FALLBACK_WEBHOOK = os.getenv("DISCORD_FALLBACK_WEBHOOK", "").strip()
HTTP_TIMEOUT             = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
FILTER_WORKSPACE_IDS_RAW = os.getenv("FILTER_WORKSPACE_IDS", "").strip()

# History scanning controls
HISTORY_STEP_MONTHS        = int(os.getenv("HISTORY_STEP_MONTHS", "12"))
HISTORY_MAX_EMPTY_WINDOWS  = int(os.getenv("HISTORY_MAX_EMPTY_WINDOWS", "2"))
HISTORY_MAX_YEARS          = int(os.getenv("HISTORY_MAX_YEARS", "15"))

# Discord rate limiting knobs
DISCORD_MIN_INTERVAL_MS        = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES            = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

# Optional wid filter (comma-separated list of workspace IDs)
FILTER_WIDS = set()
if FILTER_WORKSPACE_IDS_RAW:
    FILTER_WIDS = {int(x.strip()) for x in FILTER_WORKSPACE_IDS_RAW.split(",") if x.strip().isdigit()}

# ==========================
# Alert helper
# ==========================
def _alert(msg: str):
    print(f"[ALERT] {msg}")
    if SYSTEM_ALERT_WEBHOOK:
        try:
            requests.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
        except Exception:
            pass

# ==========================
# Sheets helpers
# ==========================
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
    resp = requests.get(url, timeout=HTTP_TIMEOUT)
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

# ==========================
# Toggl helpers (Reports v3 totals + v2 summary fallback)
# ==========================
def _headers_for_token(api_token: str) -> dict:
    return {
        "Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode(),
        "Content-Type": "application/json"
    }


def _month_bounds_utc(dt: datetime) -> tuple[datetime, datetime]:
    first = datetime(dt.year, dt.month, 1, 0, 0, 0, tzinfo=timezone.utc)
    if dt.month == 12:
        next_first = datetime(dt.year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    else:
        next_first = datetime(dt.year, dt.month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    last = next_first - timedelta(seconds=1)
    return first, last


def _shift_months(dt: datetime, months: int) -> datetime:
    """
    Shift a UTC datetime to the first day of the target month (00:00 UTC).
    E.g., shift -12 moves 1 year back to the first of that month.
    """
    y = dt.year + (dt.month - 1 + months) // 12
    m = ((dt.month - 1 + months) % 12) + 1
    return datetime(y, m, 1, 0, 0, 0, tzinfo=timezone.utc)


def _sum_seconds_to_hours(seconds: int) -> float:
    return round(max(0, int(seconds)) / 3600.0, 2)


def fetch_totals_by_user_v3(api_token: str, workspace_id: int, start_dt: datetime, end_dt: datetime) -> dict:
    """
    POST /reports/api/v3/workspace/{wid}/search/time_entries/totals
    Body: { start_date, end_date, grouping: "users", grouped: true }
    Returns: {"seconds": {uid:int}, "names": {uid:str}}
    """
    if FILTER_WIDS and workspace_id not in FILTER_WIDS:
        return {"seconds": {}, "names": {}}

    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries/totals"
    payload = {
        "start_date": start_dt.strftime("%Y-%m-%d"),
        "end_date":   end_dt.strftime("%Y-%m-%d"),
        "grouping":   "users",
        "grouped":    True
    }
    try:
        r = requests.post(url, headers=_headers_for_token(api_token), json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"ℹ️ v3 totals failed wid={workspace_id}: {r.status_code} {r.text}")
            return {"seconds": {}, "names": {}}
        data = r.json() or {}
    except Exception as e:
        _alert(f"⚠️ v3 totals exception wid={workspace_id}: {e}")
        return {"seconds": {}, "names": {}}

    seconds_by_user, names_by_user = {}, {}
    groups = data.get("groups") or data.get("data") or []
    for g in groups:
        uid   = g.get("id") or g.get("user_id") or (g.get("group") or {}).get("id")
        secs  = g.get("seconds") or g.get("total_seconds") or 0
        title = g.get("title") or g.get("name") or (g.get("group") or {}).get("name") or ""
        if uid is None:
            continue
        try:
            uid = int(uid)
        except Exception:
            continue
        seconds_by_user[uid] = int(secs or 0)
        names_by_user[uid]   = str(title).strip()

    return {"seconds": seconds_by_user, "names": names_by_user}


def fetch_totals_by_user(api_token: str, workspace_id: int, start_dt: datetime, end_dt: datetime) -> dict:
    """Try v3 totals, then v2 summary fallback."""
    res_v3 = fetch_totals_by_user_v3(api_token, workspace_id, start_dt, end_dt)
    if res_v3["seconds"]:
        return res_v3

    # v2 fallback
    url = "https://api.track.toggl.com/reports/api/v2/summary"
    params = {
        "workspace_id": workspace_id,
        "since": start_dt.strftime("%Y-%m-%d"),
        "until": end_dt.strftime("%Y-%m-%d"),
        "user_agent": "toggl-discord-bot",
        "grouping": "users"
    }
    try:
        r = requests.get(url, headers=_headers_for_token(api_token), params=params, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"⚠️ v2 summary failed wid={workspace_id}: {r.status_code} {r.text}")
            return {"seconds": {}, "names": {}}
        data = r.json() or {}
    except Exception as e:
        _alert(f"⚠️ v2 summary exception wid={workspace_id}: {e}")
        return {"seconds": {}, "names": {}}

    seconds_by_user, names_by_user = {}, {}
    rows = data.get("data") or []
    for row in rows:
        title = row.get("title") or {}
        name  = (title.get("user") or title.get("name") or "").strip()
        ms    = int(row.get("time") or 0)
        if not name:
            continue
        uid = abs(hash(name)) % (10**9)  # pseudo id for internal mapping
        seconds_by_user[uid] = ms // 1000
        names_by_user[uid]   = name

    return {"seconds": seconds_by_user, "names": names_by_user}

# ==========================
# v9 workspaces list
# ==========================
def list_workspaces(api_token: str) -> list[dict]:
    try:
        r = requests.get("https://api.track.toggl.com/api/v9/workspaces",
                         headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"⚠️ v9/workspaces failed for token …{api_token[-4:]}: {r.status_code} {r.text}")
            return []
        return r.json() or []
    except Exception as e:
        _alert(f"⚠️ v9/workspaces exception for token …{api_token[-4:]}: {e}")
        return []

# ==========================
# History aggregation (12-month windows)
# ==========================
def aggregate_history_for_workspace(api_token: str, wid: int, end_dt: datetime) -> dict[str, int]:
    """
    Walk history backwards in HISTORY_STEP_MONTHS windows and aggregate total seconds per normalized name.
    Stops after HISTORY_MAX_EMPTY_WINDOWS consecutive empty windows or HISTORY_MAX_YEARS cap.
    Returns dict[name_norm] = seconds
    """
    totals_by_name = defaultdict(int)

    # Start at the end of the current month
    _, current_end = _month_bounds_utc(end_dt)
    empty_streak = 0
    max_steps = max(1, (HISTORY_MAX_YEARS * 12) // max(1, HISTORY_STEP_MONTHS))

    for _ in range(max_steps):
        # window: [window_start, current_end]
        start_month = _shift_months(current_end, -(HISTORY_STEP_MONTHS - 1))
        window_start = start_month
        window_end   = current_end

        res = fetch_totals_by_user(api_token, wid, window_start, window_end)
        if not res["seconds"]:
            empty_streak += 1
            if empty_streak >= HISTORY_MAX_EMPTY_WINDOWS:
                break
        else:
            empty_streak = 0
            for uid, secs in res["seconds"].items():
                name = res["names"].get(uid, f"User {uid}").strip()
                key  = _norm(name)
                totals_by_name[key] += int(secs or 0)

        # move the cursor to the end of the month prior to window_start
        prev_day = window_start - timedelta(days=1)
        _, current_end = _month_bounds_utc(prev_day)

    return totals_by_name

# ==========================
# Discord rate limiting helpers
# ==========================
_last_send_ts_per_webhook = {}  # webhook_url -> monotonic timestamp
_last_send_ts_global = 0.0      # monotonic timestamp


def _sleep_ms(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)


def _respect_min_interval(webhook_url: str):
    """Ensure per-webhook spacing."""
    now = time.monotonic()
    last = _last_send_ts_per_webhook.get(webhook_url)
    if last is not None:
        elapsed_ms = int((now - last) * 1000)
        if elapsed_ms < DISCORD_MIN_INTERVAL_MS:
            _sleep_ms(DISCORD_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts_per_webhook[webhook_url] = time.monotonic()


def _respect_global_interval():
    """Ensure global spacing across ANY sends."""
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
    """Send a Discord message honoring pacing and 429/5xx backoffs."""
    # Global + per-webhook spacing
    _respect_global_interval()
    _respect_min_interval(webhook_url)

    payload = {"content": content}
    attempt = 0
    base_backoff = 1.0  # seconds
    while attempt < DISCORD_MAX_RETRIES:
        try:
            resp = requests.post(webhook_url, json=payload, timeout=timeout)
            if resp.status_code == 204:
                return True
            if resp.status_code == 429:
                # Rate limited: honor Retry-After or X-RateLimit-Reset-After or body.retry_after
                retry_after = None
                try:
                    retry_after = (resp.headers.get("Retry-After") or
                                   resp.headers.get("X-RateLimit-Reset-After"))
                    if retry_after is None:
                        data = resp.json()
                        retry_after = data.get("retry_after") or data.get("Retry-After")
                except Exception:
                    pass
                try:
                    wait_s = float(retry_after) if retry_after is not None else 2.0
                except Exception:
                    wait_s = 2.0
                wait_s += random.uniform(0.05, 0.25)  # jitter
                time.sleep(wait_s)
                attempt += 1
                continue
            if 500 <= resp.status_code < 600:
                # transient server error: exponential backoff
                wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
                time.sleep(wait_s)
                attempt += 1
                continue
            # Other non-success codes
            _alert(f"⚠️ Discord send failed: {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            # network issue: backoff and retry
            wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            if attempt >= DISCORD_MAX_RETRIES:
                _alert(f"⚠️ Discord send exception: {e}")
                return False
    return False

# ==========================
# Discord message
# ==========================
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

# ==========================
# Lambda entrypoint
# ==========================
def lambda_handler(event, context):
    team = load_team_sheet()
    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}

    workspaces = load_workspace_tokens()
    if not workspaces:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

    # Build webhook lookup by normalized name
    name_to_webhook = { _norm(t["name"]): (t.get("discord_webhook_url", "").strip()) for t in team if t.get("name") }

    # Date windows
    now = datetime.now(timezone.utc)
    m_start, m_end = _month_bounds_utc(datetime(now.year, now.month-1, now.day, 0, 0, 0, tzinfo=timezone.utc))
    m_end = datetime(m_end.year, m_end.month+1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Accumulators by normalized name
    month_sec_by_name = defaultdict(int)
    history_sec_by_name = defaultdict(int)   # lifetime totals
    display_name_for  = {}

    # Collect from each workspace under each token
    for token_entry in workspaces:
        api_token = token_entry.get("api_token")
        if not api_token:
            _alert(f"⚠️ Missing api_token in workspace row: {token_entry}")
            continue

        wid_list = list_workspaces(api_token)
        for ws in wid_list:
            wid = ws.get("id")
            if not wid:
                continue
            if FILTER_WIDS and wid not in FILTER_WIDS:
                continue

            # Monthly totals for current month
            month_res = fetch_totals_by_user(api_token, wid, m_start, m_end)
            for uid, secs in month_res["seconds"].items():
                name = month_res["names"].get(uid, f"User {uid}").strip()
                key  = _norm(name)
                month_sec_by_name[key] += int(secs or 0)
                if len(name) > len(display_name_for.get(key, "")):
                    display_name_for[key] = name

            # Lifetime totals via HISTORY_STEP_MONTHS windows
            hist_totals = aggregate_history_for_workspace(api_token, wid, now)
            for key, secs in hist_totals.items():
                history_sec_by_name[key] += int(secs or 0)
                # prefer the prettier name if we already captured it from month_res; otherwise default Title case
                display_name_for.setdefault(key, key.title())

    # Compose and send
    sent = 0
    processed = 0

    # Use union of keys so users with month but no history (or vice versa) still send
    all_keys = set(month_sec_by_name.keys()) | set(history_sec_by_name.keys())

    for key in sorted(all_keys):
        processed += 1
        name = display_name_for.get(key, key.title())
        month_hours  = _sum_seconds_to_hours(month_sec_by_name.get(key, 0))
        total_hours  = _sum_seconds_to_hours(history_sec_by_name.get(key, 0))

        # Skip completely empty users to reduce noise; comment out if you want everyone
        if month_hours == 0 and total_hours == 0:
            continue

        msg = format_discord_message(name, month_hours, total_hours, m_start, m_end)
        webhook = name_to_webhook.get(key) or DISCORD_FALLBACK_WEBHOOK
        if not webhook:
            _alert(f"ℹ️ No webhook for '{name}' and no DISCORD_FALLBACK_WEBHOOK; skipping send.")
            continue

        ok = send_discord_with_backoff(webhook, msg, timeout=HTTP_TIMEOUT)
        if ok:
            sent += 1
        else:
            _alert(f"⚠️ Discord send failed for {name} (after retries)")

    return {"status": "ok", "users_processed": processed, "sent": sent}
