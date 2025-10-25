import os
import json
import base64
import requests
import csv
import time
import random
from io import StringIO
import boto3
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Reuse a single HTTP session for all outbound requests
SESSION = requests.Session()

# ==========================
# ENVIRONMENT
# ==========================
GOOGLE_SHEET_URL     = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID             = os.getenv("TEAM_GID", "")
WORKSPACES_GID       = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK = os.getenv("SYSTEM_ALERT_WEBHOOK", "")

# Dynamo (still used for notification state / deletion detection)
DYNAMODB_TABLE      = os.getenv("DYNAMODB_TABLE", "")
TIME_WINDOW_DAYS    = int(os.getenv("TIME_WINDOW_DAYS", "24"))
MAX_SEEN_IDS        = int(os.getenv("MAX_SEEN_IDS", "50"))
HTTP_TIMEOUT        = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
HARD_START_FILTER   = os.getenv("HARD_START_FILTER", "1") == "1"  # default ON

# Discord rate limiting
DISCORD_MIN_INTERVAL_MS        = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES            = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "")
    if val == "" and default:
        return True
    s = (val or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")

# Feature flags
DRY_RUN_NO_SEND     = _env_bool("DRY_RUN_NO_SEND", False)       # keep DB updated, but don't notify
SEND_NOTIFICATIONS  = _env_bool("SEND_NOTIFICATIONS", True)     # master switch for notifications
NOTIFY_ENABLED      = (not DRY_RUN_NO_SEND) and SEND_NOTIFICATIONS

# ===== Notion env =====
NOTION_ENABLED      = _env_bool("NOTION_ENABLED", False)
NOTION_TOKEN        = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID  = os.getenv("NOTION_DATABASE_ID", "").strip()          # ⌚ Time Entries
NOTION_TEAM_DB_ID   = os.getenv("NOTION_TEAM_DB_ID", "").strip()           # 👥 Team Members
NOTION_BATCH_SIZE   = int(os.getenv("NOTION_BATCH_SIZE", "25"))            # create/update batch
NOTION_RPS          = float(os.getenv("NOTION_RPS", "0"))                  # keep < 3 RPS
NOTION_FLUSH_EVERY = int(os.getenv("NOTION_FLUSH_EVERY", "200"))  # tune 100–400

# Property names (override only if you renamed them in Notion)
# IMPORTANT: Name (title) now stores the **User Key** (uid:... or name:...)
NOTION_PROP_NAME         = os.getenv("NOTION_PROP_NAME", "User Key")                  # title
NOTION_PROP_TEAM_MEMBER  = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")   # relation to Team Members
NOTION_PROP_ENTRY_ID     = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")         # number
NOTION_PROP_PROJECT      = os.getenv("NOTION_PROP_PROJECT", "Project")           # select
NOTION_PROP_TAGS         = os.getenv("NOTION_PROP_TAGS", "Tags")                 # multi_select
NOTION_PROP_START        = os.getenv("NOTION_PROP_START", "Start")               # date
NOTION_PROP_END          = os.getenv("NOTION_PROP_END", "End")                   # date
NOTION_PROP_UPDATED      = os.getenv("NOTION_PROP_UPDATED", "Updated")           # date
NOTION_PROP_DURATION_S   = os.getenv("NOTION_PROP_DURATION_S", "Duration")       # number (seconds)
NOTION_PROP_WORKSPACE    = os.getenv("NOTION_PROP_WORKSPACE", "Workspace")       # number
NOTION_PROP_DESCRIPTION  = os.getenv("NOTION_PROP_DESCRIPTION", "Description")   # rich_text

# Optional one-shot/history backfill into Notion
NOTION_BACKFILL              = _env_bool("NOTION_BACKFILL", False)
HISTORY_STEP_MONTHS          = int(os.getenv("HISTORY_STEP_MONTHS", "12"))
HISTORY_MAX_EMPTY_WINDOWS    = int(os.getenv("HISTORY_MAX_EMPTY_WINDOWS", "2"))
HISTORY_MAX_YEARS            = int(os.getenv("HISTORY_MAX_YEARS", "15"))

# AWS clients (for state / notifications only)
dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(DYNAMODB_TABLE)

# ==========================
# Alerts
# ==========================
def _alert(msg: str):
    print(f"[ALERT] {msg}")
    if SYSTEM_ALERT_WEBHOOK:
        try:
            SESSION.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
        except Exception:
            pass

# ==========================
# Google Sheets helpers
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
    i_email= _find_header(headers, "email")
    team = []
    for r in rows[1:]:
        try:
            name = r[i_name].strip() if i_name is not None and i_name < len(r) else ""
            if not name:
                continue
            item = {"name": name}
            if i_web is not None and i_web < len(r) and r[i_web].strip():
                item["discord_webhook_url"] = r[i_web].strip()
            if i_email is not None and i_email < len(r) and r[i_email].strip():
                item["email"] = r[i_email].strip()
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
# DynamoDB helpers (state for notifications)
# ==========================
def _get_user_state(user_name: str) -> dict:
    try:
        resp = table.get_item(Key={"user_name": user_name})
        item = resp.get("Item", {}) or {}
        seen_entries   = item.get("seen_entries", []) or []
        seen_updates   = item.get("seen_updates", {}) or {}
        seen_snapshots = item.get("seen_snapshots", {}) or {}
        seen_updates   = {str(k): str(v) for k, v in seen_updates.items()}
        seen_snapshots = {str(k): v for k, v in seen_snapshots.items()}
        return {"seen_entries": seen_entries, "seen_updates": seen_updates, "seen_snapshots": seen_snapshots}
    except Exception as e:
        _alert(f"[ERROR] DynamoDB get error for {user_name}: {e}")
        return {"seen_entries": [], "seen_updates": {}, "seen_snapshots": {}}

def _save_user_state(user_name: str, seen_entries: list, seen_updates: dict, seen_snapshots: dict):
    try:
        capped_ids = list(seen_entries[-MAX_SEEN_IDS:])
        keep = set(capped_ids)
        pruned_updates   = {k: v for k, v in seen_updates.items() if k in keep}
        pruned_snapshots = {k: v for k, v in seen_snapshots.items() if k in keep}
        table.put_item(Item={
            "user_name": user_name,
            "seen_entries": capped_ids,
            "seen_updates": pruned_updates,
            "seen_snapshots": pruned_snapshots
        })
    except Exception as e:
        _alert(f"[ERROR] DynamoDB put error for {user_name}: {e}")

# ==========================
# Notion client (schema-aware upserts)
# ==========================
_NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
def _notion_ok():
    return NOTION_ENABLED and NOTION_TOKEN and NOTION_DATABASE_ID

_NOTION_SCHEMA = {}  # name -> type
_TEAM_MEM_CACHE: dict[str, str] = {}  # norm_name -> page_id

def _prop_type(name: str) -> str:
    return _NOTION_SCHEMA.get(name, "")

def _notion_backoff(attempt:int):
    if NOTION_RPS > 0:
        time.sleep(1.0 / NOTION_RPS)
    time.sleep(min(1.0, 0.15 * (2 ** attempt)))  # gentle

def notion_load_schema():
    if not _notion_ok():
        return
    try:
        r = SESSION.get(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
                        headers=_NOTION_HEADERS, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            _alert(f"[ALERT] Notion schema read failed: {r.status_code} {r.text}")
            return
        props = (r.json() or {}).get("properties", {}) or {}
        for name, meta in props.items():
            _NOTION_SCHEMA[name] = (meta or {}).get("type")
        needed = {
            NOTION_PROP_NAME, NOTION_PROP_ENTRY_ID, NOTION_PROP_PROJECT, NOTION_PROP_TAGS,
            NOTION_PROP_START, NOTION_PROP_END, NOTION_PROP_UPDATED, NOTION_PROP_DURATION_S,
            NOTION_PROP_WORKSPACE, NOTION_PROP_DESCRIPTION, NOTION_PROP_TEAM_MEMBER
        }
        missing = [p for p in needed if p not in _NOTION_SCHEMA]
        if missing:
            _alert(f"[ALERT] Notion DB missing expected properties: {missing}")
        if _prop_type(NOTION_PROP_NAME) != "title":
            _alert(f"[ALERT] '{NOTION_PROP_NAME}' must be a title property.")
    except Exception as e:
        _alert(f"[ALERT] Notion schema exception: {e}")

def _title_prop(text: str):
    return {"title": [{"type":"text","text":{"content": text or ""}}]}

def _rich_text(text: str):
    return {"rich_text": [{"type":"text","text":{"content": text or ""}}]}

def _number(n: int|float|None):
    if n is None: return {"number": None}
    return {"number": float(n)}

def _select(name: str):
    return {"select": {"name": name or "No project"}}

def _multi_select(tags: list[str]|None):
    tags = [str(x) for x in (tags or [])]
    return {"multi_select": [{"name": t} for t in tags]}

def _date(iso: str|None):
    return {"date": {"start": iso or None}}

def _relation(page_id: str|None):
    return {"relation": [{"id": page_id}]} if page_id else {"relation": []}

def _norm_contains_filter(prop_name: str, text: str):
    return {"property": prop_name, "title": {"contains": text}}

def _team_member_lookup_partial(name: str) -> str|None:
    """Find Team Members page by partial name (contains). Cache results."""
    if not NOTION_TEAM_DB_ID or not name:
        return None
    key = _norm(name)
    if key in _TEAM_MEM_CACHE:
        return _TEAM_MEM_CACHE[key]
    payload = {"filter": _norm_contains_filter("Name", name), "page_size": 3}
    try:
        r = SESSION.post(f"https://api.notion.com/v1/databases/{NOTION_TEAM_DB_ID}/query",
                         headers=_NOTION_HEADERS, json=payload, timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            results = (r.json() or {}).get("results") or []
            page_id = results[0]["id"] if results else None
            _TEAM_MEM_CACHE[key] = page_id or ""
            return page_id
        _alert(f"[ALERT] Team Members query failed: {r.status_code} {r.text}")
        return None
    except Exception as e:
        _alert(f"[ALERT] Team Members query exception: {e}")
        return None

def _prop_map_for_entry(entry_id:str, user_key:str, user_display:str,
                        project:str, tags:list[str], start_iso:str|None, end_iso:str|None,
                        updated_iso:str|None, dur_seconds:int, workspace_id:int|None,
                        description:str|None):
    props = {
        NOTION_PROP_NAME:        _title_prop(user_key),
        NOTION_PROP_ENTRY_ID:    {"number": int(entry_id)} if str(entry_id).isdigit() else _rich_text(str(entry_id)),
        NOTION_PROP_PROJECT:     _select(project),
        NOTION_PROP_TAGS:        _multi_select(tags),
        NOTION_PROP_START:       _date(start_iso),
        NOTION_PROP_END:         _date(end_iso),
        NOTION_PROP_UPDATED:     _date(updated_iso),
        NOTION_PROP_DURATION_S:  _number(int(dur_seconds or 0)),
        NOTION_PROP_WORKSPACE:   _number(int(workspace_id or 0)),
        NOTION_PROP_DESCRIPTION: _rich_text(description or ""),
    }
    # Team Member relation (partial name match)
    tm_page = _team_member_lookup_partial(user_display)
    props[NOTION_PROP_TEAM_MEMBER] = _relation(tm_page)
    return props

def _notion_query_existing_by_entry_ids(entry_ids: list[str]) -> dict[str, str]:
    """
    Returns {entry_id -> page_id} for Entry IDs that already exist.
    Uses OR number equals filters in small batches to reduce requests.
    """
    found: dict[str, str] = {}
    if not entry_ids:
        return found
    # Chunk to keep OR filter size reasonable (25)
    chunk = 25
    for i in range(0, len(entry_ids), chunk):
        batch = entry_ids[i:i+chunk]
        ors = []
        for eid in batch:
            if str(eid).isdigit() and _prop_type(NOTION_PROP_ENTRY_ID) == "number":
                ors.append({"property": NOTION_PROP_ENTRY_ID, "number": {"equals": int(eid)}})
            else:
                ors.append({"property": NOTION_PROP_ENTRY_ID, "rich_text": {"equals": str(eid)}})
        payload = {"filter": {"or": ors}, "page_size": len(batch)}
        try:
            r = SESSION.post(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                             headers=_NOTION_HEADERS, json=payload, timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                _alert(f"[ALERT] Notion bulk query failed: {r.status_code} {r.text}")
                continue
            for res in (r.json() or {}).get("results", []):
                pid = res["id"]
                props = res.get("properties", {})
                # read Entry ID back
                val = props.get(NOTION_PROP_ENTRY_ID, {})
                if val.get("type") == "number":
                    eid_val = str(int(val.get("number") or 0))
                else:
                    rt = (val.get("rich_text") or [])
                    eid_val = "".join([t.get("plain_text","") for t in rt]) if rt else ""
                if eid_val:
                    found[eid_val] = pid
        except Exception as e:
            _alert(f"[ALERT] Notion bulk query exception: {e}")
    return found

def _notion_create_pages(pages: list[dict]):
    created = 0
    for i, payload in enumerate(pages):
        body = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": payload["properties"],
            # Add description as content block once (create only)
            "children": [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": (payload.get("description") or "")}}],
                    }
                }
            ] if (payload.get("description") or "") else []
        }
        for attempt in range(2):
            r = SESSION.post("https://api.notion.com/v1/pages", headers=_NOTION_HEADERS, json=body, timeout=HTTP_TIMEOUT)
            if r.status_code in (200, 201):
                created += 1
                break
            if r.status_code in (429, 500, 502, 503, 504):
                _notion_backoff(attempt); continue
            _alert(f"[ALERT] Notion create failed: {r.status_code} {r.text}")
            break
        if NOTION_RPS > 0:
            time.sleep(1.0 / NOTION_RPS)
    return created

def _notion_update_pages(items: list[tuple[str, dict]]):
    updated = 0
    for page_id, props in items:
        body = {"properties": props}
        for attempt in range(2):
            r = SESSION.patch(f"https://api.notion.com/v1/pages/{page_id}",
                              headers=_NOTION_HEADERS, json=body, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                updated += 1
                break
            if r.status_code in (429, 500, 502, 503, 504):
                _notion_backoff(attempt); continue
            _alert(f"[ALERT] Notion update failed: {r.status_code} {r.text}")
            break
        if NOTION_RPS > 0:
            time.sleep(1.0 / NOTION_RPS)
    return updated

# Queue all intended upserts here (entry_id -> {properties..., description})
_notion_queue: dict[str, dict] = {}

def notion_queue_entry(entry_id:str, user_key:str, user_display:str,
                       project:str, tags:list[str], start_iso:str|None, end_iso:str|None,
                       updated_iso:str|None, dur_seconds:int, workspace_id:int|None,
                       description:str|None):
    if not _notion_ok():
        return
    props = _prop_map_for_entry(entry_id, user_key, user_display, project, tags,
                                start_iso, end_iso, updated_iso, dur_seconds, workspace_id,
                                description)
    _notion_queue[str(entry_id)] = {"properties": props, "description": description or ""}

def notion_flush_queue():
    if not _notion_ok() or not _notion_queue:
        return {"created": 0, "updated": 0, "skipped": 0}
    items = list(_notion_queue.items())
    _notion_queue.clear()

    # Find existing pages by Entry ID in chunks; then split into create vs update
    entry_ids = [eid for eid, _ in items]
    existing_map = _notion_query_existing_by_entry_ids(entry_ids)  # {eid->page_id}

    to_create = []
    to_update = []
    for eid, payload in items:
        if eid in existing_map:
            to_update.append((existing_map[eid], payload["properties"]))
        else:
            to_create.append({"properties": payload["properties"], "description": payload.get("description","")})

    created = updated = 0
    # Batch updates/creates
    for i in range(0, len(to_update), NOTION_BATCH_SIZE):
        updated += _notion_update_pages(to_update[i:i+NOTION_BATCH_SIZE])
    for i in range(0, len(to_create), NOTION_BATCH_SIZE):
        created += _notion_create_pages(to_create[i:i+NOTION_BATCH_SIZE])

    print(f"[INFO] Notion upsert: created={created} updated={updated} total={len(items)}")
    return {"created": created, "updated": updated, "skipped": 0}

# ==========================
# Timestamp parsers
# ==========================
def parse_iso(ts: str, default_now: datetime) -> datetime:
    try:
        ts = ts.replace("Z", "+0000").replace("+00:00", "+0000")
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return default_now

def parse_iso_opt(ts: str):
    try:
        ts = ts.replace("Z", "+0000").replace("+00:00", "+0000")
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return None

# ==========================
# Toggl API helpers
# ==========================
def _headers_for_token(api_token: str) -> dict:
    return { "Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode() }

def get_workspaces_for_token(api_token: str) -> list[dict]:
    url = "https://api.track.toggl.com/api/v9/workspaces"
    try:
        r = SESSION.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json() or []
        _alert(f"[ERROR] get_workspaces_for_token: {r.status_code} {r.text}")
    except Exception as e:
        _alert(f"[ERROR] get_workspaces_for_token exception: {e}")
    return []

def fetch_workspace_entries(api_token: str, workspace_id: int, since_dt: datetime, until_dt: datetime) -> list[dict]:
    url = "https://api.track.toggl.com/reports/api/v2/details"
    headers = _headers_for_token(api_token)
    params = {
        "workspace_id": workspace_id,
        "since": since_dt.strftime("%Y-%m-%d"),
        "until": until_dt.strftime("%Y-%m-%d"),
        "user_agent": "toggl-discord-bot",
        "page": 1,
        "per_page": 200,
    }
    all_entries = []
    last_total = 0
    MAX_PAGES = 200
    for _ in range(MAX_PAGES):
        resp = SESSION.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            _alert(f"[ERROR] details fetch failed: {resp.status_code} - {resp.text}")
            break
        data = resp.json() or {}
        entries = data.get("data") or []
        total_count = int(data.get("total_count") or 0)
        if not entries:
            break
        all_entries.extend(entries)
        if total_count and len(all_entries) >= total_count:
            break
        if len(all_entries) == last_total:
            break
        last_total = len(all_entries)
        params["page"] += 1
    else:
        _alert(f"[WARN] Hit MAX_PAGES={MAX_PAGES} for workspace {workspace_id}; stopping pagination early.")
    return all_entries

# History windows (for optional Notion backfill)
def _month_bounds_utc(dt: datetime) -> tuple[datetime, datetime]:
    first = datetime(dt.year, dt.month, 1, 0, 0, 0, tzinfo=timezone.utc)
    if dt.month == 12:
        next_first = datetime(dt.year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    else:
        next_first = datetime(dt.year, dt.month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    last = next_first - timedelta(seconds=1)
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

# ==========================
# Formatting helpers
# ==========================
def convert_ms_to_hms(milliseconds: int) -> str:
    minutes = milliseconds // 60000
    seconds = (milliseconds % 60000) // 1000
    hours   = minutes // 60
    mins    = minutes % 60
    return f"{hours}h {mins}m {seconds}s"

def _format_delta_ms(delta_ms: int) -> str:
    sign = "+" if delta_ms >= 0 else "-"
    ms = abs(delta_ms)
    minutes = ms // 60000
    seconds = (ms % 60000) // 1000
    hours   = minutes // 60
    mins    = minutes % 60
    return f"{sign}{hours}h {mins}m {seconds}s"

def _format_delta_datetimes(new_dt: datetime, old_dt: datetime) -> str:
    delta = int((new_dt - old_dt).total_seconds())
    sign = "+" if delta >= 0 else "-"
    s = abs(delta)
    mins = s // 60
    hours = mins // 60
    rem_m = mins % 60
    return f"{sign}{hours}h {rem_m}m"

def _truncate(s: str, limit: int = 140) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s if len(s) <= limit else (s[:limit-1] + "…")

# ==========================
# Discord rate limiting helpers
# ==========================
_last_send_ts_per_webhook = {}
_last_send_ts_global = 0.0

def _sleep_ms(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)

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
                retry_after = None
                try:
                    retry_after = resp.headers.get("Retry-After") or resp.headers.get("X-RateLimit-Reset-After")
                    if retry_after is None:
                        data = resp.json()
                        retry_after = data.get("retry_after") or data.get("Retry-After")
                except Exception:
                    pass
                try:
                    wait_s = float(retry_after) if retry_after is not None else 2.0
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
            print(f"[FAIL] Discord send {resp.status_code}: {resp.text}")
            return False
        except Exception as e:
            wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            if attempt >= DISCORD_MAX_RETRIES:
                print(f"[ERROR] Discord send exception: {e}")
                return False
    return False

# ==========================
# Deleted-entry message
# ==========================
def _strike(s: str) -> str:
    return f"~~{s}~~"

def _format_deleted_message(user_name: str, snap: dict) -> str:
    project = snap.get("project", "No project")
    tags    = snap.get("tags") or []
    desc    = snap.get("desc", "No description")
    dur_ms  = int(snap.get("dur", 0))
    start_s = snap.get("start") or ""
    try:
        start_dt = datetime.strptime(start_s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z")
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        start_str = start_s or "unknown"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    old_tags = ", ".join(sorted(tags)) if isinstance(tags, list) and tags else "No tags"
    lines = [
        "## 🗑️ Time Entry Deleted",
        f"👤 **User**: {_strike(user_name)}",
        f"📁 **Project:** {_strike(project)}",
        f"🏷️ **Tags:** {_strike(old_tags)}",
        f"📝 **Task:** {_strike(_truncate(desc))}",
        f"⏱️ **Time Tracked:** {_strike(convert_ms_to_hms(dur_ms))}",
        f"🗓️ **Start:** {_strike(start_str)}",
        f"🕑 **Deleted detected:** {now_str}",
        "—"
    ]
    return "\n".join(lines)

# ==========================
# Batch helpers for Discord
# ==========================
def _chunk_messages(msgs, limit=1999, sep="\n\n"):
    chunk, size = [], 0
    for m in msgs:
        mlen = len(m)
        if chunk and size + len(sep) + mlen > limit:
            yield sep.join(chunk)
            chunk, size = [m], mlen
        else:
            chunk.append(m)
            size = len(sep.join(chunk))
    if chunk:
        yield sep.join(chunk)

def _send_all_for_webhook(wh_url, msgs):
    sent = 0
    for payload in _chunk_messages(msgs):
        if send_discord_with_backoff(wh_url, payload, timeout=HTTP_TIMEOUT):
            sent += 1
    return wh_url, sent

# ==========================
# Lambda entrypoint
# ==========================
def lambda_handler(event, context):
    # Load TEAM & WORKSPACES
    team          = load_team_sheet()
    workspace_set = load_workspace_tokens()

    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}
    if not workspace_set:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

    # Notion: read schema once
    if _notion_ok():
        notion_load_schema()

    # Map user name -> webhook (only if we plan to send)
    if NOTIFY_ENABLED:
        user_map = { t["name"]: t.get("discord_webhook_url", "").strip() for t in team if t.get("discord_webhook_url") }
        user_map_norm = { _norm(k): v for k, v in user_map.items() }
    else:
        user_map = {}
        user_map_norm = {}

    now   = datetime.now(timezone.utc)
    since = now - timedelta(days=TIME_WINDOW_DAYS)

    notified = 0
    skipped_deletions = 0

    # state_key -> {"current_ids": set(), "user_name": str, "webhook": str|None, "state": dict, "dirty": bool}
    user_runtime = {}
    outbox = {}
    missing_webhook_warned = set()

    # ---- Optional Notion history backfill (monthly windows) ----
    backfilled = {"windows":0, "entries":0}
    if NOTION_BACKFILL and _notion_ok():
        for token_entry in workspace_set:
            api_token = token_entry["api_token"]
            workspaces = get_workspaces_for_token(api_token)
            for ws in workspaces:
                wid = ws.get("id")
                if not wid: continue
                empty_streak = 0
                for (win_start, win_end) in _history_windows(now):
                    if empty_streak >= HISTORY_MAX_EMPTY_WINDOWS:
                        break
                    rows = fetch_workspace_entries(api_token, wid, win_start, win_end)
                    if not rows:
                        empty_streak += 1
                        continue
                    empty_streak = 0
                    backfilled["windows"] += 1
                    if len(_notion_queue) >= NOTION_FLUSH_EVERY:
                            notion_flush_queue()
                    # queue to Notion (we only create/update what is missing/changed)
                    for e in rows:
                        entry_id    = str(e.get("id") or "")
                        user_name   = e.get("user") or ""
                        if not entry_id or not user_name:
                            continue
                        description = e.get("description") or "No description"
                        project     = e.get("project") or "No project"
                        tags        = e.get("tags") or []
                        duration_ms = int(e.get("dur") or 0)
                        start_str   = e.get("start") or ""
                        end_str     = e.get("end") or e.get("stop") or ""
                        updated_str = e.get("updated") or e.get("at") or ""
                        uid         = e.get("uid")
                        if not end_str and start_str and duration_ms > 0:
                            try:
                                sdt = parse_iso(start_str, now)
                                edt = sdt + timedelta(milliseconds=duration_ms)
                                end_str = edt.strftime("%Y-%m-%dT%H:%M:%S%z")
                            except Exception:
                                end_str = ""
                        state_key = f"uid:{uid}" if uid is not None else f"name:{_norm(user_name)}"
                        notion_queue_entry(
                            entry_id=entry_id,
                            user_key=state_key,
                            user_display=user_name,
                            project=project,
                            tags=list(tags) if isinstance(tags, list) else [],
                            start_iso=start_str or None,
                            end_iso=end_str or None,
                            updated_iso=updated_str or None,
                            dur_seconds=max(0, duration_ms // 1000),
                            workspace_id=wid,
                            description=description
                        )
                        backfilled["entries"] += 1
        # push any backfill work first
        notion_flush_queue()

    # ---- Live window fetch + notifications + Notion upserts ----
    for token_entry in workspace_set:
        api_token = token_entry["api_token"]
        workspaces = get_workspaces_for_token(api_token)
        if not workspaces:
            _alert(f"⚠️ No workspaces visible for token ending …{api_token[-4:]}")
            continue

        for ws in workspaces:
            wid = ws.get("id")
            if not wid:
                continue

            entries = fetch_workspace_entries(api_token, wid, since, now)
            print(f"[DEBUG] Fetched {len(entries)} entries from workspace {wid}")

            for entry in entries:
                entry_id    = str(entry.get("id") or "")
                user_name   = entry.get("user") or ""
                description = entry.get("description") or "No description"
                project     = entry.get("project") or "No project"
                tags        = entry.get("tags") or []
                duration_ms = int(entry.get("dur") or 0)
                start_str   = entry.get("start") or ""
                end_str     = entry.get("end") or entry.get("stop") or ""   # include End for Notion
                updated_str = entry.get("updated") or entry.get("at") or ""
                uid         = entry.get("uid")

                if not entry_id or not user_name:
                    continue

                # If end is missing, compute from start + dur
                if not end_str and start_str and duration_ms > 0:
                    try:
                        sdt = parse_iso(start_str, now)
                        edt = sdt + timedelta(milliseconds=duration_ms)
                        end_str = edt.strftime("%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        end_str = ""

                webhook_url = None
                if NOTIFY_ENABLED:
                    webhook_url = user_map.get(user_name) or user_map_norm.get(_norm(user_name))

                start_dt   = parse_iso(start_str, now) if start_str else now
                updated_dt = parse_iso(updated_str, now) if updated_str else now

                state_key = f"uid:{uid}" if uid is not None else f"name:{_norm(user_name)}"

                if state_key not in user_runtime:
                    state = _get_user_state(state_key)
                    user_runtime[state_key] = {
                        "current_ids": set(),
                        "user_name": user_name,
                        "webhook": webhook_url,
                        "state": state,
                        "dirty": False,
                    }
                else:
                    state = user_runtime[state_key]["state"]

                seen_ids       = set(state["seen_entries"])
                seen_updates   = state["seen_updates"]
                seen_snapshots = state.get("seen_snapshots", {})

                prev_updated_str = seen_updates.get(entry_id)
                prev_updated = None
                if prev_updated_str and not prev_updated_str.startswith("deleted:"):
                    prev_updated = parse_iso_opt(prev_updated_str)
                first_seen   = prev_updated is None

                prev_snap = seen_snapshots.get(entry_id, {})
                prev_dur  = int(prev_snap.get("dur", duration_ms))
                prev_desc = prev_snap.get("desc", description)
                prev_proj = prev_snap.get("project", project)
                prev_tags = prev_snap.get("tags", list(tags))
                prev_start_str = prev_snap.get("start", start_str)
                prev_start_dt = parse_iso(prev_start_str, now) if prev_start_str else start_dt

                user_runtime[state_key]["current_ids"].add(entry_id)

                seen_updates[entry_id] = updated_dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                if entry_id not in seen_ids:
                    state["seen_entries"].append(entry_id)
                user_runtime[state_key]["dirty"] = True

                curr_snap = {
                    "dur": duration_ms,
                    "desc": description,
                    "project": project,
                    "tags": list(tags) if isinstance(tags, list) else [],
                    "start": start_str,
                    "updated": updated_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
                }
                seen_snapshots[entry_id] = curr_snap

                # === Queue Notion upsert ===
                notion_queue_entry(
                    entry_id=entry_id,
                    user_key=state_key,
                    user_display=user_name,
                    project=project,
                    tags=list(tags) if isinstance(tags, list) else [],
                    start_iso=start_str or None,
                    end_iso=end_str or None,
                    updated_iso=updated_str or None,
                    dur_seconds=max(0, duration_ms // 1000),
                    workspace_id=wid,
                    description=description
                )
                
                if len(_notion_queue) >= NOTION_FLUSH_EVERY:
                    notion_flush_queue()

                # ---- notifications (unchanged behavior) ----
                if NOTIFY_ENABLED and not webhook_url:
                    if first_seen and user_name not in missing_webhook_warned:
                        _alert(f"[INFO] No webhook configured for '{user_name}' (uid: {uid}); persisting state and skipping notifications.")
                        missing_webhook_warned.add(user_name)
                    continue

                should_notify = False
                label         = "🆕 New Time Entry Added"
                diff_lines    = []

                changed_time  = duration_ms != prev_dur
                changed_proj  = project != prev_proj
                changed_desc  = description != prev_desc
                try:
                    old_set = set(prev_tags) if isinstance(prev_tags, list) else set()
                    new_set = set(tags) if isinstance(tags, list) else set()
                except Exception:
                    old_set, new_set = set(), set()
                changed_tags  = old_set != new_set
                changed_start = (start_dt != prev_start_dt)

                if first_seen:
                    if HARD_START_FILTER and start_dt < since:
                        continue
                    if updated_dt >= since:
                        should_notify = True
                        label = "🆕 New Time Entry Added"
                else:
                    if (updated_dt and prev_updated and updated_dt > prev_updated) and (updated_dt >= since):
                        should_notify = True
                        label = "✍️ Time Entry Edited"
                        if changed_time:
                            diff_lines.append(f"Δ Time {_format_delta_ms(duration_ms - prev_dur)}")
                        if changed_proj:
                            diff_lines.append("Project changed")
                        if changed_desc:
                            diff_lines.append("Description changed")
                        if changed_tags:
                            added   = sorted(list(new_set - old_set))
                            removed = sorted(list(old_set - new_set))
                            bits = []
                            if added:   bits.append("+" + ", ".join(added))
                            if removed: bits.append("-" + ", ".join(removed))
                            diff_lines.append("Tags " + " ".join(bits) if bits else "Tags changed")
                        if changed_start:
                            diff_lines.append(f"Δ Start {_format_delta_datetimes(start_dt, prev_start_dt)}")

                if not should_notify:
                    continue
                if not NOTIFY_ENABLED:
                    continue

                lines = [
                    f"## {label}",
                    f"👤 **User**: {user_name}",
                ]
                if label.startswith("✍️") and changed_proj:
                    lines.append(f"📁 **Project:** ~~{prev_proj}~~ {project}")
                else:
                    lines.append(f"📁 **Project:** {project}")

                if label.startswith("✍️") and changed_tags:
                    old_tags_s = ", ".join(sorted(old_set)) or "No tags"
                    new_tags_s = ", ".join(sorted(new_set)) or "No tags"
                    lines.append(f"🏷️ **Tags:** ~~{old_tags_s}~~ {new_tags_s}")
                else:
                    lines.append(f"🏷️ **Tags:** {', '.join(tags) if tags else 'No tags'}")

                if label.startswith("✍️") and changed_desc:
                    lines.append(f"📝 **Task:** ~~{_truncate(prev_desc)}~~ {_truncate(description)}")
                else:
                    lines.append(f"📝 **Task:** {description}")

                if label.startswith("✍️") and changed_time:
                    lines.append(f"⏱️ **Time Tracked:** ~~{convert_ms_to_hms(prev_dur)}~~ {convert_ms_to_hms(duration_ms)}")
                else:
                    lines.append(f"⏱️ **Time Tracked:** {convert_ms_to_hms(duration_ms)}")

                if label.startswith("✍️") and changed_start:
                    lines.append(f"🗓️ **Start:** ~~{prev_start_dt.strftime('%Y-%m-%d %H:%M:%S')}~~ {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    lines.append(f"🗓️ **Start:** {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

                lines.append(f"🕑 **Updated:** {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                if label.startswith("✍️") and diff_lines:
                    lines.append("")
                    lines.append("**changes:** " + "; ".join(diff_lines))
                lines.append("—")
                msg = "\n".join(lines)

                if webhook_url:
                    outbox.setdefault(webhook_url, []).append(msg)
                    notified += 1

    # ==========================
    # Deletion detection
    # ==========================
    for state_key, bucket in user_runtime.items():
        current_ids = bucket["current_ids"]
        user_name   = bucket["user_name"]
        webhook_url = bucket["webhook"]
        state       = bucket["state"]

        seen_entries   = list(state.get("seen_entries", []))
        seen_updates   = dict(state.get("seen_updates", {}))
        seen_snapshots = dict(state.get("seen_snapshots", {}))

        now = datetime.now(timezone.utc)
        since = now - timedelta(days=TIME_WINDOW_DAYS)

        for eid in list(seen_entries):
            if eid in current_ids:
                continue
            snap = seen_snapshots.get(eid)
            if not snap:
                continue
            snap_start_s = snap.get("start") or ""
            snap_start   = parse_iso_opt(snap_start_s)
            in_window = bool(snap_start and snap_start >= since)
            if not in_window:
                continue

            if NOTIFY_ENABLED and webhook_url:
                msg = _format_deleted_message(user_name, snap)
                outbox.setdefault(webhook_url, []).append(msg)
                notified += 1
            else:
                skipped_deletions += 1

            try:
                seen_entries.remove(eid)
            except ValueError:
                pass
            seen_updates.pop(eid, None)
            seen_snapshots.pop(eid, None)
            bucket["dirty"] = True

        bucket["state"]["seen_entries"]   = seen_entries
        bucket["state"]["seen_updates"]   = seen_updates
        bucket["state"]["seen_snapshots"] = seen_snapshots

    # ==========================
    # Batched state save
    # ==========================
    for state_key, bucket in user_runtime.items():
        if bucket.get("dirty"):
            st = bucket["state"]
            _save_user_state(
                state_key,
                st.get("seen_entries", []),
                st.get("seen_updates", {}),
                st.get("seen_snapshots", {})
            )

    # ==========================
    # Flush Notion queue (bulk upsert)
    # ==========================
    notion_result = notion_flush_queue()

    # ==========================
    # Send Discord messages
    # ==========================
    if NOTIFY_ENABLED and outbox:
        max_workers = min(8, len(outbox))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_send_all_for_webhook, wh, msgs) for wh, msgs in outbox.items()]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    _alert(f"[ERROR] send pool: {e}")

    return {
        "status": "complete",
        "notified": notified,
        "skipped_deletions": skipped_deletions,
        "dry_run_no_send": DRY_RUN_NO_SEND,
        "send_notifications": SEND_NOTIFICATIONS,
        "notify_enabled": NOTIFY_ENABLED,
        "notion": notion_result,
        "backfill": backfilled,
    }
