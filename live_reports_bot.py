import os, json, base64, requests, csv, time, random
from io import StringIO
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

SESSION = requests.Session()
_START_TS = time.monotonic()

# ENV
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID = os.getenv("TEAM_GID", "")
WORKSPACES_GID = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK = os.getenv("SYSTEM_ALERT_WEBHOOK", "")

TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "5"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
HARD_START_FILTER = os.getenv("HARD_START_FILTER", "1") == "1"

DISCORD_MIN_INTERVAL_MS = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "" and default: return True
    return (v or "").strip().lower() in ("1","true","yes","y","on")

DRY_RUN_NO_SEND = _env_bool("DRY_RUN_NO_SEND", False)
SEND_NOTIFICATIONS = _env_bool("SEND_NOTIFICATIONS", True)
NOTIFY_ENABLED = (not DRY_RUN_NO_SEND) and SEND_NOTIFICATIONS

# Deadline-aware controls and throughput priorities
MAX_RUNTIME_S = int(os.getenv("MAX_RUNTIME_S", "300"))  # soft cap when context missing
EXIT_MARGIN_S = int(os.getenv("EXIT_MARGIN_S", "12"))   # leave safety margin for shutdown
PRIORITIZE_NOTION = _env_bool("PRIORITIZE_NOTION", True)  # skip Discord send if time is tight
NOTIFY_FLUSH_EVERY = int(os.getenv("NOTIFY_FLUSH_EVERY", "25"))  # flush per-webhook after N messages

# Toggl API budget/rate guardrails (minimize cost and 402 spam)
MAX_TOGGL_CALLS_PER_RUN = int(os.getenv("MAX_TOGGL_CALLS_PER_RUN", "120"))
MAX_TOGGL_ERRORS_PER_TOKEN = int(os.getenv("MAX_TOGGL_ERRORS_PER_TOKEN", "3"))
TOGGL_PAGE_SIZE = int(os.getenv("TOGGL_PAGE_SIZE", "200"))
TOKENS_MAX_CONCURRENT = int(os.getenv("TOKENS_MAX_CONCURRENT", "1"))  # default 1 to be gentle

# In-run Toggl state
_TOGGL_CALLS_USED = 0
_WS_CACHE_BY_TOKEN: dict[str, list[dict]] = {}
_EXHAUSTED_TOKENS: set[str] = set()
_UNSUPPORTED_REPORTS: set[tuple[str, int]] = set()  # (token_tail, workspace_id) giving 402
_TOKEN_ERROR_COUNTS: dict[str, int] = {}

# Notion DB (replaces DynamoDB)
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()
NH = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}
P_USER_KEY    = os.getenv("NOTION_PROP_USER_KEY", "User Key")
P_ENTRY_ID    = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")
P_PROJECT     = os.getenv("NOTION_PROP_PROJECT", "Project")
P_TAGS        = os.getenv("NOTION_PROP_TAGS", "Tags")
P_DATE        = os.getenv("NOTION_PROP_DATE", "Date")     # unified range date property
P_UPDATED     = os.getenv("NOTION_PROP_UPDATED", "Updated")
P_DURATION    = os.getenv("NOTION_PROP_DURATION", "Duration")
P_WORKSPACE   = os.getenv("NOTION_PROP_WORKSPACE", "Workspace")
P_DESCRIPTION = os.getenv("NOTION_PROP_DESCRIPTION", "Description")
P_DELETED     = os.getenv("NOTION_PROP_DELETED", "Deleted")
P_TEAM_MEMBER = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")
NOTION_TEAM_DB_ID = os.getenv("NOTION_TEAM_DB_ID", "").strip()

# Notion throughput knobs
NOTION_RPS = float(os.getenv("NOTION_RPS", "3.0"))            # target rps (global per integration)
NOTION_CONCURRENCY = int(os.getenv("NOTION_CONCURRENCY", "4"))  # threads for Notion writes
NOTION_MAX_RETRIES = int(os.getenv("NOTION_MAX_RETRIES", "4"))
MAX_USERS_NOTION_INDEX = int(os.getenv("MAX_USERS_NOTION_INDEX", "80"))  # cap user-index Notion queries per run
NOTION_ADD_DESCRIPTION_BLOCKS = _env_bool("NOTION_ADD_DESCRIPTION_BLOCKS", True)
MAX_TARGETED_LOOKUPS_PER_RUN = int(os.getenv("MAX_TARGETED_LOOKUPS_PER_RUN", "0"))
MAX_DEDUPE_IDS_PER_RUN = int(os.getenv("MAX_DEDUPE_IDS_PER_RUN", "80"))
MAX_DEDUPE_MARKS_PER_ID = int(os.getenv("MAX_DEDUPE_MARKS_PER_ID", "10"))

_NOTION_LAST_CALL = 0.0
try:
    from threading import Lock
    _NOTION_LOCK = Lock()
except Exception:
    class _Noop:
        def __enter__(self): pass
        def __exit__(self, *a): pass
    _NOTION_LOCK = _Noop()

def _alert(msg: str):
    print(f"[ALERT] {msg}")
    if SYSTEM_ALERT_WEBHOOK:
        try: SESSION.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
        except Exception: pass

# Sheets
def _sheet_export_csv_url(sheet_url: str, gid: str) -> str:
    if not sheet_url or not gid: return ""
    try:
        sid = sheet_url.split("/d/")[1].split("/")[0]
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
    except Exception: return ""

def _fetch_csv_rows(sheet_url: str, gid: str) -> list[list[str]]:
    url = _sheet_export_csv_url(sheet_url, gid)
    if not url: return []
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return list(csv.reader(StringIO(r.text)))

def _find_header(headers: list[str], *cands) -> int | None:
    h = [x.strip().lower() for x in headers]
    for c in cands:
        if c in h: return h.index(c)
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
            if not name: continue
            item = {"name": name}
            if i_web is not None and i_web < len(r) and r[i_web].strip():
                item["discord_webhook_url"] = r[i_web].strip()
            team.append(item)
        except Exception: continue
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
            if name and tok: out.append({"name": name, "api_token": tok})
        except Exception: continue
    return out

# Toggl
def _headers_for_token(api_token: str) -> dict:
    return {"Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode()}

def _remaining_s(context) -> float:
    try:
        return max(0.0, context.get_remaining_time_in_millis() / 1000.0)
    except Exception:
        # best-effort runtime if no context is present
        return MAX_RUNTIME_S

def _should_exit_soon(context) -> bool:
    return _remaining_s(context) <= EXIT_MARGIN_S

def _budget_take(n: int = 1) -> bool:
    """Consume from Toggl call budget; return False if exhausted."""
    global _TOGGL_CALLS_USED
    if _TOGGL_CALLS_USED + n > MAX_TOGGL_CALLS_PER_RUN:
        return False
    _TOGGL_CALLS_USED += n
    return True

def get_workspaces_for_token(api_token: str) -> list[dict]:
    if api_token in _EXHAUSTED_TOKENS:
        return []
    if api_token in _WS_CACHE_BY_TOKEN:
        return _WS_CACHE_BY_TOKEN[api_token]
    url = "https://api.track.toggl.com/api/v9/workspaces"
    if not _budget_take():
        _alert("[INFO] Toggl call budget exhausted; skipping workspaces fetch.")
        return []
    try:
        r = SESSION.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            data = r.json() or []
            _WS_CACHE_BY_TOKEN[api_token] = data
            return data
        tok_tail = f"…{api_token[-4:]}"
        _TOKEN_ERROR_COUNTS[api_token] = _TOKEN_ERROR_COUNTS.get(api_token, 0) + 1
        if r.status_code == 402:
            _alert(f"get_workspaces_for_token: 402 You may be rate/plan limited. Token {tok_tail}. Skipping this token for this run.")
            _EXHAUSTED_TOKENS.add(api_token)
        elif 400 <= r.status_code < 500:
            _alert(f"get_workspaces_for_token: {r.status_code} {r.text.strip()} (token {tok_tail})")
            if _TOKEN_ERROR_COUNTS[api_token] >= MAX_TOGGL_ERRORS_PER_TOKEN:
                _EXHAUSTED_TOKENS.add(api_token)
        else:
            _alert(f"get_workspaces_for_token: {r.status_code} {r.text.strip()} (token {tok_tail})")
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
        "per_page": TOGGL_PAGE_SIZE,
    }
    all_entries, last_total = [], 0
    tok_tail = f"…{api_token[-4:]}"
    key = (tok_tail, int(workspace_id or 0))
    if key in _UNSUPPORTED_REPORTS:
        return []
    MAX_PAGES = 200
    attempt = 0
    while params["page"] <= MAX_PAGES:
        if not _budget_take():
            _alert("[INFO] Toggl call budget exhausted; stopping pagination early.")
            break
        try:
            resp = SESSION.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        except Exception as e:
            attempt += 1
            if attempt <= 3:
                time.sleep(min(2.0, 0.5 * attempt) + random.uniform(0.02, 0.2))
                continue
            _alert(f"[ERROR] details fetch exception (token {tok_tail}, ws {workspace_id}): {e}")
            break
        if resp.status_code == 200:
            attempt = 0
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
            continue
        # Non-200
        txt = (resp.text or "").strip()
        if resp.status_code == 402:
            _alert(f"details fetch failed: 402 (token {tok_tail}, ws {workspace_id}) - {txt}")
            _UNSUPPORTED_REPORTS.add(key)
            break
        if resp.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            if attempt <= 4:
                time.sleep(min(2.5, 0.6 * attempt) + random.uniform(0.05, 0.25))
                continue
        if resp.status_code == 405:
            _alert(f"time_entries fetch failed: 405 (token {tok_tail}, ws {workspace_id})")
            break
        _alert(f"[ERROR] details fetch failed: {resp.status_code} (token {tok_tail}, ws {workspace_id}) - {txt}")
        break
    return all_entries

# Formatting
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
    if not isinstance(s, str): return ""
    s = s.strip();  return s if len(s) <= limit else (s[:limit-1] + "…")

# Discord pacing
_last_send_ts_per_webhook = {}
_last_send_ts_global = 0.0
def _sleep_ms(ms: int):
    if ms > 0: time.sleep(ms / 1000.0)
def _respect_min_interval(webhook_url: str):
    now = time.monotonic(); last = _last_send_ts_per_webhook.get(webhook_url)
    if last is not None:
        elapsed_ms = int((now - last) * 1000)
        if elapsed_ms < DISCORD_MIN_INTERVAL_MS: _sleep_ms(DISCORD_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts_per_webhook[webhook_url] = time.monotonic()
def _respect_global_interval():
    global _last_send_ts_global
    if DISCORD_GLOBAL_MIN_INTERVAL_MS <= 0: return
    now = time.monotonic()
    if _last_send_ts_global > 0:
        elapsed_ms = int((now - _last_send_ts_global) * 1000)
        if elapsed_ms < DISCORD_GLOBAL_MIN_INTERVAL_MS: _sleep_ms(DISCORD_GLOBAL_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts_global = time.monotonic()
def send_discord_with_backoff(webhook_url: str, content: str, timeout: int = 20) -> bool:
    if not NOTIFY_ENABLED: return True
    _respect_global_interval(); _respect_min_interval(webhook_url)
    payload = {"content": content}; attempt = 0; base = 1.0
    while attempt < DISCORD_MAX_RETRIES:
        try:
            resp = SESSION.post(webhook_url, json=payload, timeout=timeout)
            if resp.status_code == 204: return True
            if resp.status_code == 429:
                try:
                    ra = resp.headers.get("Retry-After") or resp.json().get("retry_after")
                    wait_s = float(ra) if ra else 2.0
                except Exception: wait_s = 2.0
                time.sleep(wait_s + random.uniform(0.05,0.25)); attempt += 1; continue
            if 500 <= resp.status_code < 600:
                time.sleep(base * (2**attempt) + random.uniform(0.05,0.25)); attempt += 1; continue
            print(f"[FAIL] Discord send {resp.status_code}: {resp.text}"); return False
        except Exception as e:
            time.sleep(base * (2**attempt) + random.uniform(0.05,0.25)); attempt += 1
            if attempt >= DISCORD_MAX_RETRIES:
                print(f"[ERROR] Discord send exception: {e}"); return False
    return False

# Notion helpers (storage)
def _title_val(text: str): return {"title": [{"type":"text","text":{"content": text or ""}}]}
def _rt_val(text: str):    return {"rich_text": [{"type":"text","text":{"content": text or ""}}]}
def _num_val(n):           return {"number": (float(n) if n is not None else None)}
def _date_val(iso: str|None): return {"date": {"start": iso or None}}
def _date_range_val(start_iso: str|None, end_iso: str|None):
    return {"date": {"start": start_iso or None, "end": end_iso or None}}
def _select_val(name: str): return {"select": {"name": (name or "No project")[:99]}}
def _multi_val(tags: list[str]): return {"multi_select": [{"name": str(t)[:99]} for t in (tags or []) if str(t).strip()]}
def _relation(page_id: str|None):
    return {"relation": ([{"id": page_id}] if page_id else [])}

def _desc_block(text: str) -> dict:
    txt = (text or "").strip()
    if not txt:
        return {}
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [
                {"type": "text", "text": {"content": txt[:2000]}}
            ]
        }
    }

def _notion_append_blocks(page_id: str, children: list[dict]):
    if not page_id or not children:
        return
    # Filter out empties
    kids = [c for c in children if c]
    if not kids:
        return
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    _notion_request('PATCH', url, json_body={"children": kids})

def _notion_rate_limit():
    if NOTION_RPS <= 0:
        return
    global _NOTION_LAST_CALL
    with _NOTION_LOCK:
        now = time.monotonic()
        min_gap = 1.0 / max(NOTION_RPS, 0.1)
        gap = now - _NOTION_LAST_CALL
        if gap < min_gap:
            time.sleep(min_gap - gap)
        _NOTION_LAST_CALL = time.monotonic()

def _notion_request(method: str, url: str, json_body: dict|None=None, timeout: int|None=None):
    for attempt in range(NOTION_MAX_RETRIES):
        _notion_rate_limit()
        try:
            if method == 'POST':
                r = SESSION.post(url, headers=NH, json=json_body, timeout=timeout or HTTP_TIMEOUT)
            elif method == 'PATCH':
                r = SESSION.patch(url, headers=NH, json=json_body, timeout=timeout or HTTP_TIMEOUT)
            else:
                r = SESSION.get(url, headers=NH, timeout=timeout or HTTP_TIMEOUT)
        except Exception as e:
            if attempt >= NOTION_MAX_RETRIES - 1:
                _alert(f"[ERROR] Notion {method} exception: {e}")
                return None
            time.sleep(0.4 * (2 ** attempt) + random.uniform(0.02, 0.2))
            continue
        if r.status_code in (200, 201):
            return r
        if r.status_code in (409, 429, 500, 502, 503, 504):
            wait = 0.5 * (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(min(3.0, wait))
            continue
        _alert(f"[WARN] Notion {method} failed {r.status_code}: {r.text}")
        return r
    _alert(f"[WARN] Notion {method} exhausted retries: {url}")
    return None

# ---- Team Members index + fuzzy match ----
_TEAM_MEM_INDEX: list[tuple[str, str, str]] = []  # (norm_name, orig_name, page_id)
_TEAM_MEM_BEST_CACHE: dict[str, str | None] = {}

def _norm_for_match(s: str) -> str:
    s = (s or "").lower()
    out = []
    for ch in s:
        if ch.isalnum() or ch.isspace():
            out.append(ch)
        else:
            out.append(" ")
    s2 = "".join(out)
    return " ".join(s2.split())

def _tokenize(s: str) -> set[str]:
    return set(_norm_for_match(s).split())

def notion_build_team_members_index():
    if not NOTION_TEAM_DB_ID or _TEAM_MEM_INDEX:
        return
    url = f"https://api.notion.com/v1/databases/{NOTION_TEAM_DB_ID}/query"
    payload = {"page_size": 100}
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            break
        data = r.json() or {}
        for pg in data.get("results") or []:
            pid = pg.get("id")
            props = pg.get("properties", {})
            # assume title prop named "Name"
            name = ""
            t = props.get("Name", {})
            if t.get("type") == "title":
                for part in (t.get("title") or []):
                    name += part.get("plain_text", "")
            name = (name or "").strip()
            if pid and name:
                _TEAM_MEM_INDEX.append((_norm_for_match(name), name, pid))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

def _best_team_member_page_id(user_display: str) -> str | None:
    if not NOTION_TEAM_DB_ID or not user_display:
        return None
    key = _norm_for_match(user_display)
    if key in _TEAM_MEM_BEST_CACHE:
        return _TEAM_MEM_BEST_CACHE[key]
    if not _TEAM_MEM_INDEX:
        notion_build_team_members_index()
    best_pid = None
    best_score = 0.0
    in_tokens = _tokenize(user_display)
    in_norm = _norm_for_match(user_display)
    for cand_norm, _orig, cand_pid in _TEAM_MEM_INDEX:
        cand_tokens = set(cand_norm.split())
        inter = len(in_tokens & cand_tokens)
        union = len(in_tokens | cand_tokens)
        jacc = (inter / union) if union else 0.0
        bonus = 0.0
        if cand_norm.startswith(in_norm) or in_norm.startswith(cand_norm):
            bonus += 0.15
        if in_norm and cand_norm and (in_norm in cand_norm or cand_norm in in_norm):
            bonus += 0.10
        score = jacc + bonus
        if score > best_score:
            best_score = score
            best_pid = cand_pid
    pid = best_pid if best_score >= 0.25 else None
    _TEAM_MEM_BEST_CACHE[key] = pid
    return pid

# ---- Targeted Notion lookup by Entry ID (to avoid duplicates/false-new) ----
_EID_LOOKUP_CACHE: dict[str, dict] = {}
_TARGETED_LOOKUPS_USED = 0

def _notion_find_entry_by_eid(eid: str) -> dict | None:
    global _TARGETED_LOOKUPS_USED
    if not eid:
    return None

def _notion_find_all_by_eid(eid: str) -> list[dict]:
    """Return all exact-match pages for this Entry ID with snapshots."""
    if not eid:
        return []
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    def _query_filter(flt: dict):
        body = {"page_size": 50, "filter": flt}
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            return []
        data = r.json() or {}
        return data.get("results") or []

    results = []
    if eid.isdigit():
        results = _query_filter({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})
        if not results:
            results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"contains": str(eid)}})
    else:
        results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"contains": str(eid)}})

    out = []
    for pg in results:
        pid = pg.get("id")
        props = pg.get("properties", {})
        p = props.get(P_ENTRY_ID, {})
        exact = False
        if p.get("type") == "number" and eid.isdigit():
            exact = (p.get("number") is not None and int(p.get("number")) == int(eid))
        elif p.get("type") == "rich_text":
            txt = "".join([x.get("plain_text", "") for x in (p.get("rich_text") or [])]).strip()
            exact = (txt == eid)
        if not exact:
            continue
        def _get_date(n):
            d = (props.get(n, {}).get("date") or {})
            return d.get("start") or ""
        def _get_range(n):
            d = (props.get(n, {}).get("date") or {})
            return d.get("start") or "", d.get("end") or ""
        def _get_num(n):
            q = props.get(n, {})
            return q.get("number") if q.get("type") == "number" else 0
        def _get_text(n):
            q = props.get(n, {})
            return "".join([x.get("plain_text", "") for x in (q.get("rich_text") or [])]).strip() if q.get("type") == "rich_text" else ""
        def _get_sel(n):
            q = props.get(n, {})
            return (q.get("select") or {}).get("name") or "No project"
        def _get_tags(n):
            q = props.get(n, {})
            return [t.get("name", "") for t in (q.get("multi_select") or []) if t.get("name")]
        start_s, end_s = _get_range(P_DATE)
        snap = {
            "dur": int(_get_num(P_DURATION) or 0) * 1000,
            "desc": _get_text(P_DESCRIPTION),
            "project": _get_sel(P_PROJECT),
            "tags": _get_tags(P_TAGS),
            "start": start_s,
            "end": end_s,
            "updated": _get_date(P_UPDATED),
        }
        out.append({"page_id": pid, "snap": snap})
    return out

def notion_cleanup_duplicates_for_ids(eids: set[str], context) -> dict:
    """For each Entry ID, keep the latest-updated page; mark other duplicates Deleted."""
    cleaned = 0
    examined = 0
    for eid in list(eids)[:MAX_DEDUPE_IDS_PER_RUN]:
        if _should_exit_soon(context):
            break
        candidates = _notion_find_all_by_eid(eid)
        exact = candidates
        if len(exact) <= 1:
            continue
        examined += 1
        # Choose canonical: most recent Updated
        def _ts(c):
            s = (c.get("snap", {}).get("updated") or "")
            try:
                return datetime.strptime(s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)
        exact.sort(key=_ts, reverse=True)
        keep = exact[0]
        losers = exact[1:MAX_DEDUPE_MARKS_PER_ID+1]
        for loser in losers:
            if _should_exit_soon(context):
                break
            pid = loser.get("page_id")
            if pid:
                _notion_mark_deleted(pid)
                cleaned += 1
    return {"examined": examined, "cleaned": cleaned}
    if eid in _EID_LOOKUP_CACHE:
        return _EID_LOOKUP_CACHE[eid]
    # if limit <= 0, treat as unlimited
    if MAX_TARGETED_LOOKUPS_PER_RUN > 0 and _TARGETED_LOOKUPS_USED >= MAX_TARGETED_LOOKUPS_PER_RUN:
        return None
    _TARGETED_LOOKUPS_USED += 1
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    def _query_filter(flt: dict):
        body = {"page_size": 25, "filter": flt}
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            return []
        data = r.json() or {}
        return data.get("results") or []

    # Try number equals first for digit-only ids; then fallback to rich_text contains
    results = []
    if eid.isdigit():
        results = _query_filter({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})
        if not results:
            results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"contains": str(eid)}})
    else:
        results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"contains": str(eid)}})

    candidates = []
    for pg in results:
        pid = pg.get("id")
        props = pg.get("properties", {})
        # Verify exact match on Entry ID
        p = props.get(P_ENTRY_ID, {})
        exact = False
        if p.get("type") == "number" and eid.isdigit():
            exact = (p.get("number") is not None and int(p.get("number")) == int(eid))
        elif p.get("type") == "rich_text":
            txt = "".join([x.get("plain_text", "") for x in (p.get("rich_text") or [])]).strip()
            exact = (txt == eid)
        if not exact:
            continue
        # Build snapshot similar to _notion_query_user
        def _get_date(n):
            d = (props.get(n, {}).get("date") or {})
            return d.get("start") or ""
        def _get_range(n):
            d = (props.get(n, {}).get("date") or {})
            return d.get("start") or "", d.get("end") or ""
        def _get_num(n):
            q = props.get(n, {})
            return q.get("number") if q.get("type") == "number" else 0
        def _get_text(n):
            q = props.get(n, {})
            return "".join([x.get("plain_text", "") for x in (q.get("rich_text") or [])]).strip() if q.get("type") == "rich_text" else ""
        def _get_sel(n):
            q = props.get(n, {})
            return (q.get("select") or {}).get("name") or "No project"
        def _get_tags(n):
            q = props.get(n, {})
            return [t.get("name", "") for t in (q.get("multi_select") or []) if t.get("name")]
        start_s, end_s = _get_range(P_DATE)
        updated_s = _get_date(P_UPDATED)
        snap = {
            "dur": int(_get_num(P_DURATION) or 0) * 1000,
            "desc": _get_text(P_DESCRIPTION),
            "project": _get_sel(P_PROJECT),
            "tags": _get_tags(P_TAGS),
            "start": start_s,
            "end": end_s,
            "updated": updated_s,
        }
        candidates.append({"page_id": pid, "snap": snap})

    if not candidates:
        _EID_LOOKUP_CACHE[eid] = None
        return None

    # Prefer the most recently updated candidate
    def _ts(s):
        try:
            return datetime.strptime((s or "").replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    best = max(candidates, key=lambda c: _ts(c["snap"].get("updated")))
    _EID_LOOKUP_CACHE[eid] = best
    return best

def _notion_query_user(user_key: str, since: datetime, until: datetime) -> dict:
    out = {}; url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {"page_size":100, "filter": {"and":[
        {"property": P_USER_KEY, "title": {"equals": user_key}},
        {"property": P_DATE, "date": {"on_or_after": since.astimezone(timezone.utc).replace(microsecond=0).isoformat()}},
        {"property": P_DATE, "date": {"on_or_before": until.astimezone(timezone.utc).replace(microsecond=0).isoformat()}},
    ]}}
    cursor=None
    while True:
        body=dict(payload)
        if cursor: body["start_cursor"]=cursor
        r=_notion_request('POST', url, json_body=body)
        if not r or r.status_code!=200:
            break
        js=r.json() or {}
        for pg in js.get("results") or []:
            pid=pg.get("id"); props=pg.get("properties",{})
            eid=None; p=props.get(P_ENTRY_ID,{})
            if p.get("type")=="number" and p.get("number") is not None: eid=str(int(p.get("number")))
            elif p.get("type")=="rich_text": eid="".join([x.get("plain_text","") for x in (p.get("rich_text") or [])]).strip()
            if not eid: continue
            def _get_date(n): d=(props.get(n,{}).get("date") or {}); return d.get("start") or ""
            def _get_range(n): d=(props.get(n,{}).get("date") or {}); return d.get("start") or "", d.get("end") or ""
            def _get_num(n):  q=props.get(n,{}); return q.get("number") if q.get("type")=="number" else 0
            def _get_text(n): q=props.get(n,{}); return "".join([x.get("plain_text","") for x in (q.get("rich_text") or [])]).strip() if q.get("type")=="rich_text" else ""
            def _get_sel(n):  q=props.get(n,{}); return (q.get("select") or {}).get("name") or "No project"
            def _get_tags(n): q=props.get(n,{}); return [t.get("name","") for t in (q.get("multi_select") or []) if t.get("name")]
            start_s, end_s = _get_range(P_DATE)
            snap={"dur": int(_get_num(P_DURATION) or 0)*1000, "desc": _get_text(P_DESCRIPTION), "project": _get_sel(P_PROJECT), "tags": _get_tags(P_TAGS), "start": start_s, "end": end_s, "updated": _get_date(P_UPDATED)}
            out[str(eid)]={"page_id":pid,"snap":snap}
        if not js.get("has_more"): break
        cursor=js.get("next_cursor")
    return out

def _notion_upsert(existing: dict|None, user_key: str, entry: dict, workspace_id: int, add_desc_block: bool=False):
    eid=str(entry.get("id"))
    # Guard against duplicates: if we think it doesn't exist, double-check by Entry ID
    if not existing or not existing.get("page_id"):
        found = _notion_find_entry_by_eid(eid)
        if found:
            existing = found
    props={
        P_USER_KEY: _title_val(user_key),
        P_ENTRY_ID: {"number": int(eid)} if eid.isdigit() else _rt_val(eid),
        P_PROJECT:  _select_val(entry.get("project")),
        P_TAGS:     _multi_val(entry.get("tags") or []),
        P_DATE:     _date_range_val(entry.get("start"), entry.get("end")),
        P_UPDATED:  _date_val(entry.get("updated")),
        P_DURATION: _num_val(int(int(entry.get("dur",0))/1000)),
        P_WORKSPACE:_num_val(int(workspace_id or 0)),
        P_DESCRIPTION: _rt_val(entry.get("description") or ""),
        P_DELETED: {"checkbox": False},
    }
    # Team Member relation if available
    if NOTION_TEAM_DB_ID and P_TEAM_MEMBER:
        tm_pid = _best_team_member_page_id(entry.get("user") or "")
        if tm_pid:
            props[P_TEAM_MEMBER] = _relation(tm_pid)
    if existing and existing.get("page_id"):
        page_id = existing['page_id']
        _notion_request('PATCH', f"https://api.notion.com/v1/pages/{page_id}", json_body={"properties":props})
        if NOTION_ADD_DESCRIPTION_BLOCKS and add_desc_block and (entry.get("description") or "").strip():
            _notion_append_blocks(page_id, [_desc_block(entry.get("description") or "")])
    else:
        body={"parent":{"database_id":NOTION_DATABASE_ID},"properties":props}
        # include description as content on create to avoid a second call
        if NOTION_ADD_DESCRIPTION_BLOCKS and (entry.get("description") or "").strip():
            blk = _desc_block(entry.get("description") or "")
            if blk:
                body["children"] = [blk]
        _notion_request('POST', "https://api.notion.com/v1/pages", json_body=body)

def _notion_mark_deleted(page_id: str):
    if not page_id: return
    _notion_request('PATCH', f"https://api.notion.com/v1/pages/{page_id}", json_body={"properties":{P_DELETED:{"checkbox":True}}})

# Deleted msg
def _strike(s:str)->str: return f"~~{s}~~"
def _format_deleted_message(user_name: str, snap: dict) -> str:
    project=snap.get("project","No project"); tags=snap.get("tags") or []; desc=snap.get("desc","No description"); dur_ms=int(snap.get("dur",0)); start_s=snap.get("start") or ""
    try:
        start_dt=datetime.strptime(start_s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z"); start_str=start_dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception: start_str=start_s or "unknown"
    now_str=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    old_tags=", ".join(sorted(tags)) if tags else "No tags"
    return "\n".join([
        "## 🗑️ Time Entry Deleted",
        f"👤 **User**: {_strike(user_name)}",
        f"📁 **Project:** {_strike(project)}",
        f"🏷️ **Tags:** {_strike(old_tags)}",
        f"📝 **Task:** {_strike(_truncate(desc))}",
        f"⏱️ **Time Tracked:** {_strike(convert_ms_to_hms(dur_ms))}",
        f"🗓️ **Start:** {_strike(start_str)}",
        f"🕑 **Deleted detected:** {now_str}",
        "—",
    ])

# Batch
def _chunk_messages(msgs, limit=1900, sep="\n\n"):
    chunk, size = [], 0
    for m in msgs:
        if chunk and size + len(sep) + len(m) > limit:
            yield sep.join(chunk); chunk, size = [m], len(m)
        else:
            chunk.append(m); size = len(sep.join(chunk))
    if chunk: yield sep.join(chunk)
def _send_all_for_webhook(wh_url, msgs):
    sent=0
    for payload in _chunk_messages(msgs):
        if send_discord_with_backoff(wh_url, payload, timeout=HTTP_TIMEOUT): sent+=1
    return wh_url, sent

# Lambda
def lambda_handler(event, context):
    team=load_team_sheet(); workspaces=load_workspace_tokens()
    if not team: _alert("TEAM sheet empty or unreachable."); return {"status":"no_team"}
    if not workspaces: _alert("WORKSPACES sheet empty or unreachable."); return {"status":"no_workspaces"}
    if NOTIFY_ENABLED:
        user_map={t["name"]:t.get("discord_webhook_url","" ).strip() for t in team if t.get("discord_webhook_url")}
        user_map_norm={_norm(k):v for k,v in user_map.items()}
    else:
        user_map={}; user_map_norm={}
    # Load Team Members index (for relation mapping) if configured
    if NOTION_TEAM_DB_ID:
        notion_build_team_members_index()

    now=datetime.now(timezone.utc); since=now - timedelta(days=TIME_WINDOW_DAYS)
    notified=0; skipped_notifies=0; skipped_deletions=0
    user_runtime={}  # state_key -> {current_ids,set; user_name; webhook; state; notion_user_key}
    outbox={}
    warned=set()
    # Group upserts by webhook so we can send first, then write those entries
    notion_tasks_by_webhook: dict[str, list[tuple]] = {}
    notion_tasks_no_webhook: list[tuple] = []
    delete_tasks = []  # queued page_id for Deleted=true
    scheduled_upserts = set()  # (notion_user_key, eid)
    users_indexed = 0
    processed_eids: set[str] = set()

    def _flush_webhook(wh: str):
        # Send messages (chunked), then run Notion upserts for this webhook
        msgs = outbox.get(wh) or []
        if msgs:
            for payload in _chunk_messages(msgs):
                if _should_exit_soon(context):
                    break
                send_discord_with_backoff(wh, payload, timeout=HTTP_TIMEOUT)
            outbox[wh] = []
        tasks = notion_tasks_by_webhook.get(wh) or []
        if tasks and not _should_exit_soon(context):
            max_workers = max(1, min(NOTION_CONCURRENCY, len(tasks)))
            def _run_upsert(task):
                ex, ukey, ent, wid, add_block = task
                _notion_upsert(ex, ukey, ent, wid, add_desc_block=add_block)
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(_run_upsert, t) for t in tasks]
                for f in as_completed(futs):
                    try:
                        f.result()
                    except Exception as e:
                        _alert(f"[ERROR] Notion upsert task: {e}")
            notion_tasks_by_webhook[wh] = []

    for token in workspaces:
        if _should_exit_soon(context):
            break
        api_token=token["api_token"]; wss=get_workspaces_for_token(api_token)
        if not wss:
            _alert(f"⚠️ No workspaces visible for token ending …{api_token[-4:]}"); continue
        for ws in wss:
            if _should_exit_soon(context):
                break
            wid=ws.get("id");  
            if not wid: continue
            entries=fetch_workspace_entries(api_token,wid,since,now)
            print(f"[DEBUG] Fetched {len(entries)} entries from workspace {wid}")
            for e in entries:
                if _should_exit_soon(context):
                    break
                eid=str(e.get("id") or ""); user=e.get("user") or ""; desc=e.get("description") or "No description"; proj=e.get("project") or "No project"; tags=e.get("tags") or []
                dur_ms=int(e.get("dur") or 0); start_s=e.get("start") or ""; end_s=e.get("end") or ""; upd_s=e.get("updated") or e.get("at") or ""; uid=e.get("uid")
                if not eid or not user: continue
                processed_eids.add(eid)
                wh=None
                if NOTIFY_ENABLED: wh=user_map.get(user) or user_map_norm.get(_norm(user))
                start_dt=datetime.strptime(start_s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z") if start_s else now
                if end_s:
                    try:
                        end_dt = datetime.strptime(end_s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        end_dt = start_dt + timedelta(milliseconds=max(0,dur_ms))
                else:
                    end_dt = start_dt + timedelta(milliseconds=max(0,dur_ms))
                upd_dt=datetime.strptime(upd_s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z") if upd_s else now
                state_key=f"uid:{uid}" if uid is not None else f"name:{_norm(user)}"; notion_user_key=f"user:{uid}" if uid is not None else user
                if state_key not in user_runtime:
                    state={"seen_entries":[],"seen_updates":{},"seen_snapshots":{}}
                    if users_indexed < MAX_USERS_NOTION_INDEX and not _should_exit_soon(context):
                        idx=_notion_query_user(notion_user_key,since,now)
                        state={"seen_entries":list(idx.keys()),"seen_updates":{k:idx[k]["snap"].get("updated") or "" for k in idx},"seen_snapshots":{k:{**idx[k]["snap"],"page_id":idx[k]["page_id"]} for k in idx}}
                        users_indexed += 1
                    user_runtime[state_key]={"current_ids":set(),"user_name":user,"webhook":wh,"state":state,"notion_user_key":notion_user_key}
                bucket=user_runtime[state_key]; state=bucket["state"]
                seen_ids=set(state["seen_entries"]); seen_updates=state["seen_updates"]; snaps=state["seen_snapshots"]
                prev_upd_s=seen_updates.get(eid); prev_upd=None
                if prev_upd_s: 
                    try: prev_upd=datetime.strptime(prev_upd_s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z")
                    except Exception: prev_upd=None
                first_seen = prev_upd is None
                prev_snap = snaps.get(eid, {})
                # Systematic verification: if we don't have a snapshot for this EID, look it up in Notion now
                if not prev_snap:
                    found = _notion_find_entry_by_eid(eid)
                    if found:
                        fs = found.get("snap") or {}
                        snaps[eid] = {**fs, "page_id": found.get("page_id")}
                        seen_updates[eid] = fs.get("updated") or ""
                        try:
                            prev_upd = datetime.strptime((fs.get("updated") or "").replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                        except Exception:
                            prev_upd = None
                        first_seen = prev_upd is None
                        prev_snap = snaps.get(eid, {})
                prev_dur  = int(prev_snap.get("dur", dur_ms))
                prev_desc = prev_snap.get("desc", desc)
                prev_proj = prev_snap.get("project", proj)
                prev_tags = prev_snap.get("tags", list(tags))
                prev_start_s = prev_snap.get("start", start_s)
                try: prev_start_dt = datetime.strptime(prev_start_s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z") if prev_start_s else start_dt
                except Exception: prev_start_dt = start_dt

                bucket["current_ids"].add(eid)
                seen_updates[eid] = upd_dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                if eid not in seen_ids: state["seen_entries"].append(eid)
                snaps[eid] = {"dur": dur_ms, "desc": desc, "project": proj, "tags": list(tags), "start": start_s, "end": end_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(), "updated": upd_dt.strftime("%Y-%m-%dT%H:%M:%S%z")}

                if NOTIFY_ENABLED and not wh:
                    if first_seen and user not in warned:
                        _alert(f"[INFO] No webhook configured for '{user}' (uid: {uid}); persisting state and skipping notifications."); warned.add(user)
                    continue

                should_notify=False; label="🆕 New Time Entry Added"; dif=[]
                changed_time = dur_ms != prev_dur
                changed_proj = proj != prev_proj
                changed_desc = desc != prev_desc
                old_set=set(prev_tags) if isinstance(prev_tags,list) else set(); new_set=set(tags) if isinstance(tags,list) else set(); changed_tags = old_set != new_set
                changed_start = (start_dt != prev_start_dt)
                if first_seen:
                    if HARD_START_FILTER and start_dt < since: continue
                    if upd_dt >= since: should_notify=True; label="🆕 New Time Entry Added"
                else:
                    if changed_time: dif.append(f"Δ Time {_format_delta_ms(dur_ms - prev_dur)}")
                    if changed_proj: dif.append("Project changed")
                    if changed_desc: dif.append("Description changed")
                    if changed_tags:
                        add=sorted(list(new_set-old_set)); rem=sorted(list(old_set-new_set)); bits=[]
                        if add: bits.append("+"+", ".join(add))
                        if rem: bits.append("-"+", ".join(rem))
                        dif.append("Tags "+" ".join(bits) if bits else "Tags changed")
                    if changed_start: dif.append(f"Δ Start {_format_delta_datetimes(start_dt, prev_start_dt)}")
                    if dif:
                        should_notify=True; label="✍️ Time Entry Edited"
                if not should_notify:
                    continue
                if NOTIFY_ENABLED:
                    lines=[f"## {label}", f"👤 **User**: {user}"]
                    lines.append(f"📁 **Project:** {'~~'+prev_proj+'~~ '+proj if label.startswith('✍️') and changed_proj else proj}")
                    if label.startswith("✍️") and changed_tags:
                        lines.append(f"🏷️ **Tags:** ~~{', '.join(sorted(old_set)) or 'No tags'}~~ {', '.join(sorted(new_set)) or 'No tags'}")
                    else:
                        lines.append(f"🏷️ **Tags:** {', '.join(tags) if tags else 'No tags'}")
                    if label.startswith("✍️") and changed_desc:
                        lines.append(f"📝 **Task:** ~~{_truncate(prev_desc)}~~ {_truncate(desc)}")
                    else:
                        lines.append(f"📝 **Task:** {desc}")
                    if label.startswith("✍️") and changed_time:
                        lines.append(f"⏱️ **Time Tracked:** ~~{convert_ms_to_hms(prev_dur)}~~ {convert_ms_to_hms(dur_ms)}")
                    else:
                        lines.append(f"⏱️ **Time Tracked:** {convert_ms_to_hms(dur_ms)}")
                    if label.startswith("✍️") and changed_start:
                        lines.append(f"🗓️ **Start:** ~~{prev_start_dt.strftime('%Y-%m-%d %H:%M:%S')}~~ {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        lines.append(f"🗓️ **Start:** {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    lines.append(f"🕑 **Updated:** {upd_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    if label.startswith("✍️") and dif:
                        lines.append("")
                        lines.append("**changes:** "+"; ".join(dif))
                    lines.append("—"); msg="\n".join(lines)
                    if wh:
                        arr = outbox.setdefault(wh,[])
                        arr.append(msg)
                        notified+=1
                        # prepare a Notion upsert task tied to this webhook
                        existing={"page_id": prev_snap.get("page_id")} if prev_snap.get("page_id") else None
                        notion_entry={
                            "id": eid,
                            "user": user,
                            "project": proj,
                            "tags": tags,
                            "dur": dur_ms,
                            "start": start_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                            "end": end_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                            "updated": upd_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                            "description": desc,
                        }
                        key = (notion_user_key, eid)
                        if key not in scheduled_upserts:
                            scheduled_upserts.add(key)
                            # add description block on create or when description changed
                            add_block = (first_seen or changed_desc)
                            notion_tasks_by_webhook.setdefault(wh, []).append((existing, notion_user_key, notion_entry, wid, add_block))
                        if len(arr) >= NOTIFY_FLUSH_EVERY or _should_exit_soon(context):
                            _flush_webhook(wh)
                else:
                    # notifications disabled: upsert later regardless
                    existing={"page_id": prev_snap.get("page_id")} if prev_snap.get("page_id") else None
                    notion_entry={
                        "id": eid,
                        "user": user,
                        "project": proj,
                        "tags": tags,
                        "dur": dur_ms,
                        "start": start_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "end": end_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "updated": upd_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "description": desc,
                    }
                    key = (notion_user_key, eid)
                    if key not in scheduled_upserts:
                        scheduled_upserts.add(key)
                        add_block = (first_seen or changed_desc)
                        notion_tasks_no_webhook.append((existing, notion_user_key, notion_entry, wid, add_block))

    # Deletions
    for sk,b in user_runtime.items():
        cur=b["current_ids"]; user=b["user_name"]; wh=b["webhook"]; st=b["state"]; snaps=st.get("seen_snapshots",{});
        for eid in list(st.get("seen_entries",[])):
            if eid in cur: continue
            snap=snaps.get(eid);  
            if not snap: continue
            try:
                s=snap.get("start") or ""; sdt=datetime.strptime(s.replace("Z","+0000").replace("+00:00","+0000"),"%Y-%m-%dT%H:%M:%S%z") if s else None
            except Exception: sdt=None
            if not (sdt and sdt>=since): continue
            if snap.get("page_id"): delete_tasks.append(snap.get("page_id"))
            if not NOTIFY_ENABLED or not wh: skipped_deletions+=1
            else: outbox.setdefault(wh,[]).append(_format_deleted_message(user,snap)); notified+=1

    # Send each webhook batch first, then upsert only those entries that were notified for that webhook
    if NOTIFY_ENABLED and outbox:
        for wh, msgs in outbox.items():
            if _should_exit_soon(context):
                break
            for payload in _chunk_messages(msgs):
                if _should_exit_soon(context):
                    break
                send_discord_with_backoff(wh, payload, timeout=HTTP_TIMEOUT)
            tasks = notion_tasks_by_webhook.get(wh) or []
            if tasks and not _should_exit_soon(context):
                max_workers = max(1, min(NOTION_CONCURRENCY, len(tasks)))
                def _run_upsert(task):
                    ex, ukey, ent, wid, add_block = task
                    _notion_upsert(ex, ukey, ent, wid, add_desc_block=add_block)
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = [ex.submit(_run_upsert, t) for t in tasks]
                    for f in as_completed(futs):
                        try:
                            f.result()
                        except Exception as e:
                            _alert(f"[ERROR] Notion upsert task: {e}")

    # Persist entries when notifications are disabled
    if not NOTIFY_ENABLED and notion_tasks_no_webhook:
        max_workers = max(1, min(NOTION_CONCURRENCY, len(notion_tasks_no_webhook)))
        def _run_upsert2(task):
            ex, ukey, ent, wid, add_block = task
            _notion_upsert(ex, ukey, ent, wid, add_desc_block=add_block)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_run_upsert2, t) for t in notion_tasks_no_webhook]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    _alert(f"[ERROR] Notion upsert task: {e}")

    # Then process deletion marks (if any)
    if delete_tasks and not _should_exit_soon(context):
        max_workers = max(1, min(NOTION_CONCURRENCY, len(delete_tasks)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_notion_mark_deleted, pid) for pid in delete_tasks]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    _alert(f"[ERROR] Notion delete task: {e}")

    # Cleanup duplicates for processed Entry IDs (keep latest updated; mark others Deleted)
    if processed_eids and not _should_exit_soon(context):
        try:
            stats = notion_cleanup_duplicates_for_ids(processed_eids, context)
            if stats.get("cleaned"):
                _alert(f"[INFO] Dedupe: examined={stats.get('examined')} cleaned={stats.get('cleaned')}")
        except Exception as e:
            _alert(f"[ERROR] Dedupe failed: {e}")

    return {"status":"complete","notified":notified,"skipped_notifies":0,"skipped_deletions":skipped_deletions,"dry_run_no_send":DRY_RUN_NO_SEND,"send_notifications":SEND_NOTIFICATIONS,"notify_enabled":NOTIFY_ENABLED}
