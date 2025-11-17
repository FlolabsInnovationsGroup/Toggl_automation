import os, json, base64, requests, csv, time, random
from io import StringIO
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
# (DynamoDB removed)

SESSION = requests.Session()

# Reuse TCP connections aggressively to reduce latency and costs
def _init_http_session():
    try:
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0)
        SESSION.mount("https://", adapter)
        SESSION.mount("http://", adapter)
    except Exception:
        pass

_init_http_session()
_START_TS = time.monotonic()  # module import time (kept for compatibility)
_RUN_START_TS = 0.0           # set at the beginning of each invocation

# ENV
SYSTEM_ALERT_WEBHOOK = os.getenv("SYSTEM_ALERT_WEBHOOK", "https://discord.com/api/webhooks/1401155910126272553/xF3EHp8wQlA0mx4tvcUXvvzUmXBoJgfNae21TN6N-a7mZR699QdGLkqPGQjgtKkxj2_I")

TIME_WINDOW_DAYS = int(os.getenv("TIME_WINDOW_DAYS", "360"))
# Use a generous default HTTP timeout so Notion/index queries can complete
# even on slower networks. Can be overridden via HTTP_TIMEOUT_SECONDS env.
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))

DISCORD_MIN_INTERVAL_MS = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "" and default: return True
    return (v or "").strip().lower() in ("1","true","yes","y","on")

# Notion ID helpers
def _norm_notion_id(s: str | None) -> str:
    try:
        return "".join(ch for ch in str(s or "") if ch.isalnum()).lower()
    except Exception:
        return str(s or "")

def _dashify_notion_id(s: str | None) -> str:
    n = _norm_notion_id(s)
    if len(n) == 32:
        return f"{n[0:8]}-{n[8:12]}-{n[12:16]}-{n[16:20]}-{n[20:32]}"
    return s or ""

SEND_ALERT = _env_bool("SEND_ALERT", False)
SEND_NOTIFICATIONS = _env_bool("SEND_NOTIFICATIONS", False)
# SEND_NOTIFICATIONS must only influence the Notion Status property values for newly inserted entries.

# Deadline-aware controls and throughput priorities
MAX_RUNTIME_S = int(os.getenv("MAX_RUNTIME_S", "120"))  # soft cap when context missing
EXIT_MARGIN_S = int(os.getenv("EXIT_MARGIN_S", "12"))   # leave safety margin for shutdown

# Toggl API budget/rate guardrails (minimize cost and 402 spam)
MAX_TOGGL_CALLS_PER_RUN = int(os.getenv("MAX_TOGGL_CALLS_PER_RUN", "120"))
MAX_TOGGL_ERRORS_PER_TOKEN = int(os.getenv("MAX_TOGGL_ERRORS_PER_TOKEN", "3"))
TOGGL_PAGE_SIZE = int(os.getenv("TOGGL_PAGE_SIZE", "200"))

# In-run Toggl state
_TOGGL_CALLS_USED = 0
_WS_CACHE_BY_TOKEN: dict[str, list[dict]] = {}
_EXHAUSTED_TOKENS: set[str] = set()
_UNSUPPORTED_REPORTS: set[tuple[str, int]] = set()  # (token_tail, workspace_id) giving 402
_TOKEN_ERROR_COUNTS: dict[str, int] = {}

# Notion DB (replaces DynamoDB)
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "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").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "297a157695518075ab6cea94e60e4220").strip()
NH = {"Authorization": f"Bearer {NOTION_TOKEN}", "Notion-Version": "2022-06-28", "Content-Type": "application/json"}
NOTION_ENABLED = bool(NOTION_TOKEN and NOTION_DATABASE_ID)
_NOTION_DISABLED_LOGGED = False
P_USER_KEY    = os.getenv("NOTION_PROP_USER_KEY", "User Key")
P_USER_ID     = os.getenv("NOTION_PROP_USER_ID", "User ID")
P_ENTRY_ID    = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")
P_PROJECT     = os.getenv("NOTION_PROP_PROJECT", "Project")
P_TAGS        = os.getenv("NOTION_PROP_TAGS", "Tags")
P_DATE        = os.getenv("NOTION_PROP_DATE", "Date")     # unified range date property
P_UPDATED     = os.getenv("NOTION_PROP_UPDATED", "Updated")
P_DURATION    = os.getenv("NOTION_PROP_DURATION", "Duration")
P_WORKSPACE   = os.getenv("NOTION_PROP_WORKSPACE", "Workspace")
P_DESCRIPTION = os.getenv("NOTION_PROP_DESCRIPTION", "Description")
P_TEAM_MEMBER = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")
P_STATUS      = os.getenv("NOTION_PROP_STATUS", "Status")
P_WEBHOOK     = "Webhook"  # Time Entries lookup from Team Members.webhook
STATUS_NOTIFIED  = os.getenv("NOTION_STATUS_NOTIFIED", "Notified")
STATUS_PROCESSED = os.getenv("NOTION_STATUS_PROCESSED", "Processed")
STATUS_IGNORED   = os.getenv("NOTION_STATUS_IGNORED", "Ignored")
NOTION_TEAM_DB_ID = os.getenv("NOTION_TEAM_DB_ID", "286a1576955180d6acdec21f7a45dfcc").strip()
NOTION_DATABASE_ID_DASH = _dashify_notion_id(NOTION_DATABASE_ID)
NOTION_TEAM_DB_ID_DASH = _dashify_notion_id(NOTION_TEAM_DB_ID)
NOTION_TOGGL_WORKSPACES_DB_ID = os.getenv("NOTION_TOGGL_WORSPACES_DB_ID", "").strip()
NOTION_TOGGL_WORKSPACES_DB_ID_DASH = _dashify_notion_id(NOTION_TOGGL_WORKSPACES_DB_ID) if NOTION_TOGGL_WORKSPACES_DB_ID else ""

# Notion throughput knobs
NOTION_RPS = float(os.getenv("NOTION_RPS", "2"))            # target rps (global per integration)
NOTION_CONCURRENCY = int(os.getenv("NOTION_CONCURRENCY", "4"))  # threads for Notion writes
NOTION_MAX_RETRIES = int(os.getenv("NOTION_MAX_RETRIES", "3"))
# Circuit breaker thresholds to fail fast on persistent errors
NOTION_MAX_429_PER_RUN = int(os.getenv("NOTION_MAX_429_PER_RUN", "8"))
NOTION_MAX_5XX_PER_RUN = int(os.getenv("NOTION_MAX_5XX_PER_RUN", "8"))
NOTION_MAX_EXHAUSTED_PER_RUN = int(os.getenv("NOTION_MAX_EXHAUSTED_PER_RUN", "5"))
NOTION_MAX_EXC_PER_RUN = int(os.getenv("NOTION_MAX_EXC_PER_RUN", "4"))
MAX_INDEX_EIDS_PER_RUN = int(os.getenv("MAX_INDEX_EIDS_PER_RUN", "300"))
MIN_SECONDS_FOR_NOTION = int(os.getenv("MIN_SECONDS_FOR_NOTION", "60"))
MIN_SECONDS_FOR_CLEANUP = int(os.getenv("MIN_SECONDS_FOR_CLEANUP", "40"))
NOTION_ADD_DESCRIPTION_BLOCKS = _env_bool("NOTION_ADD_DESCRIPTION_BLOCKS", True)
MAX_TARGETED_LOOKUPS_PER_RUN = int(os.getenv("MAX_TARGETED_LOOKUPS_PER_RUN", "0"))
MAX_DEDUPE_IDS_PER_RUN = int(os.getenv("MAX_DEDUPE_IDS_PER_RUN", "40"))
MAX_DEDUPE_MARKS_PER_ID = int(os.getenv("MAX_DEDUPE_MARKS_PER_ID", "10"))
NOTION_INDEX_MODE = "window"  # Fixed optimal index mode
NOTION_ENABLE_DELETION_SCAN = _env_bool("NOTION_ENABLE_DELETION_SCAN", True)
INSERT_BATCH_SIZE = int(os.getenv("INSERT_BATCH_SIZE", "20"))  # number of new entries to insert per run
NOTIFY_BATCH_SIZE = int(os.getenv("NOTIFY_BATCH_SIZE", "20"))  # number of processed entries to notify per run

# Cached Notion schema (property name -> type)
_NOTION_SCHEMA: dict[str, str] = {}
_NOTION_OPTIONS: dict[str, dict[str, str]] = {}  # prop_name -> {lower_name: canonical_name}

def _load_notion_schema_and_options():
    try:
        url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}"
        r = _notion_request('GET', url)
        if not r or r.status_code != 200:
            return
        js = r.json() or {}
        props = js.get("properties") or {}
        for name, meta in props.items():
            t = (meta or {}).get("type")
            if t:
                _NOTION_SCHEMA[name] = t
            # Capture canonical option casing for select/multi_select
            if t == "select":
                opts = ((meta or {}).get("select") or {}).get("options") or []
                mapping = {str(o.get("name") or "").strip().lower(): (o.get("name") or "") for o in opts if o.get("name")}
                _NOTION_OPTIONS[name] = mapping
            elif t == "multi_select":
                opts = ((meta or {}).get("multi_select") or {}).get("options") or []
                mapping = {str(o.get("name") or "").strip().lower(): (o.get("name") or "") for o in opts if o.get("name")}
                _NOTION_OPTIONS[name] = mapping
            elif t == "status":
                opts = ((meta or {}).get("status") or {}).get("options") or []
                mapping = {str(o.get("name") or "").strip().lower(): (o.get("name") or "") for o in opts if o.get("name")}
                _NOTION_OPTIONS[name] = mapping
    except Exception:
        pass

def _notion_prop_type(prop_name: str) -> str | None:
    """Return Notion property type for the Time Entries database (cached)."""
    try:
        if not _NOTION_SCHEMA:
            _load_notion_schema_and_options()
        return _NOTION_SCHEMA.get(prop_name)
    except Exception:
        return None

def _canon_select(prop_name: str, name: str | None) -> str:
    s = (name or "").strip()
    if not s:
        return s
    try:
        if not _NOTION_SCHEMA:
            _load_notion_schema_and_options()
        mapping = _NOTION_OPTIONS.get(prop_name) or {}
        return mapping.get(s.lower(), s)
    except Exception:
        return s

def _canon_tags(prop_name: str, tags: list[str] | None) -> list[str]:
    if not tags:
        return []
    try:
        if not _NOTION_SCHEMA:
            _load_notion_schema_and_options()
        mapping = _NOTION_OPTIONS.get(prop_name) or {}
        out = []
        for t in tags:
            st = str(t or "").strip()
            if not st:
                continue
            out.append(mapping.get(st.lower(), st))
        return out
    except Exception:
        return [str(t).strip() for t in (tags or []) if str(t).strip()]

_NOTION_LAST_CALL = 0.0
_NOTION_RESUME_AT = 0.0  # monotonic timestamp when we may resume after 429
_NOTION_429_COUNT = 0
_NOTION_5XX_COUNT = 0
_NOTION_EXHAUSTED = 0
_NOTION_EXC_COUNT = 0
try:
    from threading import Lock
    _NOTION_LOCK = Lock()
    _EID_LOCKS_GUARD = Lock()
    _EID_LOCKS: dict[str, Lock] = {}
except Exception:
    class _Noop:
        def __enter__(self): pass
        def __exit__(self, *a): pass
    _NOTION_LOCK = _Noop()
    _EID_LOCKS_GUARD = _Noop()
    _EID_LOCKS = {}

def _eid_lock(eid: str):
    try:
        if not eid:
            return _NOTION_LOCK
        with _EID_LOCKS_GUARD:
            lk = _EID_LOCKS.get(eid)
            if lk is None:
                # If Lock is not available (fallback _Noop), return that
                try:
                    lk = Lock()  # type: ignore[name-defined]
                except Exception:
                    lk = _NOTION_LOCK
                _EID_LOCKS[eid] = lk
            return lk
    except Exception:
        return _NOTION_LOCK

# ---- Notion health-check ----
def _reset_notion_circuit():
    """Reset Notion circuit-breaker state at the beginning of an invocation.
    This lets warm Lambdas re-attempt a health-check instead of staying disabled.
    """
    global NOTION_ENABLED, _NOTION_DISABLED_LOGGED
    global _NOTION_429_COUNT, _NOTION_5XX_COUNT, _NOTION_EXHAUSTED, _NOTION_EXC_COUNT
    global _NOTION_RESUME_AT, _NOTION_LAST_CALL
    NOTION_ENABLED = bool(NOTION_TOKEN and NOTION_DATABASE_ID)
    _NOTION_DISABLED_LOGGED = False
    _NOTION_429_COUNT = 0
    _NOTION_5XX_COUNT = 0
    _NOTION_EXHAUSTED = 0
    _NOTION_EXC_COUNT = 0
    _NOTION_RESUME_AT = 0.0
    _NOTION_LAST_CALL = 0.0

def notion_health_check() -> tuple[bool, str]:
    """Verify Notion credentials and database access before heavy work.
    Returns (ok, reason). If not ok, NOTION calls are disabled for this run.
    """
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        _disable_notion("missing token or database id")
        return False, "missing_config"
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}"
    # Bypass _notion_request gating for health-check so we can recover after
    # a previous run disabled Notion in a warm container.
    try:
        r = SESSION.get(url, headers=NH, timeout=min(HTTP_TIMEOUT, 8))
    except Exception:
        return False, "request_failed"
    if r.status_code == 200:
        # Re-enable if a previous run disabled it
        _reset_notion_circuit()
        return True, "ok"
    if r.status_code in (401, 403):
        _disable_notion(f"{r.status_code} auth failure")
        return False, "unauthorized"
    return False, f"http_{r.status_code}"

def team_db_health_check() -> tuple[bool, str]:
    """Verify Team Members database is accessible (title property 'Name' present).
    Returns (ok, reason). If not ok, relation resolution cannot proceed this run.
    """
    if not NOTION_TEAM_DB_ID:
        return False, "missing_team_db_id"
    url = f"https://api.notion.com/v1/databases/{NOTION_TEAM_DB_ID_DASH}"
    try:
        r = SESSION.get(url, headers=NH, timeout=min(HTTP_TIMEOUT, 8))
    except Exception:
        return False, "request_failed"
    if r.status_code == 200:
        try:
            js = r.json() or {}
            props = js.get("properties") or {}
            name_prop = props.get("Name", {})
            if isinstance(name_prop, dict) and name_prop.get("type") == "title":
                return True, "ok"
            return False, "missing_name_title"
        except Exception:
            return False, "parse_failed"
    if r.status_code in (401, 403):
        return False, "unauthorized"
    return False, f"http_{r.status_code}"

def verify_team_member_relation_target() -> tuple[bool, str, str | None]:
    """Verify that the Time Entries DB relation property points to NOTION_TEAM_DB_ID.
    Returns (ok, reason, found_target_db_id).
    """
    try:
        if not (NOTION_DATABASE_ID and NOTION_TEAM_DB_ID):
            return False, "missing_config", None
        url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}"
        r = _notion_request('GET', url)
        if not r or r.status_code != 200:
            return False, "time_entries_db_get_failed", None
        js = r.json() or {}
        props = js.get("properties") or {}
        tm = props.get(P_TEAM_MEMBER, {})
        if not isinstance(tm, dict) or tm.get("type") != "relation":
            return False, "team_member_prop_not_relation", None
        target = ((tm.get("relation") or {}).get("database_id") or "").strip()
        if not target:
            return False, "relation_missing_target", None
        if _norm_notion_id(target) != _norm_notion_id(NOTION_TEAM_DB_ID):
            return False, "relation_target_mismatch", target
        return True, "ok", target
    except Exception:
        return False, "exception", None

# ------------------ DynamoDB removed ------------------

def _alert(msg: str):
    try:
        print(f"[ALERT] {msg}")
    except Exception:
        # Fallback for consoles that can't encode certain Unicode chars
        try:
            safe = (msg or "").encode('ascii', 'ignore').decode('ascii')
        except Exception:
            safe = str(msg)
        print("[ALERT] " + safe)
    if SYSTEM_ALERT_WEBHOOK and SEND_ALERT:
        try: SESSION.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
        except Exception: pass

def load_workspace_tokens() -> list[dict]:
    """Load Toggl workspace API tokens from the Notion 'Toggl Workspaces' database.

    Expected schema (properties):
      - Name: title
      - api_token: rich_text
    """
    db_id = NOTION_TOGGL_WORKSPACES_DB_ID_DASH
    if not db_id:
        _alert("Toggl Workspaces DB id (NOTION_TOGGL_WORSPACES_DB_ID) not configured")
        return []
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    out: list[dict] = []

    def _get_title(props: dict, name: str) -> str:
        p = props.get(name, {})
        if p.get("type") == "title":
            return "".join([x.get("plain_text", "") for x in (p.get("title") or [])]).strip()
        return ""

    def _get_rich_text(props: dict, name: str) -> str:
        p = props.get(name, {})
        if p.get("type") == "rich_text":
            return "".join([x.get("plain_text", "") for x in (p.get("rich_text") or [])]).strip()
        return ""

    payload = {"page_size": 100}
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request("POST", url, json_body=body)
        if not r or r.status_code != 200:
            _alert(f"[WARN] Toggl Workspaces query failed: status={getattr(r, 'status_code', None)}")
            break
        data = r.json() or {}
        for pg in (data.get("results") or []):
            props = pg.get("properties", {}) or {}
            name = _get_title(props, "Name")
            tok = _get_rich_text(props, "api_token")
            if tok:
                out.append({"name": name or "", "api_token": tok})
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    if not out:
        _alert("Toggl Workspaces DB empty or unreachable")
    return out

# Toggl
def _headers_for_token(api_token: str) -> dict:
    return {"Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode()}

def _remaining_s(context) -> float:
    """Best-effort remaining seconds for this invocation.
    Prefer the AWS Lambda context timer when available; fall back to wall-clock
    from the per-run _RUN_START_TS so we still enforce MAX_RUNTIME_S even if
    context is absent or non-decreasing (e.g., local tests).
    """
    now = time.monotonic()
    # Wall-clock remaining based on per-run start timestamp
    try:
        if _RUN_START_TS:
            elapsed = max(0.0, now - _RUN_START_TS)
            wall_remaining = max(0.0, MAX_RUNTIME_S - elapsed)
        else:
            wall_remaining = MAX_RUNTIME_S
    except Exception:
        wall_remaining = MAX_RUNTIME_S

    ctx_remaining = None
    try:
        if context and hasattr(context, "get_remaining_time_in_millis"):
            ctx_remaining = max(0.0, context.get_remaining_time_in_millis() / 1000.0)
    except Exception:
        ctx_remaining = None

    if ctx_remaining is None:
        return wall_remaining
    # Be conservative and respect the tighter of the two budgets
    return min(ctx_remaining, wall_remaining)

def _should_exit_soon(context) -> bool:
    """Return True when this run should stop scheduling work.
    If Notion is disabled, the script's main purpose (updating Notion)
    cannot be fulfilled, so we abort quickly instead of doing extra Toggl work.
    """
    if not NOTION_ENABLED:
        return True
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
            # Downgrade plan/feature limitation to DEBUG noise
            print(f"[DEBUG] get_workspaces_for_token: 402 plan-limited. Token {tok_tail}. Skipping this token for this run.")
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
            # Downgrade repeated plan/feature limitations to DEBUG noise
            print(f"[DEBUG] details fetch 402 (token {tok_tail}, ws {workspace_id}) - {txt}")
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

# Robust ISO8601 parser (handles Z, timezone, fractional seconds)
def _parse_iso8601(s: str | None, default: datetime | None = None) -> datetime:
    try:
        if not s:
            return default or datetime.now(timezone.utc)
        ss = str(s).strip()
        if ss.endswith("Z"):
            ss = ss[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(ss)
        except Exception:
            pass
        # Fallback to strptime without fractional seconds
        ss2 = ss.replace("+00:00", "+0000")
        return datetime.strptime(ss2, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return default or datetime.now(timezone.utc)

# Description comparison (case sensitive, ignore surrounding whitespace and CRLF differences)
def _desc_equal(a: str | None, b: str | None) -> bool:
    def _n(x: str | None) -> str:
        x = (x or "")
        # Normalize Windows vs Unix newlines and trim surrounding whitespace
        return x.replace("\r\n", "\n").strip()
    return _n(a) == _n(b)

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
    if not webhook_url:
        return False
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
def _sanitize_select_name(name: str | None) -> str:
    s = (name or "No project").strip()
    # Notion API may reject commas in select values; replace with middle dot
    s = s.replace(",", " · ")
    # Collapse whitespace
    s = " ".join(s.split())
    return s[:99]

def _sanitize_select_list(values: list[str] | None) -> list[str]:
    out = []
    for v in (values or []):
        sv = _sanitize_select_name(v)
        if sv:
            out.append(sv)
    return out

def _select_val(name: str):
    return {"select": {"name": _sanitize_select_name(name)}}

def _multi_val(tags: list[str]):
    return {"multi_select": [{"name": v} for v in _sanitize_select_list(tags)]}
def _relation(page_id: str|None):
    return {"relation": ([{"id": page_id}] if page_id else [])}

def _status_val(name: str | None):
    if not name:
        return None
    # Canonicalize via options mapping if available
    canon = _canon_select(P_STATUS, name)
    return {"status": {"name": canon}}

def _resolve_team_member_pid(user_display: str | None) -> str | None:
    """Resolve Team Member page id for a given display name using the Team Members DB index.
    Returns None if not configured or no reasonable match is found.
    """
    try:
        return _best_team_member_page_id(user_display or "")
    except Exception:
        return None

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

    # Prevent duplicate description blocks: fetch existing paragraph texts (best effort)
    try:
        def _para_text(b: dict) -> str:
            try:
                if not isinstance(b, dict):
                    return ""
                if b.get("type") != "paragraph":
                    return ""
                rt = ((b.get("paragraph") or {}).get("rich_text") or [])
                return "".join([x.get("plain_text", "") for x in rt]).strip()
            except Exception:
                return ""

        # Gather new paragraph texts we intend to add
        new_texts = set()
        for k in list(kids):
            t = _para_text(k)
            if t:
                new_texts.add(t)

        existing_texts = set()
        if new_texts:
            base = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
            r = _notion_request('GET', base)
            if r and r.status_code == 200:
                data = r.json() or {}
                for b in (data.get('results') or []):
                    txt = _para_text(b)
                    if txt:
                        existing_texts.add(txt)
                # Optionally follow one more page for larger pages
                if data.get('has_more') and data.get('next_cursor'):
                    r2 = _notion_request('GET', base + f"&start_cursor={data.get('next_cursor')}")
                    if r2 and r2.status_code == 200:
                        data2 = r2.json() or {}
                        for b in (data2.get('results') or []):
                            txt = _para_text(b)
                            if txt:
                                existing_texts.add(txt)

        # Remove any new blocks that already exist text-identical
        if existing_texts:
            kids = [k for k in kids if _para_text(k) not in existing_texts]
            if not kids:
                return
    except Exception:
        # On any failure, fall back to appending (better to risk a duplicate than to skip)
        pass

    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    _notion_request('PATCH', url, json_body={"children": kids})

def _notion_rate_limit():
    if NOTION_RPS <= 0:
        return
    global _NOTION_LAST_CALL
    with _NOTION_LOCK:
        now = time.monotonic()
        # Respect global resume gate from prior 429s
        if now < _NOTION_RESUME_AT:
            time.sleep(_NOTION_RESUME_AT - now)
            now = time.monotonic()
        min_gap = 1.0 / max(NOTION_RPS, 0.1)
        gap = now - _NOTION_LAST_CALL
        if gap < min_gap:
            time.sleep(min_gap - gap)
        _NOTION_LAST_CALL = time.monotonic()

def _disable_notion(reason: str):
    global NOTION_ENABLED, _NOTION_DISABLED_LOGGED
    NOTION_ENABLED = False
    if not _NOTION_DISABLED_LOGGED:
        _NOTION_DISABLED_LOGGED = True
        _alert(f"[WARN] Disabling Notion interactions for this run: {reason}")

def _notion_request(method: str, url: str, json_body: dict|None=None, timeout: int|None=None):
    if not NOTION_ENABLED:
        return None
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
            # Count exceptions and disable Notion quickly if they persist
            try:
                global _NOTION_EXC_COUNT
                _NOTION_EXC_COUNT += 1
                if _NOTION_EXC_COUNT >= NOTION_MAX_EXC_PER_RUN:
                    _alert(f"[WARN] Too many Notion exceptions; disabling Notion for this run. Last: {e}")
                    _disable_notion("too_many_exceptions")
                    return None
            except Exception:
                pass
            if attempt >= NOTION_MAX_RETRIES - 1:
                _alert(f"[ERROR] Notion {method} exception: {e}")
                return None
            time.sleep(0.4 * (2 ** attempt) + random.uniform(0.02, 0.2))
            continue
        if r.status_code in (200, 201):
            return r
        if r.status_code in (401, 403):
            _alert(f"[WARN] Notion {method} failed {r.status_code}: {r.text}")
            _disable_notion(f"{r.status_code} auth failure")
            return r
        if r.status_code in (409, 429, 500, 502, 503, 504):
            wait = 0.5 * (2 ** attempt) + random.uniform(0.05, 0.25)
            # Honor Retry-After if present (Notion rate limit)
            try:
                ra = r.headers.get('Retry-After')
                if ra is not None:
                    wait = max(wait, float(ra))
            except Exception:
                pass
            # Set a global resume gate so concurrent threads respect backoff
            try:
                from math import inf
                global _NOTION_RESUME_AT
                _NOTION_RESUME_AT = max(_NOTION_RESUME_AT, time.monotonic() + min(10.0, wait))
            except Exception:
                pass
            # Circuit breaker on too many 429s/5xx
            try:
                global _NOTION_429_COUNT, _NOTION_5XX_COUNT
                if r.status_code == 429:
                    _NOTION_429_COUNT += 1
                    if _NOTION_429_COUNT >= NOTION_MAX_429_PER_RUN:
                        _alert("[WARN] Too many Notion 429s; disabling Notion for this run.")
                        _disable_notion("too_many_429")
                        return r
                if r.status_code in (500, 502, 503, 504):
                    _NOTION_5XX_COUNT += 1
                    if _NOTION_5XX_COUNT >= NOTION_MAX_5XX_PER_RUN:
                        _alert("[WARN] Too many Notion 5xx; disabling Notion for this run.")
                        _disable_notion("too_many_5xx")
                        return r
            except Exception:
                pass
            time.sleep(min(6.0, wait))
            continue
        _alert(f"[WARN] Notion {method} failed {r.status_code}: {r.text}")
        return r
    _alert(f"[WARN] Notion {method} exhausted retries: {url}")
    try:
        global _NOTION_EXHAUSTED
        _NOTION_EXHAUSTED += 1
        if _NOTION_EXHAUSTED >= NOTION_MAX_EXHAUSTED_PER_RUN:
            _alert("[WARN] Too many Notion exhausted retries; disabling Notion for this run.")
            _disable_notion("exhausted_retries")
    except Exception:
        pass
    return None

# ---- Team Members index + fuzzy match ----
_TEAM_MEM_INDEX: list[tuple[str, str, str, str]] = []  # (norm_name, orig_name, page_id, webhook_url)
_TEAM_MEM_WEBHOOK_BY_ID: dict[str, str] = {}           # page_id -> webhook_url
_TEAM_MEM_NAME_BY_ID: dict[str, str] = {}              # page_id -> original name
_TEAM_MEM_BEST_CACHE: dict[str, str | None] = {}
_TEAM_SCHEMA_LOGGED = False
# Debug helpers: sample names and log cap so we can inspect fuzzy matches
_TEAM_DEBUG_NAMES = {"mariya", "haya al nuwab", "filip lysak", "hajni katona"}
_TEAM_DEBUG_LOG_COUNT = 0
try:
    _TEAM_DEBUG_MAX_LOGS = int(os.getenv("TEAM_MATCH_DEBUG_MAX", "10"))
except Exception:
    _TEAM_DEBUG_MAX_LOGS = 10


def _norm_for_match(s: str) -> str:
    """Normalize a human name for fuzzy matching.
    Lowercase, remove accents/diacritics, drop punctuation, and collapse spaces.
    This makes 'Ángel Caro' and 'Angel Caro' (or 'Filip Łysak' and 'Filip Lysak')
    comparable.
    """
    import unicodedata
    s = (s or "")
    try:
        # Strip accents/diacritics (NFKD + remove combining marks)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
    except Exception:
        pass
    s = s.lower()
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
    """Build an in-memory index of Team Members pages.

    Design goals:
    - Exactly one Notion query loop per warm container (guarded by existing index).
    - No use of the global Notion circuit breaker; failures here never disable Notion globally.
    - Robust name extraction: prefer TEAM_TITLE_PROP/"Name", fall back to first title property.
    """
    if not NOTION_TEAM_DB_ID or _TEAM_MEM_INDEX:
        return
    # Fast-fail when basic Notion config is missing
    if not (NOTION_TOKEN and NOTION_TEAM_DB_ID_DASH):
        return
    try:
        global _TEAM_SCHEMA_LOGGED
        _TEAM_SCHEMA_LOGGED = True
    except Exception:
        pass

    url = f"https://api.notion.com/v1/databases/{NOTION_TEAM_DB_ID_DASH}/query"
    try:
        # Smaller default page size to keep each Team Members query fast
        TM_PAGE_SIZE = int(os.getenv("TEAM_DB_QUERY_PAGE_SIZE", "25"))
    except Exception:
        TM_PAGE_SIZE = 25
    payload = {"page_size": max(1, min(100, TM_PAGE_SIZE))}
    cursor = None

    def _tm_request(body: dict) -> dict | None:
        """Dedicated lightweight request for Team Members DB.
        Does not use the global _notion_request circuit breaker.
        Uses a generous timeout because this runs once per 5 minutes."""
        timeout_s = max(HTTP_TIMEOUT, 60)
        for attempt in range(1, 3):
            try:
                r = SESSION.post(url, headers=NH, json=body, timeout=timeout_s)
            except Exception as e:
                if attempt >= 2:
                    _alert(f"[WARN] Team Members query exception: {e}")
                    return None
                time.sleep(0.3 * attempt)
                continue
            if r.status_code == 200:
                try:
                    return r.json() or {}
                except Exception:
                    return None
            # For auth/config errors, log once and stop.
            if r.status_code in (400, 401, 403, 404):
                _alert(f"[WARN] Team Members query failed {r.status_code}: {r.text}")
                return None
            # For transient 5xx/429, brief backoff then retry.
            time.sleep(0.3 * attempt)
        return None

    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        data = _tm_request(body)
        if not data:
            _alert("[WARN] Team Members query failed or timed out; stopping index build early.")
            break
        page_results = data.get("results") or []
        for pg in page_results:
            pid = pg.get("id")
            props = pg.get("properties", {})
            # Prefer TEAM_TITLE_PROP/"Name", fall back to first title property
            t = props.get(os.getenv("TEAM_TITLE_PROP", "Name"), {})
            name = ""
            if isinstance(t, dict) and t.get("type") == "title":
                for part in (t.get("title") or []):
                    name += part.get("plain_text", "")
            if not name:
                # Fallback: first title property in the page
                for v in props.values():
                    if isinstance(v, dict) and v.get("type") == "title":
                        for part in (v.get("title") or []):
                            name += part.get("plain_text", "")
                        break
            name = (name or "").strip()
            # Extract webhook URL from Team Members DB (single url property "webhook" or "Webhook")
            w = ""
            try:
                wh_prop = props.get("webhook") or props.get("Webhook") or {}
                if isinstance(wh_prop, dict) and wh_prop.get("type") == "url":
                    w = (wh_prop.get("url") or "").strip()
            except Exception:
                w = ""
            if pid and name:
                norm = _norm_for_match(name)
                _TEAM_MEM_INDEX.append((norm, name, pid, w))
                _TEAM_MEM_NAME_BY_ID[pid] = name
                if w:
                    _TEAM_MEM_WEBHOOK_BY_ID[pid] = w
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    if not _TEAM_MEM_INDEX:
        _alert("[WARN] Team Members index is empty; check NOTION_TEAM_DB_ID and that the Team Members DB has a title column.")

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
    best_name = None
    top: list[tuple[float, str, str]] = []  # (score, orig_name, pid)
    in_tokens = _tokenize(user_display)
    in_norm = _norm_for_match(user_display)
    for cand_norm, _orig, cand_pid, _wh in _TEAM_MEM_INDEX:
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
        # Track top candidates
        try:
            if len(top) < 5:
                top.append((score, _orig, cand_pid))
            else:
                low_i = min(range(len(top)), key=lambda i: top[i][0])
                if score > top[low_i][0]:
                    top[low_i] = (score, _orig, cand_pid)
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best_pid = cand_pid
            best_name = _orig
    pid = best_pid if best_score >= 0.30 else None

    # Debug logging for selected sample names (first few runs only)
    global _TEAM_DEBUG_LOG_COUNT
    try:
        if _norm_for_match(user_display) in _TEAM_DEBUG_NAMES and _TEAM_DEBUG_LOG_COUNT < _TEAM_DEBUG_MAX_LOGS:
            _TEAM_DEBUG_LOG_COUNT += 1
            _alert(
                f"[DEBUG] Team match for '{user_display}': "
                f"candidate='{best_name or ''}' score={best_score:.3f} selected_pid={pid or '<none>'}"
            )
    except Exception:
        pass

    _TEAM_MEM_BEST_CACHE[key] = pid
    return pid

# ---- Targeted Notion lookup by Entry ID (to avoid duplicates/false-new) ----
_EID_LOOKUP_CACHE: dict[str, dict] = {}
_TARGETED_LOOKUPS_USED = 0

def _notion_find_entry_by_eid(eid: str) -> dict | None:
    global _TARGETED_LOOKUPS_USED
    if not eid:
        return None
    if eid in _EID_LOOKUP_CACHE:
        return _EID_LOOKUP_CACHE[eid]
    # if limit <= 0, treat as unlimited
    if MAX_TARGETED_LOOKUPS_PER_RUN > 0 and _TARGETED_LOOKUPS_USED >= MAX_TARGETED_LOOKUPS_PER_RUN:
        return None
    _TARGETED_LOOKUPS_USED += 1
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    def _query_filter(flt: dict):
        body = {"page_size": 100, "filter": flt}
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            return []
        data = r.json() or {}
        return data.get("results") or []

    # Choose filter type based on actual Notion schema
    prop_type = _notion_prop_type(P_ENTRY_ID)
    results = []
    if prop_type == "number":
        if not eid.isdigit():
            return None
        results = _query_filter({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})
    elif prop_type == "rich_text":
        results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"equals": str(eid)}})
    else:
        # Fallback (unknown schema): prefer exact rich_text equals, then number equals if digits
        results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"equals": str(eid)}})
        if not results and eid.isdigit():
            results = _query_filter({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})

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
        def _get_status(n):
            q = props.get(n, {})
            return (q.get("status") or {}).get("name") or ""
        def _get_tags(n):
            q = props.get(n, {})
            return [t.get("name", "") for t in (q.get("multi_select") or []) if t.get("name")]
        start_s, end_s = _get_range(P_DATE)
        updated_s = _get_date(P_UPDATED) or pg.get("last_edited_time") or pg.get("created_time")
        snap = {
            "dur": int(_get_num(P_DURATION) or 0) * 1000,
            "desc": _get_text(P_DESCRIPTION),
            "project": _get_sel(P_PROJECT),
            "status": _get_status(P_STATUS),
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

def _notion_find_entry_by_eid_ws(eid: str, workspace_id: int | None, user_key: str | None = None, start_iso: str | None = None) -> dict | None:
    """Targeted lookup by Entry ID AND Workspace. Returns the most recently updated exact match.
    This avoids cross-workspace collisions when Entry IDs overlap across workspaces.
    """
    if not eid or workspace_id is None:
        return None
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    def _query_filter(flt: dict):
        body = {"page_size": 100, "filter": flt}
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            return []
        data = r.json() or {}
        return data.get("results") or []

    ptype = _notion_prop_type(P_ENTRY_ID)
    eid_filters = []
    if ptype == "number":
        if not str(eid).isdigit():
            return None
        eid_filters.append({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})
    elif ptype == "rich_text":
        eid_filters.append({"property": P_ENTRY_ID, "rich_text": {"equals": str(eid)}})
    else:
        # Unknown schema: try both
        if str(eid).isdigit():
            eid_filters.append({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})
        eid_filters.append({"property": P_ENTRY_ID, "rich_text": {"equals": str(eid)}})

    # Combine with workspace filter (number equals)
    ws_filter = {"property": P_WORKSPACE, "number": {"equals": int(workspace_id or 0)}}
    results = []
    for f in eid_filters:
        and_flt = {"and": [f, ws_filter]}
        results.extend(_query_filter(and_flt))

    candidates = []
    for pg in results:
        pid = pg.get("id")
        props = pg.get("properties", {})
        # Verify exact EID match
        p = props.get(P_ENTRY_ID, {})
        exact = False
        if p.get("type") == "number" and str(eid).isdigit():
            exact = (p.get("number") is not None and int(p.get("number")) == int(eid))
        elif p.get("type") == "rich_text":
            txt = "".join([x.get("plain_text", "") for x in (p.get("rich_text") or [])]).strip()
            exact = (txt == str(eid))
        if not exact:
            continue
        # Verify workspace exact match
        pw = props.get(P_WORKSPACE, {})
        ws_ok = (pw.get("type") == "number" and pw.get("number") is not None and int(pw.get("number")) == int(workspace_id or 0))
        if not ws_ok:
            continue
        # Build snapshot
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
        def _get_title(n):
            q = props.get(n, {})
            if q.get("type") == "title":
                return "".join([x.get("plain_text", "") for x in (q.get("title") or [])]).strip()
            if q.get("type") == "rich_text":
                return "".join([x.get("plain_text", "") for x in (q.get("rich_text") or [])]).strip()
            return ""
        def _get_sel(n):
            q = props.get(n, {})
            return (q.get("select") or {}).get("name") or "No project"
        def _get_status(n):
            q = props.get(n, {})
            return (q.get("status") or {}).get("name") or ""
        def _get_tags(n):
            q = props.get(n, {})
            return [t.get("name", "") for t in (q.get("multi_select") or []) if t.get("name")]
        start_s, end_s = _get_range(P_DATE)
        updated_s = _get_date(P_UPDATED) or pg.get("last_edited_time") or pg.get("created_time")
        # Optional strict user key match
        if user_key:
            uk = _get_title(P_USER_KEY)
            if uk.strip() != (user_key or "").strip():
                continue
        # Optional strict start match (within 2 seconds tolerance)
        if start_iso:
            try:
                a = _parse_iso8601(start_s, default=None)
                b = _parse_iso8601(start_iso, default=None)
                if not (a and b) or abs((a - b).total_seconds()) > 2.0:
                    continue
            except Exception:
                pass
        snap = {
            "dur": int(_get_num(P_DURATION) or 0) * 1000,
            "desc": _get_text(P_DESCRIPTION),
            "project": _get_sel(P_PROJECT),
            "status": _get_status(P_STATUS),
            "tags": _get_tags(P_TAGS),
            "start": start_s,
            "end": end_s,
            "updated": updated_s,
            "workspace": int(_get_num(P_WORKSPACE) or 0),
        }
        candidates.append({"page_id": pid, "snap": snap})

    if not candidates:
        return None

    def _ts(s):
        try:
            return datetime.strptime((s or "").replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    return max(candidates, key=lambda c: _ts(c["snap"].get("updated")))

def _notion_find_all_by_eid(eid: str) -> list[dict]:
    """Return all exact-match pages for this Entry ID with snapshots."""
    if not eid:
        return []
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"

    def _query_filter(flt: dict):
        body = {"page_size": 100, "filter": flt}
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            return []
        data = r.json() or {}
        return data.get("results") or []

    results = []
    prop_type = _notion_prop_type(P_ENTRY_ID)
    if prop_type == "number":
        if not eid.isdigit():
            return []
        results = _query_filter({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})
    elif prop_type == "rich_text":
        results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"equals": str(eid)}})
    else:
        # Fallback only if schema unknown; try text equals then number equals
        results = _query_filter({"property": P_ENTRY_ID, "rich_text": {"equals": str(eid)}})
        if not results and eid.isdigit():
            results = _query_filter({"property": P_ENTRY_ID, "number": {"equals": int(eid)}})

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
            "updated": _get_date(P_UPDATED) or pg.get("last_edited_time") or pg.get("created_time"),
            "workspace": int(_get_num(P_WORKSPACE) or 0),
        }
        out.append({"page_id": pid, "snap": snap})
    return out

def notion_cleanup_duplicates_for_ids(eids: set[str], context, keep_ids: set[str] | None = None) -> dict:
    """For each Entry ID, keep the latest-updated page; mark other duplicates Deleted."""
    cleaned = 0
    examined = 0
    for eid in list(eids)[:MAX_DEDUPE_IDS_PER_RUN]:
        if _should_exit_soon(context):
            break
        candidates = _notion_find_all_by_eid(eid)
        if not candidates:
            continue
        # Group by workspace to avoid cross-workspace dedupe
        groups: dict[int, list[dict]] = {}
        for c in candidates:
            ws = 0
            try:
                ws = int((c.get("snap", {}).get("workspace") or 0))
            except Exception:
                ws = 0
            groups.setdefault(ws, []).append(c)
        for ws, exact in groups.items():
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
            if keep_ids:
                exact.sort(key=lambda c: (c.get("page_id") in keep_ids, _ts(c)), reverse=True)
            else:
                exact.sort(key=_ts, reverse=True)
            keep = exact[0]
            losers = [c for c in exact[1:MAX_DEDUPE_MARKS_PER_ID+1] if not (keep_ids and c.get("page_id") in keep_ids)]
            for loser in losers:
                if _should_exit_soon(context):
                    break
                pid = loser.get("page_id")
                if pid:
                    _notion_archive_page(pid)
                    cleaned += 1
    return {"examined": examined, "cleaned": cleaned}

# (legacy _notion_query_user removed)

def _notion_upsert(existing: dict|None, user_key: str, entry: dict, workspace_id: int, add_desc_block: bool=False) -> str | None:
    if not NOTION_ENABLED:
        return existing.get("page_id") if isinstance(existing, dict) else None
    eid=str(entry.get("id"))
    # Resolve Team Member (mandatory)
    tm_pid = (entry.get("team_member_pid") if isinstance(entry, dict) else None) or _resolve_team_member_pid(entry.get("user") or "")
    if not tm_pid:
        _alert(f"[WARN] Team Member not found for '{entry.get('user')}' (eid {eid}, ws {workspace_id}); skipping upsert.")
        return existing.get("page_id") if isinstance(existing, dict) else None
    # Guard against duplicates: do lookup under an EID lock before create
    lk = _eid_lock(eid)
    # We intentionally keep the lock small in scope; we don't hold it across network requests
    props={
        # User Key now stores the Toggl display name
        P_USER_KEY: _title_val(user_key),
        # Entry ID remains the Toggl time entry id
        P_ENTRY_ID: {"number": int(eid)} if eid.isdigit() else _rt_val(eid),
        P_PROJECT:  _select_val(entry.get("project")),
        P_TAGS:     _multi_val(entry.get("tags") or []),
        P_DATE:     _date_range_val(entry.get("start"), entry.get("end")),
        P_UPDATED:  _date_val(entry.get("updated")),
        P_DURATION: _num_val(int(int(entry.get("dur",0))/1000)),
        P_WORKSPACE:_num_val(int(workspace_id or 0)),
        P_DESCRIPTION: _rt_val(entry.get("description") or ""),
        P_TEAM_MEMBER: _relation(tm_pid),
    }
    # Optional: store Toggl user id in a separate "ID" property
    try:
        user_id = entry.get("user_id")
        if P_USER_ID and user_id is not None:
            # Match the actual Notion property type for ID
            try:
                id_prop_type = _notion_prop_type(P_USER_ID)
            except Exception:
                id_prop_type = None
            if id_prop_type == "number":
                try:
                    num = int(user_id)
                except Exception:
                    num = None
                props[P_USER_ID] = _num_val(num)
            elif id_prop_type == "rich_text":
                props[P_USER_ID] = _rt_val(str(user_id))
            else:
                # Fallback: treat as rich_text to avoid validation errors on unknown schema
                props[P_USER_ID] = _rt_val(str(user_id))
    except Exception:
        pass
    st = entry.get("status")
    if P_STATUS and st:
        sv = _status_val(st)
        if sv:
            props[P_STATUS] = sv
    if existing and existing.get("page_id"):
        page_id = existing["page_id"]
        _notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", json_body={"properties": props})
        if NOTION_ADD_DESCRIPTION_BLOCKS and add_desc_block and (entry.get("description") or "").strip():
            _notion_append_blocks(page_id, [_desc_block(entry.get("description") or "")])
        return page_id
    else:
        # Try to create idempotently: lock eid, lookup, then create, then on conflict lookup and patch
        # 1) pre-create lookup under lock
        with lk:
            # Prefer workspace-scoped lookup, but fall back to global Entry ID
            # lookup so we never create a duplicate page for the same Toggl
            # time entry, even if the Workspace property type/schema changed.
            found = _notion_find_entry_by_eid_ws(eid, workspace_id, user_key=user_key, start_iso=entry.get("start"))
            if not found:
                found = _notion_find_entry_by_eid(eid)
            if found:
                page_id = found.get("page_id")
                if page_id:
                    _notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", json_body={"properties": props})
                    if NOTION_ADD_DESCRIPTION_BLOCKS and add_desc_block and (entry.get("description") or "").strip():
                        _notion_append_blocks(page_id, [_desc_block(entry.get("description") or "")])
                    return page_id
            # 2) attempt create
            body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": props}
            if NOTION_ADD_DESCRIPTION_BLOCKS and (entry.get("description") or "").strip():
                blk = _desc_block(entry.get("description") or "")
                if blk:
                    body["children"] = [blk]
            r = _notion_request("POST", "https://api.notion.com/v1/pages", json_body=body)
            if r and r.status_code in (200, 201):
                try:
                    created = r.json() or {}
                    pid = created.get("id")
                    return pid
                except Exception:
                    return None
            else:
                # 3) if failed (e.g., 409), lookup again and patch
                found2 = _notion_find_entry_by_eid_ws(eid, workspace_id, user_key=user_key, start_iso=entry.get("start"))
                if not found2:
                    found2 = _notion_find_entry_by_eid(eid)
                if found2 and found2.get("page_id"):
                    page_id = found2.get("page_id")
                    _notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", json_body={"properties": props})
                    if NOTION_ADD_DESCRIPTION_BLOCKS and add_desc_block and (entry.get("description") or "").strip():
                        _notion_append_blocks(page_id, [_desc_block(entry.get("description") or "")])
                    return page_id
        return None

def _notion_archive_page(page_id: str):
    if not NOTION_ENABLED:
        return
    if not page_id:
        return
    # Archive the page (removes it from the database view)
    _notion_request('PATCH', f"https://api.notion.com/v1/pages/{page_id}", json_body={"archived": True})

# Deleted msg
# (legacy deleted-message helpers removed)

# Batch
# (legacy batching helpers removed; staging path batches internally)

# (legacy run() removed)

# ---- Optimized Notion window indexing and fast sync ----
def notion_build_window_index(since: datetime, until: datetime, context=None) -> tuple[dict, dict]:
    """Query Notion once for the window and build indexes.
    Returns (by_eid, by_user).
    by_eid: eid -> {page_id, snap, user_key}
    by_user: user_key -> {eid -> {page_id, snap}}
    """
    by_eid: dict[str, dict] = {}
    by_user: dict[str, dict] = {}
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        return by_eid, by_user
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {"page_size": 100, "filter": {"and": [
        {"property": P_DATE, "date": {"on_or_after": since.astimezone(timezone.utc).replace(microsecond=0).isoformat()}},
        {"property": P_DATE, "date": {"on_or_before": until.astimezone(timezone.utc).replace(microsecond=0).isoformat()}},
    ]}}
    cursor = None
    while True:
        # Abort early if time budget is nearly exhausted
        if context is not None and _should_exit_soon(context):
            break
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            break
        js = r.json() or {}
        for pg in js.get("results") or []:
            if context is not None and _should_exit_soon(context):
                break
            pid = pg.get("id")
            props = pg.get("properties", {})
            # EID
            eid = None
            p = props.get(P_ENTRY_ID, {})
            if p.get("type") == "number" and p.get("number") is not None:
                try:
                    eid = str(int(p.get("number")))
                except Exception:
                    eid = None
            elif p.get("type") == "rich_text":
                eid = ("".join([x.get("plain_text", "") for x in (p.get("rich_text") or [])])).strip()
            if not eid:
                continue
            # User key (title)
            user_key = ""
            t = props.get(P_USER_KEY, {})
            if t.get("type") == "title":
                for part in (t.get("title") or []):
                    user_key += part.get("plain_text", "")
            user_key = (user_key or "").strip()
            # Snap
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
            def _get_status(n):
                q = props.get(n, {})
                return (q.get("status") or {}).get("name") or ""
            def _get_tags(n):
                q = props.get(n, {})
                return [t.get("name", "") for t in (q.get("multi_select") or []) if t.get("name")]
            start_s, end_s = _get_range(P_DATE)
            updated_s = _get_date(P_UPDATED) or pg.get("last_edited_time") or pg.get("created_time")
            snap = {
                "dur": int(_get_num(P_DURATION) or 0) * 1000,
                "desc": _get_text(P_DESCRIPTION),
                "project": _get_sel(P_PROJECT),
                "status": _get_status(P_STATUS),
                "tags": _get_tags(P_TAGS),
                "start": start_s,
                "end": end_s,
                "updated": updated_s,
                "workspace": int(_get_num(P_WORKSPACE) or 0),
            }
            by_eid[str(eid)] = {"page_id": pid, "snap": snap, "user_key": user_key}
            if user_key:
                by_user.setdefault(user_key, {})[str(eid)] = {"page_id": pid, "snap": snap}
            # Enforce a hard cap on indexed entries per run
            if MAX_INDEX_EIDS_PER_RUN > 0 and len(by_eid) >= MAX_INDEX_EIDS_PER_RUN:
                break
        # Stop if we've hit limits or there is no more data
        if (MAX_INDEX_EIDS_PER_RUN > 0 and len(by_eid) >= MAX_INDEX_EIDS_PER_RUN) or not js.get("has_more"):
            break
        cursor = js.get("next_cursor")
    return by_eid, by_user

def _get_title_text(prop: dict) -> str:
    if not isinstance(prop, dict):
        return ""
    if prop.get("type") == "title":
        return "".join([p.get("plain_text", "") for p in (prop.get("title") or [])]).strip()
    if prop.get("type") == "rich_text":
        return "".join([p.get("plain_text", "") for p in (prop.get("rich_text") or [])]).strip()
    return ""

def _get_webhook_url_from_props(props: dict) -> str:
    """Extract Webhook URL from the Time Entries DB properties.
    Supports direct url/rich_text/title as well as rollup lookups and, if missing,
    falls back to the Team Members index (using the Team Member relation).
    """
    try:
        if not isinstance(props, dict):
            return ""
        # Primary: Webhook property on Time Entries
        pw = props.get(P_WEBHOOK, {})
        if not isinstance(pw, dict):
            pw = {}
        ptype = pw.get("type")
        # Direct url property
        if ptype == "url":
            return (pw.get("url") or "").strip()
        # Text-based properties
        if ptype == "rich_text":
            return "".join([x.get("plain_text", "") for x in (pw.get("rich_text") or [])]).strip()
        if ptype == "title":
            return "".join([x.get("plain_text", "") for x in (pw.get("title") or [])]).strip()
        # Lookup/rollup from Team Members.webhook
        if ptype == "rollup":
            roll = pw.get("rollup") or {}
            rtype = roll.get("type")
            if rtype == "array":
                for it in (roll.get("array") or []):
                    if not isinstance(it, dict):
                        continue
                    it_type = it.get("type")
                    if it_type == "url":
                        u = (it.get("url") or "").strip()
                        if u:
                            return u
                    if it_type == "rich_text":
                        txt = "".join([x.get("plain_text", "") for x in (it.get("rich_text") or [])]).strip()
                        if txt:
                            return txt
                    if it_type == "title":
                        txt = "".join([x.get("plain_text", "") for x in (it.get("title") or [])]).strip()
                        if txt:
                            return txt
            elif rtype == "number":
                v = roll.get("number")
                return (str(v).strip() if v is not None else "")
            elif rtype == "date":
                d = roll.get("date") or {}
                return (d.get("start") or "").strip()
        # Fallback: use Team Member relation -> Team Members index (no extra Notion calls)
        try:
            tm = props.get(P_TEAM_MEMBER, {})
            if isinstance(tm, dict) and tm.get("type") == "relation":
                rel = (tm.get("relation") or [])
                if rel:
                    tm_pid = (rel[0].get("id") or "").strip()
                    if tm_pid and tm_pid in _TEAM_MEM_WEBHOOK_BY_ID:
                        return (_TEAM_MEM_WEBHOOK_BY_ID.get(tm_pid) or "").strip()
        except Exception:
            pass
        return ""
    except Exception:
        return ""

def notion_notify_processed(limit: int, user_map: dict, user_map_norm: dict, uid_to_name: dict, context=None) -> tuple[int, int]:
    """Notify up to `limit` Processed time entries.
    Returns (notified_count, skipped_count) where skipped_count includes
    Processed entries that were examined but had no usable webhook.
    """
    if not NOTION_ENABLED or limit <= 0:
        return 0, 0
    notified = 0
    skipped = 0
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    body = {
        "page_size": min(100, int(limit)),
        "filter": {"property": P_STATUS, "status": {"equals": _canon_select(P_STATUS, STATUS_PROCESSED)}},
        "sorts": [{"timestamp": "created_time", "direction": "ascending"}],
    }
    cursor = None

    def _chunk_pairs(pairs, limit_chars=1900, sep="\n\n"):
        chunk = []
        size = 0
        chunk_ids = []
        for pid, msg in pairs:
            add = (len(sep) if chunk else 0) + len(msg)
            if chunk and size + add > limit_chars:
                yield sep.join(chunk), list(chunk_ids)
                chunk, chunk_ids = [msg], [pid]
                size = len(msg)
            else:
                chunk.append(msg)
                chunk_ids.append(pid)
                size = len(sep.join(chunk))
        if chunk:
            yield sep.join(chunk), list(chunk_ids)

    while notified < limit:
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request('POST', url, json_body=body)
        if not r or r.status_code != 200:
            _alert(f"[WARN] notion_notify_processed query failed: status={getattr(r, 'status_code', None)}")
            break
        js = r.json() or {}
        results = js.get("results") or []
        try:
            _alert(f"[DEBUG] notion_notify_processed batch: results={len(results)} notified_so_far={notified}")
        except Exception:
            pass
        if not results:
            # No more Processed items to notify
            break

        outbox_pairs: dict[str, list[tuple[str, str]]] = {}

        for pg in results:
            if notified >= limit or _should_exit_soon(context):
                break
            pid = pg.get("id")
            props = pg.get("properties", {})
            user_key = _get_title_text(props.get(P_USER_KEY, {}))
            desc = _get_title_text(props.get(P_DESCRIPTION, {})) or "No description"
            proj = (props.get(P_PROJECT, {}).get("select") or {}).get("name") or "No project"
            tags = [t.get("name", "") for t in (props.get(P_TAGS, {}).get("multi_select") or []) if t.get("name")]
            dur_s = props.get(P_DURATION, {}).get("number") or 0
            dur_ms = int(float(dur_s)) * 1000
            dt = props.get(P_DATE, {}).get("date") or {}
            start_s = dt.get("start") or (pg.get("created_time") or "")
            start_dt = _parse_iso8601(start_s, default=datetime.now(timezone.utc))
            upd_prop = props.get(P_UPDATED, {}).get("date") or {}
            updated_s = upd_prop.get("start") or pg.get("last_edited_time") or pg.get("created_time") or ""
            updated_dt = _parse_iso8601(updated_s, default=start_dt)
            # Webhook URL is stored on the Time Entries DB as a lookup from Team Members.webhook
            wh = _get_webhook_url_from_props(props)
            if not wh:
                # Helpful debug: show why a Processed item is being skipped
                skipped += 1
                try:
                    _alert(f"[DEBUG] Skipping Processed entry without webhook: user_key={user_key} proj={proj}")
                except Exception:
                    pass
                continue
            # Prefer Team Member name from relation, then fall back to User Key
            name = user_key or ""
            try:
                tm = props.get(P_TEAM_MEMBER, {})
                if isinstance(tm, dict) and tm.get("type") == "relation":
                    rel = (tm.get("relation") or [])
                    if rel:
                        tm_pid = (rel[0].get("id") or "").strip()
                        if tm_pid:
                            tm_name = _TEAM_MEM_NAME_BY_ID.get(tm_pid) or ""
                            if tm_name:
                                name = tm_name
            except Exception:
                pass
            lines = [
                f"## 🆕 Time Entry",
                f"👤 **User**: {name or user_key}",
                f"📁 **Project:** {proj}",
                f"🏷️ **Tags:** {', '.join(tags) if tags else 'No tags'}",
                f"📝 **Task:** {desc}",
                f"⏱️ **Time Tracked:** {convert_ms_to_hms(dur_ms)}",
                f"🗓️ **Start:** {start_dt.strftime('%Y-%m-%d %H:%M:%S')}",
                f"🕑 **Updated:** {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}",
                "—",
            ]
            msg = "\n".join(lines)
            outbox_pairs.setdefault(wh, []).append((pid, msg))

        # Send batched per webhook
        for wh, pairs in outbox_pairs.items():
            if _should_exit_soon(context) or notified >= limit:
                break
            for payload, pids in _chunk_pairs(pairs):
                if _should_exit_soon(context) or notified >= limit:
                    break
                if send_discord_with_backoff(wh, payload, timeout=HTTP_TIMEOUT):
                    # Mark all included pages as Notified
                    for pid in pids:
                        _notion_request('PATCH', f"https://api.notion.com/v1/pages/{pid}", json_body={"properties": {P_STATUS: {"status": {"name": _canon_select(P_STATUS, STATUS_NOTIFIED)}}}})
                    notified += len(pids)

        if not js.get("has_more"):
            break
        cursor = js.get("next_cursor")

    return notified, skipped

def fast_sync(context=None):
    # Track per-invocation runtime so _remaining_s can enforce MAX_RUNTIME_S
    global _RUN_START_TS
    _RUN_START_TS = time.monotonic()

    # Reset circuit so warm containers can re-attempt health-checks
    _reset_notion_circuit()
    # Fail fast if Notion is not available (invalid token/DB permissions)
    ok, reason = notion_health_check()
    if not ok:
        _alert(f"[WARN] Notion health-check failed: {reason}. Aborting sync.")
        return {"status": "notion_auth_failed", "reason": reason}
    # Verify Team Members DB access early to avoid noisy logs
    t_ok, t_reason = team_db_health_check()
    if not t_ok:
        _alert(f"[WARN] Team Members DB health-check failed: {t_reason}. Aborting sync.")
        return {"status": "team_db_unreachable", "reason": t_reason}
    # Ensure Time Entries relation points to the configured Team Members DB
    rel_ok, rel_reason, rel_target = verify_team_member_relation_target()
    if not rel_ok:
        if rel_reason == "relation_target_mismatch":
            _alert(f"[WARN] Team Member relation target mismatch. NOTION_TEAM_DB_ID={NOTION_TEAM_DB_ID} actual_target={rel_target}")
        else:
            _alert(f"[WARN] Team Member relation verify failed: {rel_reason}")
        return {"status": "team_db_relation_mismatch", "reason": rel_reason, "found_target": rel_target}
    # Ensure we have enough time left to perform Notion work
    try:
        if context and hasattr(context, 'get_remaining_time_in_millis'):
            if (_remaining_s(context) < MIN_SECONDS_FOR_NOTION):
                _alert("[WARN] Low time budget; skipping Notion sync this run.")
                return {"status": "low_time_budget"}
    except Exception:
        pass
    workspaces = load_workspace_tokens()
    if not workspaces:
        _alert("Toggl Workspaces DB empty or unreachable.")
        return {"status": "no_workspaces"}

    # Legacy user_map/user_map_norm (from Google Sheets) are no longer used.
    # Keep empty dicts to preserve notion_notify_processed signature.
    user_map = {}
    user_map_norm = {}

    try:
        if not _NOTION_SCHEMA:
            _load_notion_schema_and_options()
    except Exception:
        pass
    if NOTION_TEAM_DB_ID and NOTION_ENABLED:
        try:
            # Skip building team index if time is tight
            if not (context and _remaining_s(context) < MIN_SECONDS_FOR_NOTION * 1.5):
                notion_build_team_members_index()
        except Exception:
            pass

    now=datetime.now(timezone.utc); since=now - timedelta(days=TIME_WINDOW_DAYS)
    by_eid, by_user = notion_build_window_index(since, now, context=context)

    # Counters for notifications; actual notify step runs later,
    # after we know whether there are any new entries this run.
    notified = 0
    skipped_notifies = 0
    uid_to_name: dict[str, str] = {}

    # Stage tasks separately to prioritize NEW inserts over updates
    staged_new: list[tuple] = []
    staged_updates: list[tuple] = []
    scheduled_upserts: set[tuple] = set()
    WRITES_LIMIT = min(INSERT_BATCH_SIZE, int(os.getenv("MAX_NOTION_WRITES_PER_RUN", "300")))
    processed_eids: set[str] = set()
    current_ids_by_user: dict[str, set[str]] = {}
    keep_upserted_ids: set[str] = set()

    def _queue_new(existing, notion_user_key, notion_entry, wid, add_block):
        if not NOTION_ENABLED:
            return
        key = (notion_user_key, notion_entry.get("id"))
        if key in scheduled_upserts:
            return
        scheduled_upserts.add(key)
        staged_new.append((existing, notion_user_key, notion_entry, wid, add_block))

    def _queue_update(existing, notion_user_key, notion_entry, wid, add_block):
        if not NOTION_ENABLED:
            return
        key = (notion_user_key, notion_entry.get("id"))
        if key in scheduled_upserts:
            return
        scheduled_upserts.add(key)
        staged_updates.append((existing, notion_user_key, notion_entry, wid, add_block))

    for token in workspaces:
        if _should_exit_soon(context):
            break
        api_token = token.get("api_token")
        wss = get_workspaces_for_token(api_token)
        if not wss:
            _alert(f"⚠️ No workspaces visible for token endinng …{api_token[-4:]}")
            continue
        for ws in wss:
            if _should_exit_soon(context):
                break
            wid = ws.get("id")
            if not wid:
                continue
            entries = fetch_workspace_entries(api_token, wid, since, now)
            print(f"[DEBUG] Fetched {len(entries)} entries from workspace {wid}")
            for e in entries:
                if _should_exit_soon(context):
                    break
                eid = str(e.get("id") or "").strip()
                user = e.get("user") or ""
                if not eid or not user:
                    continue
                project = _canon_select(P_PROJECT, e.get("project") or "No project")
                tags = _canon_tags(P_TAGS, e.get("tags") or [])
                desc = e.get("description") or "No description"
                dur_ms = int(e.get("dur") or 0)
                start_s = e.get("start") or ""
                end_s = e.get("end") or ""
                upd_s = e.get("updated") or e.get("at") or ""
                uid = e.get("uid")

                processed_eids.add(eid)
                # Use the Toggl display name as the Notion User Key value
                notion_user_key = user
                current_ids_by_user.setdefault(notion_user_key, set()).add(eid)
                # Map Notion user_key to display name for notifications
                if notion_user_key and user:
                    uid_to_name.setdefault(notion_user_key, user)

                try:
                    start_dt = datetime.strptime(start_s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z") if start_s else now
                except Exception:
                    start_dt = now
                if end_s:
                    try:
                        end_dt = datetime.strptime(end_s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        end_dt = start_dt + timedelta(milliseconds=max(0, dur_ms))
                else:
                    end_dt = start_dt + timedelta(milliseconds=max(0, dur_ms))
                try:
                    upd_dt = datetime.strptime(upd_s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z") if upd_s else now
                except Exception:
                    upd_dt = now

                prev = by_eid.get(eid)
                prev_snap = (prev or {}).get("snap") or {}
                # Guard against cross-workspace collisions: ignore prev if workspace mismatches
                try:
                    prev_ws = int(prev_snap.get("workspace") or 0)
                except Exception:
                    prev_ws = 0
                if prev and prev_ws != int(wid or 0):
                    prev = None
                    prev_snap = {}
                # Guard against user mismatch if index provides user_key.
                # Legacy pages may store user_key as "user:{uid}", while new
                # pages store the Toggl display name. Accept either form so
                # we do not treat existing pages as "new" and re-insert them.
                if prev and isinstance(prev, dict) and prev.get("user_key"):
                    prev_uk = (prev.get("user_key") or "").strip()
                    expected = (user or "").strip()
                    # Backwards compatibility for older "user:{uid}" keys.
                    if uid is not None and prev_uk.startswith("user:"):
                        expected = f"user:{uid}"
                    if prev_uk != expected:
                        prev = None
                        prev_snap = {}
                first_seen = prev is None
                prev_dur = int(prev_snap.get("dur", dur_ms))
                prev_desc = prev_snap.get("desc", desc)
                prev_proj = prev_snap.get("project", project)
                prev_tags = prev_snap.get("tags", list(tags))
                prev_start_s = prev_snap.get("start", start_s)
                try:
                    prev_start_dt = datetime.strptime(prev_start_s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z") if prev_start_s else start_dt
                except Exception:
                    prev_start_dt = start_dt

                changed_time = dur_ms != prev_dur
                changed_proj = project != prev_proj
                changed_desc = not _desc_equal(desc, prev_desc)
                old_set = set(prev_tags) if isinstance(prev_tags, list) else set()
                new_set = set(tags) if isinstance(tags, list) else set()
                changed_tags = old_set != new_set
                changed_start = (start_dt != prev_start_dt)

                # Insert only first-seen entries here (batch-limited)
                if first_seen:
                    # Require Team Member relation on insert
                    tm_pid = _resolve_team_member_pid(user)
                    if not tm_pid:
                        _alert(f"[WARN] Skipping insert: no Team Member match for '{user}' (eid {eid}, ws {wid}).")
                        continue
                    notion_entry = {
                        "id": eid,
                        "user": user,
                        "user_id": uid,
                        "team_member_pid": tm_pid,
                        "project": project,
                        "tags": tags,
                        "dur": dur_ms,
                        "start": start_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "end": end_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "updated": upd_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "description": desc,
                    }
                    existing = None
                    add_block = True
                    # For new entries, choose Status based on SEND_NOTIFICATIONS:
                    # when False, insert as Ignored; when True, insert as Processed.
                    notion_entry["status"] = STATUS_PROCESSED if SEND_NOTIFICATIONS else STATUS_IGNORED
                    _queue_new(existing, notion_user_key, notion_entry, wid, add_block)

                # For existing entries, upsert when any key field changed
                elif (changed_time or changed_proj or changed_desc or changed_tags or changed_start):
                    notion_entry = {
                        "id": eid,
                        "user": user,
                        "user_id": uid,
                        "project": project,
                        "tags": tags,
                        "dur": dur_ms,
                        "start": start_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "end": end_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "updated": upd_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
                        "description": desc,
                        # Reset status to Processed so staged notifier can send updates
                        "status": STATUS_PROCESSED,
                    }
                    existing = {"page_id": (prev or {}).get("page_id")}
                    add_block = changed_desc
                    _queue_update(existing, notion_user_key, notion_entry, wid, add_block)

    # Build prioritized task list
    # Requirement: As long as there are new (not-yet-in-DB) entries,
    # do NOT run any updates. Only when there are zero new entries
    # do we schedule updates.
    notion_tasks: list[tuple] = []
    if staged_new:
        for t in staged_new:
            if len(notion_tasks) >= WRITES_LIMIT:
                break
            notion_tasks.append(t)
    else:
        for t in staged_updates:
            if len(notion_tasks) >= WRITES_LIMIT:
                break
            notion_tasks.append(t)

    # Run Notion upserts for all scheduled entries
    if notion_tasks and not _should_exit_soon(context):
        max_workers = max(1, min(NOTION_CONCURRENCY, len(notion_tasks)))
        def _run_upsert2(task):
            ex, ukey, ent, wid, add_block = task
            return _notion_upsert(ex, ukey, ent, wid, add_desc_block=add_block)
        with ThreadPoolExecutor(max_workers=max_workers) as ex2:
            futs = [ex2.submit(_run_upsert2, t) for t in notion_tasks]
            for f in as_completed(futs):
                try:
                    pid = f.result()
                    if pid:
                        keep_upserted_ids.add(pid)
                except Exception as e:
                    _alert(f"[ERROR] Notion upsert task: {e}")

    # Stage-2: notify a batch of already-Processed items and mark as Notified.
    # Only run this when there are no new entries to insert from any workspace
    # in this invocation, so existing pages are patched only when the run
    # has no pending new inserts.
    if NOTION_ENABLED and not _should_exit_soon(context) and not staged_new:
        try:
            to_notify = int(NOTIFY_BATCH_SIZE)
            n, skipped = notion_notify_processed(to_notify, user_map, user_map_norm, uid_to_name, context)
            notified += n
            skipped_notifies += skipped
        except Exception as e:
            _alert(f"[ERROR] notify processed failed: {e}")

    # Deletions based on Notion index vs current run (window index always available)
    delete_tasks = []
    if not _should_exit_soon(context) and NOTION_ENABLE_DELETION_SCAN:
        for ukey, pages in by_user.items():
            cur = current_ids_by_user.get(ukey, set())
            for eid, item in pages.items():
                if eid in cur:
                    continue
                snap = item.get("snap") or {}
                pid = item.get("page_id")
                try:
                    s = snap.get("start") or ""
                    sdt = datetime.strptime(s.replace("Z","+0000").replace("+00:00","+0000"), "%Y-%m-%dT%H:%M:%S%z") if s else None
                except Exception:
                    sdt = None
                if not (sdt and sdt >= since):
                    continue
                if pid:
                    delete_tasks.append(pid)

    if delete_tasks and not _should_exit_soon(context) and (not context or _remaining_s(context) >= MIN_SECONDS_FOR_CLEANUP):
        max_workers = max(1, min(NOTION_CONCURRENCY, len(delete_tasks)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex3:
            futs = [ex3.submit(_notion_archive_page, pid) for pid in delete_tasks]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    _alert(f"[ERROR] Notion archive task: {e}")

    # Dedupe processed eids
    if processed_eids and not _should_exit_soon(context) and (not context or _remaining_s(context) >= MIN_SECONDS_FOR_CLEANUP):
        try:
            stats = notion_cleanup_duplicates_for_ids(processed_eids, context, keep_ids=keep_upserted_ids)
            if stats.get("cleaned"):
                _alert(f"[INFO] Dedupe: examined={stats.get('examined')} cleaned={stats.get('cleaned')}")
        except Exception as e:
            _alert(f"[ERROR] Dedupe failed: {e}")

    return {"status":"complete","notified":notified,"skipped_notifies":skipped_notifies,"send_notifications": SEND_NOTIFICATIONS}

# ---- Local main for testing ----
# (legacy lambda_handler overload removed)

class _LocalContext:
    def __init__(self, ms: int = 5 * 60 * 1000):
        self._ms = ms
    def get_remaining_time_in_millis(self):
        return self._ms

def lambda_handler(event, context):
    """AWS Lambda entrypoint: run optimized fast_sync only."""
    return fast_sync(context=context)

def _norm_for_match(s: str) -> str:
    """Normalize a human name for fuzzy matching.
    Lowercase, remove accents/diacritics, map Latin letters with strokes/variants
    (e.g. Ł/ł) to their ASCII base, drop punctuation, and collapse spaces.
    This makes "Ángel Caro" vs "Angel Caro" and "Filip Łysak" vs "Filip Lysak"
    comparable.
    """
    import unicodedata

    s = (s or "")
    try:
        # First apply compatibility decomposition so composed accents are split out.
        s = unicodedata.normalize("NFKD", s)

        def _latin_base(ch: str) -> str:
            """Best-effort ASCII base for Latin letters like Ł, Ø, Đ, etc."""
            # Keep fast path for plain ASCII
            if ord(ch) < 128:
                return ch
            name = unicodedata.name(ch, "")
            if "LATIN" in name and " LETTER " in name:
                # E.g. 'LATIN CAPITAL LETTER L WITH STROKE' -> 'L'
                try:
                    after = name.split(" LETTER ", 1)[1]
                    base_word = after.split(" WITH ", 1)[0].strip().split(" ")[0]
                    if base_word:
                        # Preserve case of the original character
                        return base_word[0].lower() if ch.islower() else base_word[0].upper()
                except Exception:
                    pass
            return ch

        # Strip combining marks, then map remaining Latin letters with diacritics/strokes
        # to their ASCII base characters.
        s = "".join(_latin_base(ch) for ch in s if not unicodedata.combining(ch))
    except Exception:
        # On any failure, fall back to simple lowercasing and spacing rules below.
        s = (s or "")

    s = s.lower()
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

if __name__ == "__main__":
    print("Running live_reports_bot locally…")
    ctx_ms = int(os.getenv("LOCAL_CONTEXT_MS", "300000"))
    res = lambda_handler(None, _LocalContext(ctx_ms))
    print("Result:", json.dumps(res, indent=2))
