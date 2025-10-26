import os
import json
import base64
import requests
import csv
import time
import random
import re
from io import StringIO
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================
# Robust HTTP session (shared)
# ==========================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SESSION = requests.Session()

def _mount_retries_on_session():
    """Mount a retrying HTTPAdapter that also retries on read/connect timeouts and 429/5xx."""
    total = int(os.getenv("REQUESTS_RETRY_TOTAL", "6"))
    backoff = float(os.getenv("REQUESTS_RETRY_BACKOFF", "0.6"))
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=64)
    SESSION.mount("https://", adapter)
    SESSION.mount("http://", adapter)

_mount_retries_on_session()

# ==========================
# ENVIRONMENT
# ==========================
GOOGLE_SHEET_URL     = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID             = os.getenv("TEAM_GID", "")
WORKSPACES_GID       = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK = os.getenv("SYSTEM_ALERT_WEBHOOK", "")

# --- Runtime safety caps ---
MAX_RUNTIME_S             = int(os.getenv("MAX_RUNTIME_S", "170"))    # hard wall-clock cap
MAX_TOGGL_ROWS_PER_RUN    = int(os.getenv("MAX_TOGGL_ROWS_PER_RUN", "800"))
TIGHT_EXIT_SAFETY_MARGIN  = int(os.getenv("TIGHT_EXIT_SAFETY_MARGIN", "3"))

# General window / HTTP
TIME_WINDOW_DAYS    = int(os.getenv("TIME_WINDOW_DAYS", "24"))
HTTP_TIMEOUT        = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))            # default for non-Notion
NOTION_HTTP_TIMEOUT = int(os.getenv("NOTION_HTTP_TIMEOUT_SECONDS", "45"))     # longer for Notion calls
HARD_START_FILTER   = os.getenv("HARD_START_FILTER", "1") == "1"  # default ON

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
NOTION_BATCH_SIZE   = int(os.getenv("NOTION_BATCH_SIZE", "50"))
NOTION_RPS          = float(os.getenv("NOTION_RPS", "0"))                  # < 3 RPS (0 = no delay)
NOTION_FLUSH_EVERY  = int(os.getenv("NOTION_FLUSH_EVERY", "400"))          # amortize queries

# *** Performance caps (prevents timeouts) ***
NOTION_MAX_UPSERTS_PER_RUN = int(os.getenv("NOTION_MAX_UPSERTS_PER_RUN", "300"))
NOTION_CREATE_ADD_BLOCKS   = _env_bool("NOTION_CREATE_ADD_BLOCKS", True)
NOTION_SET_TEAM_RELATION   = _env_bool("NOTION_SET_TEAM_RELATION", True)
NOTION_MAX_RETRIES         = int(os.getenv("NOTION_MAX_RETRIES", "6"))     # explicit Notion call retries

# NEW: wide index flags (make DB a real backup)
NOTION_WIDE_EXISTING_INDEX = _env_bool("NOTION_WIDE_EXISTING_INDEX", True)  # build global existing Entry-ID set
NOTION_MAX_INDEX_PAGES     = int(os.getenv("NOTION_MAX_INDEX_PAGES", "10000")) # safety cap (200 * 100 = 20k rows)

# Property names
NOTION_PROP_NAME         = os.getenv("NOTION_PROP_NAME", "User Key")       # title
NOTION_PROP_TEAM_MEMBER  = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")
NOTION_PROP_ENTRY_ID     = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")   # number or rich_text
NOTION_PROP_PROJECT      = os.getenv("NOTION_PROP_PROJECT", "Project")
NOTION_PROP_TAGS         = os.getenv("NOTION_PROP_TAGS", "Tags")
NOTION_PROP_START        = os.getenv("NOTION_PROP_START", "Start")
NOTION_PROP_END          = os.getenv("NOTION_PROP_END", "End")
NOTION_PROP_UPDATED      = os.getenv("NOTION_PROP_UPDATED", "Updated")
NOTION_PROP_DURATION_S   = os.getenv("NOTION_PROP_DURATION_S", "Duration")
NOTION_PROP_WORKSPACE    = os.getenv("NOTION_PROP_WORKSPACE", "Workspace")
NOTION_PROP_DESCRIPTION  = os.getenv("NOTION_PROP_DESCRIPTION", "Description")
NOTION_PROP_DELETED      = os.getenv("NOTION_PROP_DELETED", "Deleted")
_existing_eid_to_pid: dict[str, str] = {}

# Optional Notion backfill
NOTION_BACKFILL              = _env_bool("NOTION_BACKFILL", True)
HISTORY_STEP_MONTHS          = int(os.getenv("HISTORY_STEP_MONTHS", "12"))
HISTORY_MAX_EMPTY_WINDOWS    = int(os.getenv("HISTORY_MAX_EMPTY_WINDOWS", "2"))
HISTORY_MAX_YEARS            = int(os.getenv("HISTORY_MAX_YEARS", "15"))

# Discord rate limiting
DISCORD_MIN_INTERVAL_MS        = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES            = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

_START_TS = None  # set at lambda start

def _elapsed_s() -> float:
    return time.monotonic() - _START_TS if _START_TS is not None else 0.0

def _time_budget_exhausted(context) -> bool:
    if _elapsed_s() > (MAX_RUNTIME_S - TIGHT_EXIT_SAFETY_MARGIN):
        return True
    try:
        return context.get_remaining_time_in_millis() <= TIGHT_EXIT_SAFETY_MARGIN * 1000
    except Exception:
        return False

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
# Notion client
# ==========================
_NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
def _notion_ok():
    return NOTION_ENABLED and NOTION_TOKEN and NOTION_DATABASE_ID

_NOTION_SCHEMA = {}  # name -> type
_TEAM_MEM_INDEX: list[tuple[str, str, str]] = []
_TEAM_MEM_FASTMAP: dict[str, str] = {}
_TEAM_MEM_BEST_CACHE: dict[str, str | None] = {}

def _prop_type(name: str) -> str:
    return _NOTION_SCHEMA.get(name, "")

def _notion_backoff(attempt:int):
    if NOTION_RPS > 0:
        time.sleep(1.0 / NOTION_RPS)
    time.sleep(min(2.0, 0.25 * (2 ** attempt)) + random.uniform(0.02, 0.25))

def notion_load_schema():
    if not _notion_ok():
        return
    try:
        for attempt in range(NOTION_MAX_RETRIES):
            try:
                r = SESSION.get(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
                                headers=_NOTION_HEADERS, timeout=NOTION_HTTP_TIMEOUT)
                if r.status_code == 200:
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"[ALERT] Notion schema read failed: {r.status_code} {r.text}")
                return
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"[WARN] Notion schema timeout/retry: {e}")
                _notion_backoff(attempt)
        else:
            _alert("[ALERT] Notion schema read exhausted retries"); return

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

def _checkbox(val: bool | None):
    return {"checkbox": bool(val) if val is not None else False}

def _relation(page_id: str|None):
    return {"relation": [{"id": page_id}]} if page_id else {"relation": []}

def _norm_for_match(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokenize(s: str) -> set[str]:
    return set(_norm_for_match(s).split())

def _title_from_props(props: dict, title_prop: str) -> str:
    p = props.get(title_prop, {})
    if p.get("type") == "title":
        parts = p.get("title") or []
        return "".join([t.get("plain_text", "") for t in parts]).strip()
    return ""

def notion_build_team_members_index(team_sheet: list[dict] | None = None):
    if not NOTION_SET_TEAM_RELATION:
        return
    global _TEAM_MEM_INDEX, _TEAM_MEM_FASTMAP
    if not NOTION_TEAM_DB_ID or _TEAM_MEM_INDEX:
        if team_sheet and not _TEAM_MEM_FASTMAP and _TEAM_MEM_INDEX:
            names = { _norm(x["name"]) for x in team_sheet if x.get("name") }
            for cand_norm, _orig, pid in _TEAM_MEM_INDEX:
                if cand_norm in names:
                    _TEAM_MEM_FASTMAP[cand_norm] = pid
        return

    url = f"https://api.notion.com/v1/databases/{NOTION_TEAM_DB_ID}/query"
    payload = {"page_size": 100}
    start_cursor = None
    while True:
        body = dict(payload)
        if start_cursor:
            body["start_cursor"] = start_cursor
        try:
            for attempt in range(NOTION_MAX_RETRIES):
                try:
                    r = SESSION.post(url, headers=_NOTION_HEADERS, json=body, timeout=NOTION_HTTP_TIMEOUT)
                    if r.status_code == 200:
                        break
                    if r.status_code in (429, 500, 502, 503, 504):
                        _notion_backoff(attempt); continue
                    _alert(f"[ALERT] Team Members index load failed: {r.status_code} {r.text}")
                    return
                except (requests.Timeout, requests.ConnectionError) as e:
                    _alert(f"[WARN] Team index timeout/retry: {e}")
                    _notion_backoff(attempt)
            else:
                _alert("[ALERT] Team Members index exhausted retries"); return

            data = r.json() or {}
            results = data.get("results") or []
            for pg in results:
                pid = pg.get("id")
                nm  = _title_from_props(pg.get("properties", {}), "Name")
                if pid and nm:
                    nm_norm = _norm_for_match(nm)
                    _TEAM_MEM_INDEX.append((nm_norm, nm, pid))
            if not data.get("has_more"):
                break
            start_cursor = data.get("next_cursor")
            if NOTION_RPS > 0:
                time.sleep(1.0 / NOTION_RPS)
        except Exception as e:
            _alert(f"[ALERT] Team Members index exception: {e}")
            break

    if team_sheet:
        names = { _norm(x["name"]) for x in team_sheet if x.get("name") }
        for cand_norm, _orig, pid in _TEAM_MEM_INDEX:
            if cand_norm in names:
                _TEAM_MEM_FASTMAP[cand_norm] = pid

def _best_team_member_page_id(user_display: str) -> str | None:
    if not NOTION_SET_TEAM_RELATION or not user_display:
        return None
    key = _norm_for_match(user_display)
    pid = _TEAM_MEM_FASTMAP.get(key)
    if pid:
        return pid
    if key in _TEAM_MEM_BEST_CACHE:
        return _TEAM_MEM_BEST_CACHE[key]
    if not _TEAM_MEM_INDEX:
        notion_build_team_members_index()
    best_pid = None
    best_score = 0.0
    in_tokens = _tokenize(user_display)
    in_norm   = _norm_for_match(user_display)
    for cand_norm, _cand_orig, cand_pid in _TEAM_MEM_INDEX:
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

def _prop_map_for_entry(entry_id:str, user_key:str, user_display:str,
                        project:str, tags:list[str], start_iso:str|None, end_iso:str|None,
                        updated_iso:str|None, dur_seconds:int, workspace_id:int|None,
                        description:str|None, deleted:bool|None=False):
    props = {
        NOTION_PROP_NAME:        _title_prop(user_key),
        NOTION_PROP_PROJECT:     _select(project),
        NOTION_PROP_TAGS:        _multi_select(tags),
        NOTION_PROP_START:       _date(start_iso),
        NOTION_PROP_END:         _date(end_iso),
        NOTION_PROP_UPDATED:     _date(updated_iso),
        NOTION_PROP_DURATION_S:  _number(int(dur_seconds or 0)),
        NOTION_PROP_WORKSPACE:   _number(int(workspace_id or 0)),
        NOTION_PROP_DESCRIPTION: _rich_text(description or ""),
    }
    if str(entry_id).isdigit() and _prop_type(NOTION_PROP_ENTRY_ID) == "number":
        props[NOTION_PROP_ENTRY_ID] = {"number": int(entry_id)}
    else:
        props[NOTION_PROP_ENTRY_ID] = _rich_text(str(entry_id))

    if NOTION_PROP_DELETED in _NOTION_SCHEMA and _prop_type(NOTION_PROP_DELETED) == "checkbox":
        props[NOTION_PROP_DELETED] = _checkbox(deleted)

    if NOTION_SET_TEAM_RELATION and (NOTION_PROP_TEAM_MEMBER in _NOTION_SCHEMA):
        tm_page = _best_team_member_page_id(user_display)
        props[NOTION_PROP_TEAM_MEMBER] = _relation(tm_page)
    return props

# ---------- NOTION STATE INDEX (windowed) ----------
def _extract_text(rt: list[dict]) -> str:
    try:
        return "".join([t.get("plain_text","") for t in (rt or [])]).strip()
    except Exception:
        return ""

def _get_prop_value(props: dict, name: str):
    return props.get(name, {})

def _prop_to_entry_id(props: dict) -> str:
    p = _get_prop_value(props, NOTION_PROP_ENTRY_ID)
    t = p.get("type")
    if t == "number":
        num = p.get("number")
        return str(int(num)) if num is not None else ""
    elif t == "rich_text":
        return _extract_text(p.get("rich_text"))
    return ""

def _prop_to_text(props: dict, name:str) -> str:
    p = _get_prop_value(props, name)
    if p.get("type") == "rich_text":
        return _extract_text(p.get("rich_text"))
    if p.get("type") == "title":
        return _extract_text(p.get("title"))
    return ""

def _prop_to_select_name(props: dict, name:str) -> str:
    p = _get_prop_value(props, name)
    if p.get("type") == "select":
        sel = p.get("select") or {}
        return sel.get("name") or "No project"
    return "No project"

def _prop_to_tags(props: dict, name:str) -> list[str]:
    p = _get_prop_value(props, name)
    if p.get("type") == "multi_select":
        return [t.get("name","") for t in (p.get("multi_select") or []) if t.get("name")]
    return []

def _prop_to_date_iso(props: dict, name:str) -> str|None:
    p = _get_prop_value(props, name)
    if p.get("type") == "date":
        d = p.get("date") or {}
        return d.get("start")
    return None

def _prop_to_number(props: dict, name:str) -> float|int|None:
    p = _get_prop_value(props, name)
    if p.get("type") == "number":
        return p.get("number")
    return None

def _prop_to_checkbox(props: dict, name:str) -> bool:
    p = _get_prop_value(props, name)
    if p.get("type") == "checkbox":
        return bool(p.get("checkbox"))
    return False

def _prop_to_title_text(props: dict, name:str) -> str:
    p = _get_prop_value(props, name)
    if p.get("type") == "title":
        return _extract_text(p.get("title"))
    return ""

def _notion_fetch_entries_in_window(since_dt: datetime, until_dt: datetime) -> list[dict]:
    if not _notion_ok():
        return []
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    results = []
    payload = {
        "filter": {
            "and": [
                {"property": NOTION_PROP_START, "date": {"on_or_after": since_dt.strftime("%Y-%m-%d")}},
                {"property": NOTION_PROP_START, "date": {"on_or_before": until_dt.strftime("%Y-%m-%d")}},
            ]
        },
        "page_size": 100
    }
    start_cursor = None
    while True:
        body = dict(payload)
        if start_cursor:
            body["start_cursor"] = start_cursor
        for attempt in range(NOTION_MAX_RETRIES):
            try:
                r = SESSION.post(url, headers=_NOTION_HEADERS, json=body, timeout=NOTION_HTTP_TIMEOUT)
                if r.status_code == 200:
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"[ALERT] Notion state query failed: {r.status_code} {r.text}")
                return results
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"[WARN] Notion query timeout/retry: {e}")
                _notion_backoff(attempt)
        else:
            _alert("[ALERT] Notion state query exhausted retries")
            return results

        data = r.json() or {}
        results.extend(data.get("results") or [])
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        if NOTION_RPS > 0:
            time.sleep(1.0 / NOTION_RPS)
    return results

def _build_notion_state_index(since_dt: datetime, until_dt: datetime):
    entries_by_id: dict[str, dict] = {}
    user_current_ids: dict[str, set] = {}
    pages = _notion_fetch_entries_in_window(since_dt, until_dt)
    for pg in pages:
        pid   = pg.get("id")
        props = pg.get("properties", {}) or {}
        eid   = _prop_to_entry_id(props)
        if not eid:
            continue
        user_key   = _prop_to_title_text(props, NOTION_PROP_NAME) or ""
        project    = _prop_to_select_name(props, NOTION_PROP_PROJECT)
        tags       = _prop_to_tags(props, NOTION_PROP_TAGS)
        start_iso  = _prop_to_date_iso(props, NOTION_PROP_START)
        end_iso    = _prop_to_date_iso(props, NOTION_PROP_END)
        updated_iso= _prop_to_date_iso(props, NOTION_PROP_UPDATED)
        dur_s      = _prop_to_number(props, NOTION_PROP_DURATION_S) or 0
        desc       = _prop_to_text(props, NOTION_PROP_DESCRIPTION)
        deleted    = _prop_to_checkbox(props, NOTION_PROP_DELETED) if NOTION_PROP_DELETED in _NOTION_SCHEMA else False

        snap = {
            "dur_ms": int(dur_s * 1000),
            "desc": desc or "No description",
            "project": project or "No project",
            "tags": list(tags) if isinstance(tags, list) else [],
            "start_iso": start_iso or "",
            "end_iso": end_iso or "",
            "updated_iso": updated_iso or "",
            "deleted": bool(deleted),
        }
        entries_by_id[str(eid)] = {
            "page_id": pid,
            "user_key": user_key,
            "snapshot": snap,
        }
        if user_key:
            user_current_ids.setdefault(user_key, set()).add(str(eid))
    return entries_by_id, user_current_ids

# ---------- WIDE EXISTING INDEX (global Entry IDs) ----------
def _notion_collect_all_entry_ids() -> dict[str, str]:
    """
    Build a global map of Entry ID -> page_id present in the Notion DB.
    Uses a single paginated query with 'Entry ID is not empty'.
    """
    eid_to_pid: dict[str, str] = {}
    if not _notion_ok() or not NOTION_WIDE_EXISTING_INDEX:
        return eid_to_pid

    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": { "property": NOTION_PROP_ENTRY_ID, _prop_type(NOTION_PROP_ENTRY_ID): { "is_not_empty": True } },
        "page_size": 100
    }
    start_cursor = None
    pages_scanned = 0
    while True and pages_scanned < NOTION_MAX_INDEX_PAGES:
        body = dict(payload)
        if start_cursor:
            body["start_cursor"] = start_cursor
        try:
            for attempt in range(NOTION_MAX_RETRIES):
                try:
                    r = SESSION.post(url, headers=_NOTION_HEADERS, json=body, timeout=NOTION_HTTP_TIMEOUT)
                    if r.status_code == 200:
                        break
                    if r.status_code in (429, 500, 502, 503, 504):
                        _notion_backoff(attempt); continue
                    _alert(f"[ALERT] Wide index query failed: {r.status_code} {r.text}")
                    return eid_to_pid
                except (requests.Timeout, requests.ConnectionError) as e:
                    _alert(f"[WARN] Wide index timeout/retry: {e}")
                    _notion_backoff(attempt)
            else:
                _alert("[ALERT] Wide index exhausted retries")
                return eid_to_pid

            data = r.json() or {}
            results = data.get("results") or []
            for res in results:
                pid   = res.get("id")
                props = res.get("properties", {}) or {}
                eid   = _prop_to_entry_id(props)
                if eid and pid:
                    eid_to_pid[str(eid)] = pid

            pages_scanned += 1
            if not data.get("has_more"):
                break
            start_cursor = data.get("next_cursor")
            if NOTION_RPS > 0:
                time.sleep(1.0 / NOTION_RPS)
        except Exception as e:
            _alert(f"[ALERT] Wide index exception: {e}")
            break

    print(f"[INFO] Wide Notion existing index size={len(eid_to_pid)} pages_scanned={pages_scanned}")
    return eid_to_pid

# ---------- Query existing by Entry IDs (fast batches) ----------
def _notion_query_existing_by_entry_ids(entry_ids: list[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    if not entry_ids:
        return found
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
            for attempt in range(NOTION_MAX_RETRIES):
                try:
                    r = SESSION.post(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                                     headers=_NOTION_HEADERS, json=payload, timeout=NOTION_HTTP_TIMEOUT)
                    if r.status_code == 200:
                        break
                    if r.status_code in (429, 500, 502, 503, 504):
                        _notion_backoff(attempt); continue
                    _alert(f"[ALERT] Notion bulk query failed: {r.status_code} {r.text}")
                    break
                except (requests.Timeout, requests.ConnectionError) as e:
                    _alert(f"[WARN] Notion bulk query timeout/retry: {e}")
                    _notion_backoff(attempt)
            else:
                _alert("[ALERT] Notion bulk query exhausted retries")
                continue

            for res in (r.json() or {}).get("results", []):
                pid = res["id"]
                props = res.get("properties", {})
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

def _notion_create_pages(pages: list[dict], context=None):
    created = 0
    for payload in pages:
        if context and _time_budget_exhausted(context):
            break
        body = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": payload["properties"],
        }
        if NOTION_CREATE_ADD_BLOCKS and (payload.get("description") or ""):
            body["children"] = [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": (payload.get("description") or "")}}],
                    }
                }
            ]
        ok = False
        for attempt in range(NOTION_MAX_RETRIES):
            try:
                r = SESSION.post("https://api.notion.com/v1/pages",
                                 headers=_NOTION_HEADERS, json=body, timeout=NOTION_HTTP_TIMEOUT)
                if r.status_code in (200, 201):
                    created += 1
                    ok = True
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"[ALERT] Notion create failed: {r.status_code} {r.text}")
                break
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"[WARN] Notion create timeout/retry: {e}")
                _notion_backoff(attempt)
        if not ok:
            _alert("[ERROR] Create page failed after retries; continuing")
        if NOTION_RPS > 0:
            time.sleep(1.0 / NOTION_RPS)
    return created

def _notion_update_pages(items: list[tuple[str, dict]], context=None):
    updated = 0
    for page_id, props in items:
        if context and _time_budget_exhausted(context):
            break
        body = {"properties": props}
        ok = False
        for attempt in range(NOTION_MAX_RETRIES):
            try:
                r = SESSION.patch(f"https://api.notion.com/v1/pages/{page_id}",
                                  headers=_NOTION_HEADERS, json=body, timeout=NOTION_HTTP_TIMEOUT)
                if r.status_code == 200:
                    updated += 1
                    ok = True
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"[ALERT] Notion update failed: {r.status_code} {r.text}")
                break
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"[WARN] Notion update timeout/retry: {e}")
                _notion_backoff(attempt)
        if not ok:
            _alert(f"[ERROR] Update page {page_id} failed after retries; continuing")
        if NOTION_RPS > 0:
            time.sleep(1.0 / NOTION_RPS)
    return updated

# Queue all intended upserts here (entry_id -> {properties..., description})
_notion_queue: dict[str, dict] = {}
_notion_ops_done = 0  # created + updated this invocation

def _notion_budget_remaining() -> int:
    return max(0, NOTION_MAX_UPSERTS_PER_RUN - _notion_ops_done)

def notion_queue_entry(entry_id:str, user_key:str, user_display:str,
                       project:str, tags:list[str], start_iso:str|None, end_iso:str|None,
                       updated_iso:str|None, dur_seconds:int, workspace_id:int|None,
                       description:str|None, deleted:bool|None=False):
    if not _notion_ok():
        return False
    if _notion_budget_remaining() <= 0:
        return False
    props = _prop_map_for_entry(entry_id, user_key, user_display, project, tags,
                                start_iso, end_iso, updated_iso, dur_seconds, workspace_id,
                                description, deleted=deleted)
    _notion_queue[str(entry_id)] = {"properties": props, "description": description or ""}
    return True

def _pop_queue_items(limit: int | None) -> list[tuple[str, dict]]:
    if not _notion_queue:
        return []
    items = list(_notion_queue.items())
    if limit is not None:
        items = items[:max(0, limit)]
    for eid, _ in items:
        _notion_queue.pop(eid, None)
    return items

def notion_flush_queue(limit_by_budget: bool = True, context=None):
    global _notion_ops_done, _existing_eid_to_pid
    if not _notion_ok() or not _notion_queue:
        return {"created": 0, "updated": 0, "skipped": 0}
    if context and _time_budget_exhausted(context):
        return {"created": 0, "updated": 0, "skipped": len(_notion_queue)}

    budget = _notion_budget_remaining() if limit_by_budget else None
    if budget == 0:
        return {"created": 0, "updated": 0, "skipped": len(_notion_queue)}

    low_time = context and (context.get_remaining_time_in_millis() <= (TIGHT_EXIT_SAFETY_MARGIN + 1) * 1000)
    take_cap = 100 if low_time else NOTION_FLUSH_EVERY

    to_take = min(budget, take_cap) if budget is not None else take_cap
    items = _pop_queue_items(to_take)
    if not items:
        return {"created": 0, "updated": 0, "skipped": 0}

    # 1) Route using our local eid->pid map (zero Notion calls)
    to_update, to_create = [], []
    unknown_eids = []

    for eid, payload in items:
        pid = _existing_eid_to_pid.get(eid)
        if pid:
            to_update.append((pid, payload["properties"]))
        else:
            unknown_eids.append(eid)
            to_create.append({"eid": eid, "properties": payload["properties"], "description": payload.get("description","")})

    # 2) For unknown_eids, perform ONE batch lookup to catch rows added by other processes
    if unknown_eids:
        lookup = _notion_query_existing_by_entry_ids(unknown_eids)
        if lookup:
            # move those into updates
            still_create = []
            for item in to_create:
                eid = item["eid"]
                if eid in lookup:
                    to_update.append((lookup[eid], item["properties"]))
                    _existing_eid_to_pid[eid] = lookup[eid]
                else:
                    still_create.append(item)
            to_create = still_create

    # 3) Execute
    created = updated = 0

    # Updates
    if to_update:
        for i in range(0, len(to_update), NOTION_BATCH_SIZE):
            if context and _time_budget_exhausted(context): break
            updated += _notion_update_pages(to_update[i:i+NOTION_BATCH_SIZE], context=context)
            if _notion_budget_remaining() <= 0:
                break

    # Creates (note: we can't learn new page IDs without parsing responses;
    # fine for routing on the same run; the next wide index will catch them)
    if to_create and _notion_budget_remaining() > 0:
        # Convert to the create payload shape
        create_batches = []
        batch = []
        for item in to_create:
            batch.append({"properties": item["properties"], "description": item.get("description","")})
            if len(batch) >= NOTION_BATCH_SIZE:
                create_batches.append(batch); batch=[]
        if batch:
            create_batches.append(batch)

        for b in create_batches:
            if context and _time_budget_exhausted(context): break
            if _notion_budget_remaining() <= 0: break
            created += _notion_create_pages(b, context=context)

    _notion_ops_done += (created + updated)
    print(f"[INFO] Notion upsert: created={created} updated={updated} total={created+updated} budget_used={_notion_ops_done}")
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

def iter_workspace_entries_pages(api_token: str, workspace_id: int, since_dt: datetime, until_dt: datetime, per_page:int=200):
    url = "https://api.track.toggl.com/reports/api/v2/details"
    headers = _headers_for_token(api_token)
    params = {
        "workspace_id": workspace_id,
        "since": since_dt.strftime("%Y-%m-%d"),
        "until": until_dt.strftime("%Y-%m-%d"),
        "user_agent": "toggl-discord-bot",
        "page": 1,
        "per_page": per_page,
    }
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
        yield entries
        if total_count and params["page"] * per_page >= total_count:
            break
        if total_count and len(entries) == 0:
            break
        if (params["page"] > 1) and (total_count == last_total):
            break
        last_total = total_count
        params["page"] += 1

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
    dur_ms  = int(snap.get("dur_ms", 0))
    start_s = snap.get("start_iso") or ""
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
    global _notion_ops_done, _START_TS
    _START_TS = time.monotonic()
    _notion_ops_done = 0

    # Load TEAM & WORKSPACES
    team          = load_team_sheet()
    workspace_set = load_workspace_tokens()

    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}
    if not workspace_set:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

    # Notion: read schema once + build team index (optional, off by default)
    if _notion_ok():
        notion_load_schema()
        notion_build_team_members_index(team_sheet=team)

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
    outbox = {}
    missing_webhook_warned = set()

    # Build Notion-based state snapshot (window) + wide existing index of Entry IDs
    notion_entries_by_id, notion_user_ids = ({}, {})
    if _notion_ok():
        global _existing_eid_to_pid
        notion_entries_by_id, notion_user_ids = _build_notion_state_index(since, now)
        eid_to_pid = _notion_collect_all_entry_ids() if NOTION_WIDE_EXISTING_INDEX else {}
        wide_existing_eids = set(eid_to_pid.keys())  # keep this line if other code expects the set
        _existing_eid_to_pid = dict(eid_to_pid)  # seed routing map
    else:
        wide_existing_eids = set()

    # Counters
    scanned_rows   = 0
    acted_rows     = 0

    # Short-circuit: if we have no Notion budget and notifications are off, exit early.
    if _notion_ok() and _notion_budget_remaining() <= 0 and not NOTIFY_ENABLED:
        return {
            "status": "complete",
            "notified": 0,
            "dry_run_no_send": DRY_RUN_NO_SEND,
            "send_notifications": SEND_NOTIFICATIONS,
            "notify_enabled": NOTIFY_ENABLED,
            "state_backend": "notion",
            "notion": {
                "budget_used": _notion_ops_done,
                "budget_cap": NOTION_MAX_UPSERTS_PER_RUN,
                "queue_left": len(_notion_queue),
            },
            "runtime": {
                "elapsed_s": round(_elapsed_s(), 3),
                "max_runtime_s": MAX_RUNTIME_S,
                "scanned_rows": scanned_rows,
                "acted_rows": acted_rows,
                "max_actions_per_run": MAX_TOGGL_ROWS_PER_RUN,
            },
        }

    # ---- Optional Notion history backfill (unchanged) ----
    backfilled = {"windows":0, "entries":0}
    if NOTION_BACKFILL and _notion_ok() and _notion_budget_remaining() > 0 and not _time_budget_exhausted(context):
        for token_entry in workspace_set:
            if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN or _notion_budget_remaining() <= 0:
                break
            api_token = token_entry["api_token"]
            workspaces = get_workspaces_for_token(api_token)
            for ws in workspaces:
                if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN or _notion_budget_remaining() <= 0:
                    break
                wid = ws.get("id")
                if not wid:
                    continue
                empty_streak = 0
                for (win_start, win_end) in _history_windows(now):
                    if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN or _notion_budget_remaining() <= 0:
                        break
                    if empty_streak >= HISTORY_MAX_EMPTY_WINDOWS:
                        break
                    any_page_had_rows = False
                    for page in iter_workspace_entries_pages(api_token, wid, win_start, win_end, per_page=200):
                        if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN or _notion_budget_remaining() <= 0:
                            break
                        any_page_had_rows = True
                        for e in page:
                            if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN or _notion_budget_remaining() <= 0:
                                break
                            scanned_rows += 1
                            entry_id    = str(e.get("id") or "")
                            user_name   = e.get("user") or ""
                            if not entry_id or not user_name:
                                continue
                            if entry_id in wide_existing_eids:
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
                            queued = notion_queue_entry(
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
                                description=description,
                                deleted=False
                            )
                            if queued:
                                acted_rows += 1; wide_existing_eids.add(entry_id)
                            backfilled["entries"] += 1
                            if len(_notion_queue) >= NOTION_FLUSH_EVERY:
                                notion_flush_queue(limit_by_budget=True, context=context)
                    if not any_page_had_rows:
                        empty_streak += 1
                    else:
                        empty_streak = 0
                if _notion_queue:
                    notion_flush_queue(limit_by_budget=True, context=context)
        if _notion_queue and _notion_budget_remaining() > 0:
            notion_flush_queue(limit_by_budget=True, context=context)

    # ---- Live window fetch + notifications + Notion upserts (Notion = state/backup) ----
    toggl_ids_by_user: dict[str, set] = {}

    for token_entry in workspace_set:
        if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN:
            break
        api_token = token_entry["api_token"]
        workspaces = get_workspaces_for_token(api_token)
        if not workspaces:
            _alert(f"⚠️ No workspaces visible for token ending …{api_token[-4:]}")
            continue

        for ws in workspaces:
            if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN:
                break
            wid = ws.get("id")
            if not wid:
                continue

            print(f"[DEBUG] Streaming entries from workspace {wid} (action budget left={MAX_TOGGL_ROWS_PER_RUN - acted_rows})")
            for page in iter_workspace_entries_pages(api_token, wid, since, now, per_page=200):
                if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN:
                    break
                print(f"[DEBUG] Page fetched with {len(page)} entries from workspace {wid}")
                for entry in page:
                    if _time_budget_exhausted(context) or acted_rows >= MAX_TOGGL_ROWS_PER_RUN:
                        break
                    scanned_rows += 1

                    entry_id    = str(entry.get("id") or "")
                    user_name   = entry.get("user") or ""
                    if not entry_id or not user_name:
                        continue

                    description = entry.get("description") or "No description"
                    project     = entry.get("project") or "No project"
                    tags        = entry.get("tags") or []
                    duration_ms = int(entry.get("dur") or 0)
                    start_str   = entry.get("start") or ""
                    end_str     = entry.get("end") or entry.get("stop") or ""
                    updated_str = entry.get("updated") or entry.get("at") or ""
                    uid         = entry.get("uid")

                    if not end_str and start_str and duration_ms > 0:
                        try:
                            sdt = parse_iso(start_str, now)
                            edt = sdt + timedelta(milliseconds=duration_ms)
                            end_str = edt.strftime("%Y-%m-%dT%H:%M:%S%z")
                        except Exception:
                            end_str = ""

                    start_dt   = parse_iso(start_str, now) if start_str else now
                    updated_dt = parse_iso(updated_str, now) if updated_str else now

                    state_key = f"uid:{uid}" if uid is not None else f"name:{_norm(user_name)}"
                    toggl_ids_by_user.setdefault(state_key, set()).add(entry_id)

                    # Snapshot in the window (for diffs), but "existence" comes from wide index
                    prev_snap = notion_entries_by_id.get(entry_id, {}).get("snapshot") if _notion_ok() else None
                    first_seen = (entry_id not in wide_existing_eids)

                    if prev_snap:
                        prev_dur  = int(prev_snap.get("dur_ms", duration_ms))
                        prev_desc = prev_snap.get("desc", description)
                        prev_proj = prev_snap.get("project", project)
                        prev_tags = prev_snap.get("tags", list(tags))
                        prev_start_str = prev_snap.get("start_iso", start_str)
                        prev_start_dt = parse_iso(prev_start_str, now) if prev_start_str else start_dt
                        prev_updated_str = prev_snap.get("updated_iso", updated_str)
                        prev_updated = parse_iso_opt(prev_updated_str) if prev_updated_str else None
                    else:
                        prev_dur = duration_ms
                        prev_desc = description
                        prev_proj = project
                        prev_tags = list(tags)
                        prev_start_dt = start_dt
                        prev_updated = None

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

                    # --- Queue to Notion ---
                    queue_for_notion = False
                    if first_seen:
                        # backup guarantee: if we fetched it from Toggl, we create it
                        queue_for_notion = True
                    else:
                        queue_for_notion = (changed_time or changed_proj or changed_desc or changed_tags or changed_start)

                    if _notion_budget_remaining() > 0 and queue_for_notion:
                        if notion_queue_entry(
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
                            description=description,
                            deleted=False
                        ):
                            acted_rows += 1
                            if first_seen:
                                wide_existing_eids.add(entry_id)  # avoid duplicate creates in same run
                        if len(_notion_queue) >= NOTION_FLUSH_EVERY or (context and _time_budget_exhausted(context)):
                            notion_flush_queue(limit_by_budget=True, context=context)

                    # Notifications (unchanged; still optional)
                    if NOTIFY_ENABLED:
                        webhook_url = user_map.get(user_name) or user_map_norm.get(_norm(user_name))
                        should_notify = False
                        diff_lines = []
                        label = "🆕 New Time Entry Added"

                        if first_seen:
                            if updated_dt >= since:
                                should_notify = True
                        else:
                            if (updated_dt and prev_updated and updated_dt > prev_updated) and (updated_dt >= since) and queue_for_notion:
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

                        if should_notify and webhook_url:
                            lines = [
                                f"## {label}",
                                f"👤 **User**: {user_name}",
                                f"📁 **Project:** {project if not (label.startswith('✍️') and changed_proj) else f'~~{prev_proj}~~ {project}'}",
                                f"🏷️ **Tags:** " + (
                                    (", ".join(tags) if tags else "No tags") if not (label.startswith("✍️") and changed_tags)
                                    else f"~~{', '.join(sorted(old_set)) or 'No tags'}~~ {', '.join(sorted(new_set)) or 'No tags'}"
                                ),
                                f"📝 **Task:** " + (
                                    f"~~{_truncate(prev_desc)}~~ {_truncate(description)}" if (label.startswith('✍️') and changed_desc)
                                    else _truncate(description)
                                ),
                                f"⏱️ **Time Tracked:** " + (
                                    f"~~{convert_ms_to_hms(prev_dur)}~~ {convert_ms_to_hms(duration_ms)}" if (label.startswith('✍️') and changed_time)
                                    else convert_ms_to_hms(duration_ms)
                                ),
                                f"🗓️ **Start:** " + (
                                    f"~~{prev_start_dt.strftime('%Y-%m-%d %H:%M:%S')}~~ {start_dt.strftime('%Y-%m-%d %H:%M:%S')}" if (label.startswith('✍️') and changed_start)
                                    else start_dt.strftime('%Y-%m-%d %H:%M:%S')
                                ),
                                f"🕑 **Updated:** {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}",
                            ]
                            if label.startswith("✍️") and diff_lines:
                                lines.append("")
                                lines.append("**changes:** " + "; ".join(diff_lines))
                            lines.append("—")
                            outbox.setdefault(webhook_url, []).append("\n".join(lines))
                            notified += 1
                            acted_rows += 1

                if _notion_queue and _notion_budget_remaining() > 0:
                    notion_flush_queue(limit_by_budget=True, context=context)

    # ==========================
    # Deletion detection (Notion is the state) — action/time-aware
    # ==========================
    if _notion_ok() and _notion_budget_remaining() > 0 and not _time_budget_exhausted(context) and acted_rows < MAX_TOGGL_ROWS_PER_RUN:
        for eid, info in notion_entries_by_id.items():
            if _time_budget_exhausted(context) or _notion_budget_remaining() <= 0 or acted_rows >= MAX_TOGGL_ROWS_PER_RUN:
                break
            snap = info["snapshot"]
            user_key = info["user_key"] or ""
            start_iso = snap.get("start_iso")
            if not start_iso:
                continue
            start_dt = parse_iso_opt(start_iso)
            # keep same window for deletions
            if not start_dt:
                continue
            current_ids = toggl_ids_by_user.get(user_key, set())
            if eid in current_ids:
                continue  # still present
            if notion_queue_entry(
                entry_id=eid,
                user_key=user_key,
                user_display=user_key,
                project=snap.get("project") or "No project",
                tags=list(snap.get("tags") or []),
                start_iso=snap.get("start_iso"),
                end_iso=snap.get("end_iso"),
                updated_iso=snap.get("updated_iso"),
                dur_seconds=max(0, int(snap.get("dur_ms",0)) // 1000),
                workspace_id=None,
                description=snap.get("desc") or "",
                deleted=True
            ):
                acted_rows += 1

            if NOTIFY_ENABLED and acted_rows < MAX_TOGGL_ROWS_PER_RUN:
                disp = user_key.replace("name:","").replace("uid:","").strip() or "Unknown"
                msg = _format_deleted_message(disp, {
                    "project": snap.get("project"),
                    "tags": snap.get("tags"),
                    "desc": snap.get("desc"),
                    "dur_ms": snap.get("dur_ms"),
                    "start_iso": snap.get("start_iso"),
                })
                webhook_url = user_map.get(disp) or user_map_norm.get(_norm(disp))
                if webhook_url:
                    outbox.setdefault(webhook_url, []).append(msg)
                    notified += 1
                    acted_rows += 1

        if _notion_queue and _notion_budget_remaining() > 0:
            notion_flush_queue(limit_by_budget=True, context=context)

    # ==========================
    # Send Discord messages
    # ==========================
    if NOTIFY_ENABLED and outbox and not _time_budget_exhausted(context):
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
        "dry_run_no_send": DRY_RUN_NO_SEND,
        "send_notifications": SEND_NOTIFICATIONS,
        "notify_enabled": NOTIFY_ENABLED,
        "state_backend": "notion" if _notion_ok() else "none",
        "notion": {
            "budget_used": _notion_ops_done,
            "budget_cap": NOTION_MAX_UPSERTS_PER_RUN,
            "queue_left": len(_notion_queue),
        },
        "runtime": {
            "elapsed_s": round(_elapsed_s(), 3),
            "max_runtime_s": MAX_RUNTIME_S,
            "scanned_rows": scanned_rows,
            "acted_rows": acted_rows,
            "max_actions_per_run": MAX_TOGGL_ROWS_PER_RUN,
        },
    }
