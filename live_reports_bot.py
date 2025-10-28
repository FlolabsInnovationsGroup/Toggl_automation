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
from typing import Any

# In-run caches and guards to reduce duplicate Toggl calls
_WS_CACHE_BY_TOKEN: dict[str, list[dict]] = {}
_USERS_CACHE: dict[tuple[str,int], dict[int,str]] = {}
_PROJECTS_CACHE: dict[tuple[str,int], dict[int,str]] = {}
_EXHAUSTED_TOKENS: set[str] = set()

# ==========================
# Robust HTTP session (shared)
# ==========================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SESSION = requests.Session()

def _mount_retries_on_session():
    """Mount a retrying HTTPAdapter that retries on read/connect timeouts and 429/5xx."""
    total = int(os.getenv("REQUESTS_RETRY_TOTAL", "4"))
    backoff = float(os.getenv("REQUESTS_RETRY_BACKOFF", "0.5"))
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        status=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"]),
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

# --- Runtime safety caps & stage reserves ---
MAX_RUNTIME_S             = int(os.getenv("MAX_RUNTIME_S", "240"))  # soft cap when context missing
MAX_TOGGL_ROWS_PER_RUN    = int(os.getenv("MAX_TOGGL_ROWS_PER_RUN", "1000000"))
TIGHT_EXIT_SAFETY_MARGIN  = int(os.getenv("TIGHT_EXIT_SAFETY_MARGIN", "10"))
SEND_RESERVE_S            = int(os.getenv("SEND_RESERVE_S", "60"))   # only used in notify mode
COMMIT_RESERVE_S          = int(os.getenv("COMMIT_RESERVE_S", "45")) # only used in notify mode
QUIET_CREATE_RESERVE_S    = int(os.getenv("QUIET_CREATE_RESERVE_S", "25"))
FINAL_RESERVE_S           = int(os.getenv("FINAL_RESERVE_S", "10"))

# New: extra conservative scanning stop threshold & page cap to avoid timeouts
SCAN_STOP_REMAINING_S     = int(os.getenv("SCAN_STOP_REMAINING_S", "120"))  # stop scanning when <= this many seconds remain
SCAN_ROW_SOFT_CAP         = int(os.getenv("SCAN_ROW_SOFT_CAP", "5000"))     # total rows per invocation
SCAN_PAGE_SOFT_CAP        = int(os.getenv("SCAN_PAGE_SOFT_CAP", "40"))      # pages per workspace cap (defensive)

# General window / HTTP
TIME_WINDOW_DAYS    = int(os.getenv("TIME_WINDOW_DAYS", "24"))
HTTP_TIMEOUT        = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
NOTION_HTTP_TIMEOUT = int(os.getenv("NOTION_HTTP_TIMEOUT_SECONDS", "25"))
HARD_START_FILTER   = os.getenv("HARD_START_FILTER", "1") == "1"
TOGGL_REPORTS_USER_AGENT = os.getenv("TOGGL_REPORTS_USER_AGENT", "TogglAutomationBot/1.0")
TOGGL_USE_REPORTS_ONLY = os.getenv("TOGGL_USE_REPORTS_ONLY", "1").strip().lower() in ("1","true","yes","y","on")

def _env_bool(name: str, default: bool = False) -> bool:
    s = (os.getenv(name, "").strip().lower())
    if s == "" and default:
        return True
    return s in ("1", "true", "yes", "y", "on")

# Single feature flag: SEND_NOTIFICATIONS controls behavior
SEND_NOTIFICATIONS  = _env_bool("SEND_NOTIFICATIONS", True)
NOTIFY_ENABLED      = SEND_NOTIFICATIONS  # True = notify mode, False = dry-fill mode

# ===== Notion env (MANDATORY) =====
NOTION_TOKEN        = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID  = os.getenv("NOTION_DATABASE_ID", "").strip()
NOTION_TEAM_DB_ID   = os.getenv("NOTION_TEAM_DB_ID", "").strip()
# Default to slightly higher throughput while respecting Notion limits
NOTION_BATCH_SIZE   = int(os.getenv("NOTION_BATCH_SIZE", "50"))  # larger default for faster backfill
NOTION_RPS          = float(os.getenv("NOTION_RPS", "3.0"))      # Notion soft limit ~3 rps
NOTION_FLUSH_EVERY  = int(os.getenv("NOTION_FLUSH_EVERY", "100"))

# (Instrumentation only; no gating)
NOTION_MAX_UPSERTS_PER_RUN  = int(os.getenv("NOTION_MAX_UPSERTS_PER_RUN", "100000000"))
NOTION_CREATE_ADD_BLOCKS    = _env_bool("NOTION_CREATE_ADD_BLOCKS", True)
NOTION_SET_TEAM_RELATION    = _env_bool("NOTION_SET_TEAM_RELATION", True)
NOTION_MAX_RETRIES          = int(os.getenv("NOTION_MAX_RETRIES", "4"))

# Property names
NOTION_PROP_NAME         = os.getenv("NOTION_PROP_NAME", "User Key")       # title
NOTION_PROP_TEAM_MEMBER  = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")
NOTION_PROP_ENTRY_ID     = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")
NOTION_PROP_PROJECT      = os.getenv("NOTION_PROP_PROJECT", "Project")
NOTION_PROP_TAGS         = os.getenv("NOTION_PROP_TAGS", "Tags")
NOTION_PROP_START        = os.getenv("NOTION_PROP_START", "Start")
NOTION_PROP_END          = os.getenv("NOTION_PROP_END", "End")
NOTION_PROP_UPDATED      = os.getenv("NOTION_PROP_UPDATED", "Updated")
NOTION_PROP_DURATION_S   = os.getenv("NOTION_PROP_DURATION_S", "Duration")
NOTION_PROP_WORKSPACE    = os.getenv("NOTION_PROP_WORKSPACE", "Workspace")
NOTION_PROP_DESCRIPTION  = os.getenv("NOTION_PROP_DESCRIPTION", "Description")
NOTION_PROP_DELETED      = os.getenv("NOTION_PROP_DELETED", "Deleted")

# Backfill window settings (used by dry-fill mode)
HISTORY_STEP_MONTHS          = int(os.getenv("HISTORY_STEP_MONTHS", "12"))
HISTORY_MAX_EMPTY_WINDOWS    = int(os.getenv("HISTORY_MAX_EMPTY_WINDOWS", "2"))
HISTORY_MAX_YEARS            = int(os.getenv("HISTORY_MAX_YEARS", "4"))

# Discord rate limiting
DISCORD_MIN_INTERVAL_MS        = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES            = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

_START_TS = None  # set at lambda start

def _elapsed_s() -> float:
    return time.monotonic() - _START_TS if _START_TS is not None else 0.0

def _remaining_s(context) -> float:
    """Best-effort remaining seconds based on Lambda context; fallback to MAX_RUNTIME_S."""
    try:
        return max(0.0, context.get_remaining_time_in_millis() / 1000.0)
    except Exception:
        return max(0.0, MAX_RUNTIME_S - _elapsed_s())

def _time_budget_exhausted(context) -> bool:
    try:
        return context.get_remaining_time_in_millis() <= TIGHT_EXIT_SAFETY_MARGIN * 1000
    except Exception:
        return _elapsed_s() > (MAX_RUNTIME_S - TIGHT_EXIT_SAFETY_MARGIN)

def _must_leave_for_send_and_commit(context) -> bool:
    """Stop scanning when we need to reserve time to SEND + COMMIT writes (notify mode only)."""
    return _remaining_s(context) <= (SEND_RESERVE_S + COMMIT_RESERVE_S + TIGHT_EXIT_SAFETY_MARGIN)

def _must_leave_for_quiet_create(context) -> bool:
    return _remaining_s(context) <= (QUIET_CREATE_RESERVE_S + TIGHT_EXIT_SAFETY_MARGIN)

def _should_stop_scanning(context) -> bool:
    """More conservative stop to avoid timeouts even if we never notify."""
    return _remaining_s(context) <= SCAN_STOP_REMAINING_S or _time_budget_exhausted(context)

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
# Notion client (mandatory)
# ==========================
_NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def _require_notion_config():
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        _alert("Notion is misconfigured. Provide NOTION_TOKEN and NOTION_DATABASE_ID.")
        raise RuntimeError("Notion configuration missing")

_NOTION_SCHEMA = {}  # name -> type
_TEAM_MEM_INDEX: list[tuple[str, str, str]] = []
_TEAM_MEM_FASTMAP: dict[str, str] = {}
_TEAM_MEM_BEST_CACHE: dict[str, str | None] = {}

def _prop_type(name: str) -> str:
    return _NOTION_SCHEMA.get(name, "")

def _notion_backoff(attempt:int):
    if NOTION_RPS > 0:
        time.sleep(1.0 / NOTION_RPS)
    time.sleep(min(1.5, 0.25 * (2 ** attempt)) + random.uniform(0.02, 0.2))

def notion_load_schema():
    _require_notion_config()
    try:
        for attempt in range(NOTION_MAX_RETRIES):
            try:
                r = SESSION.get(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
                                headers=_NOTION_HEADERS, timeout=NOTION_HTTP_TIMEOUT)
                if r.status_code == 200:
                    break
                if r.status_code in (429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"Notion schema read failed: {r.status_code} {r.text}")
                return
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"Notion schema timeout/retry: {e}")
                _notion_backoff(attempt)
        else:
            _alert("Notion schema read exhausted retries"); return

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
            _alert(f"Notion DB missing expected properties: {missing}")
        if _prop_type(NOTION_PROP_NAME) != "title":
            _alert(f"'{NOTION_PROP_NAME}' must be a title property.")
    except Exception as e:
        _alert(f"Notion schema exception: {e}")

def _title_prop(text: str):
    return {"title": [{"type":"text","text":{"content": text or ""}}]}

def _rich_text(text: str):
    return {"rich_text": [{"type":"text","text":{"content": text or ""}}]}

def _number(n: int|float|None):
    if n is None: return {"number": None}
    return {"number": float(n)}

# ---------- Notion-safe date helpers ----------
def parse_iso(ts: str, default_now: datetime) -> datetime:
    """Parse common ISO forms; accept 'Z', '+00:00', '+0000'."""
    try:
        if not ts:
            return default_now
        ts2 = ts.strip().replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", ts2):
            ts2 = ts2[:-5] + ts2[-5:-2] + ":" + ts2[-2:]
        return datetime.fromisoformat(ts2)
    except Exception:
        try:
            ts3 = ts.replace("Z", "+0000").replace("+00:00", "+0000")
            return datetime.strptime(ts3, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return default_now

def parse_iso_opt(ts: str):
    try:
        if not ts:
            return None
        ts2 = ts.strip().replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", ts2):
            ts2 = ts2[:-5] + ts2[-5:-2] + ":" + ts2[-2:]
        return datetime.fromisoformat(ts2)
    except Exception:
        try:
            ts3 = ts.replace("Z", "+0000").replace("+00:00", "+0000")
            return datetime.strptime(ts3, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return None

def _to_notion_datetime_str(ts: str|None, default_now: datetime) -> str|None:
    """Canonicalize to RFC3339 'YYYY-MM-DDTHH:MM:SS+00:00' (UTC)."""
    if not ts:
        return None
    dt = parse_iso(ts, default_now).astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat()

def _title_from_props(props: dict, title_prop: str) -> str:
    p = props.get(title_prop, {})
    if p.get("type") == "title":
        parts = p.get("title") or []
        return "".join([t.get("plain_text", "") for t in parts]).strip()
    return ""

def _sanitize_select_value(name: str, fallback: str = "No project") -> str:
    s = (name or fallback).strip()
    s = re.sub(r"[\r\n]", " ", s)
    s = s.replace(",", " / ")
    s = re.sub(r"\s+", " ", s)
    if not s:
        s = fallback
    if len(s) > 99:
        s = s[:99]
    return s

def _sanitize_tag_value(tag: str) -> str:
    s = str(tag or "").strip()
    s = re.sub(r"[\r\n]", " ", s)
    s = s.replace(",", " / ")
    s = re.sub(r"\s+", " ", s)
    if len(s) > 99:
        s = s[:99]
    return s

def _select(name: str):
    return {"select": {"name": _sanitize_select_value(name, "No project")}}

def _multi_select(tags: list[str]|None):
    tags = [ _sanitize_tag_value(str(x)) for x in (tags or []) if str(x).strip() ]
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

# Team relation search
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
                    _alert(f"Team Members index load failed: {r.status_code} {r.text}")
                    return
                except (requests.Timeout, requests.ConnectionError) as e:
                    _alert(f"Team index timeout/retry: {e}")
                    _notion_backoff(attempt)
            else:
                _alert("Team Members index exhausted retries"); return

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
            _alert(f"Team Members index exception: {e}")
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
    now = datetime.now(timezone.utc)
    start_iso_n   = _to_notion_datetime_str(start_iso, now) if start_iso else None
    end_iso_n     = _to_notion_datetime_str(end_iso, now) if end_iso else None
    updated_iso_n = _to_notion_datetime_str(updated_iso, now) if updated_iso else None

    props = {
        NOTION_PROP_NAME:        _title_prop(user_key),
        NOTION_PROP_PROJECT:     _select(project),
        NOTION_PROP_TAGS:        _multi_select(tags),
        NOTION_PROP_START:       _date(start_iso_n),
        NOTION_PROP_END:         _date(end_iso_n),
        NOTION_PROP_UPDATED:     _date(updated_iso_n),
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
    _require_notion_config()
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
                _alert(f"Notion state query failed: {r.status_code} {r.text}")
                return results
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"Notion query timeout/retry: {e}")
                _notion_backoff(attempt)
        else:
            _alert("Notion state query exhausted retries")
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

# ---------- Query existing by Entry IDs (targeted batch) ----------
def _notion_query_existing_by_entry_ids(entry_ids: list[str], with_snapshots: bool = False) -> dict[str, Any]:
    """Query Notion for a set of Entry IDs. If with_snapshots, returns eid -> {page_id, snapshot, user_key}.
    Otherwise returns eid -> page_id.
    """
    found: dict[str, Any] = {}
    if not entry_ids:
        return found
    chunk = 25
    for i in range(0, len(entry_ids), chunk):
        batch = [str(e) for e in entry_ids[i:i+chunk]]
        ors = []
        for eid in batch:
            if str(eid).isdigit() and _prop_type(NOTION_PROP_ENTRY_ID) == "number":
                ors.append({"property": NOTION_PROP_ENTRY_ID, "number": {"equals": int(eid)}})
            else:
                ors.append({"property": NOTION_PROP_ENTRY_ID, "rich_text": {"contains": str(eid)}})
        payload = {"filter": {"or": ors}, "page_size": min(len(batch), 100)}
        try:
            for attempt in range(NOTION_MAX_RETRIES):
                try:
                    r = SESSION.post(f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                                     headers=_NOTION_HEADERS, json=payload, timeout=NOTION_HTTP_TIMEOUT)
                    if r.status_code == 200:
                        break
                    if r.status_code in (429, 500, 502, 503, 504):
                        _notion_backoff(attempt); continue
                    _alert(f"Notion bulk query failed: {r.status_code} {r.text}")
                    break
                except (requests.Timeout, requests.ConnectionError) as e:
                    _alert(f"Notion bulk query timeout/retry: {e}")
                    _notion_backoff(attempt)
            else:
                _alert("Notion bulk query exhausted retries")
                continue

            for res in (r.json() or {}).get("results", []):
                pid = res.get("id")
                props = res.get("properties", {})
                val = props.get(NOTION_PROP_ENTRY_ID, {})
                if (val or {}).get("type") == "number":
                    eid_val = str(int(val.get("number") or 0))
                else:
                    rt = (val.get("rich_text") or [])
                    eid_val = "".join([t.get("plain_text", "") for t in rt]) if rt else ""
                if not eid_val:
                    continue
                if not with_snapshots:
                    found[eid_val] = pid
                else:
                    user_key   = _prop_to_title_text(props, NOTION_PROP_NAME)
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
                    found[eid_val] = {"page_id": pid, "snapshot": snap, "user_key": user_key}
        except Exception as e:
            _alert(f"Notion bulk query exception: {e}")
    return found

# ==========================
# Creation / Update helpers
# ==========================
def _respect_rps():
    if NOTION_RPS > 0:
        time.sleep(1.0 / NOTION_RPS)

# Queue all intended upserts here (entry_id -> {properties..., description})
_notion_queue: dict[str, dict] = {}
_notion_ops_done = 0        # created + updated this invocation
_existing_eid_to_pid: dict[str, str] = {}  # seeded from window; updated on the fly

def notion_queue_entry(entry_id:str, user_key:str, user_display:str,
                       project:str, tags:list[str], start_iso:str|None, end_iso:str|None,
                       updated_iso:str|None, dur_seconds:int, workspace_id:int|None,
                       description:str|None, deleted:bool|None=False):
    props = _prop_map_for_entry(entry_id, user_key, user_display, project, tags,
                                start_iso, end_iso, updated_iso, dur_seconds, workspace_id,
                                description, deleted=deleted)
    _notion_queue[str(entry_id)] = {"properties": props, "description": description or "", "eid": entry_id}
    return True

def _notion_create_pages(items: list[dict], context=None, learn_ids: bool=False):
    """Create pages; if Notion returns 409 conflict, look up and update instead."""
    global _notion_ops_done, _existing_eid_to_pid
    created = 0
    for payload in items:
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
        resp_json = None
        for attempt in range(NOTION_MAX_RETRIES):
            try:
                r = SESSION.post("https://api.notion.com/v1/pages",
                                 headers=_NOTION_HEADERS, json=body, timeout=NOTION_HTTP_TIMEOUT)
                if r.status_code in (200, 201):
                    created += 1
                    _notion_ops_done += 1
                    ok = True
                    try:
                        resp_json = r.json()
                    except Exception:
                        resp_json = None
                    break
                if r.status_code == 409:
                    eid = str(payload.get("eid") or "")
                    if eid:
                        lookup = _notion_query_existing_by_entry_ids([eid])
                        pid = lookup.get(eid)
                        if pid:
                            _existing_eid_to_pid[eid] = pid
                            _notion_update_pages([(pid, payload["properties"])], context=context)
                            ok = True
                            break
                    _notion_backoff(attempt); continue
                if r.status_code in (429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"Notion create failed: {r.status_code} {r.text}")
                break
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"Notion create timeout/retry: {e}")
                _notion_backoff(attempt)
        if not ok:
            _alert("Create page failed after retries; continuing")
        else:
            if learn_ids and resp_json and payload.get("eid"):
                pid = resp_json.get("id")
                if pid:
                    _existing_eid_to_pid[str(payload["eid"])] = pid
        _respect_rps()
    return created

def _notion_update_pages(items: list[tuple[str, dict]], context=None):
    global _notion_ops_done
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
                    _notion_ops_done += 1
                    ok = True
                    break
                if r.status_code in (409, 429, 500, 502, 503, 504):
                    _notion_backoff(attempt); continue
                _alert(f"Notion update failed: {r.status_code} {r.text}")
                break
            except (requests.Timeout, requests.ConnectionError) as e:
                _alert(f"Notion update timeout/retry: {e}")
                _notion_backoff(attempt)
        if not ok:
            _alert(f"Update page {page_id} failed after retries; continuing")
        _respect_rps()
    return updated

def _pop_queue_items(limit: int | None) -> list[tuple[str, dict]]:
    if not _notion_queue:
        return []
    items = list(_notion_queue.items())
    if limit is not None:
        items = items[:max(0, limit)]
    for eid, _ in items:
        _notion_queue.pop(eid, None)
    return items

def notion_flush_queue(context=None):
    """Flush everything in the queue (adaptive batching)."""
    if not _notion_queue:
        return {"created": 0, "updated": 0, "skipped": 0}
    created = updated = 0

    while _notion_queue and not (context and _time_budget_exhausted(context)):
        if _remaining_s(context) <= FINAL_RESERVE_S:
            break

        take_cap = NOTION_FLUSH_EVERY
        items = _pop_queue_items(take_cap)
        if not items:
            break

        # Route via eid->pid map; fallback to one batch lookup for unknowns
        to_update, to_create = [], []
        unknown_eids = []
        for eid, payload in items:
            pid = _existing_eid_to_pid.get(eid)
            if pid:
                to_update.append((pid, payload["properties"]))
            else:
                unknown_eids.append(eid)
                to_create.append({"eid": eid, "properties": payload["properties"], "description": payload.get("description","")})

        if unknown_eids:
            lookup = _notion_query_existing_by_entry_ids(unknown_eids)
            if lookup:
                still_create = []
                for item in to_create:
                    eid = item["eid"]
                    if eid in lookup:
                        to_update.append((lookup[eid], item["properties"]))
                        _existing_eid_to_pid[eid] = lookup[eid]
                    else:
                        still_create.append(item)
                to_create = still_create

        # Updates first
        if to_update:
            for i in range(0, len(to_update), NOTION_BATCH_SIZE):
                if _remaining_s(context) <= FINAL_RESERVE_S:
                    break
                updated += _notion_update_pages(to_update[i:i+NOTION_BATCH_SIZE], context=context)

        # Creates
        if to_create and _remaining_s(context) > FINAL_RESERVE_S:
            batch = []
            for item in to_create:
                batch.append(item)
                if len(batch) >= NOTION_BATCH_SIZE:
                    created += _notion_create_pages(batch, context=context, learn_ids=True)
                    batch = []
                if _remaining_s(context) <= FINAL_RESERVE_S:
                    break
            if batch and _remaining_s(context) > FINAL_RESERVE_S:
                created += _notion_create_pages(batch, context=context, learn_ids=True)

    print(f"[INFO] Notion flush: created={created} updated={updated} total={created+updated}")
    return {"created": created, "updated": updated, "skipped": 0}

# ==========================
# Toggl API helpers (use v9, not Reports API)
# ==========================
def _headers_for_token(api_token: str) -> dict:
    return {
        "Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode(),
        "Accept": "application/json",
    }

def get_workspaces_for_token(api_token: str) -> list[dict]:
    # Avoid re-calling for tokens that already exceeded quota this run
    if api_token in _EXHAUSTED_TOKENS:
        return []
    # In-run caching to limit repeated calls across code paths
    if api_token in _WS_CACHE_BY_TOKEN:
        return _WS_CACHE_BY_TOKEN.get(api_token, [])

    url = "https://api.track.toggl.com/api/v9/workspaces"
    try:
        r = SESSION.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            data = r.json() or []
            _WS_CACHE_BY_TOKEN[api_token] = data
            return data
        # Handle quota/plan errors gracefully and avoid retry storms in-run
        if r.status_code == 402:
            _EXHAUSTED_TOKENS.add(api_token)
        _alert(f"get_workspaces_for_token: {r.status_code} {r.text}")
    except Exception as e:
        _alert(f"get_workspaces_for_token exception: {e}")
    return []

def get_workspace_users_map(api_token: str, workspace_id: int) -> dict[int, str]:
    """Map user_id -> display name for a workspace."""
    # In-run cache keyed by (token, workspace)
    cache_key = (api_token, int(workspace_id))
    if api_token in _EXHAUSTED_TOKENS:
        return _USERS_CACHE.get(cache_key, {})
    if cache_key in _USERS_CACHE:
        return _USERS_CACHE[cache_key]

    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/workspace_users"
    try:
        r = SESSION.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            out = {}
            for u in (r.json() or []):
                # Try common fields; fall back to generic
                uid = u.get("user_id") or u.get("id")
                nm = u.get("name") or u.get("fullname") or u.get("email") or f"user:{uid}"
                if uid is not None:
                    out[int(uid)] = nm
            _USERS_CACHE[cache_key] = out
            return out
        if r.status_code == 402:
            _EXHAUSTED_TOKENS.add(api_token)
        _alert(f"workspace_users fetch failed: {r.status_code} - {r.text}")
    except Exception as e:
        _alert(f"workspace_users exception: {e}")
    return {}

def get_workspace_projects_map(api_token: str, workspace_id: int) -> dict[int, str]:
    """Map project_id -> project name."""
    cache_key = (api_token, int(workspace_id))
    if api_token in _EXHAUSTED_TOKENS:
        return _PROJECTS_CACHE.get(cache_key, {})
    if cache_key in _PROJECTS_CACHE:
        return _PROJECTS_CACHE[cache_key]

    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/projects"
    try:
        r = SESSION.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            out = {}
            for p in (r.json() or []):
                pid = p.get("id")
                nm = p.get("name") or f"project:{pid}"
                if pid is not None:
                    out[int(pid)] = nm
            _PROJECTS_CACHE[cache_key] = out
            return out
        if r.status_code == 402:
            _EXHAUSTED_TOKENS.add(api_token)
        _alert(f"projects fetch failed: {r.status_code} - {r.text}")
    except Exception as e:
        _alert(f"projects exception: {e}")
    return {}

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def iter_workspace_entries_pages(api_token: str, workspace_id: int,
                                 since_dt: datetime, until_dt: datetime,
                                 per_page:int=200, context=None):
    """
    Use Toggl Reports API (v2 details) to fetch time entries for a workspace.
    Yields one list of entries per page. Stops on 402 (plan/quota), timeout guard, or page caps.
    """
    if api_token in _EXHAUSTED_TOKENS:
        return
    headers = _headers_for_token(api_token)
    url = "https://api.track.toggl.com/reports/api/v2/details"

    page = 1
    total_count = None
    page_count = 0
    # Clamp per_page to Reports API limits
    try:
        per_page = int(per_page)
    except Exception:
        per_page = 200
    if per_page <= 0 or per_page > 200:
        per_page = 200
    while True:
        # Conservative time guard
        if context and _should_stop_scanning(context):
            break
        if total_count is not None and (page - 1) * per_page >= total_count:
            break
        if page_count >= SCAN_PAGE_SOFT_CAP:
            break

        params = {
            "workspace_id": int(workspace_id),
            "since": since_dt.strftime("%Y-%m-%d"),
            "until": until_dt.strftime("%Y-%m-%d"),
            "user_agent": TOGGL_REPORTS_USER_AGENT,
            "page": page,
            "per_page": per_page,
        }
        try:
            resp = SESSION.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            code = resp.status_code
            if code == 402:
                _EXHAUSTED_TOKENS.add(api_token)
                _alert(f"details fetch failed: 402 - {resp.text}")
                break
            if code != 200:
                _alert(f"details fetch failed: {code} - {resp.text}")
                break
            body = resp.json() or {}
            entries = body.get("data") or []
            total_count = body.get("total_count") or (total_count if total_count is not None else 0)
            if not entries:
                break
            yield entries
            page_count += 1
            # per_page from response may differ; respect param used
            if len(entries) < per_page:
                break
            page += 1
        except Exception as e:
            _alert(f"details exception: {e}")
            break

# History windows
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

def _history_windows_oldest_first(end_dt: datetime):
    return list(reversed(list(_history_windows(end_dt))))

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
# Deleted-entry message (notify mode)
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
        start_dt = parse_iso(start_s, datetime.now(timezone.utc))
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
# Normalization helper (supports Reports+v9 shapes)
# ==========================
def _normalize_toggl_entry(e: dict, now: datetime) -> dict:
    entry_id    = str(e.get("id") or "")
    # Prefer explicit 'user', else resolve from uid/user_id; fallback to key-ish label
    uid         = e.get("uid") or e.get("user_id")
    user_name   = e.get("user") or (f"uid:{uid}" if uid is not None else "")
    description = e.get("description") or "No description"
    project     = e.get("project") or "No project"
    tags        = e.get("tags") or []
    # duration: Reports 'dur' in ms; v9 'duration' in seconds (possibly negative while running)
    if e.get("dur") is not None:
        duration_ms = int(e.get("dur") or 0)
    else:
        duration_ms = max(0, int(e.get("duration") or 0)) * 1000
    start_str   = e.get("start") or ""
    end_str     = e.get("end") or e.get("stop") or ""
    updated_str = e.get("updated") or e.get("at") or ""
    if not end_str and start_str and duration_ms > 0:
        try:
            sdt = parse_iso(start_str, now)
            edt = sdt + timedelta(milliseconds=duration_ms)
            end_str = edt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
        except Exception:
            end_str = ""

    # Canonicalize all datetimes to Notion format (UTC RFC3339)
    start_iso = _to_notion_datetime_str(start_str, now) if start_str else None
    end_iso   = _to_notion_datetime_str(end_str, now) if end_str else None
    updated_iso = _to_notion_datetime_str(updated_str, now) if updated_str else None

    state_key = f"uid:{uid}" if uid is not None else f"name:{_norm(user_name)}"
    return {
        "eid": entry_id,
        "user_display": user_name,
        "user_key": state_key,
        "project": project,
        "tags": list(tags) if isinstance(tags, list) else [],
        "start_iso": start_iso,
        "end_iso": end_iso,
        "updated_iso": updated_iso,
        "dur_seconds": max(0, duration_ms // 1000),
        "description": description,
    }

# ==========================
# DRY-FILL mode (no notifications): oldest-first create-only backfill
# ==========================
def _collect_all_workspaces(workspace_set: list[dict], context):
    """Return unique list of tuples (api_token, workspace_id). Avoid scanning the same workspace twice."""
    pairs = []
    seen_wids = set()
    for token_entry in workspace_set:
        if _time_budget_exhausted(context):
            break
        api_token = token_entry["api_token"]
        if api_token in _EXHAUSTED_TOKENS:
            continue
        for ws in get_workspaces_for_token(api_token):
            wid = ws.get("id")
            if wid and wid not in seen_wids:
                pairs.append((api_token, wid))
                seen_wids.add(wid)
    return pairs

def _dry_fill_db_from_oldest(team, workspace_set, context):
    """
    When NOTIFY_ENABLED is False:
    - Iterate historical windows from oldest to newest.
    - For each window, create any missing Notion pages for Toggl entries in that window.
    - No Discord notifications, no edits/deletes.
    """
    _require_notion_config()

    # Load schema + team index (for relations)
    notion_load_schema()
    notion_build_team_members_index(team_sheet=team)

    now = datetime.now(timezone.utc)
    windows = _history_windows_oldest_first(now)  # oldest -> newest
    ws_pairs = _collect_all_workspaces(workspace_set, context)

    total_created = 0
    windows_processed = 0
    skipped_windows = 0
    consecutive_empty = 0
    # Exponential jump-through for empty windows so we approach present quickly in one run
    jump_size = 1  # in window units
    idx = 0
    while idx < len(windows):
        win_start, win_end = windows[idx]
        if _time_budget_exhausted(context) or _remaining_s(context) <= FINAL_RESERVE_S:
            _alert("Time budget nearly exhausted during dry-fill; stopping.")
            break

        # Build Notion state for this window; seed eid->pid so we create only missing ones
        notion_entries_by_id, _ = _build_notion_state_index(win_start, win_end)
        existing_eids = set(notion_entries_by_id.keys())

        created_this_window = 0
        scanned_this_window = 0

        for (api_token, wid) in ws_pairs:
            if (api_token, wid) in globals().get('_TE_UNSUPPORTED', set()):
                continue
            if api_token in _EXHAUSTED_TOKENS:
                continue
            if _time_budget_exhausted(context) or _remaining_s(context) <= FINAL_RESERVE_S:
                break

            # For dry-fill we only need entries; no need to hydrate user/project names here
            for page in iter_workspace_entries_pages(api_token, wid, win_start, win_end, per_page=200, context=context):
                if _time_budget_exhausted(context) or _remaining_s(context) <= FINAL_RESERVE_S:
                    break
                for e in page:
                    if _time_budget_exhausted(context) or _remaining_s(context) <= FINAL_RESERVE_S:
                        break
                    scanned_this_window += 1
                    if scanned_this_window >= MAX_TOGGL_ROWS_PER_RUN:
                        break

                    norm = _normalize_toggl_entry(e, now)
                    eid = norm["eid"]
                    if not eid or not norm["user_display"]:
                        continue

                    if eid in existing_eids:
                        continue  # already in Notion for this window

                    # Queue create
                    notion_queue_entry(
                        entry_id=eid,
                        user_key=norm["user_key"],
                        user_display=norm["user_display"],
                        project=norm["project"],
                        tags=norm["tags"],
                        start_iso=norm["start_iso"],
                        end_iso=norm["end_iso"],
                        updated_iso=norm["updated_iso"],
                        dur_seconds=norm["dur_seconds"],
                        workspace_id=wid,
                        description=norm["description"],
                        deleted=False
                    )
                    existing_eids.add(eid)  # prevent duplicates in same run

                    if len(_notion_queue) >= NOTION_FLUSH_EVERY:
                        res = notion_flush_queue(context=context)
                        created_this_window += res.get("created", 0)
                        total_created += res.get("created", 0)

                if scanned_this_window >= MAX_TOGGL_ROWS_PER_RUN or _should_stop_scanning(context):
                    break

            if scanned_this_window >= MAX_TOGGL_ROWS_PER_RUN or _should_stop_scanning(context):
                break

        # Final flush for the window
        if _notion_queue and not _time_budget_exhausted(context) and _remaining_s(context) > FINAL_RESERVE_S:
            res = notion_flush_queue(context=context)
            created_this_window += res.get("created", 0)
            total_created += res.get("created", 0)

        if created_this_window > 0:
            windows_processed += 1
            print(f"[DRY-FILL] Window {win_start.date()}..{win_end.date()}: created {created_this_window}")
            consecutive_empty = 0
            jump_size = 1
        else:
            skipped_windows += 1
            print(f"[DRY-FILL] Window {win_start.date()}..{win_end.date()}: nothing to create")
            consecutive_empty += 1
            if consecutive_empty >= HISTORY_MAX_EMPTY_WINDOWS:
                # Skip ahead exponentially to reach present faster
                idx = min(idx + max(1, jump_size), len(windows))
                jump_size = min(jump_size * 2, 24)  # cap jumps to 24 windows
                consecutive_empty = 0
                continue

        if _remaining_s(context) <= FINAL_RESERVE_S:
            break

        idx += 1

    # Drain any remainder
    if _notion_queue and not _time_budget_exhausted(context):
        res = notion_flush_queue(context=context)
        total_created += res.get("created", 0)

    return {
        "created": total_created,
        "windows_processed": windows_processed,
        "skipped_windows": skipped_windows
    }

# ==========================
# Batch helpers for Discord (notify mode)
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
    sent_chunks = 0
    for payload in _chunk_messages(msgs):
        if send_discord_with_backoff(wh_url, payload, timeout=HTTP_TIMEOUT):
            sent_chunks += 1
    return wh_url, sent_chunks

# ==========================
# Lambda entrypoint
# ==========================
def lambda_handler(event, context):
    global _notion_ops_done, _START_TS, _existing_eid_to_pid
    _START_TS = time.monotonic()
    _notion_ops_done = 0
    _existing_eid_to_pid = {}

    # Fail fast if Notion is not configured
    _require_notion_config()

    # Load TEAM & WORKSPACES
    team          = load_team_sheet()
    workspace_set = load_workspace_tokens()

    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}
    if not workspace_set:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

    # If notifications are disabled -> DRY-FILL MODE (create-only, oldest-first), then exit
    if not NOTIFY_ENABLED:
        dry_result = _dry_fill_db_from_oldest(team, workspace_set, context)
        return {
            "status": "complete_dry_fill",
            "send_notifications": SEND_NOTIFICATIONS,
            "notion": {"budget_used": _notion_ops_done, "queue_left": len(_notion_queue)},
            "dry_fill": dry_result,
            "runtime": {
                "elapsed_s": round(_elapsed_s(), 3),
                "remaining_s": round(_remaining_s(context), 3),
                "max_runtime_s": MAX_RUNTIME_S,
            },
        }

    # ==========================
    # NOTIFY MODE: detect changes, notify, then commit writes
    # ==========================
    notion_load_schema()
    notion_build_team_members_index(team_sheet=team)

    # Map user name -> webhook
    user_map = { t["name"]: t.get("discord_webhook_url", "").strip() for t in team if t.get("discord_webhook_url") }
    user_map_norm = { _norm(k): v for k, v in user_map.items() }

    now   = datetime.now(timezone.utc)
    since = now - timedelta(days=TIME_WINDOW_DAYS)

    notified = 0
    outbox = {}  # webhook -> [message strings]
    pending_by_webhook = {}  # webhook -> [upsert_payloads]
    webhook_by_user_key = {} # "uid:123" or "name:john doe" -> webhook
    display_by_user_key = {} # user_key -> user_display (for deletions)

    # Defer expensive Notion window scan; we will look up only EIDs we see from Toggl
    notion_entries_by_id = {}

    # Counters
    scanned_rows   = 0
    acted_rows     = 0

    toggl_map: dict[str, dict] = {}  # eid -> normalized snapshot for quiet-run create
    toggl_ids_by_user: dict[str, set] = {}

    # ---- Live window: detect changes, compose notifications, and DEFER Notion writes until after notify ----
    early_send_triggered = False
    # Deduplicate workspaces across tokens to avoid re-scanning and API quota waste
    ws_pairs = _collect_all_workspaces(workspace_set, context)

    for (api_token, wid) in ws_pairs:
        if _should_stop_scanning(context) or _must_leave_for_send_and_commit(context):
            early_send_triggered = True
            break
        if api_token in _EXHAUSTED_TOKENS:
            continue

        # Hydrate maps so we can label notifications properly
        if TOGGL_USE_REPORTS_ONLY:
            # Reports details include 'user' and 'project' strings; avoid extra calls
            user_name_map = {}
            project_name_map = {}
        else:
            user_name_map = get_workspace_users_map(api_token, wid)
            project_name_map = get_workspace_projects_map(api_token, wid)

        print(f"[DEBUG] Streaming reports entries from workspace {wid}")
        for page in iter_workspace_entries_pages(api_token, wid, since, now, per_page=200, context=context):
            if _should_stop_scanning(context) or _must_leave_for_send_and_commit(context):
                early_send_triggered = True
                break
            print(f"[DEBUG] Page fetched with {len(page)} entries from workspace {wid}")
            # Preload Notion state for EIDs in this page only (fast path)
            eids_in_page = [str((e.get("id") or "")) for e in page if (e.get("id") or "")]
            # If time is getting tight, limit the unknown lookup size to keep queries bounded
            if _remaining_s(context) <= SCAN_STOP_REMAINING_S + 15:
                eids_in_page = eids_in_page[:100]
            unknown = [eid for eid in eids_in_page if eid not in _existing_eid_to_pid and eid not in notion_entries_by_id]
            if unknown:
                lookup = _notion_query_existing_by_entry_ids(unknown, with_snapshots=True)
                for eid, info in lookup.items():
                    pid = info.get("page_id")
                    if pid:
                        _existing_eid_to_pid[eid] = pid
                        notion_entries_by_id[eid] = {"page_id": pid, "snapshot": info.get("snapshot"), "user_key": info.get("user_key")}
            for entry in page:
                if _should_stop_scanning(context) or _must_leave_for_send_and_commit(context):
                    early_send_triggered = True
                    break
                scanned_rows += 1
                if scanned_rows >= SCAN_ROW_SOFT_CAP:
                    early_send_triggered = True
                    break

                # Enrich v9 record with names for normalization / notifications
                uid = entry.get("user_id") or entry.get("uid")
                pid = entry.get("project_id") or entry.get("pid")
                e = dict(entry)  # copy
                if "user" not in e:
                    if uid is not None:
                        e["uid"] = uid
                        e["user"] = user_name_map.get(int(uid), f"uid:{uid}")
                if "project" not in e:
                    if pid is not None:
                        e["pid"] = pid
                        e["project"] = project_name_map.get(int(pid), "No project")
                if "updated" not in e and "at" in e:
                    e["updated"] = e.get("at")
                if "dur" not in e and "duration" in e:
                    e["dur"] = max(0, int(e.get("duration") or 0)) * 1000

                norm = _normalize_toggl_entry(e, now)
                entry_id   = norm["eid"]
                user_name  = norm["user_display"]
                user_key   = norm["user_key"]
                if not entry_id or not user_name:
                    continue

                toggl_map[entry_id] = dict(norm)
                toggl_ids_by_user.setdefault(user_key, set()).add(entry_id)

                # Map webhook for this user_key
                wh = user_map.get(user_name) or user_map_norm.get(_norm(user_name)) or ""
                if wh:
                    webhook_by_user_key[user_key] = wh
                    display_by_user_key[user_key] = user_name

                prev_info = notion_entries_by_id.get(entry_id)
                prev_snap = prev_info.get("snapshot") if prev_info else None
                first_seen = (entry_id not in _existing_eid_to_pid)

                # Determine differences
                changed_time = changed_proj = changed_desc = changed_tags = changed_start = False
                start_dt = parse_iso(norm["start_iso"], now) if norm["start_iso"] else now
                prev_updated = None
                if prev_snap:
                    prev_dur  = int(prev_snap.get("dur_ms", norm["dur_seconds"]*1000))
                    prev_desc = prev_snap.get("desc", norm["description"])
                    prev_proj = prev_snap.get("project", norm["project"])
                    prev_tags = prev_snap.get("tags", list(norm["tags"]))
                    prev_start_str = prev_snap.get("start_iso", norm["start_iso"])
                    prev_start_dt = parse_iso(prev_start_str, now) if prev_start_str else start_dt
                    prev_updated_str = prev_snap.get("updated_iso", norm["updated_iso"])
                    prev_updated = parse_iso_opt(prev_updated_str) if prev_updated_str else None

                    changed_time  = (norm["dur_seconds"]*1000) != prev_dur
                    changed_proj  = norm["project"] != prev_proj
                    changed_desc  = norm["description"] != prev_desc
                    try:
                        old_set = set(prev_tags) if isinstance(prev_tags, list) else set()
                        new_set = set(norm["tags"]) if isinstance(norm["tags"], list) else set()
                    except Exception:
                        old_set, new_set = set(), set()
                    changed_tags  = (old_set != new_set)
                    changed_start = (start_dt != prev_start_dt)

                needs_upsert = first_seen or changed_time or changed_proj or changed_desc or changed_tags or changed_start
                if not needs_upsert:
                    continue  # no DB change & no notification

                # --- Build message & decide notification (with emojis) ---
                webhook_url = wh
                should_notify = False
                diff_lines = []
                updated_dt = parse_iso(norm["updated_iso"], now) if norm["updated_iso"] else now
                label = "🆕 New Time Entry Added"

                if first_seen:
                    should_notify = True
                    if updated_dt < since:
                        label = "🆕 Backfill Time Entry Added"
                else:
                    if (prev_updated and updated_dt > prev_updated) and (changed_time or changed_proj or changed_desc or changed_tags or changed_start):
                        should_notify = True
                        label = "✍️ Time Entry Edited"
                        if changed_time:
                            prev_dur_ms = int(prev_snap.get("dur_ms", 0))
                            diff_lines.append(f"Δ Time {_format_delta_ms(norm['dur_seconds']*1000 - prev_dur_ms)}")
                        if changed_proj:
                            diff_lines.append("Project changed")
                        if changed_desc:
                            diff_lines.append("Description changed")
                        if changed_tags:
                            old_set = set(prev_snap.get("tags") or [])
                            new_set = set(norm["tags"] or [])
                            added   = sorted(list(new_set - old_set))
                            removed = sorted(list(old_set - new_set))
                            bits = []
                            if added:   bits.append("+" + ", ".join(added))
                            if removed: bits.append("-" + ", ".join(removed))
                            diff_lines.append("Tags " + " ".join(bits) if bits else "Tags changed")
                        if changed_start:
                            prev_start_dt = parse_iso(prev_snap.get("start_iso") or "", now) if prev_snap.get("start_iso") else start_dt
                            diff_lines.append(f"Δ Start {_format_delta_datetimes(start_dt, prev_start_dt)}")

                if not webhook_url or not should_notify:
                    continue

                prev_dur_ms = int(prev_snap.get("dur_ms", norm["dur_seconds"]*1000)) if prev_snap else norm["dur_seconds"]*1000
                prev_start_dt = parse_iso(prev_snap.get("start_iso") or "", now) if (prev_snap and prev_snap.get("start_iso")) else start_dt
                prev_desc = prev_snap.get("desc") if prev_snap else norm["description"]
                prev_proj = prev_snap.get("project") if prev_snap else norm["project"]
                old_set = set(prev_snap.get("tags") or []) if prev_snap else set(norm["tags"] or [])

                is_edit = label.startswith("✍️")
                changed_proj_flag = (is_edit and changed_proj)
                changed_tags_flag = (is_edit and changed_tags)
                changed_desc_flag = (is_edit and changed_desc)
                changed_time_flag = (is_edit and changed_time)
                changed_start_flag= (is_edit and changed_start)

                lines = [
                    f"## {label}",
                    f"👤 **User**: {user_name}",
                    f"📁 **Project:** {norm['project'] if not changed_proj_flag else f'~~{prev_proj}~~ {norm['project']}'}",
                    "🏷️ **Tags:** " + (
                        (", ".join(norm["tags"]) if norm["tags"] else "No tags") if not changed_tags_flag
                        else f"~~{', '.join(sorted(old_set)) or 'No tags'}~~ {', '.join(sorted(norm['tags'])) or 'No tags'}"
                    ),
                    "📝 **Task:** " + (
                        f"~~{_truncate(prev_desc)}~~ {_truncate(norm['description'])}" if changed_desc_flag
                        else _truncate(norm["description"])
                    ),
                    "⏱️ **Time Tracked:** " + (
                        f"~~{convert_ms_to_hms(prev_dur_ms)}~~ {convert_ms_to_hms(norm['dur_seconds']*1000)}" if changed_time_flag
                        else convert_ms_to_hms(norm["dur_seconds"]*1000)
                    ),
                    "🗓️ **Start:** " + (
                        f"~~{prev_start_dt.strftime('%Y-%m-%d %H:%M:%S')}~~ {start_dt.strftime('%Y-%m-%d %H:%M:%S')}" if changed_start_flag
                        else start_dt.strftime('%Y-%m-%d %H:%M:%S')
                    ),
                    f"🕑 **Updated:** {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}",
                    "—"
                ]
                if is_edit and diff_lines:
                    lines.insert(-1, "**changes:** " + "; ".join(diff_lines))

                outbox.setdefault(wh, []).append("\n".join(lines))
                notified += 1
                acted_rows += 1

                # Stash the Notion upsert to execute ONLY AFTER the notify succeeds
                upsert_payload = dict(
                    entry_id=entry_id,
                    user_key=user_key,
                    user_display=user_name,
                    project=norm["project"],
                    tags=norm["tags"],
                    start_iso=norm["start_iso"],
                    end_iso=norm["end_iso"],
                    updated_iso=norm["updated_iso"],
                    dur_seconds=norm["dur_seconds"],
                    workspace_id=wid,
                    description=norm["description"],
                    deleted=False
                )
                pending_by_webhook.setdefault(wh, []).append(upsert_payload)

                if early_send_triggered:
                    break
            if early_send_triggered:
                break
        if early_send_triggered:
            break

    # If we're running out of time and have not queued any notifications,
    # perform the quiet-run create-only right now and exit early.
    if notified == 0 and not outbox and _must_leave_for_quiet_create(context):
        quiet_created = 0
        # Query Notion only for the EIDs we saw from Toggl
        notion_lookup = _notion_query_existing_by_entry_ids(list(toggl_map.keys()))
        notion_ids = set(notion_lookup.keys())
        toggl_ids  = set(toggl_map.keys())
        missing_ids = [eid for eid in (toggl_ids - notion_ids)]
        for eid in missing_ids:
            t = toggl_map[eid]
            notion_queue_entry(
                entry_id=eid,
                user_key=t["user_key"],
                user_display=t["user_display"],
                project=t["project"],
                tags=t["tags"],
                start_iso=t["start_iso"],
                end_iso=t["end_iso"],
                updated_iso=t["updated_iso"],
                dur_seconds=t["dur_seconds"],
                workspace_id=None,
                description=t["description"],
                deleted=False
            )
            if len(_notion_queue) >= NOTION_FLUSH_EVERY or (context and _time_budget_exhausted(context)):
                res = notion_flush_queue(context=context)
                quiet_created += res["created"]
            if _remaining_s(context) <= FINAL_RESERVE_S:
                break
        if _notion_queue and _remaining_s(context) > FINAL_RESERVE_S:
            res = notion_flush_queue(context=context)
            quiet_created += res["created"]

        return {
            "status": "complete_quiet_early",
            "send_notifications": SEND_NOTIFICATIONS,
            "notion": {"budget_used": _notion_ops_done, "queue_left": len(_notion_queue)},
            "quiet_add": {"created": quiet_created},
            "runtime": {
                "elapsed_s": round(_elapsed_s(), 3),
                "remaining_s": round(_remaining_s(context), 3),
                "max_runtime_s": MAX_RUNTIME_S,
                "scanned_rows": scanned_rows,
                "acted_rows": acted_rows,
                "max_actions_per_run": MAX_TOGGL_ROWS_PER_RUN,
            },
        }

    # ==========================
    # Deletion detection (notify mode only) — best-effort if time allows
    # ==========================
    if not _time_budget_exhausted(context) and _remaining_s(context) > (SEND_RESERVE_S // 2):
        for eid, info in notion_entries_by_id.items():
            if _time_budget_exhausted(context) or _must_leave_for_send_and_commit(context):
                break
            if eid in toggl_map:
                continue
            snap = info["snapshot"]
            user_key = info.get("user_key") or ""
            if not user_key:
                continue
            webhook_url = webhook_by_user_key.get(user_key)
            if not webhook_url:
                # Can't notify this user -> skip DB change to respect policy
                continue
            user_display = display_by_user_key.get(user_key, user_key)
            msg = _format_deleted_message(user_display, snap)
            outbox.setdefault(webhook_url, []).append(msg)
            notified += 1
            acted_rows += 1

            pending_by_webhook.setdefault(webhook_url, []).append(dict(
                entry_id=eid,
                user_key=user_key,
                user_display=user_display,
                project=snap.get("project") or "No project",
                tags=list(snap.get("tags") or []),
                start_iso=snap.get("start_iso"),
                end_iso=snap.get("end_iso"),
                updated_iso=snap.get("updated_iso"),
                dur_seconds=max(0, int(snap.get("dur_ms",0)) // 1000),
                workspace_id=None,
                description=snap.get("desc") or "",
                deleted=True
            ))

    # ==========================
    # Send Discord messages (top priority)
    # ==========================
    # ==========================
    # Send + Commit inline (per message)
    # ==========================
    committed_ops = {"created": 0, "updated": 0}
    if NOTIFY_ENABLED and not _time_budget_exhausted(context):
        for wh, msgs in outbox.items():
            payloads = pending_by_webhook.get(wh, [])
            count = min(len(msgs), len(payloads))
            for i in range(count):
                if _time_budget_exhausted(context) or _remaining_s(context) <= FINAL_RESERVE_S:
                    break
                msg = msgs[i]
                p = payloads[i]
                sent = send_discord_with_backoff(wh, msg, timeout=HTTP_TIMEOUT)
                if not sent:
                    continue
                notion_queue_entry(
                    entry_id=p["entry_id"],
                    user_key=p["user_key"],
                    user_display=p["user_display"],
                    project=p["project"],
                    tags=p["tags"],
                    start_iso=p["start_iso"],
                    end_iso=p["end_iso"],
                    updated_iso=p["updated_iso"],
                    dur_seconds=p["dur_seconds"],
                    workspace_id=p["workspace_id"],
                    description=p["description"],
                    deleted=p["deleted"]
                )
                res = notion_flush_queue(context=context)
                committed_ops["created"] += res.get("created", 0)
                committed_ops["updated"] += res.get("updated", 0)
    # ==========================
    # Quiet-run catch-up (notify mode): if zero notifications, create-only in live window
    # ==========================
    quiet_add_summary = {}
    if notified == 0 and not _time_budget_exhausted(context) and _remaining_s(context) > (FINAL_RESERVE_S + 2):
        created = 0
        # Query Notion only for the EIDs we saw from Toggl
        notion_lookup = _notion_query_existing_by_entry_ids(list(toggl_map.keys()))
        notion_ids = set(notion_lookup.keys())
        toggl_ids  = set(toggl_map.keys())
        missing_ids = [eid for eid in (toggl_ids - notion_ids)]
        for eid in missing_ids:
            t = toggl_map[eid]
            notion_queue_entry(
                entry_id=eid,
                user_key=t["user_key"],
                user_display=t["user_display"],
                project=t["project"],
                tags=t["tags"],
                start_iso=t["start_iso"],
                end_iso=t["end_iso"],
                updated_iso=t["updated_iso"],
                dur_seconds=t["dur_seconds"],
                workspace_id=None,
                description=t["description"],
                deleted=False
            )
            if len(_notion_queue) >= NOTION_FLUSH_EVERY or (context and _time_budget_exhausted(context)):
                res = notion_flush_queue(context=context)
                created += res["created"]
            if _remaining_s(context) <= FINAL_RESERVE_S:
                break
        if _notion_queue and _remaining_s(context) > FINAL_RESERVE_S:
            res = notion_flush_queue(context=context)
            created += res["created"]
        quiet_add_summary = {"created": created}

    if _notion_queue and not _time_budget_exhausted(context):
        res = notion_flush_queue(context=context)
        committed_ops["created"] += res.get("created", 0)
        committed_ops["updated"] += res.get("updated", 0)

    return {
        "status": "complete",
        "send_notifications": SEND_NOTIFICATIONS,
        "notion": {
            "budget_used": _notion_ops_done,
            "queue_left": len(_notion_queue),
            "committed_created": committed_ops["created"],
            "committed_updated": committed_ops["updated"],
        },
        "quiet_add": quiet_add_summary,
        "runtime": {
            "elapsed_s": round(_elapsed_s(), 3),
            "remaining_s": round(_remaining_s(context), 3),
            "max_runtime_s": MAX_RUNTIME_S,
            "scanned_rows": scanned_rows,
            "acted_rows": acted_rows,
            "max_actions_per_run": MAX_TOGGL_ROWS_PER_RUN,
        },
    }
