import os
import json
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import unicodedata
import re
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# -----------------------------
# Session
# -----------------------------
SESSION = requests.Session()

# ==========================
# ENVIRONMENT (minimal)
# ==========================
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
MAX_SEND_WORKERS = int(os.getenv("MAX_SEND_WORKERS", "8"))

# Google Sheet (Discord webhooks)
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL", "").strip()
TEAM_GID = os.getenv("TEAM_GID", "").strip()

# Notion
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28").strip()
NH = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# Property names (allow override via env, default to repo schema)
P_USER_KEY = os.getenv("NOTION_PROP_USER_KEY", "User Key")
P_PROJECT = os.getenv("NOTION_PROP_PROJECT", "Project")
P_DATE = os.getenv("NOTION_PROP_DATE", "Date")  # date range (start used)
P_DURATION = os.getenv("NOTION_PROP_DURATION", "Duration")  # seconds (number)
P_HOURS = os.getenv("NOTION_PROP_HOURS", "Hours")  # hours (formula/number)
P_TEAM_MEMBER = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")
NOTION_TEAM_DB_ID = os.getenv("NOTION_TEAM_DB_ID", "").strip()
P_ENTRY_ID = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")

# Week timezone (IANA name preferred). Example: "Europe/Sofia", "America/New_York".
WEEK_TZ = os.getenv("WEEK_TZ", "UTC").strip()
WEEK_TZ_OFFSET_HOURS = int(os.getenv("WEEK_TZ_OFFSET_HOURS", "0"))

# Optional Team Members database to resolve display names from relation
# (same as above, kept for clarity of purpose)


# ==========================
# Utilities
# ==========================
def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _alert(msg: str):
    print(f"[ALERT] {msg}")


def format_hours_to_hm(hours_float: float) -> str:
    try:
        h = int(hours_float)
        m = int(round((hours_float - h) * 60))
        if m == 60:
            h += 1
            m = 0
        if h or m:
            return f"{h}h {m}m"
        return "0m"
    except Exception:
        return "0m"


# ==========================
# Gamification (kept from original)
# ==========================
def gamified_badge(total_hours: float) -> str:
    steps = [
        (70, "👹 Oni mode! Inhuman productivity week!"),
        (65, "🦄 Mythical productivity!"),
        (60, "🐉 Legendary productivity week!"),
        (55, "🦖 Epic productivity week!"),
        (50, "🐜 Ant-like productivity week!"),
        (45, "🥇 Elite productivity week!"),
        (40, "🏆 Great productivity week!"),
        (35, "🏅 Strong productivity week!"),
        (30, "🔥 High productivity week!"),
        (25, "👍 Solid productivity week!"),
        (20, "⚡ Moderate productivity week"),
        (15, "🚀 Getting traction"),
        (10, "🛠️ Building momentum"),
        (5,  "🐣 Warm-up week"),
        (0,  "💀 Bro (Barely present week)"),
    ]
    for bound, line in steps:
        if total_hours >= bound:
            return line
    return "💀 Bro (Barely present week)"



# ==========================
# Notion minimal client
# ==========================
def _notion_request(method: str, url: str, body: dict | None = None):
    try:
        if method == "POST":
            return SESSION.post(url, headers=NH, json=body, timeout=HTTP_TIMEOUT)
        if method == "PATCH":
            return SESSION.patch(url, headers=NH, json=body, timeout=HTTP_TIMEOUT)
        return SESSION.get(url, headers=NH, timeout=HTTP_TIMEOUT)
    except Exception as e:
        _alert(f"[ERROR] Notion request failed: {e}")
        return None


def _extract_title_text(prop: dict) -> str:
    if not isinstance(prop, dict):
        return ""
    t = prop.get("type")
    if t == "title":
        return "".join([x.get("plain_text", "") for x in (prop.get("title") or [])]).strip()
    if t == "rich_text":
        return "".join([x.get("plain_text", "") for x in (prop.get("rich_text") or [])]).strip()
    return ""


def _extract_select_name(prop: dict) -> str:
    if not isinstance(prop, dict):
        return "No project"
    if prop.get("type") == "select":
        return (prop.get("select") or {}).get("name") or "No project"
    return "No project"


def _extract_number(prop: dict) -> float:
    if not isinstance(prop, dict):
        return 0.0
    t = prop.get("type")
    if t == "number":
        v = prop.get("number")
        try:
            return float(v or 0)
        except Exception:
            return 0.0
    if t == "formula":
        f = prop.get("formula") or {}
        if f.get("type") == "number":
            try:
                return float(f.get("number") or 0)
            except Exception:
                return 0.0
        if f.get("type") == "string":
            s = (f.get("string") or "").strip()
            try:
                return float(s.replace(",", ".")) if s else 0.0
            except Exception:
                return 0.0
    if t == "rollup":
        r = prop.get("rollup") or {}
        if r.get("type") == "number":
            try:
                return float(r.get("number") or 0)
            except Exception:
                return 0.0
    return 0.0


def _extract_entry_id(props: dict) -> str:
    """Return canonical Entry ID string from the Notion property.
    Supports number or rich_text types. Returns "" if missing.
    """
    try:
        p = props.get(P_ENTRY_ID) or {}
        t = p.get("type")
        if t == "number":
            v = p.get("number")
            if v is None:
                return ""
            try:
                return str(int(v))
            except Exception:
                return str(v)
        if t == "rich_text":
            txt = "".join([x.get("plain_text", "") for x in (p.get("rich_text") or [])]).strip()
            return txt
    except Exception:
        pass
    return ""


def _extract_relation_ids(prop: dict) -> list[str]:
    if not isinstance(prop, dict):
        return []
    if prop.get("type") == "relation":
        rel = prop.get("relation") or []
        out = []
        for item in rel:
            pid = (item or {}).get("id")
            if pid:
                out.append(pid)
        return out
    return []


def query_time_entries_between(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Return Notion pages for time entries broadly around [start, end).
    Uses a widened window to avoid Notion filter edge cases, then we filter locally.
    """
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    pages = []
    cursor = None
    # Widen by one day on both sides
    start_wide = (start_dt - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_wide = (end_dt + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    while True:
        body = {
            "page_size": 100,
            "filter": {
                "and": [
                    {"property": P_DATE, "date": {"on_or_after": start_wide}},
                    {"property": P_DATE, "date": {"before": end_wide}},
                ]
            },
        }
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request("POST", url, body)
        if not r or r.status_code != 200:
            txt = r.text if r is not None else "<no response>"
            _alert(f"[WARN] Notion query failed {r.status_code if r else 'NA'}: {txt}")
            break
        data = r.json() or {}
        pages.extend(data.get("results") or [])
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


# ==========================
# Aggregation & formatting
# ==========================
_PAGE_TITLE_CACHE: dict[str, str] = {}
_TEAM_MEM_ID_TO_NAME: dict[str, str] = {}

def notion_build_team_members_index():
    """Build a cache of Team Member page_id -> display name from the Team Members DB."""
    global _TEAM_MEM_ID_TO_NAME
    if not NOTION_TEAM_DB_ID or _TEAM_MEM_ID_TO_NAME:
        return
    url = f"https://api.notion.com/v1/databases/{NOTION_TEAM_DB_ID}/query"
    cursor = None
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request("POST", url, body)
        if not r or r.status_code != 200:
            break
        data = r.json() or {}
        for pg in data.get("results") or []:
            pid = pg.get("id")
            props = pg.get("properties", {})
            name = ""
            t = props.get("Name", {})
            if (t or {}).get("type") == "title":
                name = "".join([x.get("plain_text", "") for x in (t.get("title") or [])]).strip()
            if pid and name:
                _TEAM_MEM_ID_TO_NAME[pid] = name
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

def _user_display_name_from_props(props: dict) -> str:
    """Resolve display name strictly from Team Member relation. No fallback."""
    rel_ids = _extract_relation_ids(props.get(P_TEAM_MEMBER) or {}) if P_TEAM_MEMBER else []
    # Try Team Members DB index if configured
    if rel_ids and NOTION_TEAM_DB_ID:
        notion_build_team_members_index()
        for rid in rel_ids:
            nm = _TEAM_MEM_ID_TO_NAME.get(rid)
            if nm:
                return nm
    # Otherwise, fetch page title for the relation id(s) directly (no DB id needed)
    if rel_ids:
        for rid in rel_ids:
            nm = _PAGE_TITLE_CACHE.get(rid)
            if not nm:
                try:
                    r = _notion_request("GET", f"https://api.notion.com/v1/pages/{rid}")
                    if r and r.status_code == 200:
                        js = r.json() or {}
                        props2 = js.get("properties") or {}
                        # Find first title property dynamically
                        title_txt = ""
                        for _k, meta in props2.items():
                            if isinstance(meta, dict) and meta.get("type") == "title":
                                title_txt = "".join([x.get("plain_text", "") for x in (meta.get("title") or [])]).strip()
                                if title_txt:
                                    break
                        if title_txt:
                            _PAGE_TITLE_CACHE[rid] = title_txt
                            nm = title_txt
                except Exception:
                    nm = None
            if nm:
                return nm
    return ""
def _parse_iso_opt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        s2 = s.replace('Z', '+00:00')
        # Date-only like YYYY-MM-DD
        if len(s2) == 10 and s2[4] == '-' and s2[7] == '-':
            return datetime.strptime(s2, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s2)
    except Exception:
        try:
            return datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except Exception:
            return None


def aggregate_week(pages: list[dict], week_start_local: datetime, week_end_local: datetime) -> dict:
    """Aggregate totals (in hours) per user and per project from Notion pages."""
    users = {}
    seen_ids_by_user: dict[str, set[str]] = {}
    for pg in pages or []:
        props = pg.get("properties", {})
        user_name = _user_display_name_from_props(props)
        if not user_name:
            continue
        # Local time window filter using Date range (include any overlap with window)
        date_prop = (props.get(P_DATE) or {}).get('date') or {}
        start_s = date_prop.get('start') or ''
        end_s = date_prop.get('end') or ''
        start_dt = _parse_iso_opt(start_s)
        end_dt = _parse_iso_opt(end_s)
        if not start_dt:
            continue
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        # Compute end_dt if missing using Duration or Hours
        if end_dt is None:
            dur_seconds = _extract_number(props.get(P_DURATION) or {})
            if dur_seconds and dur_seconds > 0:
                end_dt = start_dt + timedelta(seconds=float(dur_seconds))
            else:
                hrs = _extract_number(props.get(P_HOURS) or {})
                end_dt = start_dt + timedelta(hours=float(hrs or 0))
        if end_dt is None:
            # still unknown; treat as zero-length
            end_dt = start_dt
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        # Compare in local week timezone; compute overlap
        start_local = start_dt.astimezone(week_start_local.tzinfo)
        end_local = end_dt.astimezone(week_start_local.tzinfo)
        if end_local < start_local:
            end_local = start_local
        # Skip if no overlap
        if end_local <= week_start_local or start_local >= week_end_local:
            continue
        # De-duplicate by Entry ID per user to avoid double-counting
        eid = _extract_entry_id(props)
        s = seen_ids_by_user.setdefault(user_name, set())
        if eid and eid in s:
            continue
        # Add only the overlapped portion
        overlap_seconds = (min(end_local, week_end_local) - max(start_local, week_start_local)).total_seconds()
        if overlap_seconds <= 0:
            continue
        hours_val = overlap_seconds / 3600.0
        project = _extract_select_name(props.get(P_PROJECT) or {}) or "No project"

        u = users.get(user_name)
        if not u:
            u = {"total_hours": 0.0, "total_entries": 0, "per_project": defaultdict(float)}
            users[user_name] = u
        val = max(0.0, float(hours_val))
        u["total_hours"] += val
        u["total_entries"] += 1
        u["per_project"][project] += val
        if eid:
            s.add(eid)
    return users


def _format_project_breakdown(per_project_hours: dict[str, float], total_hours: float, max_rows: int = 12) -> str:
    if not per_project_hours or total_hours <= 0:
        return ""
    rows = sorted(per_project_hours.items(), key=lambda kv: kv[1], reverse=True)
    lines = []
    for i, (proj, secs) in enumerate(rows):
        if i >= max_rows:
            hidden = rows[max_rows:]
            hhrs = sum(s for _, s in hidden)
            hpct = (hhrs / total_hours * 100.0) if total_hours > 0 else 0.0
            lines.append(f"- **…and {len(hidden)} more** — {format_hours_to_hm(hhrs)} ({hpct:.1f}%)")
            break
        pct = (secs / total_hours * 100.0) if total_hours > 0 else 0.0
        lines.append(f"- **{proj}** — {format_hours_to_hm(secs)} ({pct:.1f}%)")
    return "\n".join(lines)


def build_weekly_message(username: str, week_start: datetime, week_end: datetime, total_hours: float, total_entries: int, per_project_hours: dict[str, float]) -> str:
    parts = [
        "# 📊 WEEKLY TIME TRACKING REPORT",
        "=" * 40,
        f"## 👤 Team member: {username}",
        # Show through the previous day (end is start-of-today)
        f"📅 **Week:** {week_start.strftime('%B %d')} - {(week_end - timedelta(days=1)).strftime('%B %d, %Y')}",
        f"⏰ **Total Time:** {format_hours_to_hm(total_hours)} ({total_hours:.1f} hours)",
        f"📝 **Total Entries:** {total_entries}",
        "",
        gamified_badge(total_hours),
    ]
    if total_hours > 0 and per_project_hours:
        parts.append("\n### ⛏️ Time by Project")
        parts.append(_format_project_breakdown(per_project_hours, total_hours, max_rows=12))
    parts.append(f"\n🤖 *Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC*")
    return "\n".join(parts)


# ==========================
# Discord sending (kept behavior)
# ==========================
def _send_one(webhook_url: str, content: str) -> bool:
    try:
        r = SESSION.post(webhook_url, json={"content": content}, timeout=HTTP_TIMEOUT)
        return r.status_code == 204
    except Exception:
        return False


def send_reports_parallel(reports: list[dict], name_to_webhook: dict) -> int:
    sent = 0
    futures = []
    with ThreadPoolExecutor(max_workers=min(MAX_SEND_WORKERS, max(1, len(reports)))) as ex:
        for r in reports:
            uname = r.get("username")
            wh = name_to_webhook.get(_norm(uname))
            if not wh:
                _alert(f"⚠️ Missing Discord webhook for user: '{uname}'")
                continue
            futures.append(ex.submit(_send_one, wh, r.get("message", "")))
        for f in as_completed(futures):
            try:
                if f.result():
                    sent += 1
            except Exception as e:
                _alert(f"[ERROR] send_reports_parallel: {e}")
    return sent


# ==========================
# Google Sheet helpers (same approach as live_reports_bot)
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
    r = SESSION.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return list(csv.reader(StringIO(r.text)))


def _find_header(headers: list[str], *cands) -> int | None:
    h = [x.strip().lower() for x in headers]
    for c in cands:
        if c in h:
            return h.index(c)
    return None


def load_team_sheet() -> list[dict]:
    rows = _fetch_csv_rows(GOOGLE_SHEET_URL, TEAM_GID)
    if not rows:
        _alert("TEAM tab not reachable or empty")
        return []
    headers = rows[0]
    i_name = _find_header(headers, "name", "full name", "fullname")
    i_web = _find_header(headers, "discord_webhook_url", "webhook", "discord webhook")
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


# ==========================
# Name matching (closest sheet name)
# ==========================
_QUOTE_CHARS = "\"'`‘’‚‛“”‟‹›«»"

def _strip_accents(s: str) -> str:
    """Remove accents and transliterate a few special letters that NFKD won't decompose.
    Handles cases like Polish Ł/ł which otherwise remain unchanged.
    """
    try:
        # Targeted transliterations first
        special = {
            'Ł': 'L', 'ł': 'l',
            'Đ': 'D', 'đ': 'd',
            'Ø': 'O', 'ø': 'o',
            'Æ': 'AE', 'æ': 'ae',
            'Œ': 'OE', 'œ': 'oe',
            'Þ': 'TH', 'þ': 'th',
            'ß': 'ss',
        }
        for k, v in special.items():
            s = s.replace(k, v)
        # Then strip diacritics
        return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    except Exception:
        return s

def _norm_for_match(s: str) -> str:
    s = _strip_accents(s or "").lower()
    # Remove quoted nicknames and parentheses content
    s = re.sub(r"[\(\[].*?[\)\]]", " ", s)
    s = re.sub(rf"[{re.escape(_QUOTE_CHARS)}].*?[{re.escape(_QUOTE_CHARS)}]", " ", s)
    # Drop suffix after comma (e.g., ", ms")
    s = s.split(',', 1)[0]
    # Keep alnum and spaces only
    s = ''.join(ch if (ch.isalnum() or ch.isspace()) else ' ' for ch in s)
    return ' '.join(s.split())

def _tokenize(s: str) -> set[str]:
    return set(_norm_for_match(s).split())

def _best_sheet_name(notion_name: str, team_names: list[str]) -> str | None:
    base = _norm_for_match(notion_name)
    if not base:
        return None
    tokens_in = _tokenize(notion_name)
    best = None
    best_score = 0.0
    for cand in team_names:
        cn = _norm_for_match(cand)
        if not cn:
            continue
        t2 = _tokenize(cand)
        inter = len(tokens_in & t2)
        union = len(tokens_in | t2)
        jacc = (inter / union) if union else 0.0
        bonus = 0.0
        if cn.startswith(base) or base.startswith(cn):
            bonus += 0.2
        if base and cn and (base in cn or cn in base):
            bonus += 0.15
        score = jacc + bonus
        if score > best_score:
            best_score = score
            best = cand
    # Threshold tuned to tolerate accents/suffixes but avoid bad matches
    return best if best_score >= 0.35 else None

# ==========================
# Week window helpers
# ==========================
def _get_week_tz():
    if WEEK_TZ and ZoneInfo is not None:
        try:
            return ZoneInfo(WEEK_TZ)
        except Exception:
            pass
    # Fallback fixed offset
    try:
        return timezone(timedelta(hours=WEEK_TZ_OFFSET_HOURS))
    except Exception:
        return timezone.utc


def _previous_week_bounds_local(now_utc: datetime) -> tuple[datetime, datetime, datetime, datetime]:
    """Return (start_local, end_local, start_utc, end_utc) for the past week window.
    Uses local day boundaries: from start-of-day 7 days ago up to start-of-today.
    This matches typical Notion/Toggl "Past week" summaries at day precision.
    """
    tz = _get_week_tz()
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_local = now_utc.astimezone(tz)
    # End at start of current day
    week_end_local = datetime.combine(now_local.date(), datetime.min.time(), tzinfo=tz)
    # Start at start of day 7 days ago
    week_start_local = week_end_local - timedelta(days=7)
    # Convert to UTC
    week_start_utc = week_start_local.astimezone(timezone.utc)
    week_end_utc = week_end_local.astimezone(timezone.utc)
    return week_start_local, week_end_local, week_start_utc, week_end_utc


# ==========================
# Lambda entry
# ==========================
def lambda_handler(event, context):
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        return {"status": "error", "error": "Missing NOTION_TOKEN or NOTION_DATABASE_ID"}

    now_utc = datetime.now(timezone.utc)
    week_start_local, week_end_local, week_start_utc, week_end_utc = _previous_week_bounds_local(now_utc)

    # Load TEAM sheet for webhook resolution (same method as live_reports_bot)
    team_cfg = load_team_sheet()
    name_to_webhook = { _norm(u.get("name","")): (u.get("discord_webhook_url") or "").strip() for u in team_cfg if u.get("discord_webhook_url") }
    team_names = [u.get("name", "") for u in team_cfg if u.get("name")] 

    pages = query_time_entries_between(week_start_utc, week_end_utc)
    agg = aggregate_week(pages, week_start_local, week_end_local)

    # Combine groups that map to the same TEAM sheet name
    combined: dict[str, dict] = {}
    for notion_username, data in agg.items():
        if not notion_username:
            continue
        best_name = _best_sheet_name(notion_username, team_names)
        if not best_name:
            _alert(f"⚠️ Missing Discord webhook for user: '{notion_username}'")
            continue
        c = combined.setdefault(best_name, {"total_hours": 0.0, "total_entries": 0, "per_project": defaultdict(float)})
        c["total_hours"] += float(data.get("total_hours", 0.0))
        c["total_entries"] += int(data.get("total_entries", 0))
        for proj, hrs in (data.get("per_project", {}) or {}).items():
            c["per_project"][proj] += float(hrs)

    # Build one message per sheet user
    reports = []
    for sheet_name, data in combined.items():
        msg = build_weekly_message(
            username=sheet_name,
            week_start=week_start_local,
            week_end=week_end_local,
            total_hours=float(data.get("total_hours", 0.0)),
            total_entries=int(data.get("total_entries", 0)),
            per_project_hours=dict(data.get("per_project", {})),
        )
        reports.append({"username": sheet_name, "message": msg})

    sent = send_reports_parallel(reports, name_to_webhook)

    return {
        "status": "complete",
        "users": len(reports),
        "sent": sent,
        "week_start": week_start_local.strftime("%Y-%m-%d"),
        "week_end": week_end_local.strftime("%Y-%m-%d"),
    }
