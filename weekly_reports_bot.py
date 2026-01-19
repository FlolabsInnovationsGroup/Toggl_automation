#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import math
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# -----------------------------
# Session
# -----------------------------
SESSION = requests.Session()

# ==========================
# ENVIRONMENT
# ==========================
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
MAX_SEND_WORKERS = int(os.getenv("MAX_SEND_WORKERS", "8"))

# Notion
NOTION_TOKEN = os.getenv(
    "NOTION_TOKEN", "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"
).strip()
NOTION_DATABASE_ID = os.getenv(
    "NOTION_DATABASE_ID", "297a157695518075ab6cea94e60e4220"
).strip()  # Time Entries DB
NOTION_TEAM_DB_ID = os.getenv("NOTION_TEAM_DB_ID", "").strip()
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28").strip()

NH = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# Time Entries DB property names
P_USER_KEY = os.getenv("NOTION_PROP_USER_KEY", "User Key")
P_PROJECT = os.getenv("NOTION_PROP_PROJECT", "Project")
P_DATE = os.getenv("NOTION_PROP_DATE", "Date")  # date range
P_DURATION = os.getenv("NOTION_PROP_DURATION", "Duration")  # seconds (number)
P_HOURS = os.getenv("NOTION_PROP_HOURS", "Hours")  # hours (formula/number)
P_TEAM_MEMBER = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member")  # relation
P_ENTRY_ID = os.getenv("NOTION_PROP_ENTRY_ID", "Entry ID")
P_WEBHOOK = os.getenv("NOTION_PROP_WEBHOOK", "Webhook")  # rollup from Team Members.webhook
P_TIME_COMMITMENT = os.getenv(
    "NOTION_PROP_TIME_COMMITMENT", "Time Commitment"
)  # numeric rollup, hours/week

# Week timezone (IANA name preferred). Example: "Europe/Sofia", "America/New_York".
WEEK_TZ = os.getenv("WEEK_TZ", "UTC").strip()
WEEK_TZ_OFFSET_HOURS = int(os.getenv("WEEK_TZ_OFFSET_HOURS", "0"))
try:
    _commitment_alert_below_pct = float(
        os.getenv("WEEKLY_COMMITMENT_ALERT_BELOW_PCT", "60")
    )
except Exception:
    _commitment_alert_below_pct = 50.0
COMMITMENT_ALERT_BELOW_PCT = max(0.0, _commitment_alert_below_pct)
try:
    _commitment_reconsider_above_pct = float(
        os.getenv("WEEKLY_COMMITMENT_RECONSIDER_ABOVE_PCT", "25")
    )
except Exception:
    _commitment_reconsider_above_pct = 25.0
COMMITMENT_RECONSIDER_ABOVE_PCT = max(0.0, _commitment_reconsider_above_pct)
WEEKLY_QUERY_PAGE_SIZE = max(1, int(os.getenv("WEEKLY_QUERY_PAGE_SIZE", "500")))


# ==========================
# Utilities
# ==========================
def _alert(msg: str) -> None:
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
# Minimal Notion client
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
        return "".join(
            [x.get("plain_text", "") for x in (prop.get("title") or [])]
        ).strip()
    if t == "rich_text":
        return "".join(
            [x.get("plain_text", "") for x in (prop.get("rich_text") or [])]
        ).strip()
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
    """Return canonical Entry ID string from the Notion property."""
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
            txt = "".join(
                [x.get("plain_text", "") for x in (p.get("rich_text") or [])]
            ).strip()
            return txt
    except Exception:
        pass
    return ""


def _extract_relation_ids(prop: dict) -> list[str]:
    if not isinstance(prop, dict):
        return []
    if prop.get("type") == "relation":
        rel = prop.get("relation") or []
        out: list[str] = []
        for item in rel:
            pid = (item or {}).get("id")
            if pid:
                out.append(pid)
        return out
    return []


def _get_webhook_from_props(props: dict) -> str:
    """Extract webhook URL/string from the Time Entries DB properties."""
    try:
        if not isinstance(props, dict):
            return ""
        pw = props.get(P_WEBHOOK, {})
        if not isinstance(pw, dict):
            return ""
        ptype = pw.get("type")
        # Direct url
        if ptype == "url":
            return (pw.get("url") or "").strip()
        # Direct text properties
        if ptype == "rich_text":
            return "".join(
                [x.get("plain_text", "") for x in (pw.get("rich_text") or [])]
            ).strip()
        if ptype == "title":
            return "".join(
                [x.get("plain_text", "") for x in (pw.get("title") or [])]
            ).strip()
        # Rollup from Team Members.webhook
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
                        txt = "".join(
                            [x.get("plain_text", "") for x in (it.get("rich_text") or [])]
                        ).strip()
                        if txt:
                            return txt
                    if it_type == "title":
                        txt = "".join(
                            [x.get("plain_text", "") for x in (it.get("title") or [])]
                        ).strip()
                        if txt:
                            return txt
            elif rtype == "number":
                v = roll.get("number")
                return (str(v).strip() if v is not None else "")
            elif rtype == "date":
                d = roll.get("date") or {}
                return (d.get("start") or "").strip()
        return ""
    except Exception:
        return ""


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def query_time_entries_between(start_dt: datetime, end_dt: datetime) -> list[dict]:
    """Return Notion pages for the provided [start, end) UTC window."""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    pages: list[dict] = []
    cursor = None
    start_iso = _iso_utc(start_dt)
    end_iso = _iso_utc(end_dt)
    _alert(
        f"[DEBUG] Weekly window query from {start_iso} to {end_iso}, page_size={WEEKLY_QUERY_PAGE_SIZE}"
    )
    while True:
        body: dict = {
            "page_size": WEEKLY_QUERY_PAGE_SIZE,
            "filter": {
                "and": [
                    {"property": P_DATE, "date": {"on_or_after": start_iso}},
                    {"property": P_DATE, "date": {"before": end_iso}},
                ]
            },
        }
        if cursor:
            body["start_cursor"] = cursor
        r = _notion_request("POST", url, body)
        if not r or r.status_code != 200:
            txt = r.text if r is not None else "<no response>"
            _alert(
                f"[WARN] Notion query failed {r.status_code if r else 'NA'}: {txt}"
            )
            break
        data = r.json() or {}
        results = data.get("results") or []
        _alert(
            f"[DEBUG] Weekly window page fetched; cursor={cursor} results={len(results)}"
        )
        pages.extend(results)
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return pages


# ==========================
# Team Members: name resolution
# ==========================
_PAGE_TITLE_CACHE: dict[str, str] = {}
_TEAM_MEM_ID_TO_NAME: dict[str, str] = {}
_TEAM_MEM_ID_TO_COMMITMENT: dict[str, float] = {}


def notion_build_team_members_index() -> None:
    """Build a cache of Team Member page_id -> display name from the Team Members DB."""
    global _TEAM_MEM_ID_TO_NAME
    if not NOTION_TEAM_DB_ID or (
        _TEAM_MEM_ID_TO_NAME and _TEAM_MEM_ID_TO_COMMITMENT
    ):
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
            props = pg.get("properties", {}) or {}
            name = ""
            t = props.get("Name", {})
            if isinstance(t, dict) and t.get("type") == "title":
                name = "".join(
                    [x.get("plain_text", "") for x in (t.get("title") or [])]
                ).strip()
            if pid and name:
                _TEAM_MEM_ID_TO_NAME[pid] = name
            if pid and P_TIME_COMMITMENT and P_TIME_COMMITMENT in props:
                _TEAM_MEM_ID_TO_COMMITMENT[pid] = float(
                    _extract_number(props.get(P_TIME_COMMITMENT) or {}) or 0.0
                )
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")


def _user_display_name_from_props(props: dict) -> str:
    """Resolve display name strictly from Team Member relation."""
    rel_ids = (
        _extract_relation_ids(props.get(P_TEAM_MEMBER) or {}) if P_TEAM_MEMBER else []
    )
    # Try Team Members DB index if configured
    if rel_ids and NOTION_TEAM_DB_ID:
        notion_build_team_members_index()
        for rid in rel_ids:
            nm = _TEAM_MEM_ID_TO_NAME.get(rid)
            if nm:
                return nm
    # Otherwise, fetch page title for the relation id(s) directly
    if rel_ids:
        for rid in rel_ids:
            nm = _PAGE_TITLE_CACHE.get(rid)
            if not nm:
                try:
                    r = _notion_request("GET", f"https://api.notion.com/v1/pages/{rid}")
                    if r and r.status_code == 200:
                        js = r.json() or {}
                        props2 = js.get("properties") or {}
                        title_txt = ""
                        for _k, meta in props2.items():
                            if isinstance(meta, dict) and meta.get("type") == "title":
                                title_txt = "".join(
                                    [
                                        x.get("plain_text", "")
                                        for x in (meta.get("title") or [])
                                    ]
                                ).strip()
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


def _get_team_member_commitment(team_member_id: str) -> float:
    if not team_member_id:
        return 0.0
    if team_member_id in _TEAM_MEM_ID_TO_COMMITMENT:
        return _TEAM_MEM_ID_TO_COMMITMENT[team_member_id]
    if NOTION_TEAM_DB_ID:
        notion_build_team_members_index()
        if team_member_id in _TEAM_MEM_ID_TO_COMMITMENT:
            return _TEAM_MEM_ID_TO_COMMITMENT[team_member_id]
    try:
        r = _notion_request("GET", f"https://api.notion.com/v1/pages/{team_member_id}")
        if r and r.status_code == 200:
            props2 = (r.json() or {}).get("properties") or {}
            if P_TIME_COMMITMENT and P_TIME_COMMITMENT in props2:
                commitment = _extract_number(props2.get(P_TIME_COMMITMENT) or {})
            else:
                commitment = 0.0
            commitment_val = float(commitment or 0.0)
            _TEAM_MEM_ID_TO_COMMITMENT[team_member_id] = commitment_val
            return commitment_val
    except Exception:
        pass
    _TEAM_MEM_ID_TO_COMMITMENT[team_member_id] = 0.0
    return 0.0


def _get_team_member_ids_from_props(props: dict) -> list[str]:
    if not isinstance(props, dict) or not P_TEAM_MEMBER:
        return []
    return _extract_relation_ids(props.get(P_TEAM_MEMBER) or {})


def _parse_iso_opt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        s2 = s.replace("Z", "+00:00")
        # Date-only like YYYY-MM-DD
        if len(s2) == 10 and s2[4] == "-" and s2[7] == "-":
            return datetime.strptime(s2, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s2)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            return None


# ==========================
# Aggregation & formatting
# ==========================
def aggregate_week(
    pages: list[dict], week_start_local: datetime, week_end_local: datetime
) -> dict[str, dict]:
    """Aggregate totals per Team Member from Notion pages.

    Output:
      user_key (Team Member page_id) -> {
        "user_id": str,
        "username": str,
        "webhook": str,
        "time_commitment": float,
        "total_hours": float,
        "total_entries": int,
        "per_project": dict[str, float],
      }
    """
    users: dict[str, dict] = {}
    seen_ids_by_user: dict[str, set[str]] = {}
    for pg in pages or []:
        props = pg.get("properties", {}) or {}
        tm_ids = _get_team_member_ids_from_props(props)
        if not tm_ids:
            continue
        user_key = tm_ids[0]
        user_name = _user_display_name_from_props(props) or ""
        webhook = _get_webhook_from_props(props) or ""
        time_commitment = (
            _extract_number(props.get(P_TIME_COMMITMENT) or {})
            if P_TIME_COMMITMENT
            else 0.0
        )
        if time_commitment <= 0 and user_key:
            time_commitment = _get_team_member_commitment(user_key)

        # Local time window filter using Date range (include any overlap with window)
        date_prop = (props.get(P_DATE) or {}).get("date") or {}
        start_s = date_prop.get("start") or ""
        end_s = date_prop.get("end") or ""
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
        s = seen_ids_by_user.setdefault(user_key, set())
        if eid and eid in s:
            continue

        # Add only the overlapped portion
        overlap_seconds = (
            min(end_local, week_end_local) - max(start_local, week_start_local)
        ).total_seconds()
        if overlap_seconds <= 0:
            continue
        hours_val = overlap_seconds / 3600.0
        project = _extract_select_name(props.get(P_PROJECT) or {}) or "No project"

        u = users.get(user_key)
        if not u:
            u = {
                "user_id": user_key,
                "username": user_name,
                "webhook": webhook,
                "time_commitment": float(time_commitment or 0.0),
                "total_hours": 0.0,
                "total_entries": 0,
                "per_project": defaultdict(float),
            }
            users[user_key] = u
        else:
            if not u.get("username") and user_name:
                u["username"] = user_name
            if not u.get("webhook") and webhook:
                u["webhook"] = webhook
            if (u.get("time_commitment") in (None, 0.0)) and time_commitment:
                u["time_commitment"] = float(time_commitment)

        val = max(0.0, float(hours_val))
        u["total_hours"] += val
        u["total_entries"] += 1
        u["per_project"][project] += val
        if eid:
            s.add(eid)
    return users


def _format_project_breakdown(
    per_project_hours: dict[str, float], total_hours: float, max_rows: int = 12
) -> str:
    if not per_project_hours or total_hours <= 0:
        return ""
    rows = sorted(per_project_hours.items(), key=lambda kv: kv[1], reverse=True)
    lines: list[str] = []
    for i, (proj, hrs) in enumerate(rows):
        if i >= max_rows:
            hidden = rows[max_rows:]
            hidden_hours = sum(v for _, v in hidden)
            hpct = (hidden_hours / total_hours * 100.0) if total_hours > 0 else 0.0
            lines.append(
                f"- **…and {len(hidden)} more** – {format_hours_to_hm(hidden_hours)} ({hpct:.1f}%)"
            )
            break
        pct = (hrs / total_hours * 100.0) if total_hours > 0 else 0.0
        lines.append(
            f"- **{proj}** – {format_hours_to_hm(hrs)} ({pct:.1f}%)"
        )
    return "\n".join(lines)


def _commitment_alert_line(
    username: str,
    total_hours: float,
    commitment_hours: float,
    achievement_pct: float,
) -> str | None:
    if commitment_hours <= 0:
        return None
    if achievement_pct < COMMITMENT_ALERT_BELOW_PCT:
        return (
            f"> ⚠️ {username} at {achievement_pct:.1f}% of weekly commitment "
            f"({total_hours:.1f}h / {commitment_hours:.1f}h)"
        )
    over_threshold = 100.0 + COMMITMENT_RECONSIDER_ABOVE_PCT
    if (
        COMMITMENT_RECONSIDER_ABOVE_PCT > 0
        and achievement_pct >= over_threshold
    ):
        over_pct = achievement_pct - 100.0
        recommended = max(total_hours, commitment_hours)
        # Round recommendation to the nearest 0.5h to keep it tidy.
        recommended = math.ceil(recommended * 2.0) / 2.0
        return (
            f"> 💡 {username} exceeded the weekly commitment by {over_pct:.1f}% "
            f"({total_hours:.1f}h / {commitment_hours:.1f}h). "
            f"Consider raising their weekly commitment target to about {recommended:.1f}h."
        )
    return None


def build_weekly_message(
    username: str,
    week_start: datetime,
    week_end: datetime,
    total_hours: float,
    total_entries: int,
    per_project_hours: dict[str, float],
    time_commitment_hours: float | None = None,
) -> str:
    parts: list[str] = [
        "# 📊 WEEKLY TIME TRACKING REPORT",
        "=" * 40,
        f"## 👤 Team member: {username}",
        # Show through the previous day (end is start-of-today)
        f"📅 **Week:** {week_start.strftime('%B %d')} - {(week_end - timedelta(days=1)).strftime('%B %d, %Y')}",
        f"⏰ **Total Time:** {format_hours_to_hm(total_hours)} ({total_hours:.1f} hours)",
        f"📝 **Total Entries:** {total_entries}",
    ]

    commitment_lines: list[str] = []
    if time_commitment_hours and time_commitment_hours > 0:
        diff = total_hours - time_commitment_hours
        pct = (
            (total_hours / time_commitment_hours) * 100.0
            if time_commitment_hours > 0
            else 0.0
        )
        commitment_lines.append(
            f"🎯 **Weekly Commitment:** {time_commitment_hours:.1f}h"
        )
        commitment_lines.append(
            f"📊 **Variance vs commitment:** {diff:+.1f} hours ({pct:.1f}% of commitment)"
        )
        commitment_lines.append(
            f"✅ **Achievement:** {pct:.1f}% ({total_hours:.1f}h / {time_commitment_hours:.1f}h)"
        )
        alert_line = _commitment_alert_line(
            username, total_hours, time_commitment_hours, pct
        )
        if alert_line:
            commitment_lines.append("")
            commitment_lines.append(alert_line)

    parts.append("")
    parts.append(gamified_badge(total_hours))

    if total_hours > 0 and per_project_hours:
        parts.append("\n### ⛏️ Time by Project")
        parts.append(_format_project_breakdown(per_project_hours, total_hours, max_rows=12))
        if commitment_lines:
            parts.append("")

    if commitment_lines:
        parts.extend(commitment_lines)

    parts.append(
        f"\n🤖 *Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC*"
    )
    return "\n".join(parts)


# ==========================
# Gamification (kept from original)
# ==========================
def gamified_badge(total_hours: float) -> str:
    productivity = [
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
        (5, "🐣 Warm-up week"),
        (0, "💀 Bro (Barely present week)"),
    ]
    for bound, line in productivity:
        if total_hours >= bound:
            return line
    return "💀 Bro (Barely present week)"


# ==========================
# Discord sending
# ==========================
def _send_one(webhook_url: str, content: str) -> bool:
    try:
        r = SESSION.post(webhook_url, json={"content": content}, timeout=HTTP_TIMEOUT)
        return r.status_code == 204
    except Exception:
        return False


def send_reports_parallel(reports: list[dict]) -> int:
    """Send each report to its own webhook in parallel."""
    sent = 0
    futures: list = []
    with ThreadPoolExecutor(
        max_workers=min(MAX_SEND_WORKERS, max(1, len(reports)))
    ) as ex:
        for r in reports:
            uname = r.get("username")
            wh = (r.get("webhook") or "").strip()
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


def _previous_week_bounds_local(
    now_utc: datetime,
) -> tuple[datetime, datetime, datetime, datetime]:
    """Return (start_local, end_local, start_utc, end_utc) for the past week window.

    Uses local week boundaries: from the start of the previous Monday up to the
    start of the current Monday.
    """
    tz = _get_week_tz()
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_local = now_utc.astimezone(tz)
    # Align to Monday boundaries (weekday 0). Report week ends at the current Monday.
    weekday = now_local.weekday()
    this_week_monday = now_local.date() - timedelta(days=weekday)
    week_end_local = datetime.combine(
        this_week_monday, datetime.min.time(), tzinfo=tz
    )
    # Start at previous Monday
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
        return {
            "status": "error",
            "error": "Missing NOTION_TOKEN or NOTION_DATABASE_ID",
        }

    now_utc = datetime.now(timezone.utc)
    (
        week_start_local,
        week_end_local,
        week_start_utc,
        week_end_utc,
    ) = _previous_week_bounds_local(now_utc)

    pages = query_time_entries_between(week_start_utc, week_end_utc)
    agg = aggregate_week(pages, week_start_local, week_end_local)

    reports: list[dict] = []
    for data in agg.values():
        username = data.get("username") or "Unknown"
        webhook = data.get("webhook") or ""
        total_hours = float(data.get("total_hours", 0.0))
        total_entries = int(data.get("total_entries", 0))
        per_project = dict(data.get("per_project", {}) or {})
        time_commitment = float(data.get("time_commitment", 0.0) or 0.0)

        msg = build_weekly_message(
            username=username,
            week_start=week_start_local,
            week_end=week_end_local,
            total_hours=total_hours,
            total_entries=total_entries,
            per_project_hours=per_project,
            time_commitment_hours=time_commitment if time_commitment > 0 else None,
        )
        reports.append(
            {
                "username": username,
                "webhook": webhook,
                "message": msg,
            }
        )

    sent = send_reports_parallel(reports)

    return {
        "status": "complete",
        "users": len(reports),
        "sent": sent,
        "week_start": week_start_local.strftime("%Y-%m-%d"),
        "week_end": week_end_local.strftime("%Y-%m-%d"),
    }
