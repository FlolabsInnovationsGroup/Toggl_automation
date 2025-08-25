import os
import csv
import json
import base64
import requests
from io import StringIO
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict

# ==========================
# ENV / CONFIG
# ==========================
GOOGLE_SHEET_URL         = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID                 = os.getenv("TEAM_GID", "")
WORKSPACES_GID           = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK     = os.getenv("SYSTEM_ALERT_WEBHOOK", "")
HISTORY_START_DATE_STR   = os.getenv("HISTORY_START_DATE", "2015-01-01")
FILTER_WORKSPACE_IDS_RAW = os.getenv("FILTER_WORKSPACE_IDS", "").strip()
DISCORD_FALLBACK_WEBHOOK = os.getenv("DISCORD_FALLBACK_WEBHOOK", "").strip()
HTTP_TIMEOUT             = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))

# Optional wid filter
FILTER_WIDS = set()
if FILTER_WORKSPACE_IDS_RAW:
    FILTER_WIDS = {int(x.strip()) for x in FILTER_WORKSPACE_IDS_RAW.split(",") if x.strip().isdigit()}

def _alert(msg: str):
    print(f"[ALERT] {msg}")
    if SYSTEM_ALERT_WEBHOOK:
        try:
            requests.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
        except Exception:
            pass

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

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

def load_team_sheet() -> list[dict]:
    rows = _fetch_csv_rows(GOOGLE_SHEET_URL, TEAM_GID)
    if not rows:
        _alert("TEAM tab not reachable or empty")
        return []
    headers = rows[0]
    i_name   = _find_header(headers, "name", "full name", "fullname")
    i_web    = _find_header(headers, "discord_webhook_url", "webhook", "discord webhook")
    i_u_tok  = _find_header(headers, "user_api_token", "user token", "user api token")
    i_email  = _find_header(headers, "email")

    team = []
    for r in rows[1:]:
        try:
            name = r[i_name].strip() if i_name is not None and i_name < len(r) else ""
            if not name:
                continue
            item = {"name": name}
            if i_web is not None and i_web < len(r) and r[i_web].strip():
                item["discord_webhook_url"] = r[i_web].strip()
            if i_u_tok is not None and i_u_tok < len(r) and r[i_u_tok].strip():
                item["user_api_token"] = r[i_u_tok].strip()
            if i_email is not None and i_email < len(r) and r[i_email].strip():
                item["email"] = r[i_email].strip()
            team.append(item)
        except Exception:
            continue
    return team

# -----------------------------
# Toggl Core API v9 helpers (user token)
# -----------------------------

def _headers_for_user_token(user_api_token: str) -> dict:
    auth_header = base64.b64encode(f"{user_api_token}:api_token".encode()).decode()
    return {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }

def fetch_me_time_entries(user_api_token: str, start_iso: str, end_iso: str) -> list[dict]:
    """
    GET /api/v9/me/time_entries?start_date=...&end_date=...
    Returns list of TE dicts. We do not page here; Toggl v9 returns entries within the range.
    """
    url = "https://api.track.toggl.com/api/v9/me/time_entries"
    params = {"start_date": start_iso, "end_date": end_iso}
    hdrs = _headers_for_user_token(user_api_token)
    resp = requests.get(url, headers=hdrs, params=params, timeout=HTTP_TIMEOUT)
    if not resp.ok:
        return []
    return resp.json() or []

def _sum_hours(entries: list[dict]) -> float:
    total_seconds = 0
    for te in entries:
        wid = te.get("wid")
        if FILTER_WIDS and wid not in FILTER_WIDS:
            continue
        dur = te.get("duration", 0) or 0
        # Negative duration means running entry; ignore or clamp to 0
        total_seconds += max(0, int(dur))
    return round(total_seconds / 3600.0, 2)

def _month_bounds_utc(dt: datetime) -> tuple[datetime, datetime]:
    first = datetime(dt.year, dt.month, 1, 0, 0, 0, tzinfo=timezone.utc)
    # next month’s first day - 1sec
    if dt.month == 12:
        next_first = datetime(dt.year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    else:
        next_first = datetime(dt.year, dt.month + 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    last = next_first - timedelta(seconds=1)
    return first, last

def _year_bounds_utc(y: int) -> tuple[datetime, datetime]:
    start = datetime(y, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end   = datetime(y, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    return start, end

# -----------------------------
# Monthly report builder
# -----------------------------

def build_user_monthly(user_name: str, user_token: str) -> dict:
    """Return {'name', 'hours_month', 'hours_total'} using the user's token."""
    now = datetime.now(timezone.utc)
    m_start, m_end = _month_bounds_utc(now)

    # Month range (single call)
    month_entries = fetch_me_time_entries(
        user_token, m_start.strftime("%Y-%m-%dT%H:%M:%SZ"), m_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    hours_month = _sum_hours(month_entries)

    # Total history range (chunk by year for safety)
    try:
        hist_start = datetime.strptime(HISTORY_START_DATE_STR, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        hist_start = datetime(2015, 1, 1, tzinfo=timezone.utc)

    hours_total = 0.0
    y = hist_start.year
    last_year = now.year
    while y <= last_year:
        y_start, y_end = _year_bounds_utc(y)
        # clamp to hist_start / now
        start = max(y_start, hist_start)
        end   = min(y_end, now)
        if start > end:
            y += 1
            continue
        y_entries = fetch_me_time_entries(
            user_token, start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        hours_total += _sum_hours(y_entries)
        y += 1

    return {
        "name": user_name,
        "hours_month": round(hours_month, 2),
        "hours_total": round(hours_total, 2),
    }

def format_discord_message(name: str, hours_month: float, hours_total: float) -> str:
    now = datetime.now(timezone.utc)
    m_start, m_end = _month_bounds_utc(now)
    period = f"{m_start.strftime('%b %d')}–{m_end.strftime('%b %d, %Y')}"

    return "\n".join([
        "📆 **MONTHLY TIME REPORT**",
        "—" * 30,
        f"👤 **Employee:** {name}",
        f"🗓️ **Period:** {period}",
        f"⏰ **Hours this month:** {hours_month:.2f} h",
        f"📈 **Total historical hours:** {hours_total:.2f} h",
        "",
        f"🤖 *Generated on {now.strftime('%Y-%m-%d %H:%M:%S')} UTC*"
    ])

# -----------------------------
# Lambda entrypoint
# -----------------------------

def lambda_handler(event, context):
    team = load_team_sheet()
    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}

    sent = 0
    results = []

    for row in team:
        name = row.get("name", "").strip()
        user_token = row.get("user_api_token", "").strip()
        webhook = row.get("discord_webhook_url", "").strip()

        if not name:
            continue

        if not user_token:
            _alert(f"🔎 Missing `user_api_token` for '{name}' in TEAM sheet.")
            # Skip this user (per your requirement: only alert for missing token)
            continue

        try:
            summary = build_user_monthly(name, user_token)
        except Exception as e:
            _alert(f"⚠️ Error building monthly report for {name}: {e}")
            continue

        msg = format_discord_message(summary["name"], summary["hours_month"], summary["hours_total"])
        results.append(summary)

        # Send to user’s webhook or fallback (if provided)
        target_webhook = webhook or DISCORD_FALLBACK_WEBHOOK
        if not target_webhook:
            # If no webhook configured anywhere, do nothing (no alert requested).
            continue

        try:
            r = requests.post(target_webhook, json={"content": msg}, timeout=HTTP_TIMEOUT)
            if r.status_code == 204:
                sent += 1
            else:
                _alert(f"⚠️ Discord send failed for {name}: {r.status_code} {r.text}")
        except Exception as e:
            _alert(f"⚠️ Discord send exception for {name}: {e}")

    return {"status": "ok", "sent": sent, "users_processed": len(results)}
