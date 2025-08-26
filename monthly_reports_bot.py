import os, csv, json, base64, requests
from io import StringIO
from datetime import datetime, timedelta, timezone
from collections import defaultdict

"""
Monthly Toggl Reports (Lambda)

Sends one Discord message per person with:
- Hours this month
- Total hours in the last 12 months (across all selected workspaces)

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

TEAM tab columns (case-insensitive):
- name, discord_webhook_url
WORKSPACES tab columns (case-insensitive):
- name, api_token
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


def _twelve_months_ago(dt: datetime) -> datetime:
    # approx 365 days back is fine for the "last 12 months" window
    return dt - timedelta(days=365)


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
        r = requests.post(url, headers=_headers_for_token(api_token)),
        # (typo guard) ensure variable is the Response
        r = r[0]
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

    _alert(
        f"{'✅' if seconds_by_user else 'ℹ️'} v3 totals returned {len(seconds_by_user)} users for wid={workspace_id} "
        f"{start_dt.date()}→{end_dt.date()}"
    )
    return {"seconds": seconds_by_user, "names": names_by_user}


def fetch_totals_by_user(api_token: str, workspace_id: int, start_dt: datetime, end_dt: datetime) -> dict:
    """Try v3 totals, then v2 summary fallback."""
    res_v3 = fetch_totals_by_user_v3(api_token, workspace_id, start_dt, end_dt)
    if res_v3["seconds"]:
        return res_v3

    # v2 fallback
    _alert(f"↩️ Falling back to v2 summary wid={workspace_id} {start_dt.date()}→{end_dt.date()}")
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

    _alert(
        f"{'✅' if seconds_by_user else 'ℹ️'} v2 summary wid={workspace_id} users={len(seconds_by_user)} "
        f"for {start_dt.date()}→{end_dt.date()}"
    )
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
        f"📈 **Total hours in past 12 months:** {hours_total:.2f} h",
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
    m_start, m_end = _month_bounds_utc(now)
    hist_start     = _twelve_months_ago(now)

    # Accumulators by normalized name
    month_sec_by_name = defaultdict(int)
    total_sec_by_name = defaultdict(int)
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

            # Monthly totals
            month_res = fetch_totals_by_user(api_token, wid, m_start, m_end)
            for uid, secs in month_res["seconds"].items():
                name = month_res["names"].get(uid, f"User {uid}").strip()
                key  = _norm(name)
                month_sec_by_name[key] += int(secs or 0)
                if len(name) > len(display_name_for.get(key, "")):
                    display_name_for[key] = name

            # Last 12 months totals
            hist_res = fetch_totals_by_user(api_token, wid, hist_start, now)
            for uid, secs in hist_res["seconds"].items():
                name = hist_res["names"].get(uid, f"User {uid}").strip()
                key  = _norm(name)
                total_sec_by_name[key] += int(secs or 0)
                if len(name) > len(display_name_for.get(key, "")):
                    display_name_for[key] = name

    # Compose and send
    sent = 0
    processed = 0

    # Use union of keys so users with month but no total (or vice versa) still send
    all_keys = set(month_sec_by_name.keys()) | set(total_sec_by_name.keys())

    for key in sorted(all_keys):
        processed += 1
        name = display_name_for.get(key, key.title())
        month_hours = _sum_seconds_to_hours(month_sec_by_name.get(key, 0))
        total_hours = _sum_seconds_to_hours(total_sec_by_name.get(key, 0))

        # Skip completely empty users (0 & 0) to reduce noise; comment this block out if you want everyone
        if month_hours == 0 and total_hours == 0:
            continue

        msg = format_discord_message(name, month_hours, total_hours, m_start, m_end)
        webhook = name_to_webhook.get(key) or DISCORD_FALLBACK_WEBHOOK
        if not webhook:
            _alert(f"ℹ️ No webhook for '{name}' and no DISCORD_FALLBACK_WEBHOOK; skipping send.")
            continue

        try:
            resp = requests.post(webhook, json={"content": msg}, timeout=HTTP_TIMEOUT)
            if resp.status_code == 204:
                sent += 1
            else:
                _alert(f"⚠️ Discord send failed for {name}: {resp.status_code} {resp.text}")
        except Exception as e:
            _alert(f"⚠️ Discord send exception for {name}: {e}")

    return {"status": "ok", "users_processed": processed, "sent": sent}
