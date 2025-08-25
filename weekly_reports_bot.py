import os
import json
import base64
import requests
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone
from collections import defaultdict

"""
Toggl Report Automation (single Google Sheet, two tabs)

Google Sheet structure (two tabs only):
- TEAM tab columns (case-insensitive):
  name, discord_webhook_url, user_api_token, Weekly_Time_Commitment, Check_up
- WORKSPACES tab columns (case-insensitive):
  name, api_token

ENV VARS you must set:
- GOOGLE_SHEET_URL      -> the Google Sheets URL (edit/view link is fine)
- TEAM_GID              -> gid of the TEAM tab (e.g. "0")
- WORKSPACES_GID        -> gid of the WORKSPACES tab
- SYSTEM_ALERT_WEBHOOK  -> (optional) Discord webhook for admin alerts

Notes:
- Sheet must be accessible publicly (Anyone with the link can view) or Published to Web.
- No S3 is used. All data is pulled from the single Google Sheet.
- For security, if you prefer, you can override workspace tokens via TOKENS_JSON (env var JSON string) and leave the WORKSPACES tab blank.
"""

# ==========================
# ENVIRONMENT CONFIG (no S3)
# ==========================
GOOGLE_SHEET_URL   = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID           = os.getenv("TEAM_GID", "")
WORKSPACES_GID     = os.getenv("WORKSPACES_GID", "")
TOKENS_JSON        = os.getenv("TOKENS_JSON", "")  # optional secure override for workspace tokens
SYSTEM_ALERT_WEBHOOK = os.getenv("SYSTEM_ALERT_WEBHOOK", "")

HEADERS = {}
PROJECT_CACHE = {}

# --------------------------------------
# Helpers: Google Sheets public CSV fetch
# --------------------------------------

def _sheet_export_csv_url(sheet_url: str, gid: str) -> str:
    """Builds an export CSV URL from a Google Sheets link and a gid (tab id)."""
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
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return list(csv.reader(StringIO(resp.text)))


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _find_header(headers: list[str], *cands) -> int | None:
    h = [x.strip().lower() for x in headers]
    for c in cands:
        if c in h:
            return h.index(c)
    return None

# -----------------------------
# Load tokens & team (from one sheet)
# -----------------------------

def _alert(msg: str):
    print(f"[ALERT] {msg}")
    try:
        if SYSTEM_ALERT_WEBHOOK:
            requests.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=8)
    except Exception:
        pass

def _format_json_list(items, indent=2):
    """Pretty-prints a list of dicts for alert messages (brief)."""
    if not items:
        return "[]"
    out = []
    for it in items:
        pairs = []
        for k, v in it.items():
            # keep only a few useful fields
            if k in ("id", "fullname", "name", "email", "is_active", "inactive"):
                pairs.append(f'{k}={v}')
        out.append("{" + ", ".join(pairs) + "}")
    return "[\n  " + (",\n  ".join(out)) + "\n]"


def load_workspace_tokens() -> list[dict]:
    # Prefer secure JSON override
    if TOKENS_JSON:
        try:
            return json.loads(TOKENS_JSON)
        except Exception as e:
            _alert(f"Invalid TOKENS_JSON: {e}")
    # Else read WORKSPACES tab
    rows = _fetch_csv_rows(GOOGLE_SHEET_URL, WORKSPACES_GID)
    if not rows:
        _alert("No workspace tokens found (WORKSPACES tab empty or not reachable)")
        return []
    headers = rows[0]
    i_name = _find_header(headers, "name")
    i_tok  = _find_header(headers, "api_token", "token", "api token")
    out = []
    for r in rows[1:]:
        try:
            name = r[i_name].strip() if i_name is not None and i_name < len(r) else ""
            tok  = r[i_tok].strip() if i_tok is not None and i_tok < len(r) else ""
            if name and tok:
                out.append({"name": name, "api_token": tok})
        except Exception:
            continue
    return out


def load_team_sheet() -> list[dict]:
    rows = _fetch_csv_rows(GOOGLE_SHEET_URL, TEAM_GID)
    if not rows:
        _alert("TEAM tab not reachable or empty")
        return []
    headers = rows[0]
    i_name   = _find_header(headers, "name", "full name", "fullname")
    i_web    = _find_header(headers, "discord_webhook_url", "webhook", "discord webhook")
    i_u_tok  = _find_header(headers, "user_api_token", "user token", "user api token")
    i_hours  = _find_header(headers, "weekly_time_commitment", "weekly commitment", "weekly hours", "hours per week", "hrs/week")
    i_check  = _find_header(headers, "check_up", "checkup", "check up")
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
            if i_hours is not None and i_hours < len(r) and r[i_hours].strip():
                try:
                    item["weekly_commitment_hours"] = float(str(r[i_hours]).replace(",", "."))
                except Exception:
                    pass
            if i_check is not None and i_check < len(r) and r[i_check].strip():
                item["check_up"] = r[i_check].strip()
            if i_email is not None and i_email < len(r) and r[i_email].strip():
                item["email"] = r[i_email].strip()
            team.append(item)
        except Exception:
            continue
    return team

# -----------------------------
# Auth & Toggl API helpers
# -----------------------------

HEADERS = {}
PROJECT_CACHE = {}

def get_headers(api_token):
    auth_header = base64.b64encode(f"{api_token}:api_token".encode()).decode()
    global HEADERS
    HEADERS = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }


def get_workspaces():
    url = "https://api.track.toggl.com/api/v9/workspaces"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    return resp.json() if resp.ok else []


def get_workspace_projects(workspace_id):
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/projects"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    return resp.json() if resp.ok else []


def cache_workspace_projects(workspace_id):
    global PROJECT_CACHE
    for p in get_workspace_projects(workspace_id):
        PROJECT_CACHE[p['id']] = p['name']


def get_workspace_users(workspace_id):
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/users"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    return resp.json() if resp.ok else []


def get_detailed_report(workspace_id, start_date, end_date):
    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries"
    payload = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "grouped": True,
        "sub_grouping": "users",
        "grouping": "projects"
    }
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    return resp.json() if resp.status_code == 200 else None

def _with_headers_for(token: str):
    """Context-like helper that temporarily switches HEADERS to use a given token."""
    class _Ctx:
        def __enter__(self_inner):
            self_inner.prev = HEADERS.copy()
            get_headers(token)
            return self_inner
        def __exit__(self_inner, exc_type, exc, tb):
            # restore previous headers
            prev = getattr(self_inner, "prev", {})
            if prev:
                # mutate global HEADERS back
                global HEADERS
                HEADERS = prev
    return _Ctx()

def get_detailed_report_for_user_with_token(workspace_id, start_date, end_date, user_api_token, user_id=None):
    """
    Same endpoint as get_detailed_report, but using the *user's* API token.
    Optionally filter by a specific user_id for safety.
    """
    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries"
    payload = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "grouped": True,
        "sub_grouping": "users",
        "grouping": "projects"
    }
    if user_id is not None:
        payload["user_ids"] = [user_id]

    with _with_headers_for(user_api_token):
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        return None

def get_v9_me_time_entries_for_user_token(workspace_id, start_date, end_date, user_api_token, username_hint=None, user_id_hint=None):
    """
    Fallback for members who cannot access Reports API v3.
    Uses GET /api/v9/me/time_entries with the user's token, filters by workspace_id,
    and returns a list in the same shape your report generator expects:
      [
        {
          'user_id': <int>,
          'username': <str>,
          'project_id': <int or None>,
          'time_entries': [ { 'seconds': <int>, 'start': <iso> }, ... ]
        },
        ...
      ]
    Grouped by project_id.
    """
    # Build the request
    url = f"https://api.track.toggl.com/api/v9/me/time_entries"
    params = {
        "start_date": start_date.strftime('%Y-%m-%dT00:00:00Z'),
        "end_date":   end_date.strftime('%Y-%m-%dT23:59:59Z')
    }

    # Temporarily switch auth to the user's token
    with _with_headers_for(user_api_token):
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if not resp.ok:
            return []

        rows = resp.json() or []
        # Filter to entries in the desired workspace and group by project
        grouped = defaultdict(list)
        for te in rows:
            if te.get("wid") != workspace_id:
                continue
            proj_id = te.get("project_id") or te.get("pid")  # pid in some responses
            # compute seconds; toggl duration can be negative if running
            dur = te.get("duration", 0) or 0
            seconds = max(0, int(dur))
            start_iso = te.get("start") or te.get("at") or ""
            grouped[proj_id].append({
                "seconds": seconds,
                "start": start_iso
            })

        # Shape into your report entries list
        out = []
        for proj_id, time_entries in grouped.items():
            out.append({
                "user_id": user_id_hint if user_id_hint is not None else 0,
                "username": username_hint or "Unknown",
                "project_id": proj_id,
                "time_entries": time_entries
            })
        return out


# -----------------------------
# Formatting helpers
# -----------------------------

def get_project_name_from_id(project_id):
    if project_id is None:
        return "Personal/No Project"
    return PROJECT_CACHE.get(project_id, f"Project {project_id}")


def parse_toggl_date(date_string):
    try:
        date_part = date_string.split('+')[0].replace('Z', '')
        return datetime.fromisoformat(date_part)
    except Exception:
        return datetime.utcnow()


def format_seconds_to_hm(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m" if hours or minutes else "0m"

# -----------------------------
# Report gamification
# -----------------------------

def gamified_badge(total_hours: float) -> str:
    """
    Returns a short, fun line based on total weekly hours.
    Steps every 5h. First: skull (lowest). Last: Japanese demon (highest).
    """
    # thresholds are inclusive lower-bounds
    steps = [
        (60, "👹 Oni mode! Inhuman productivity!"),
        (55, "🦄 Mythical productivity!"),
        (50, "🐉 Legendary productivity week!"),
        (45, "🥇 Elite productivity week!"),
        (40, "🏆 Great productivity week!"),
        (35, "🏅 Strong productivity week!"),
        (30, "🔥 High productivity week!"),
        (25, "👍 Solid productivity week!"),
        (20, "⚡ Moderate productivity week"),
        (15, "🚀 Getting traction"),
        (10, "🛠️ Building momentum"),
        (5,  "🐣 Warm‑up week"),
        (0,  "💀 Bro (Barely present week)"),
    ]
    for bound, line in steps:
        if total_hours >= bound:
            return line
    return "💀 Bro (Barely present week)"

# -----------------------------
# Report generation
# -----------------------------

def generate_discord_reports(toggl_data, week_start_date, week_end_date):
    user_reports = defaultdict(lambda: {
        'username': '',
        'total_seconds': 0,
        'total_entries': 0,
        'projects': defaultdict(int),
        'all_entries': []
    })

    for entry in toggl_data:
        user_id = entry['user_id']
        username = entry['username']
        project_id = entry['project_id']
        description = entry.get('description', 'No description')
        user_reports[user_id]['username'] = username
        for te in entry.get('time_entries', []):
            sec = te.get('seconds', 0)
            user_reports[user_id]['total_seconds'] += sec
            user_reports[user_id]['total_entries'] += 1
            user_reports[user_id]['all_entries'].append({
                'project': get_project_name_from_id(project_id),
                'description': description,
                'duration': sec,
                'start': parse_toggl_date(te.get('start', '')),
                'date': te.get('start', '')[:10]
            })

    discord_messages = {}
    for user_id, report in user_reports.items():
        username = report['username']
        total_hours = report['total_seconds'] / 3600
        parts = [
            "📊 **WEEKLY TIME TRACKING REPORT**",
            "=" * 40,
            f"👤 **Employee:** {username}",
            f"📅 **Week:** {week_start_date.strftime('%B %d')} - {week_end_date.strftime('%B %d, %Y')}",
            f"⏰ **Total Time:** {format_seconds_to_hm(report['total_seconds'])} ({total_hours:.1f} hours)",
            f"📝 **Total Entries:** {report['total_entries']}",
            ""
        ]
        
        # Gamification based on total hours
        parts.append(gamified_badge(total_hours))
            
        discord_messages[user_id] = {
            'username': username,
            'message': "\n".join(parts),
            'total_hours': total_hours
        }
    return discord_messages

# -----------------------------
# Main Lambda logic (single sheet)
# -----------------------------

def lambda_handler(event, context):
    workspace_tokens = load_workspace_tokens()   # from WORKSPACES tab or TOKENS_JSON
    team_cfg        = load_team_sheet()         # from TEAM (Toggl_users) tab

    if not workspace_tokens:
        _alert("🚨 No workspace tokens found. Check WORKSPACES tab or TOKENS_JSON.")
        return {"status": "no_tokens"}

    # Quick lookup for user webhooks & weekly commitments
    name_to_webhook = { _norm(u['name']): u.get('discord_webhook_url') for u in team_cfg if 'discord_webhook_url' in u }
    name_to_commit  = { _norm(u['name']): u.get('weekly_commitment_hours') for u in team_cfg if 'weekly_commitment_hours' in u }
    
    # Map names to user-level Toggl tokens for fallback
    name_to_user_token  = { _norm(u['name']): u.get('user_api_token') for u in team_cfg if u.get('user_api_token') }

    all_reports = []
    PROJECT_CACHE.clear()

    week_end   = datetime.now(timezone.utc)
    week_start = week_end - timedelta(days=7)

    for token in workspace_tokens:
        try:
            get_headers(token['api_token'])
        except Exception as e:
            _alert(f"🚨 Invalid Toggl API token for '{token.get('name','(unnamed)')}': {e}")
            continue

        workspaces = get_workspaces()
        if not workspaces:
            _alert(f"⚠️ No workspaces visible for token '{token.get('name')}'.")
            continue

        for ws in workspaces:
            wid = ws['id']
            wname = ws.get('name','(no-name)')
            cache_workspace_projects(wid)

            data = get_detailed_report(wid, week_start, week_end)
            if data is None:
                _alert(f"🚨 Reports API returned no data for workspace '{wname}' (ID {wid}).")
                data = []  # continue, we’ll try to backfill per-user

            # Pull workspace users (active) and see who’s missing from the report
            ws_users = get_workspace_users(wid)
            active_users = [u for u in ws_users if u.get("is_active") and not u.get("inactive")]

            # user_ids present in current dataset
            present_user_ids = set()
            for entry in (data or []):
                try:
                    present_user_ids.add(entry.get("user_id"))
                except Exception:
                    pass

            # Backfill: for each active user with no data, try their own API token
            for u in active_users:
                uid = u.get("id")
                if uid in present_user_ids:
                    continue

                uname = u.get("fullname") or u.get("name") or ""
                uname_norm = _norm(uname)

                u_token = name_to_user_token.get(uname_norm)
                if not u_token and not uname_norm in name_to_user_token:
                    # No user token available; specific alert
                    _alert(
                        "🔎 Missing user API token for active Toggl user with no weekly entries:\n"
                        f"• Workspace: '{wname}' (ID {wid})\n"
                        f"• User: {uname}\n"
                        "→ Add their `user_api_token` in the TEAM (Toggl_users) sheet to backfill their report."
                    )
                    continue

                # 1) Try Reports API v3 with the user's token (may be empty due to permissions)
                per_user = get_detailed_report_for_user_with_token(
                    wid, week_start, week_end, u_token, user_id=uid
                )
                if per_user and isinstance(per_user, list) and len(per_user) > 0:
                    data.extend(per_user)
                    continue

                # 2) Fallback: use v9/me/time_entries with the user's token and map to your shape
                fallback = get_v9_me_time_entries_for_user_token(
                    workspace_id=wid,
                    start_date=week_start,
                    end_date=week_end,
                    user_api_token=u_token,
                    username_hint=uname,
                    user_id_hint=uid
                )
                if fallback:
                    data.extend(fallback)
                else:
                    _alert(
                        "🧪 User-level backfill returned no entries either (Reports v3 empty and /me/time_entries empty):\n"
                        f"• Workspace: '{wname}' (ID {wid})\n"
                        f"• User: {uname}"
                    )


            # Now build reports with the enriched data
            reports = generate_discord_reports(data, week_start, week_end)


            # Build reports (per-user) from Toggl data
            reports = generate_discord_reports(data, week_start, week_end)

            # Pull workspace users to help alerts & name matching
            ws_users = get_workspace_users(wid)
            active_users = [u for u in ws_users if u.get("is_active") and not u.get("inactive")]
            id_to_name = {u.get("id"): _norm(u.get("fullname") or u.get("name") or "") for u in ws_users}
            
            # If there are active users but no reports at all, alert
            if not reports and active_users:
                _alert(
                    "🚨 Empty weekly report "
                    f"for workspace '{wname}' (ID {wid}) with active members:\n"
                    f"{_format_json_list(active_users)}"
                )
                
                # Alert when an active user has no entries AND no user_api_token in TEAM
                present_user_ids = set(reports.keys())  # user_ids present in the built report
                for u in active_users:
                    uid = u.get("id")
                    if uid in present_user_ids:
                        continue  # they already have data this week

                    uname = u.get("fullname") or u.get("name") or ""
                    uname_norm = _norm(uname)

                    has_token = bool(name_to_user_token.get(uname_norm))
                    if not has_token:
                        _alert(
                            "🔎 Missing user API token for active Toggl user with no weekly entries:\n"
                            f"• Workspace: '{wname}' (ID {wid})\n"
                            f"• User: {uname}\n"
                            "→ Add their `user_api_token` in the TEAM (Toggl_users) sheet to backfill their report."
                        )
            
            # Enrich message with weekly commitment + collect for sending
            for uid, r in reports.items():
                uname_norm = id_to_name.get(uid, _norm(r['username']))
                weekly = name_to_commit.get(uname_norm)
                if weekly is not None:
                    try:
                        pct = round((r['total_hours'] / weekly * 100.0), 1) if weekly > 0 else 0.0
                        r['message'] += f"\n🎯 **Weekly Commitment:** {weekly:.1f}h\n✅ **Achievement:** {pct}%"
                    except Exception:
                        # don’t fail the whole run if commit parse fails
                        r['message'] += f"\n🎯 **Weekly Commitment:** {weekly}h"
                    
                    # 🔔 Alert if below 50% of commitment (and commitment is > 0)
                    if weekly > 0 and pct < 50.0:
                        r['message'] += (
                            f"⚠️ {r['username']} at {pct}% of weekly commitment "
                            f"({r['total_hours']:.1f}h / {weekly:.1f}h)"
                        )
                    
                    r['message'] += f"\n\n🤖 *Generated on {week_end.strftime('%Y-%m-%d %H:%M:%S')} UTC*"
                    
                all_reports.append({"username": r['username'], "message": r['message']})

    # Send to the per-user Discord webhooks defined in TEAM tab
    sent = 0
    for report in all_reports:
        uname_norm = _norm(report['username'])
        webhook_url = name_to_webhook.get(uname_norm)
        if not webhook_url:
            _alert(f"⚠️ Missing Discord webhook for user: '{report['username']}' (TEAM tab).")
            continue
        try:
            resp = requests.post(webhook_url, json={"content": report['message']}, timeout=20)
            if resp.status_code == 204:
                sent += 1
            else:
                _alert(f"⚠️ Failed to send report for {report['username']} — {resp.status_code}: {resp.text}")
        except Exception as e:
            _alert(f"⚠️ Exception sending report for {report['username']}: {e}")

    return {"status": "complete", "sent": sent, "total_reports": len(all_reports)}

