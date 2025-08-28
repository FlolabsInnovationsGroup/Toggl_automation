import os
import json
import base64
import requests
import csv
import time
import random
from io import StringIO
from datetime import datetime, timedelta
from collections import defaultdict

"""
Toggl Report Automation (single Google Sheet, two tabs)

Google Sheet structure (two tabs only):
- TEAM tab columns (case-insensitive):
  name, discord_webhook_url, user_api_token, Weekly_Time_Commitment, Check_up, [email optional]
- WORKSPACES tab columns (case-insensitive):
  name, api_token

ENV VARS you must set:
- GOOGLE_SHEET_URL      -> the Google Sheets URL (edit/view link is fine)
- TEAM_GID              -> gid of the TEAM tab (e.g. "0")
- WORKSPACES_GID        -> gid of the WORKSPACES tab
- SYSTEM_ALERT_WEBHOOK  -> (optional) Discord webhook for admin alerts
- DISCORD_MIN_INTERVAL_MS -> (optional) min spacing per webhook send (default 500ms)
- DISCORD_MAX_RETRIES     -> (optional) max retries on 429/5xx (default 5)

Notes:
- Sheet must be accessible publicly (Anyone with the link can view) or Published to Web.
- No S3 is used. All data is pulled from the single Google Sheet.
- For security, if you prefer, you can override workspace tokens via TOKENS_JSON (env var JSON string) and leave the WORKSPACES tab blank.
"""

# ==========================
# ENVIRONMENT CONFIG (no S3)
# ==========================
GOOGLE_SHEET_URL      = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID              = os.getenv("TEAM_GID", "")
WORKSPACES_GID        = os.getenv("WORKSPACES_GID", "")
TOKENS_JSON           = os.getenv("TOKENS_JSON", "")  # optional secure override for workspace tokens
SYSTEM_ALERT_WEBHOOK  = os.getenv("SYSTEM_ALERT_WEBHOOK", "")
DISCORD_MIN_INTERVAL_MS = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_MAX_RETRIES      = int(os.getenv("DISCORD_MAX_RETRIES", "5"))
HTTP_TIMEOUT             = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))

HEADERS = {}
PROJECT_CACHE = {}

# --------------------------------------
# Alerts
# --------------------------------------

def _alert(msg: str):
    print(f"[ALERT] {msg}")
    try:
        if SYSTEM_ALERT_WEBHOOK:
            requests.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
    except Exception:
        pass

# --------------------------------------
# Helpers: Google Sheets public CSV fetch
# --------------------------------------

def _sheet_export_csv_url(sheet_url: str, gid: str) -> str:
    """Build an export CSV URL from a Google Sheets link and a gid (tab id)."""
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
            if i_email is not None and i_email < len(r) and r[i_email].strip():
                item["email"] = r[i_email].strip()
            team.append(item)
        except Exception:
            continue
    return team

# -----------------------------
# Toggl API helpers
# -----------------------------

def get_headers(api_token):
    auth_header = base64.b64encode(f"{api_token}:api_token".encode()).decode()
    global HEADERS
    HEADERS = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }


def get_workspaces():
    url = "https://api.track.toggl.com/api/v9/workspaces"
    resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
    return resp.json() if resp.ok else []


def get_workspace_projects(workspace_id):
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/projects"
    resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
    return resp.json() if resp.ok else []


def cache_workspace_projects(workspace_id):
    global PROJECT_CACHE
    for p in get_workspace_projects(workspace_id):
        PROJECT_CACHE[p['id']] = p['name']


def get_workspace_users(workspace_id):
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/users"
    resp = requests.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT)
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
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=HTTP_TIMEOUT)
    return resp.json() if resp.status_code == 200 else None

# -----------------------------
# Formatting helpers & report gen
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
        if total_hours >= 40:
            parts.append("🔥 High productivity week!")
        elif total_hours >= 30:
            parts.append("👍 Good productivity week!")
        elif total_hours >= 20:
            parts.append("⚡ Moderate productivity week")
        else:
            parts.append("📈 Room for improvement")
        parts.append("")
        parts.append(f"🤖 *Generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC*")

        discord_messages[user_id] = {
            'username': username,
            'message': "\n".join(parts),
            'total_hours': total_hours
        }
    return discord_messages

# -----------------------------
# Discord rate limiting helpers
# -----------------------------

_last_send_ts = {}  # webhook_url -> monotonic timestamp of last send


def _sleep_ms(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)


def _respect_min_interval(webhook_url: str):
    now = time.monotonic()
    last = _last_send_ts.get(webhook_url)
    if last is not None:
        elapsed_ms = int((now - last) * 1000)
        if elapsed_ms < DISCORD_MIN_INTERVAL_MS:
            _sleep_ms(DISCORD_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts[webhook_url] = time.monotonic()


def send_discord_with_backoff(webhook_url: str, content: str, timeout: int = 20) -> bool:
    """Send a Discord message honoring per-webhook pacing and 429 backoffs."""
    # Ensure a minimum spacing between messages to the same webhook
    _respect_min_interval(webhook_url)

    payload = {"content": content}
    attempt = 0
    base_backoff = 1.0  # seconds
    while attempt < DISCORD_MAX_RETRIES:
        try:
            resp = requests.post(webhook_url, json=payload, timeout=timeout)
            if resp.status_code == 204:
                return True
            if resp.status_code == 429:
                # Rate limited: honor Retry-After header or JSON
                retry_after = None
                try:
                    # Header is milliseconds in some contexts, seconds in others; check both
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after is None and resp.headers.get("X-RateLimit-Reset-After"):
                        retry_after = resp.headers.get("X-RateLimit-Reset-After")
                    if retry_after is None:
                        data = resp.json()
                        retry_after = data.get("retry_after") or data.get("Retry-After")
                except Exception:
                    pass
                try:
                    wait_s = float(retry_after) if retry_after is not None else 2.0
                except Exception:
                    wait_s = 2.0
                # Add small jitter to avoid thundering herd
                wait_s += random.uniform(0.05, 0.25)
                time.sleep(wait_s)
                attempt += 1
                continue
            if 500 <= resp.status_code < 600:
                # transient server error: exponential backoff
                wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
                time.sleep(wait_s)
                attempt += 1
                continue
            # Other non-success codes: log and stop
            print(f"[FAIL] Discord send {resp.status_code}: {resp.text}")
            return False
        except Exception as e:
            # network issue: backoff and retry
            wait_s = base_backoff * (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            if attempt >= DISCORD_MAX_RETRIES:
                print(f"[ERROR] Discord send exception: {e}")
                return False
    return False

# -----------------------------
# Main Lambda logic (single sheet)
# -----------------------------

def lambda_handler(event, context):
    workspace_tokens = load_workspace_tokens()  # from WORKSPACES tab or TOKENS_JSON
    team_cfg        = load_team_sheet()        # from TEAM tab

    if not workspace_tokens:
        return {"status": "no_tokens"}

    # Build quick lookup for user webhooks by name
    name_to_webhook = { _norm(u['name']): u.get('discord_webhook_url') for u in team_cfg if 'discord_webhook_url' in u }

    all_reports = []
    PROJECT_CACHE.clear()

    week_end = datetime.utcnow()
    week_start = week_end - timedelta(days=7)

    for token in workspace_tokens:
        get_headers(token['api_token'])
        workspaces = get_workspaces()
        for ws in workspaces:
            wid = ws['id']
            cache_workspace_projects(wid)
            data = get_detailed_report(wid, week_start, week_end)
            if not data:
                print(f"[WARN] No report data for token {token['name']} in workspace {ws['name']}")
                continue
            reports = generate_discord_reports(data, week_start, week_end)

            # Map Toggl user IDs -> fullname to match TEAM tab by name
            ws_users = get_workspace_users(wid)
            id_to_name = {u.get("id"): _norm(u.get("fullname") or u.get("name") or "") for u in ws_users}

            # Enrich and collect
            for uid, r in reports.items():
                uname_norm = id_to_name.get(uid, _norm(r['username']))
                all_reports.append({"username": r['username'], "message": r['message'], "_uname_norm": uname_norm})

    # Send to each user's webhook from TEAM tab (WITH rate-limit & retries)
    sent = 0
    for report in all_reports:
        webhook_url = name_to_webhook.get(report.get('_uname_norm') or _norm(report['username']))
        if not webhook_url:
            print(f"[WARN] No webhook for {report['username']}")
            continue
        ok = send_discord_with_backoff(webhook_url, report['message'], timeout=HTTP_TIMEOUT)
        if ok:
            print(f"[OK] Sent report for {report['username']}")
            sent += 1
        else:
            print(f"[FAIL] Could not send report for {report['username']}")

    return {"status": "complete", "sent": sent}

# -----------------------------
# Minimal tests (run locally only)
# -----------------------------

def _test_sheet_export_url():
    url = "https://docs.google.com/spreadsheets/d/AAA/edit?gid=123#gid=123"
    out = _sheet_export_csv_url(url, "123")
    assert out == "https://docs.google.com/spreadsheets/d/AAA/export?format=csv&gid=123"


def _test_find_header():
    headers = ["Name", "Discord_Webhook_URL", "Email"]
    assert _find_header(headers, "name") == 0
    assert _find_header(headers, "discord_webhook_url", "webhook") == 1
    assert _find_header(headers, "email") == 2
    assert _find_header(headers, "missing") is None


def _test_rate_limit_spacing():
    global DISCORD_MIN_INTERVAL_MS
    old = DISCORD_MIN_INTERVAL_MS
    DISCORD_MIN_INTERVAL_MS = 50
    wh = "https://discord.example/webhook/1"
    t0 = time.monotonic()
    _respect_min_interval(wh)
    _respect_min_interval(wh)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    DISCORD_MIN_INTERVAL_MS = old
    assert elapsed_ms >= 50


if __name__ == "__main__":
    # Run simple self-tests (no network calls)
    _test_sheet_export_url()
    _test_find_header()
    _test_rate_limit_spacing()
    print("All local tests passed.")
