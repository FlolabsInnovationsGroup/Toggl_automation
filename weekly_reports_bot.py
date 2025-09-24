import os
import json
import base64
import csv
import requests
import boto3
from io import StringIO
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# Globals / Sessions
# -----------------------------
SESSION = requests.Session()

# ==========================
# ENVIRONMENT
# ==========================
GOOGLE_SHEET_URL       = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID               = os.getenv("TEAM_GID", "")
WORKSPACES_GID         = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK   = os.getenv("SYSTEM_ALERT_WEBHOOK", "")
DYNAMODB_TABLE         = os.getenv("DYNAMODB_TABLE", "")

HTTP_TIMEOUT           = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
MAX_SEND_WORKERS       = int(os.getenv("MAX_SEND_WORKERS", "8"))

# AWS clients
dynamodb = boto3.resource("dynamodb")
table    = dynamodb.Table(DYNAMODB_TABLE)

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
# Google Sheets helpers (public CSV export)
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
# DynamoDB helpers (mirror live script schema)
# ==========================
def _get_user_state(user_key: str) -> dict:
    """
    Returns a dict with keys:
      seen_entries: [ids...]
      seen_updates: {id: updated_iso}
      seen_snapshots: {id: {dur:int, desc:str, project:str, tags:list, start:str, updated:str}}
    """
    try:
        resp = table.get_item(Key={"user_name": user_key})
        item = resp.get("Item", {}) or {}
        seen_entries   = item.get("seen_entries", []) or []
        seen_updates   = item.get("seen_updates", {}) or {}
        seen_snapshots = item.get("seen_snapshots", {}) or {}
        # normalize shapes
        seen_updates   = {str(k): str(v) for k, v in seen_updates.items()}
        seen_snapshots = {str(k): v for k, v in seen_snapshots.items()}
        return {"seen_entries": seen_entries, "seen_updates": seen_updates, "seen_snapshots": seen_snapshots}
    except Exception as e:
        _alert(f"[ERROR] DynamoDB get error for {user_key}: {e}")
        return {"seen_entries": [], "seen_updates": {}, "seen_snapshots": {}}

# ==========================
# Toggl helpers (only to discover user IDs for building uid: keys)
# ==========================
def _headers_for_token(api_token: str) -> dict:
    return {"Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode()}

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

def get_workspace_users(api_token: str, workspace_id: int) -> list[dict]:
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/users"
    try:
        r = SESSION.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json() or []
        _alert(f"[ERROR] get_workspace_users({workspace_id}): {r.status_code} {r.text}")
    except Exception as e:
        _alert(f"[ERROR] get_workspace_users({workspace_id}) exception: {e}")
    return []

# ==========================
# ISO parsing / formatting
# ==========================
def _parse_iso_opt(ts: str):
    try:
        ts = ts.replace("Z", "+0000").replace("+00:00", "+0000")
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return None

def format_seconds_to_hm(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m" if hours or minutes else "0m"

# ==========================
# Gamification
# ==========================
def gamified_badge(total_hours: float) -> str:
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
        (5,  "🐣 Warm-up week"),
        (0,  "💀 Bro (Barely present week)"),
    ]
    for bound, line in steps:
        if total_hours >= bound:
            return line
    return "💀 Bro (Barely present week)"

# ==========================
# Aggregation from DynamoDB (across ALL keys; de-dupe by entry id)
# ==========================
def aggregate_week_from_state(user_display_name: str,
                              possible_keys: list[str],
                              week_start: datetime,
                              week_end: datetime):
    """
    Sum this week's time from DynamoDB snapshots across all candidate keys
    (e.g., uid:<id> and name:<normalized>). De-dupe by entry_id.
    """
    total_seconds = 0
    total_entries = 0
    per_project_seconds = defaultdict(int)
    seen_eids = set()

    for state_key in possible_keys:
        st = _get_user_state(state_key)
        snaps = st.get("seen_snapshots", {}) or {}
        for eid, snap in snaps.items():
            if eid in seen_eids:
                continue
            start_s = snap.get("start") or ""
            dur_ms  = int(snap.get("dur") or 0)
            project = (snap.get("project") or "No project").strip() or "No project"

            start_dt = _parse_iso_opt(start_s)
            if not start_dt:
                continue
            # normalize to aware UTC if missing tz
            if not start_dt.tzinfo:
                start_dt = start_dt.replace(tzinfo=timezone.utc)

            if week_start <= start_dt <= week_end:
                secs = max(0, dur_ms // 1000)
                total_seconds += secs
                total_entries += 1
                per_project_seconds[project] += secs
                seen_eids.add(eid)

    return {
        "username": user_display_name,
        "total_seconds": total_seconds,
        "total_entries": total_entries,
        "per_project_seconds": dict(per_project_seconds),
    }

# ==========================
# Report formatting
# ==========================
def _format_project_breakdown(per_project_seconds: dict[str, int],
                              total_seconds: int,
                              max_rows: int = 15) -> str:
    """
    Returns a markdown list of per-project times, sorted by time desc.
    Limits output to max_rows (adds a summary line if truncated).
    """
    if not per_project_seconds or total_seconds <= 0:
        return ""

    rows = sorted(per_project_seconds.items(), key=lambda kv: kv[1], reverse=True)
    lines = []
    shown = 0
    for proj, secs in rows:
        pct = (secs / total_seconds * 100.0) if total_seconds > 0 else 0.0
        lines.append(f"- **{proj}** — {format_seconds_to_hm(secs)} ({pct:.1f}%)")
        shown += 1
        if shown >= max_rows:
            break

    if len(rows) > max_rows:
        hidden = rows[max_rows:]
        hidden_secs = sum(s for _, s in hidden)
        hidden_pct = (hidden_secs / total_seconds * 100.0) if total_seconds > 0 else 0.0
        lines.append(f"- **…and {len(hidden)} more** — {format_seconds_to_hm(hidden_secs)} ({hidden_pct:.1f}%)")

    return "\n".join(lines)

def build_weekly_message(username: str,
                         week_start: datetime,
                         week_end: datetime,
                         total_seconds: int,
                         total_entries: int,
                         per_project_seconds: dict[str, int] | None,
                         weekly_commitment_hours: float | None) -> str:
    total_hours = total_seconds / 3600.0
    parts = [
        "# 📊 WEEKLY TIME TRACKING REPORT",
        "=" * 40,
        f"## 👤 Team member: {username}",
        f"📅 **Week:** {week_start.strftime('%B %d')} - {week_end.strftime('%B %d, %Y')}",
        f"⏰ **Total Time:** {format_seconds_to_hm(total_seconds)} ({total_hours:.1f} hours)",
        f"📝 **Total Entries:** {total_entries}",
        "",
        gamified_badge(total_hours)
    ]

    # Per-project breakdown (only if we have time)
    if total_seconds > 0 and per_project_seconds:
        parts.append("\n### ⛏️ Time by Project")
        parts.append(_format_project_breakdown(per_project_seconds, total_seconds, max_rows=12))

    if weekly_commitment_hours is not None:
        try:
            pct = round((total_hours / weekly_commitment_hours * 100.0), 1) if weekly_commitment_hours > 0 else 0.0
        except Exception:
            pct = 0.0
        parts.append(f"\n🎯 **Weekly Commitment:** {weekly_commitment_hours:.1f}h")
        parts.append(f"✅ **Achievement:** {pct}%")
        if weekly_commitment_hours > 0 and pct < 50.0:
            parts.append(
                f"\n> ⚠️ {username} at {pct}% of weekly commitment "
                f"({total_hours:.1f}h / {weekly_commitment_hours:.1f}h)"
            )

    parts.append(f"\n🤖 *Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC*")
    return "\n".join(parts)

# ==========================
# Discord sending (parallel)
# ==========================
def _send_one(webhook_url: str, content: str) -> bool:
    try:
        resp = SESSION.post(webhook_url, json={"content": content}, timeout=HTTP_TIMEOUT)
        return resp.status_code == 204
    except Exception:
        return False

def send_reports_parallel(reports: list[dict], name_to_webhook: dict) -> int:
    """
    reports: [{"username": str, "message": str}]
    name_to_webhook: normalized name -> webhook
    """
    futures = []
    sent = 0
    with ThreadPoolExecutor(max_workers=min(MAX_SEND_WORKERS, len(reports) or 1)) as ex:
        for r in reports:
            uname = r["username"]
            wh = name_to_webhook.get(_norm(uname))
            if not wh:
                _alert(f"⚠️ Missing Discord webhook for user: '{uname}' (TEAM tab).")
                continue
            futures.append(ex.submit(_send_one, wh, r["message"]))
        for f in as_completed(futures):
            try:
                if f.result():
                    sent += 1
            except Exception as e:
                _alert(f"[ERROR] send_reports_parallel: {e}")
    return sent

# ==========================
# Main Lambda
# ==========================
def lambda_handler(event, context):
    team_cfg         = load_team_sheet()
    workspace_tokens = load_workspace_tokens()

    if not team_cfg:
        _alert("🚨 TEAM tab is empty or unreachable.")
        return {"status": "no_team"}

    # Build lookup maps from TEAM sheet
    name_to_webhook = { _norm(u['name']): u.get('discord_webhook_url') for u in team_cfg if u.get('discord_webhook_url') }
    name_to_commit  = { _norm(u['name']): u.get('weekly_commitment_hours') for u in team_cfg if u.get('weekly_commitment_hours') is not None }
    name_to_email   = { _norm(u['name']): (u.get('email') or "").strip().lower() for u in team_cfg }

    # Candidate state keys per team member:
    # Always include name:<normalized>. We will try to enrich with uid:<id> discovered from Toggl.
    name_to_state_keys = defaultdict(list)
    for u in team_cfg:
        nm = _norm(u["name"])
        k = f"name:{nm}"
        name_to_state_keys[nm].append(k)

    # Discover uid:<id> keys by scanning workspace users from provided tokens
    for tok in workspace_tokens:
        api_token = tok.get("api_token")
        if not api_token:
            continue
        workspaces = get_workspaces_for_token(api_token)
        for ws in workspaces or []:
            wid = ws.get("id")
            if not wid:
                continue
            users = get_workspace_users(api_token, wid)
            for usr in users or []:
                fullname = usr.get("fullname") or usr.get("name") or ""
                nm_ws    = _norm(fullname)
                email_ws = (usr.get("email") or "").strip().lower()
                uid      = usr.get("id")
                if uid is None:
                    continue
                uid_key = f"uid:{uid}"

                # Match by normalized name
                if nm_ws:
                    if nm_ws in name_to_state_keys and uid_key not in name_to_state_keys[nm_ws]:
                        # prefer uid (put first)
                        name_to_state_keys[nm_ws].insert(0, uid_key)

                # Match by email for reliability (name mismatch cases)
                if email_ws:
                    for nm_team, team_email in name_to_email.items():
                        if team_email and team_email == email_ws and uid_key not in name_to_state_keys[nm_team]:
                            name_to_state_keys[nm_team].insert(0, uid_key)

    # Define inclusive weekly window (end of day)
    now_utc   = datetime.now(timezone.utc)
    week_end  = now_utc.replace(hour=23, minute=59, second=59, microsecond=999999)
    week_start = week_end - timedelta(days=7)

    # Aggregate per team member from DynamoDB
    all_reports = []
    for u in team_cfg:
        username = u["name"]
        nm = _norm(username)
        keys = name_to_state_keys.get(nm, [])
        if not keys:
            # no keys — still produce a minimal report
            agg = {"username": username, "total_seconds": 0, "total_entries": 0, "per_project_seconds": {}}
        else:
            a = aggregate_week_from_state(username, keys, week_start, week_end)
            agg = a

        msg = build_weekly_message(
            username=username,
            week_start=week_start,
            week_end=week_end,
            total_seconds=agg["total_seconds"],
            total_entries=agg["total_entries"],
            per_project_seconds=agg.get("per_project_seconds", {}),
            weekly_commitment_hours=name_to_commit.get(nm)
        )
        all_reports.append({"username": username, "message": msg})

    # Send to per-user webhooks in parallel
    sent = send_reports_parallel(all_reports, name_to_webhook)

    return {
        "status": "complete",
        "sent": sent,
        "total_reports": len(all_reports),
        "week_start": week_start.strftime("%Y-%m-%d"),
        "week_end": week_end.strftime("%Y-%m-%d")
    }
