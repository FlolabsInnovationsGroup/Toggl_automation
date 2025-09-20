import os
import json
import base64
import requests
import csv
import time
import random
from io import StringIO
import boto3
from datetime import datetime, timedelta, timezone

# ==========================
# ENVIRONMENT
# ==========================
GOOGLE_SHEET_URL     = os.getenv("GOOGLE_SHEET_URL", "")
TEAM_GID             = os.getenv("TEAM_GID", "")
WORKSPACES_GID       = os.getenv("WORKSPACES_GID", "")
SYSTEM_ALERT_WEBHOOK = os.getenv("SYSTEM_ALERT_WEBHOOK", "")

DYNAMODB_TABLE      = os.getenv("DYNAMODB_TABLE", "")
TIME_WINDOW_DAYS    = int(os.getenv("TIME_WINDOW_DAYS", "24"))
MAX_SEEN_IDS        = int(os.getenv("MAX_SEEN_IDS", "50"))
HTTP_TIMEOUT        = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
HARD_START_FILTER   = os.getenv("HARD_START_FILTER", "1") == "1"  # default ON

# Discord rate limiting knobs
DISCORD_MIN_INTERVAL_MS        = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))   # per-webhook gap
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))  # global gap
DISCORD_MAX_RETRIES            = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

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
            requests.post(SYSTEM_ALERT_WEBHOOK, json={"content": msg}, timeout=HTTP_TIMEOUT)
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
# DynamoDB helpers
# ==========================
def _get_user_state(user_name: str) -> dict:
    """
    Returns:
      {
        "seen_entries": [ids...],
        "seen_updates": {id: updated_iso},
        "seen_snapshots": {id: {dur:int, desc:str, project:str, tags:list, start:str, updated:str}}
      }
    Backwards compatible if item only has seen_entries/seen_updates.
    """
    try:
        resp = table.get_item(Key={"user_name": user_name})
        item = resp.get("Item", {}) or {}
        seen_entries   = item.get("seen_entries", []) or []
        seen_updates   = item.get("seen_updates", {}) or {}
        seen_snapshots = item.get("seen_snapshots", {}) or {}
        # normalize
        seen_updates   = {str(k): str(v) for k, v in seen_updates.items()}
        seen_snapshots = {str(k): v for k, v in seen_snapshots.items()}
        return {"seen_entries": seen_entries, "seen_updates": seen_updates, "seen_snapshots": seen_snapshots}
    except Exception as e:
        _alert(f"[ERROR] DynamoDB get error for {user_name}: {e}")
        return {"seen_entries": [], "seen_updates": {}, "seen_snapshots": {}}

def _save_user_state(user_name: str, seen_entries: list, seen_updates: dict, seen_snapshots: dict):
    try:
        # cap IDs; keep snapshots only for retained IDs
        capped_ids = list(seen_entries[-MAX_SEEN_IDS:])
        keep = set(capped_ids)
        pruned_updates   = {k: v for k, v in seen_updates.items() if k in keep}
        pruned_snapshots = {k: v for k, v in seen_snapshots.items() if k in keep}
        table.put_item(Item={
            "user_name": user_name,
            "seen_entries": capped_ids,
            "seen_updates": pruned_updates,
            "seen_snapshots": pruned_snapshots
        })
    except Exception as e:
        _alert(f"[ERROR] DynamoDB put error for {user_name}: {e}")

# ==========================
# Toggl API helpers
# ==========================
def _headers_for_token(api_token: str) -> dict:
    return {
        "Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode()
    }

def get_workspaces_for_token(api_token: str) -> list[dict]:
    url = "https://api.track.toggl.com/api/v9/workspaces"
    try:
        r = requests.get(url, headers=_headers_for_token(api_token), timeout=HTTP_TIMEOUT)
        if r.status_code == 200:
            return r.json() or []
        _alert(f"[ERROR] get_workspaces_for_token: {r.status_code} {r.text}")
    except Exception as e:
        _alert(f"[ERROR] get_workspaces_for_token exception: {e}")
    return []

def fetch_workspace_entries(api_token: str, workspace_id: int, since_dt: datetime, until_dt: datetime) -> list[dict]:
    """
    Uses Reports API v2 /details for a short time window.
    """
    url = "https://api.track.toggl.com/reports/api/v2/details"
    headers = _headers_for_token(api_token)
    params = {
        "workspace_id": workspace_id,
        "since": since_dt.strftime("%Y-%m-%d"),
        "until": until_dt.strftime("%Y-%m-%d"),
        "user_agent": "toggl-discord-bot",
        "page": 1
    }
    all_entries = []
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            _alert(f"[ERROR] details fetch failed: {resp.status_code} - {resp.text}")
            break
        data = resp.json()
        entries = data.get("data", []) or []
        all_entries.extend(entries)
        if len(all_entries) >= data.get("total_count", 0):
            break
        params["page"] += 1
    return all_entries

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
    # new - old (positive means moved later)
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
_last_send_ts_per_webhook = {}  # webhook_url -> monotonic timestamp
_last_send_ts_global = 0.0      # monotonic timestamp

def _sleep_ms(ms: int):
    if ms > 0:
        time.sleep(ms / 1000.0)

def _respect_min_interval(webhook_url: str):
    """Ensure per-webhook spacing."""
    now = time.monotonic()
    last = _last_send_ts_per_webhook.get(webhook_url)
    if last is not None:
        elapsed_ms = int((now - last) * 1000)
        if elapsed_ms < DISCORD_MIN_INTERVAL_MS:
            _sleep_ms(DISCORD_MIN_INTERVAL_MS - elapsed_ms)
    _last_send_ts_per_webhook[webhook_url] = time.monotonic()

def _respect_global_interval():
    """Ensure global spacing across ANY sends."""
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
    """Send a Discord message honoring pacing and 429/5xx backoffs."""
    # Global + per-webhook spacing
    _respect_global_interval()
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
# Lambda entrypoint
# ==========================
def lambda_handler(event, context):
    # Load TEAM & WORKSPACES from Google Sheet
    team          = load_team_sheet()
    workspace_set = load_workspace_tokens()

    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}
    if not workspace_set:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

    # Map user name -> webhook
    user_map = { t["name"]: t.get("discord_webhook_url", "").strip() for t in team if t.get("discord_webhook_url") }
    user_map_norm = { _norm(k): v for k, v in user_map.items() }

    now   = datetime.now(timezone.utc)
    since = now - timedelta(days=TIME_WINDOW_DAYS)

    notified = 0

    # Iterate over each workspace token
    for token_entry in workspace_set:
        api_token = token_entry["api_token"]
        workspaces = get_workspaces_for_token(api_token)
        if not workspaces:
            _alert(f"⚠️ No workspaces visible for token ending …{api_token[-4:]}")
            continue

        for ws in workspaces:
            wid = ws.get("id")
            if not wid:
                continue

            entries = fetch_workspace_entries(api_token, wid, since, now)
            print(f"[DEBUG] Fetched {len(entries)} entries from workspace {wid}")

            for entry in entries:
                entry_id    = str(entry.get("id") or "")
                user_name   = entry.get("user") or ""
                description = entry.get("description") or "No description"
                project     = entry.get("project") or "No project"
                tags        = entry.get("tags") or []
                duration_ms = int(entry.get("dur") or 0)
                start_str   = entry.get("start") or ""
                updated_str = entry.get("updated") or entry.get("at") or ""  # 'updated' preferred; 'at' fallback
                uid         = entry.get("uid")

                if not entry_id or not user_name:
                    continue

                webhook_url = user_map.get(user_name) or user_map_norm.get(_norm(user_name))
                if not webhook_url:
                    _alert(f"[INFO] No webhook configured for '{user_name}' (uid: {uid}), skipping {entry_id}")
                    continue

                # parse timestamps
                def _parse(ts: str) -> datetime:
                    try:
                        ts = ts.replace("Z", "+0000").replace("+00:00", "+0000")
                        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
                    except Exception:
                        return now

                start_dt   = _parse(start_str) if start_str else now
                updated_dt = _parse(updated_str) if updated_str else now

                # --- stable state key for DynamoDB (prefer uid, fallback to normalized name) ---
                state_key = f"uid:{uid}" if uid is not None else f"name:{_norm(user_name)}"

                # --- load state ---
                state          = _get_user_state(state_key)
                seen_ids       = set(state["seen_entries"])
                seen_updates   = state["seen_updates"]
                seen_snapshots = state.get("seen_snapshots", {})

                prev_updated_str = seen_updates.get(entry_id)
                prev_updated = _parse(prev_updated_str) if prev_updated_str else None
                first_seen   = prev_updated is None  # source of truth is the updates map

                prev_snap = seen_snapshots.get(entry_id, {})
                prev_dur  = int(prev_snap.get("dur", duration_ms))
                prev_desc = prev_snap.get("desc", description)
                prev_proj = prev_snap.get("project", project)
                prev_tags = prev_snap.get("tags", list(tags))
                prev_start_str = prev_snap.get("start", start_str)
                prev_start_dt = _parse(prev_start_str) if prev_start_str else start_dt

                # Always persist latest 'updated' we observed
                seen_updates[entry_id] = updated_dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                if entry_id not in seen_ids:
                    state["seen_entries"].append(entry_id)  # keep for quick membership; capped on save

                # Build current snapshot (for future comparisons)
                curr_snap = {
                    "dur": duration_ms,
                    "desc": description,
                    "project": project,
                    "tags": list(tags) if isinstance(tags, list) else [],
                    "start": start_str,
                    "updated": updated_dt.strftime("%Y-%m-%dT%H:%M:%S%z"),
                }
                seen_snapshots[entry_id] = curr_snap

                # Decide notification:
                should_notify = False
                label         = "🆕 New Time Entry Added"
                diff_lines    = []  # for "changes: ..." summary

                # Detect changes (for rendering)
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

                if first_seen:
                    if HARD_START_FILTER and start_dt < since:
                        _save_user_state(state_key, state["seen_entries"], seen_updates, seen_snapshots)
                        continue
                    if updated_dt >= since:
                        should_notify = True
                        label = "🆕 New Time Entry Added"
                else:
                    if (updated_dt > prev_updated) and (updated_dt >= since):
                        should_notify = True
                        label = "✍️ Time Entry Edited"

                        # Build summary deltas
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
                            if added:
                                bits.append("+" + ", ".join(added))
                            if removed:
                                bits.append("-" + ", ".join(removed))
                            diff_lines.append("Tags " + " ".join(bits) if bits else "Tags changed")
                        if changed_start:
                            diff_lines.append(f"Δ Start {_format_delta_datetimes(start_dt, prev_start_dt)}")

                # Persist latest state before any network call
                _save_user_state(state_key, state["seen_entries"], seen_updates, seen_snapshots)

                if not should_notify:
                    continue

                # Build message (edited version mirrors "New" layout; changed fields show strikethrough → new)
                lines = [
                    f"## {label}",
                    f"👤 **User**: {user_name}",
                ]

                # Project
                if label.startswith("✍️") and changed_proj:
                    lines.append(f"📁 **Project:** ~~{prev_proj}~~ {project}")
                else:
                    lines.append(f"📁 **Project:** {project}")

                # Tags
                if label.startswith("✍️") and changed_tags:
                    old_tags = ", ".join(sorted(old_set)) or "No tags"
                    new_tags = ", ".join(sorted(new_set)) or "No tags"
                    lines.append(f"🏷️ **Tags:** ~~{old_tags}~~ {new_tags}")
                else:
                    lines.append(f"🏷️ **Tags:** {', '.join(tags) if tags else 'No tags'}")

                # Description
                if label.startswith("✍️") and changed_desc:
                    lines.append(f"📝 **Task:** ~~{_truncate(prev_desc)}~~ {_truncate(description)}")
                else:
                    lines.append(f"📝 **Task:** {description}")

                # Time
                if label.startswith("✍️") and changed_time:
                    lines.append(f"⏱️ **Time Tracked:** ~~{convert_ms_to_hms(prev_dur)}~~ {convert_ms_to_hms(duration_ms)}")
                else:
                    lines.append(f"⏱️ **Time Tracked:** {convert_ms_to_hms(duration_ms)}")

                # Start
                if label.startswith("✍️") and changed_start:
                    lines.append(f"🗓️ **Start:** ~~{prev_start_dt.strftime('%Y-%m-%d %H:%M')}~~ {start_dt.strftime('%Y-%m-%d %H:%M')}")
                else:
                    lines.append(f"🗓️ **Start:** {start_dt.strftime('%Y-%m-%d %H:%M')}")

                # Updated (always current)
                lines.append(f"🕑 **Updated:** {updated_dt.strftime('%Y-%m-%d %H:%M')}")

                if label.startswith("✍️") and diff_lines:
                    lines.append("")
                    lines.append("**changes:** " + "; ".join(diff_lines))

                lines.append("—")
                msg = "\n".join(lines)

                # send (rate-limited)
                ok = send_discord_with_backoff(webhook_url, msg, timeout=HTTP_TIMEOUT)
                if ok:
                    notified += 1
                else:
                    _alert(f"[ERROR] Discord send failed for {user_name} (entry {entry_id})")

    return {"status": "complete", "notified": notified}
