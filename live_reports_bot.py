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
from concurrent.futures import ThreadPoolExecutor, as_completed

# Reuse a single HTTP session for all outbound requests (TLS/connection reuse)
SESSION = requests.Session()

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

def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name, "")
    if val == "" and default:
        return True
    s = (val or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")

# Feature flags
DRY_RUN_NO_SEND     = _env_bool("DRY_RUN_NO_SEND", False)       # keep DB updated, but don't notify
SEND_NOTIFICATIONS  = _env_bool("SEND_NOTIFICATIONS", True)     # master switch for notifications

# Effective "should we send anything to Discord at all?"
NOTIFY_ENABLED = (not DRY_RUN_NO_SEND) and SEND_NOTIFICATIONS

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
# Timestamp parsers (shared, no re-def per entry)
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
    return {
        "Authorization": "Basic " + base64.b64encode(f"{api_token}:api_token".encode()).decode()
    }

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

def fetch_workspace_entries(api_token: str, workspace_id: int, since_dt: datetime, until_dt: datetime) -> list[dict]:
    """
    Uses Reports API v2 /details for a short time window.
    Adds robust pagination:
      - per_page to reduce round trips
      - break on empty page / no progress
      - safety cap on pages
    """
    url = "https://api.track.toggl.com/reports/api/v2/details"
    headers = _headers_for_token(api_token)
    params = {
        "workspace_id": workspace_id,
        "since": since_dt.strftime("%Y-%m-%d"),
        "until": until_dt.strftime("%Y-%m-%d"),
        "user_agent": "toggl-discord-bot",
        "page": 1,
        "per_page": 200,
    }

    all_entries = []
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

        all_entries.extend(entries)

        if total_count and len(all_entries) >= total_count:
            break

        if len(all_entries) == last_total:
            break
        last_total = len(all_entries)

        params["page"] += 1
    else:
        _alert(f"[WARN] Hit MAX_PAGES={MAX_PAGES} for workspace {workspace_id}; stopping pagination early.")

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
    delta = int((new_dt - old_dt).total_seconds())  # positive = moved later
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
    # If notifications are disabled, never make a network call.
    if not NOTIFY_ENABLED:
        return True

    # Global + per-webhook spacing
    _respect_global_interval()
    _respect_min_interval(webhook_url)

    payload = {"content": content}
    attempt = 0
    base_backoff = 1.0  # seconds
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
# Helpers for deleted-entry messages
# ==========================
def _strike(s: str) -> str:
    return f"~~{s}~~"

def _format_deleted_message(user_name: str, snap: dict) -> str:
    project = snap.get("project", "No project")
    tags    = snap.get("tags") or []
    desc    = snap.get("desc", "No description")
    dur_ms  = int(snap.get("dur", 0))
    start_s = snap.get("start") or ""
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
def _chunk_messages(msgs, limit=1900, sep="\n\n"):
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
    # Load TEAM & WORKSPACES from Google Sheet
    team          = load_team_sheet()
    workspace_set = load_workspace_tokens()

    if not team:
        _alert("TEAM sheet empty or unreachable.")
        return {"status": "no_team"}
    if not workspace_set:
        _alert("WORKSPACES sheet empty or unreachable.")
        return {"status": "no_workspaces"}

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
    skipped_notifies = 0
    skipped_deletions = 0

    # runtime aggregation per user for deletion detection + batched saves
    # state_key -> {"current_ids": set(), "user_name": str, "webhook": str|None, "state": dict, "dirty": bool}
    user_runtime = {}

    # per-webhook outbox for batched sending
    outbox = {}  # webhook_url -> list[str]

    # Track which users we already warned about missing webhooks (avoid per-entry spam)
    missing_webhook_warned = set()

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

                # Lookup webhook only if we're going to send
                webhook_url = None
                if NOTIFY_ENABLED:
                    webhook_url = user_map.get(user_name) or user_map_norm.get(_norm(user_name))
                    # NOTE: do NOT early-continue on missing webhook; we still want to persist state.

                # parse timestamps (shared parsers)
                start_dt   = parse_iso(start_str, now) if start_str else now
                updated_dt = parse_iso(updated_str, now) if updated_str else now

                # --- stable state key for DynamoDB (prefer uid, fallback to normalized name) ---
                state_key = f"uid:{uid}" if uid is not None else f"name:{_norm(user_name)}"

                # --- load/init runtime bucket ---
                if state_key not in user_runtime:
                    state = _get_user_state(state_key)
                    user_runtime[state_key] = {
                        "current_ids": set(),
                        "user_name": user_name,
                        "webhook": webhook_url,  # may be None
                        "state": state,
                        "dirty": False,
                    }
                else:
                    state = user_runtime[state_key]["state"]

                seen_ids       = set(state["seen_entries"])
                seen_updates   = state["seen_updates"]
                seen_snapshots = state.get("seen_snapshots", {})

                prev_updated_str = seen_updates.get(entry_id)
                prev_updated = None
                if prev_updated_str and not prev_updated_str.startswith("deleted:"):
                    prev_updated = parse_iso_opt(prev_updated_str)
                first_seen   = prev_updated is None  # source of truth is the updates map (ignoring tombstones)

                prev_snap = seen_snapshots.get(entry_id, {})
                prev_dur  = int(prev_snap.get("dur", duration_ms))
                prev_desc = prev_snap.get("desc", description)
                prev_proj = prev_snap.get("project", project)
                prev_tags = prev_snap.get("tags", list(tags))
                prev_start_str = prev_snap.get("start", start_str)
                prev_start_dt = parse_iso(prev_start_str, now) if prev_start_str else start_dt

                # Track that we saw this ID this run
                user_runtime[state_key]["current_ids"].add(entry_id)

                # Always persist latest 'updated' we observed (in-memory; save later)
                seen_updates[entry_id] = updated_dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                if entry_id not in seen_ids:
                    state["seen_entries"].append(entry_id)  # keep for quick membership; capped on save
                user_runtime[state_key]["dirty"] = True  # mark for batched save

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

                # If we plan to notify but webhook is missing:
                #   - Only warn once per user per run
                #   - Never warn for entries we've already seen (e.g., from a previous dry run)
                if NOTIFY_ENABLED and not webhook_url:
                    if first_seen and user_name not in missing_webhook_warned:
                        _alert(f"[INFO] No webhook configured for '{user_name}' (uid: {uid}); persisting state and skipping notifications.")
                        missing_webhook_warned.add(user_name)
                    # Do not try to notify; continue to next entry
                    continue

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
                        # no notify; just keep state
                        continue
                    if updated_dt >= since:
                        should_notify = True
                        label = "🆕 New Time Entry Added"
                else:
                    if (updated_dt and prev_updated and updated_dt > prev_updated) and (updated_dt >= since):
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

                if not should_notify:
                    continue

                # If notifications are disabled, skip building/queuing messages entirely.
                if not NOTIFY_ENABLED:
                    skipped_notifies += 1
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
                    old_tags_s = ", ".join(sorted(old_set)) or "No tags"
                    new_tags_s = ", ".join(sorted(new_set)) or "No tags"
                    lines.append(f"🏷️ **Tags:** ~~{old_tags_s}~~ {new_tags_s}")
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
                    lines.append(f"🗓️ **Start:** ~~{prev_start_dt.strftime('%Y-%m-%d %H:%M:%S')}~~ {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    lines.append(f"🗓️ **Start:** {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

                # Updated (always current)
                lines.append(f"🕑 **Updated:** {updated_dt.strftime('%Y-%m-%d %H:%M:%S')}")

                if label.startswith("✍️") and diff_lines:
                    lines.append("")
                    lines.append("**changes:** " + "; ".join(diff_lines))

                lines.append("—")
                msg = "\n".join(lines)

                # Queue for batched send
                if webhook_url:
                    outbox.setdefault(webhook_url, []).append(msg)
                    notified += 1

    # ==========================
    # Deletion detection (after all fetches)
    # ==========================
    for state_key, bucket in user_runtime.items():
        current_ids = bucket["current_ids"]
        user_name   = bucket["user_name"]
        webhook_url = bucket["webhook"]
        state       = bucket["state"]

        # nothing to compare if no state present
        seen_entries   = list(state.get("seen_entries", []))
        seen_updates   = dict(state.get("seen_updates", {}))
        seen_snapshots = dict(state.get("seen_snapshots", {}))

        # candidate deletions = previously seen but not seen this run
        for eid in list(seen_entries):
            if eid in current_ids:
                continue
            snap = seen_snapshots.get(eid)
            if not snap:
                continue

            # Consider deletion ONLY if the entry's START is inside the same window we fetched
            snap_start_s = snap.get("start") or ""
            snap_start   = parse_iso_opt(snap_start_s)
            in_window = bool(snap_start and snap_start >= since)
            if not in_window:
                continue

            if not NOTIFY_ENABLED or not webhook_url:
                skipped_deletions += 1
            else:
                msg = _format_deleted_message(user_name, snap)
                outbox.setdefault(webhook_url, []).append(msg)
                notified += 1

            # Remove the tombstoned entry from state to avoid repeat notifications
            try:
                seen_entries.remove(eid)
            except ValueError:
                pass
            seen_updates.pop(eid, None)
            seen_snapshots.pop(eid, None)

            # mark dirty to save pruned state later
            bucket["dirty"] = True

        # write back the mutated local state into the bucket
        bucket["state"]["seen_entries"]   = seen_entries
        bucket["state"]["seen_updates"]   = seen_updates
        bucket["state"]["seen_snapshots"] = seen_snapshots

    # ==========================
    # Batched state save: exactly once per user
    # ==========================
    for state_key, bucket in user_runtime.items():
        if bucket.get("dirty"):
            st = bucket["state"]
            _save_user_state(
                state_key,
                st.get("seen_entries", []),
                st.get("seen_updates", {}),
                st.get("seen_snapshots", {})
            )

    # ==========================
    # Send all queued Discord messages in parallel across webhooks
    # ==========================
    if NOTIFY_ENABLED and outbox:
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
        "skipped_notifies": skipped_notifies,
        "skipped_deletions": skipped_deletions,
        "dry_run_no_send": DRY_RUN_NO_SEND,
        "send_notifications": SEND_NOTIFICATIONS,
        "notify_enabled": NOTIFY_ENABLED,
    }
