import os
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

SESSION = requests.Session()

NOTION_TOKEN = os.getenv("NOTION_TOKEN", "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").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "297a157695518075ab6cea94e60e4220").strip()
NOTION_ONBOARDINGS_DB_ID = os.getenv("NOTION_ONBOARDINGS_DB_ID", "a1a106fab63548089279dc54d63eab89").strip()
P_DATE = os.getenv("NOTION_PROP_DATE", "Date").strip()
P_DURATION = os.getenv("NOTION_PROP_DURATION", "Duration").strip()
P_TEAM_MEMBER = os.getenv("NOTION_PROP_TEAM_MEMBER", "Team Member").strip()
P_WEBHOOK = os.getenv("NOTION_PROP_WEBHOOK", "Webhook").strip()
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))

DISCORD_MIN_INTERVAL_MS = int(os.getenv("DISCORD_MIN_INTERVAL_MS", "500"))
DISCORD_GLOBAL_MIN_INTERVAL_MS = int(os.getenv("DISCORD_GLOBAL_MIN_INTERVAL_MS", "0"))
DISCORD_MAX_RETRIES = int(os.getenv("DISCORD_MAX_RETRIES", "5"))

NH = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

_last_send_ts_per_webhook: dict[str, float] = {}
_last_send_ts_global = 0.0
_TEAM_MEM_WEBHOOK_BY_ID: dict[str, str] = {}
_TEAM_MEM_NAME_BY_ID: dict[str, str] = {}


def _dashify_notion_id(value: str | None) -> str:
    if not value:
        return ""
    norm = "".join(ch for ch in value if ch.isalnum()).lower()
    if len(norm) == 32:
        return f"{norm[0:8]}-{norm[8:12]}-{norm[12:16]}-{norm[16:20]}-{norm[20:32]}"
    return value


def _require_config():
    if not (NOTION_TOKEN and NOTION_DATABASE_ID and NOTION_ONBOARDINGS_DB_ID):
        print("Missing Notion credentials/configuration. Please define NOTION_TOKEN, NOTION_DATABASE_ID, and NOTION_ONBOARDINGS_DB_ID.")
        sys.exit(1)


def _notion_query(database_id: str, payload: dict) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    try:
        resp = SESSION.post(url, headers=NH, json=payload, timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        print(f"Notion request failed: {exc}")
        sys.exit(1)
    if resp.status_code != 200:
        print(f"Notion query failed: status={resp.status_code} body={resp.text.strip()}")
        sys.exit(1)
    return resp.json() or {}


def _parse_iso8601(value: str | None, default: datetime | None = None) -> datetime | None:
    if not value:
        return default
    try:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.strptime(normalized, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            return default


def _sanitize_webhook_url(value: str | None) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return ""
    if candidate.lower() in {"n/a", "na", "none", "-", "no"}:
        return ""
    return candidate


def _get_webhook_url_from_props(props: dict) -> str:
    try:
        if not isinstance(props, dict):
            return ""
        pw = props.get(P_WEBHOOK, {}) or {}
        if not isinstance(pw, dict):
            pw = {}
        ptype = pw.get("type")
        if ptype == "url":
            return _sanitize_webhook_url(pw.get("url"))
        if ptype == "rich_text":
            txt = "".join([x.get("plain_text", "") for x in (pw.get("rich_text") or [])]).strip()
            return _sanitize_webhook_url(txt)
        if ptype == "title":
            txt = "".join([x.get("plain_text", "") for x in (pw.get("title") or [])]).strip()
            return _sanitize_webhook_url(txt)
        if ptype == "rollup":
            roll = pw.get("rollup") or {}
            rtype = roll.get("type")
            if rtype == "array":
                for it in (roll.get("array") or []):
                    if not isinstance(it, dict):
                        continue
                    it_type = it.get("type")
                    if it_type == "url":
                        u = _sanitize_webhook_url(it.get("url"))
                        if u:
                            return u
                    if it_type == "rich_text":
                        txt = _sanitize_webhook_url("".join([x.get("plain_text", "") for x in (it.get("rich_text") or [])]).strip())
                        if txt:
                            return txt
                    if it_type == "title":
                        txt = _sanitize_webhook_url("".join([x.get("plain_text", "") for x in (it.get("title") or [])]).strip())
                        if txt:
                            return txt
            elif rtype == "number":
                v = roll.get("number")
                return _sanitize_webhook_url(str(v).strip() if v is not None else "")
            elif rtype == "date":
                d = roll.get("date") or {}
                return _sanitize_webhook_url(d.get("start"))
        try:
            tm = props.get(P_TEAM_MEMBER, {}) or {}
            if isinstance(tm, dict) and tm.get("type") == "relation":
                rel = (tm.get("relation") or [])
                if rel:
                    tm_pid = (rel[0].get("id") or "").strip()
                    if tm_pid and tm_pid in _TEAM_MEM_WEBHOOK_BY_ID:
                        return (_TEAM_MEM_WEBHOOK_BY_ID.get(tm_pid) or "").strip()
        except Exception:
            pass
        return ""
    except Exception:
        return ""


def _get_team_member_name(page_id: str) -> str:
    pid = (page_id or "").strip()
    if not pid:
        return ""
    if pid in _TEAM_MEM_NAME_BY_ID:
        return _TEAM_MEM_NAME_BY_ID[pid]
    try:
        resp = SESSION.get(f"https://api.notion.com/v1/pages/{pid}", headers=NH, timeout=HTTP_TIMEOUT)
    except requests.RequestException:
        _TEAM_MEM_NAME_BY_ID[pid] = ""
        return ""
    if resp.status_code != 200:
        _TEAM_MEM_NAME_BY_ID[pid] = ""
        return ""
    data = resp.json() or {}
    props = data.get("properties") or {}
    for meta in props.values():
        if isinstance(meta, dict) and meta.get("type") == "title":
            name = "".join([x.get("plain_text", "") for x in (meta.get("title") or [])]).strip()
            if name:
                _TEAM_MEM_NAME_BY_ID[pid] = name
                return name
    _TEAM_MEM_NAME_BY_ID[pid] = ""
    return ""


def _get_member_info(props: dict) -> tuple[str, str]:
    tm = props.get(P_TEAM_MEMBER, {}) or {}
    if tm.get("type") != "relation":
        return "", ""
    for rel in (tm.get("relation") or []):
        pid = (rel.get("id") or "").strip()
        name = (rel.get("name") or "").strip()
        if not name and pid:
            name = _get_team_member_name(pid)
        if pid:
            return pid, name
    return "", ""


def _collect_time_by_webhook(start: datetime, end: datetime) -> tuple[dict[str, dict[str, str]], dict[str, float], dict[str, float]]:
    metadata: dict[str, dict[str, str]] = {}
    monthly: dict[str, float] = defaultdict(float)
    history: dict[str, float] = defaultdict(float)
    payload = {"page_size": 100}
    db_id = _dashify_notion_id(NOTION_DATABASE_ID)
    cursor = None
    while True:
        body = dict(payload)
        if cursor:
            body["start_cursor"] = cursor
        result = _notion_query(db_id, body)
        entries = result.get("results") or []
        for page in entries:
            props = page.get("properties", {}) or {}
            webhook = _get_webhook_url_from_props(props)
            if not webhook:
                continue
            duration = props.get(P_DURATION, {}).get("number") or 0.0
            try:
                duration = float(duration)
            except Exception:
                continue
            history[webhook] += duration
            start_date = (props.get(P_DATE, {}).get("date") or {}).get("start")
            entry_start = _parse_iso8601(start_date)
            if entry_start and start <= entry_start < end:
                monthly[webhook] += duration
            pid, name = _get_member_info(props)
            if webhook not in metadata:
                metadata[webhook] = {
                    "name": name or "Team member",
                    "pid": pid,
                }
            elif name and not metadata[webhook].get("name"):
                metadata[webhook]["name"] = name
        if not result.get("has_more"):
            break
        cursor = result.get("next_cursor")
    return metadata, monthly, history


def _month_window(reference: datetime) -> tuple[datetime, datetime]:
    current = datetime(reference.year, reference.month, 1, tzinfo=timezone.utc)
    if current.month == 1:
        previous = datetime(current.year - 1, 12, 1, tzinfo=timezone.utc)
    else:
        previous = datetime(current.year, current.month - 1, 1, tzinfo=timezone.utc)
    return previous, current


def _format_report(hours_month: float, hours_total: float, start: datetime, end: datetime, employee: str) -> str:
    period = f"{start.strftime('%b %d')}–{end.strftime('%b %d, %Y')}"
    generated_on = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return "\n".join([
        "# 📆 MONTHLY TIME REPORT",
        "——————————————————————————————",
        f"## 👤 **Team member:** {employee}",
        f"🗓️ **Period:** {period}",
        f"⏰ **Hours this month:** {hours_month:.2f} h",
        f"📈 **Total hours (company history):** {hours_total:.2f} h",
        "",
        f"🤖 *Generated on {generated_on} UTC*",
    ])


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
    _respect_global_interval()
    _respect_min_interval(webhook_url)
    payload = {"content": content}
    attempt = 0
    while attempt < DISCORD_MAX_RETRIES:
        resp = None
        err = None
        try:
            resp = SESSION.post(webhook_url, json=payload, timeout=timeout)
        except requests.RequestException as exc:
            err = exc
        if resp is not None and resp.status_code == 204:
            return True
        if resp is not None and resp.status_code == 429:
            ra = resp.headers.get("Retry-After") or resp.headers.get("X-RateLimit-Reset-After")
            wait_s = float(ra) if ra else 2.0
            wait_s += random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            continue
        if resp is not None and 500 <= resp.status_code < 600:
            wait_s = (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            continue
        if err:
            wait_s = (2 ** attempt) + random.uniform(0.05, 0.25)
            time.sleep(wait_s)
            attempt += 1
            continue
        print(f"[WARN] Discord send failed: status={getattr(resp,'status_code',None)} body={getattr(resp,'text','')}")
        return False
    return False


def main():
    _require_config()
    now = datetime.now(timezone.utc)
    window_start, window_end = _month_window(now)
    metadata, monthly_totals, history_totals = _collect_time_by_webhook(window_start, window_end)
    if not metadata:
        print("[WARN] No time entries with webhooks found.")
        return
    sent = 0
    for webhook, info in sorted(metadata.items(), key=lambda item: (item[1].get("name") or item[0])):
        hours_month = round(monthly_totals.get(webhook, 0.0) / 3600.0, 2)
        hours_total = round(history_totals.get(webhook, 0.0) / 3600.0, 2)
        payload = _format_report(hours_month, hours_total, window_start, window_end, info.get("name", "Team member"))
        if send_discord_with_backoff(webhook, payload, timeout=HTTP_TIMEOUT):
            sent += 1
        else:
            print(f"[WARN] failed to send monthly report to {info.get('name')} ({webhook})")
    print(f"[INFO] Monthly messaging complete (sent={sent} out of {len(metadata)})")


if __name__ == "__main__":
    main()


def lambda_handler(event, context):
    """AWS Lambda entrypoint."""
    try:
        main()
        return {"status": "complete"}
    except SystemExit as exc:
        return {"status": "error", "code": getattr(exc, "code", 1)}
