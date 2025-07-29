import json
import base64
import requests
from datetime import datetime, timedelta
from collections import defaultdict
import boto3
import os

S3_BUCKET = os.environ["S3_BUCKET"]
USERS_FILE = "toggl_users.json"
TOKENS_FILE = "toggl_api_tokens.json"

s3 = boto3.client("s3")
HEADERS = {}
PROJECT_CACHE = {}

# -----------------------------
# Load data from S3
# -----------------------------
def get_s3_json(filename):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=filename)
    return json.loads(obj["Body"].read().decode("utf-8"))

# -----------------------------
# Auth & API setup
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
    response = requests.get(url, headers=HEADERS)
    return response.json() if response.ok else []

def get_workspace_projects(workspace_id):
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/projects"
    response = requests.get(url, headers=HEADERS)
    return response.json() if response.ok else []

def cache_workspace_projects(workspace_id):
    global PROJECT_CACHE
    projects = get_workspace_projects(workspace_id)
    for project in projects:
        PROJECT_CACHE[project['id']] = project['name']

def get_project_name_from_id(project_id):
    if project_id is None:
        return "Personal/No Project"
    return PROJECT_CACHE.get(project_id, f"Project {project_id}")

def parse_toggl_date(date_string):
    try:
        date_part = date_string.split('+')[0].replace('Z', '')
        return datetime.fromisoformat(date_part)
    except:
        return datetime.now()

def format_seconds_to_hm(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m" if hours or minutes else "0m"

# -----------------------------
# Report API v3
# -----------------------------
def get_detailed_report(workspace_id, start_date, end_date):
    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries"
    payload = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "grouped": True,
        "sub_grouping": "users",
        "grouping": "projects"
    }
    response = requests.post(url, headers=HEADERS, json=payload)
    return response.json() if response.status_code == 200 else None

# -----------------------------
# Report formatting
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

        for time_entry in entry.get('time_entries', []):
            seconds = time_entry.get('seconds', 0)
            user_reports[user_id]['total_seconds'] += seconds
            user_reports[user_id]['total_entries'] += 1
            user_reports[user_id]['all_entries'].append({
                'project': get_project_name_from_id(project_id),
                'description': description,
                'duration': seconds,
                'start': parse_toggl_date(time_entry.get('start', '')),
                'date': time_entry['start'][:10]
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
            'message': "\n".join(parts)
        }

    return discord_messages

# -----------------------------
# Main Lambda Logic
# -----------------------------
def lambda_handler(event, context):
    api_tokens = get_s3_json(TOKENS_FILE)
    users = get_s3_json(USERS_FILE)
    all_reports = []
    global PROJECT_CACHE
    PROJECT_CACHE = {}

    week_end = datetime.utcnow()
    week_start = week_end - timedelta(days=7)

    for token in api_tokens:
        get_headers(token['api_token'])
        workspaces = get_workspaces()

        for workspace in workspaces:
            workspace_id = workspace['id']
            cache_workspace_projects(workspace_id)
            toggl_data = get_detailed_report(workspace_id, week_start, week_end)

            if toggl_data:
                reports = generate_discord_reports(toggl_data, week_start, week_end)
                all_reports.extend([
                    {
                        "username": report['username'],
                        "message": report['message']
                    }
                    for report in reports.values()
                ])
            else:
                print(f"[WARN] No report data for {token['name']}")

    for report in all_reports:
        webhook_url = next((u['discord_webhook_url'] for u in users if u['name'] == report['username']), None)
        if not webhook_url:
            print(f"[WARN] No webhook for {report['username']}")
            continue

        payload = {"content": report["message"]}
        resp = requests.post(webhook_url, json=payload)
        if resp.status_code == 204:
            print(f"[OK] Sent report for {report['username']}")
        else:
            print(f"[FAIL] {report['username']} - {resp.status_code}: {resp.text}")

    return {"status": "complete", "sent": len(all_reports)}
