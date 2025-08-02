import requests
import base64
from datetime import datetime, timedelta
import json
from collections import defaultdict

# === Discord webhook for system alerts ===
SYSTEM_ALERT_WEBHOOK = "https://discord.com/api/webhooks/1401155910126272553/xF3EHp8wQlA0mx4tvcUXvvzUmXBoJgfNae21TN6N-a7mZR699QdGLkqPGQjgtKkxj2_I"

# === workspace id ===
WORKSPACE_ID = "200169844"  # Replace with an actual workspace ID

# === headers for API requests ===
HEADERS = {}

# === Global project cache ===
PROJECT_CACHE = {}

#******************************************
# Helper functions for API requests
#******************************************
def get_headers(api_token):
    """
    Create the authorization header for Toggl API requests.
    
    Args:
        api_token (str): Your Toggl API token.
    
    updates the global headers variable with the authorization token.:
        dict: Headers with the authorization token.
    """
    auth_header = base64.b64encode(f"{api_token}:api_token".encode()).decode()
    """    Update the global HEADERS variable with the authorization token.
    This function sets the headers for API requests to Toggl.
    Args:
        auth_header (str): Base64 encoded authorization header.
    """
    # Check if the API token is valid
    if not api_token:
        raise ValueError("API token is required for authentication.")
    if not isinstance(api_token, str):
        raise TypeError("API token must be a string.")
    
    # Encode the API token for basic authentication
    auth_header = base64.b64encode(f"{api_token}:api_token".encode()).decode()
    
    # Update the global HEADERS variable with the authorization token
    global HEADERS
    # Set the headers for API requests
    HEADERS = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }

def get_workspaces():
    """
    Fetch all workspaces for the authenticated user.
    Returns:
        list: List of workspaces in JSON format.
    """
    url = "https://api.track.toggl.com/api/v9/workspaces"
    response = requests.get(url, headers=HEADERS)
    
    return response.json()

def get_workspace_projects(workspace_id):
    """
    Fetch all projects for a specific workspace.
    
    Args:
        workspace_id (str): The workspace ID
    
    Returns:
        list: List of projects in JSON format
    """
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/projects"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching projects for workspace {workspace_id}: {response.status_code}")
        return []

def cache_workspace_projects(workspace_id):
    """
    Cache all projects for a workspace in the global PROJECT_CACHE.
    
    Args:
        workspace_id (str): The workspace ID
    """
    global PROJECT_CACHE
    
    projects = get_workspace_projects(workspace_id)
    for project in projects:
        PROJECT_CACHE[project['id']] = project['name']

def get_workspace_users(workspace_id):
    url = f"https://api.track.toggl.com/api/v9/workspaces/{workspace_id}/users"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()
    return []

def check_time_entries():
    url = "https://api.track.toggl.com/api/v9/me/time_entries"
    response = requests.get(url, headers=HEADERS)
    print(response.json())
    
def get_fallback_time_entries(start_date, end_date):
    url = f"https://api.track.toggl.com/api/v9/me/time_entries?start_date={start_date.isoformat()}Z&end_date={end_date.isoformat()}Z"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        entries = response.json()
        grouped = defaultdict(lambda: defaultdict(list))
        for entry in entries:
            print(entry)
            grouped[entry['user_id']][entry.get('project_id')].append(entry)
            
        return [
            {
                'user_id': user_id,
                'username': username,
                'project_id': project_id,
                'time_entries': entries
            } for user_id, projects in grouped.items() for project_id, entries in projects.items()
        ]
    return None

#******************************************
# Load API tokens and user data
#******************************************
# === api tokens ===
try:
    with open('toggl_api_tokens.json') as f:
        api_tokens = json.load(f)
except FileNotFoundError:
    print("Error: 'toggl_api_tokens.json' not found.")
    api_tokens = None  # or handle the error appropriately
except Exception as e:
    print(f"An error occurred while reading 'toggl_api_tokens.json': {e}")
    api_tokens = None  # or handle the error appropriately

# === Load user data ===
try:
    with open('toggl_users.json') as f:
        users = json.load(f)
except FileNotFoundError:
    print("Error: 'toggl_users.json' not found.")
    users = None  # or handle the error appropriately
except Exception as e:
    print(f"An error occurred while reading 'toggl_users.json': {e}")
    users = None  # or handle the error appropriately

# Check if API tokens and users are loaded
if not api_tokens:
    print("Error: API tokens could not be loaded. Exiting.")
    exit(1)
elif  not users:
    print("Error: user data could not be loaded. Exiting.")
    exit(1)
    
# Set the headers for API requests using the first API token
if api_tokens:
    API_TOKEN = api_tokens[0]["api_token"]  # Get the first token from the dictionary
    get_headers(API_TOKEN)

#******************************************
# Function to get detailed report from Toggl Reports API
#******************************************
def get_detailed_report(workspace_id, start_date, end_date):
    """Get detailed report using Reports API"""
    url = f"https://api.track.toggl.com/reports/api/v3/workspace/{workspace_id}/search/time_entries"
    ''' Old payload format'''
    payload = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "grouped": True,
        "sub_grouping": "users",
        "grouping": "projects"
    }
    
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        if len(data) == 0:
            users = get_workspace_users(workspace_id)
            if len(users) == 1:
                return get_fallback_time_entries(start_date, end_date)
        return data
    else:
        print(f"Error fetching detailed report: {response.status_code}")
        return None

#******************************************
# Helper functions for report generation
#******************************************

def format_seconds_to_hours_minutes(seconds):
    """Convert seconds to hours and minutes format"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    
    if hours > 0 and minutes > 0:
        return f"{hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h"
    elif minutes > 0:
        return f"{minutes}m"
    else:
        return "0m"

#******************************************
# Function to get project name from project_id
#******************************************
def get_project_name_from_id(project_id):
    """
    Helper function to get project name from project_id using the cached projects
    """
    if project_id is None:
        return "Personal/No Project"
    
    # Check if project name is in cache
    if project_id in PROJECT_CACHE:
        return PROJECT_CACHE[project_id]
    
    # If not in cache, return a fallback name
    return f"Project {project_id}"

#******************************************
# Function to parse Toggl date string
#******************************************
def parse_toggl_date(date_string):
    """Parse Toggl date string and return a datetime object"""
    try:
        # Remove timezone info and parse
        if '+' in date_string:
            date_part = date_string.split('+')[0]
        elif 'Z' in date_string:
            date_part = date_string.replace('Z', '')
        else:
            date_part = date_string
        
        return datetime.fromisoformat(date_part)
    except:
        return datetime.now()

#******************************************
# Function to generate Discord messages from Toggl Reports API response
#******************************************
def generate_discord_reports(toggl_data, week_start_date=None, week_end_date=None):
    """
    Generate Discord messages for each user from Toggl Reports API response
    
    Args:
        toggl_data: List of dictionaries from Toggl Reports API
        week_start_date: Start date of the week (optional)
        week_end_date: End date of the week (optional)
    
    Returns:
        Dictionary with user_id as key and Discord message as value
    """
    
    # Set default week dates if not provided
    today = datetime.now()
    week_start_date = today - timedelta(days=today.weekday())
    week_end_date = week_start_date + timedelta(days=6)
    
    # Process each entry
    user_reports = defaultdict(lambda: {
        'username': '',
        'total_seconds': 0,
        'total_entries': 0,
        'projects': defaultdict(int),
        'daily_breakdown': defaultdict(int),
        'all_entries': []
    })
    
    for entry in toggl_data:
        user_id = entry['user_id']
        username = entry['username']
        project_id = entry['project_id']
        description = entry.get('description', 'No description')
        
        # Update user info
        user_reports[user_id]['username'] = username
        
        # Process each time entry
        for time_entry in entry.get('time_entries', []):
            seconds = time_entry.get('seconds', 0)
            
            # Add to totals
            user_reports[user_id]['total_seconds'] += seconds
            user_reports[user_id]['total_entries'] += 1
            
            # Store entry for detailed view
            user_reports[user_id]['all_entries'].append({
                'project': get_project_name_from_id(project_id),
                'description': description,
                'duration': seconds,
                'start': parse_toggl_date(time_entry.get('start', '')),
                'date': time_entry['start'][:10]
            })
    
    # Generate Discord messages
    discord_messages = {}
    
    for user_id, report in user_reports.items():
        username = report['username']
        total_hours = report['total_seconds'] / 3600
        
        # Start building the message
        message_parts = []
        
        # Header
        message_parts.append("📊 **WEEKLY TIME TRACKING REPORT**")
        message_parts.append("=" * 40)
        message_parts.append(f"👤 **Employee:** {username}")
        message_parts.append(f"📅 **Week:** {week_start_date.strftime('%B %d')} - {week_end_date.strftime('%B %d, %Y')}")
        message_parts.append(f"⏰ **Total Time:** {format_seconds_to_hours_minutes(report['total_seconds'])} ({total_hours:.1f} hours)")
        message_parts.append(f"📝 **Total Entries:** {report['total_entries']}")
        message_parts.append("")
        
        # Productivity level emoji
        if total_hours >= 40:
            productivity_emoji = "🔥"
            productivity_text = "High productivity week!"
        elif total_hours >= 30:
            productivity_emoji = "👍"
            productivity_text = "Good productivity week!"
        elif total_hours >= 20:
            productivity_emoji = "⚡"
            productivity_text = "Moderate productivity week"
        else:
            productivity_emoji = "📈"
            productivity_text = "Room for improvement"
        
        message_parts.append(f"{productivity_emoji} {productivity_text}")
        message_parts.append("")
        message_parts.append(f"🤖 *Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC*")
        
        # Join all parts
        full_message = "\n".join(message_parts)
        
        discord_messages[user_id] = {
            'username': username,
            'message': full_message,
            'total_hours': total_hours,
            'total_entries': report['total_entries']
        }
    
    return discord_messages

#******************************************
# Main function to get all weekly reports
#******************************************
def get_all_weekly_reports(toggl_data):
    """
    Main function to generate all weekly reports
    Returns a list of Discord messages ready to be sent
    """
    reports = generate_discord_reports(toggl_data)
    
    # Convert to a list format for easy iteration
    report_list = []
    for user_id, report in reports.items():
        report_list.append({
            'user_id': user_id,
            'username': report['username'],
            'message': report['message'],
            'total_hours': report['total_hours'],
            'total_entries': report['total_entries']
        })
    
    return report_list

#******************************************
# Discord integration
#******************************************

def send_to_discord(reports):
    """
    Send the generated reports to Discord using webhooks
    """
    for report in reports:
        webhook_url = next((user['discord_webhook_url'] for user in users if user['name'] == report['username']), None)
        if not webhook_url:
            send_alert_to_discord(f"🚨 Missing webhook for user: {report['username']}")
            continue
        
        discord_data = {
            "content": report['message']
        }
        ''' Section commented out to avoid sending messages during identification of new users 
        # Send the report to Discord
        response = requests.post(webhook_url, json=discord_data)
        
        if response.status_code == 204:
            print(f"Sent report for {report['username']}")
        else:
            print(f"Failed to send report for {report['username']}: {response.text}")
        '''

def send_alert_to_discord(message):
    payload = {"content": message}
    try:
        requests.post(SYSTEM_ALERT_WEBHOOK, json=payload)
    except Exception as e:
        print(f"Error sending alert to Discord: {e}")

def check_and_alert_issues(users, reports, workspace_name, workspace_id):
    if len(users) > 1 and len(reports) == 0:
        users = get_workspace_users(workspace_id)
        active_users = [user for user in users if user.get('inactive') == False]
        inactive_users = [user for user in users if user.get('inactive') == True]
        print(f"Active users in workspace {workspace_id}: {active_users}")
        
        '''        If there are no reports for the workspace, check for inactive users'''
        if inactive_users:
            alert_msg = "⚠️ Users with no entries this week in workspace '{}':\n{}".format(
                workspace_name,
                "\n".join(f"- {name}" for name in inactive_users)
            )
            send_alert_to_discord(alert_msg)
        
        if len(active_users) == 1:
            user = active_users[0]
            send_alert_to_discord(f"🚨 Empty report for workspace '{workspace_name}' with single member: {user['name']} (ID: {workspace_id})")
        else:
            send_alert_to_discord(f"🚨 Empty report for workspace '{workspace_name}' with multiple active members (ID: {workspace_id})")
        
#******************************************
# Main function to get all weekly reports from all owners
#******************************************
def get_all_weekly_reports_from_all_owners():
    """
    This function returns the lists of weekly reports for each api token in the toggl_api_tokens.json file.
    It uses the get_detailed_report function to fetch data and then formats it for Discord.
    
    Returns:
        List of dictionaries containing user_id, username, and message for each report.
    """
    if not api_tokens:
        print("Error: No API tokens available. Exiting.")
        return []
    
    all_reports = []
    global WORKSPACE_ID
    
    for token in api_tokens:
        get_headers(token['api_token'])
        
        workspaces = get_workspaces()
        #print(f"Token: {token['name']}, Workspaces: {workspaces}")

        if not workspaces:
            send_alert_to_discord(f"🚨No workspaces found for token: {token['name']}")
            continue
        
        for workspace in workspaces:
            WORKSPACE_ID = workspace['id']
            
            # Cache projects for this workspace
            cache_workspace_projects(WORKSPACE_ID)
            
            # Fetch detailed report for the last 7 days
            toggl_data = get_detailed_report(WORKSPACE_ID, datetime.now() - timedelta(days=7), datetime.now())
            #print(f"Workspace {WORKSPACE_ID} report length: {len(toggl_data) if toggl_data else 0}")

            reports = get_all_weekly_reports(toggl_data)
            
            if toggl_data:
                all_reports.extend(reports)
            else:
                print(f"Failed to fetch data for token: {token['name']}")
                
            check_and_alert_issues(users, reports, workspace['name'], WORKSPACE_ID)
    
    return all_reports

#******************************************
# Main execution flow
#******************************************
# Example usage:
if __name__ == "__main__":
    # Generate all weekly reports
    reports = get_all_weekly_reports_from_all_owners()
    if not reports:
        print("No reports generated. Exiting.")
        exit(1)
    
    # Send reports to Discord
    send_to_discord(reports)
