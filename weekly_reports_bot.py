import requests
import base64
from datetime import datetime, timedelta
import json
from collections import defaultdict

# === api token ===
API_TOKEN = "2d8b16d7923728381944f76b258e8a1a"  # Replace with an actual API token

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
    
    payload = {
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "grouped": True,
        "sub_grouping": "users",
        "grouping": "projects"
    }
    
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code == 200:
        return response.json()
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
            print(f"No Discord webhook URL found for {report['username']}")
            continue
        
        discord_data = {
            "content": report['message']
        }
        
        response = requests.post(webhook_url, json=discord_data)
        
        if response.status_code == 204:
            print(f"Sent report for {report['username']}")
        else:
            print(f"Failed to send report for {report['username']}: {response.text}")


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
        if not workspaces:
            print(f"No workspaces found for token: {token['name']}")
            continue
        
        for workspace in workspaces:
            WORKSPACE_ID = workspace['id']
            
            # Cache projects for this workspace
            cache_workspace_projects(WORKSPACE_ID)
            
            # Fetch detailed report for the last 7 days
            toggl_data = get_detailed_report(WORKSPACE_ID, datetime.now() - timedelta(days=7), datetime.now())
            
            if toggl_data:
                reports = get_all_weekly_reports(toggl_data)
                all_reports.extend(reports)
            else:
                print(f"Failed to fetch data for token: {token['name']}")
    
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
