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
headers = {
        "Authorization": "Basic xxxxxxxx",  # Replace with a base64 encoded API token
        "Content-Type": "application/json"
    }

#******************************************
# Helper functions for API requests
#******************************************
def get_header(api_token):
    """
    Create the authorization header for Toggl API requests.
    
    Args:
        api_token (str): Your Toggl API token.
    
    Returns:
        dict: Headers with the authorization token.
    """
    auth_header = base64.b64encode(f"{api_token}:api_token".encode()).decode()
    return {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }

def get_workspaces():
    """Fetch all workspaces for the authenticated user."""
    url = "https://api.track.toggl.com/api/v9/workspaces"
    response = requests.get(url, headers=headers)
    return response.json()

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
    API_TOKEN = next(iter(api_tokens.values()))  # Get the first token from the dictionary
    headers = get_header(API_TOKEN)

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
    
    response = requests.post(url, headers=headers, json=payload)
    
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
    Helper function to get project name from project_id
    You should implement this based on your project mapping
    For now, returning a placeholder
    """
    project_mapping = {
        200169844: "Program Tasks",
        200169894: "Team Meetings",
        None: "Personal/No Project"
    }
    return project_mapping.get(project_id, f"Project {project_id}")

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
    
    # Group data by user
    user_reports = defaultdict(lambda: {
        'username': '',
        'total_seconds': 0,
        'total_entries': 0,
        'projects': defaultdict(lambda: {'seconds': 0, 'entries': 0, 'descriptions': set()}),
        'daily_breakdown': defaultdict(int),
        'all_entries': []
    })
    
    # Set default week dates if not provided
    if not week_start_date or not week_end_date:
        today = datetime.now()
        week_start_date = today - timedelta(days=today.weekday())
        week_end_date = week_start_date + timedelta(days=6)
    
    # Process each entry
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
            start_time = parse_toggl_date(time_entry.get('start', ''))
            
            # Add to totals
            user_reports[user_id]['total_seconds'] += seconds
            user_reports[user_id]['total_entries'] += 1
            
            # Group by project
            project_name = get_project_name_from_id(project_id)
            user_reports[user_id]['projects'][project_name]['seconds'] += seconds
            user_reports[user_id]['projects'][project_name]['entries'] += 1
            user_reports[user_id]['projects'][project_name]['descriptions'].add(description)
            
            # Daily breakdown
            day_key = start_time.strftime('%A, %B %d')
            user_reports[user_id]['daily_breakdown'][day_key] += seconds
            
            # Store entry for detailed view
            user_reports[user_id]['all_entries'].append({
                'project': project_name,
                'description': description,
                'duration': seconds,
                'start': start_time,
                'date': start_time.strftime('%Y-%m-%d')
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
        
        # Project breakdown
        message_parts.append("🎯 **PROJECT BREAKDOWN:**")
        message_parts.append("```")
        for project_name, project_data in sorted(report['projects'].items(), 
                                                key=lambda x: x[1]['seconds'], reverse=True):
            project_hours = project_data['seconds'] / 3600
            percentage = (project_data['seconds'] / report['total_seconds'] * 100) if report['total_seconds'] > 0 else 0
            message_parts.append(f"• {project_name}")
            message_parts.append(f"  Time: {format_seconds_to_hours_minutes(project_data['seconds'])} ({percentage:.1f}%)")
            message_parts.append(f"  Entries: {project_data['entries']}")
            message_parts.append("")
        message_parts.append("```")
        
        # Daily breakdown
        message_parts.append("📅 **DAILY BREAKDOWN:**")
        message_parts.append("```")
        for day, seconds in sorted(report['daily_breakdown'].items()):
            if seconds > 0:
                message_parts.append(f"• {day}: {format_seconds_to_hours_minutes(seconds)}")
        message_parts.append("```")
        
        # Task descriptions by project
        message_parts.append("📋 **TASK DESCRIPTIONS:**")
        for project_name, project_data in report['projects'].items():
            if project_data['descriptions']:
                message_parts.append(f"**{project_name}:**")
                for desc in sorted(project_data['descriptions']):
                    if desc.strip():  # Only show non-empty descriptions
                        message_parts.append(f"• {desc}")
                message_parts.append("")
        
        # Performance insights
        avg_hours_per_day = total_hours / 7
        working_days = len([d for d in report['daily_breakdown'].values() if d > 0])
        
        message_parts.append("📈 **INSIGHTS:**")
        message_parts.append(f"• Average hours per day: {avg_hours_per_day:.1f}h")
        message_parts.append(f"• Active working days: {working_days}")
        if working_days > 0:
            avg_hours_per_working_day = total_hours / working_days
            message_parts.append(f"• Average hours per working day: {avg_hours_per_working_day:.1f}h")
        
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
        
        message_parts.append(f"• {productivity_emoji} {productivity_text}")
        message_parts.append("")
        
        # Footer
        message_parts.append("---")
        message_parts.append(f"🤖 *Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        
        # Join all parts
        full_message = "\n".join(message_parts)
        
        # Discord has a 2000 character limit, so we might need to split
        if len(full_message) > 2000:
            # Create a shorter version
            short_message_parts = []
            short_message_parts.append("📊 **WEEKLY TIME TRACKING REPORT**")
            short_message_parts.append(f"👤 **Employee:** {username}")
            short_message_parts.append(f"📅 **Week:** {week_start_date.strftime('%B %d')} - {week_end_date.strftime('%B %d, %Y')}")
            short_message_parts.append(f"⏰ **Total Time:** {format_seconds_to_hours_minutes(report['total_seconds'])} ({total_hours:.1f} hours)")
            short_message_parts.append(f"📝 **Total Entries:** {report['total_entries']}")
            short_message_parts.append("")
            
            # Top 3 projects only
            short_message_parts.append("🎯 **TOP PROJECTS:**")
            for project_name, project_data in sorted(report['projects'].items(), 
                                                    key=lambda x: x[1]['seconds'], reverse=True)[:3]:
                project_hours = project_data['seconds'] / 3600
                percentage = (project_data['seconds'] / report['total_seconds'] * 100) if report['total_seconds'] > 0 else 0
                short_message_parts.append(f"• {project_name}: {format_seconds_to_hours_minutes(project_data['seconds'])} ({percentage:.1f}%)")
            
            short_message_parts.append("")
            short_message_parts.append(f"📈 {productivity_emoji} {productivity_text}")
            short_message_parts.append(f"🤖 *Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
            
            full_message = "\n".join(short_message_parts)
        
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

"""
Example JSON structure for toggl_api_tokens.json:
[
    {
        "name": "Flomad Students",
        "api_token": "210bc42419994fc26f4e97d03d5e208f"
    },
    {
        "name": "Flomad Participants 1",
        "api_token": "0e6df8bf00f9cb20d5d5418b1121ca55"
    },
    {
        "name": "Toggl 2 Flomad",
        "api_token": "2d8b16d7923728381944f76b258e8a1a"
    },
    {
        "name": "Flomad Labs R&D 4",
        "api_token": "ea044057fca51c7afd0a670df5958ea5"
    },
    {
        "name": "Participants 5",
        "api_token": "01b41265c55ca83f488f279b54268921"
    },
    {
        "name": "Flomad Labs R&D 6",
        "api_token": "c7e6eff82cbf6ebb73dc1416d42b6665"
    },
    {
        "name": "Flomad Labs EMP",
        "api_token": "07e59620a2f68413d4f4adc87a4c0fc0"
    },
    {
        "name": "Mc Toggl1",
        "api_token": "a927c5d2dbe6cbd852517dda1627928d"
    },
    {
        "name": "Flomad Labs Reg 3",
        "api_token": "d0a234fe27176f93da850e74948c11c3"
    },
    {
        "name": "toggl8.participants@flomad.com",
        "api_token": "4c125d5fd0fd9ba93a4e5bd5b17fc59d"
    }
]
"""
def get_all_weekly_reports():
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
    
    for token in api_tokens:
        headers = get_header(token['api_token'])
        
        workspaces = get_workspaces()
        if not workspaces:
            print(f"No workspaces found for token: {token['name']}")
            continue
        
        for workspace in workspaces:
            WORKSPACE_ID = workspace['id']
            print(f"Workspace ID: {WORKSPACE_ID}, Name: {workspace['name']}")
            print(f"Fetching data for token: {token['name']} of the workspace: {workspace['name']}")
            # Fetch detailed report for the last 7 days
            toggl_data = get_detailed_report(WORKSPACE_ID, datetime.now() - timedelta(days=7), datetime.now())
            
            if toggl_data:
                reports = generate_discord_reports(toggl_data)
                all_reports.extend(reports)
            else:
                print(f"Failed to fetch data for token: {token['name']}")
    
    return all_reports

#******************************************
# Main execution flow
#******************************************
# Example usage:
if __name__ == "__main__":
    
    reports = get_all_weekly_reports()
    if not reports:
        print("No reports generated. Exiting.")
        exit(1)
    
    send_to_discord(reports)
