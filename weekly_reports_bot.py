import requests
import base64
from datetime import datetime, timedelta
import json
from collections import defaultdict

# Your Toggl API token
API_TOKEN = "210bc42419994fc26f4e97d03d5e208f"

# === Load user data ===
with open('toggl_users.json') as f:
    users = json.load(f)
    
# Create authentication header
auth_header = base64.b64encode(f"{API_TOKEN}:api_token".encode()).decode()
headers = {
    "Authorization": f"Basic {auth_header}",
    "Content-Type": "application/json"
}

def get_workspaces():
    url = "https://api.track.toggl.com/api/v9/workspaces"
    response = requests.get(url, headers=headers)
    return response.json()

workspaces = get_workspaces()
for workspace in workspaces:
        WORKSPACE_ID = workspace['id']
        # This works because each owner has only one workspace
        print(f"Workspace ID: {WORKSPACE_ID}, Name: {workspace['name']}")

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
            
# Example usage:
if __name__ == "__main__":
    # Define the week range
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)  # Sunday
        
    # Get detailed report for the week
    weekly_reports = get_detailed_report(WORKSPACE_ID, start_of_week, end_of_week)

    # Generate reports
    reports = get_all_weekly_reports(weekly_reports)
    
    # Print reports
    for report in reports:
        print(f"\n{'='*50}")
        print(f"Report for {report['username']} (ID: {report['user_id']})")
        print(f"{'='*50}")
        print(report['message'])
        print(f"Message length: {len(report['message'])} characters")
    
    send_to_discord(reports)
    
    

