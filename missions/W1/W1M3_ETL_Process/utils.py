from datetime import datetime

LOG_FILE = 'log/etl_project_with_sql_log.txt'

def log_message(message):
    with open(LOG_FILE, 'a') as f:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"{current_time}, {message}\n")

def log_separator():
    with open(LOG_FILE, 'a') as f:
        f.write("\n" + "="*50 + "\n")
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"🚀 New Execution at {current_time}\n")
        f.write("="*50 + "\n\n")