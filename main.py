import os
import sqlite3
import requests
import json
import subprocess
import argparse
import shutil
import tempfile
from datetime import datetime, timedelta, timezone

parser = argparse.ArgumentParser(description="Extract logs from Docker containers and send them to Loki.")
parser.add_argument("--loki-hostname", default="localhost", help="Hostname of the Loki server.")
parser.add_argument("--loki-port", default="3100", help="Port of the Loki server.")
parser.add_argument("--containers-dir", default="/volume1/@docker/containers", help="Directory where Docker containers are stored.")
parser.add_argument("--time-range", default=30, help="How many minutes worth of logs to send to Loki.")
args = parser.parse_args()

# Loki Push API URL
LOKI_URL = f"http://{args.loki_hostname}:{args.loki_port}/loki/api/v1/push"
CONTAINERS_DIR = args.containers_dir

def find_log_db_files(base_directory):
    log_db_files = []
    for root, _, files in os.walk(base_directory):
        for file in files:
            if file == "log.db":
                log_db_files.append(os.path.join(root, file))
    return log_db_files

current_time = datetime.now(timezone.utc) 
cutoff_time = current_time - timedelta(minutes=int(args.time_range))

def get_parent_directory(path):
    if os.path.isfile(path):
        path = os.path.dirname(path)
    return os.path.basename(os.path.normpath(path))

# Function to extract logs from the database
def extract_logs(db_path):
    logs = []
    
    temp_dir = tempfile.gettempdir()
    temp_db = os.path.join(temp_dir, "temp_log.db")
    shutil.copy(db_path, temp_db)
    
    connection = sqlite3.connect(temp_db)
    connection.execute("PRAGMA busy_timeout = 60000")
    cursor = connection.cursor()
    try:
      query = "SELECT created, text FROM log WHERE created >= ?;"
      cursor.execute(query, (cutoff_time.isoformat() + 'Z',))
      rows = cursor.fetchall()
    finally:
      cursor.close()
      connection.close()
      if os.path.exists(temp_db):
            os.remove(temp_db)
      
    for row in rows:
        text = row[1]
        dt = datetime.fromisoformat(row[0].replace("Z", "+00:00"))
        # Convert to epoch (nanoseconds)
        epoch_ns = int(dt.timestamp() * 1_000_000_000)
        logs.append([str(epoch_ns), text])

    return logs

def get_container_name(container_id):
    result = subprocess.run(
          ["docker", "ps", "-a", "--filter", f"id={container_id}", "--format", "{{.Names}}"],
          capture_output=True, text=True, check=True
    )
    container_name = result.stdout.strip()
    return container_name

# Function to send logs to Loki
def send_to_loki(container_name, container_id, logs):
    if not logs:
        print("No new logs to send.")
        return
    headers = {"Content-Type": "application/json"}
    payload = {
        "streams": [
            {
                "stream": {
                    "job": "docker-logs",
                    "service_name": container_name,
                    "container_id": container_id
                },
                "values": logs
            }
        ]
    }
    response = requests.post(LOKI_URL, headers=headers, data=json.dumps(payload))
    
    print(payload)
    
    if response.status_code == 204:
        print("Logs successfully sent to Loki.")
    else:
        print(f"Failed to send logs to Loki: {response.status_code} - {response.text}")

# Main execution
if __name__ == "__main__":
    log_db_files = find_log_db_files(CONTAINERS_DIR)

    # Print the results
    if log_db_files:
        print("Found the following log.db files:")
        for file in log_db_files:
            print(file)
    else:
        print("No log.db files found.")
        os.abort()
    
    print(f"Fetching logs newer than {cutoff_time.isoformat()}Z...")
    
    for log_file in log_db_files:
      container_id = get_parent_directory(log_file)
      container_name = get_container_name(container_id) 
      print(container_name)
      print(container_id)
      logs = extract_logs(log_file)
      
      print(len(logs))
      
      send_to_loki(container_name, container_id, logs)
