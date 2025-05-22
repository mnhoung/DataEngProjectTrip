import json
import os
import time
import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

# Base URL template
BASE_URL = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={}"

# Read vehicle IDs from a CSV file
VEHICLE_ID_CSV = "/home/nhoung/DataEngProjectTrip/vehicle_ids.csv"

def load_vehicle_ids(csv_path):
    vehicle_ids = []
    try:
        with open(csv_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row:  # skip empty rows
                    vehicle_ids.append(row[0].strip())
    except FileNotFoundError:
        print(f"[Error] CSV file not found: {csv_path}")
    return vehicle_ids

# Directory to save output
timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y%m%d')
OUTPUT_DIR = f"../trip_data_{timestamp}/"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_breadcrumb_data(vehicle_id):
    url = BASE_URL.format(vehicle_id)
    try:
        with urlopen(url) as response:
            charset = response.headers.get_content_charset() or 'utf-8'
            data = json.loads(response.read().decode(charset))

            # Timestamped filename
            timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(OUTPUT_DIR, f"vehicle_{vehicle_id}_{timestamp}.json")
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)

            print(f"[✓] Saved data for vehicle {vehicle_id}")
    except Exception as e:
        print(f"[Error] Vehicle {vehicle_id}: {e}")

def main():
    vehicle_ids = load_vehicle_ids(VEHICLE_ID_CSV)
    for vid in vehicle_ids:
        fetch_breadcrumb_data(vid)

if __name__ == "__main__":
    main()
