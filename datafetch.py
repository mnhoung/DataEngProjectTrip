import json
import os
import time
import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from bs4 import BeautifulSoup
import pandas as pd

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
            soup = BeautifulSoup(response, "lxml")
            rows = soup.find_all('tr')
            # clean the html
            clean_list = []
            for row in rows:
                row_td = row.find_all('td')
                str_cells = str(row_td)
                cleantext = BeautifulSoup(str_cells, "lxml").get_text()
                clean_list.append(cleantext)
            # change to df for easy transformation
            df = pd.DataFrame(clean_list)
            df = df[0].str.split(',', expand=True)
            df[0] = df[0].str.strip('[')
            df[23] = df[23].str.strip(']')
            # grab and transform the header labels
            col_labels = soup.find_all('th')
            all_header = []
            col_str = str(col_labels)
            cleantext2 = BeautifulSoup(col_str, "lxml").get_text()
            all_header.append(cleantext2)
            # get rid of duplicate header labels
            header_str = [th.get_text(strip=True) for th in col_labels]
            header_list = list(dict.fromkeys(header_str))
            # add header list as to the table
            df.columns = header_list
            # drop NaN columns
            df = df.dropna(axis=0, how='any')
            data = df.to_json(orient='records', indent=2)
            

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
