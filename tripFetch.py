import csv
import json
from urllib.request import urlopen

class DataFetcher:
    def __init__(self, vehicle_csv_path):
        self.vehicle_csv_path = vehicle_csv_path
        self.base_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={}"

    def load_vehicle_ids(self):
        vehicle_ids = []
        try:
            with open(self.vehicle_csv_path, newline='') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if row:
                        vehicle_ids.append(row[0].strip())
        except FileNotFoundError:
            print(f"[Error] CSV file not found: {self.vehicle_csv_path}")
        return vehicle_ids

    def fetch_breadcrumb_data(self, vehicle_id):
        url = self.base_url.format(vehicle_id)
        try:
            with urlopen(url) as response:
                charset = response.headers.get_content_charset() or 'utf-8'
                data = json.loads(response.read().decode(charset))
                print(f"[✓] Fetched data for vehicle {vehicle_id}")
                return data
        except Exception as e:
            print(f"[Error] Vehicle {vehicle_id}: {e}")
            return None
