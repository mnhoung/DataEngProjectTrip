import csv
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re

class DataFetcher:
    def __init__(self, vehicle_csv_path):
        self.vehicle_csv_path = vehicle_csv_path
        self.base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={}"

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

    def fetch_trip_data(self, vehicle_id):
        url = self.base_url.format(vehicle_id)
        try:
            with urlopen(url) as response:
                soup = BeautifulSoup(response, "lxml")
                all_dfs = []
                
                # loop through each <h2> for every trip id
                for h2 in soup.find_all('h2'):
                    # extract trip ID from the <h2> text
                    match = re.search(r'PDX_TRIP\s+(\d+)', h2.get_text())
                    if not match:
                        continue
                    trip_id = match.group(1)

                    # get the table after the <h2>
                    table = h2.find_next('table')
                    if not table:
                        continue
                    
                    # Parse rows
                    rows = []
                    for row_tr in table.find_all('tr')[1:]:  # skip header row
                        row_td = row_tr.find_all('td')
                        str_cells = str(row_td)
                        cleantext = BeautifulSoup(str_cells, "lxml").get_text()
                        rows.append(cleantext)
                        
                    # Create DataFrame for this table
                    df = pd.DataFrame(rows)
                    df = df[0].str.split(',', expand=True)
                    df[0] = df[0].str.strip('[')
                    df[23] = df[23].str.strip(']')

                    # Parse header
                    header = table.find_all('th')
                    headers = [th.get_text(strip=True) for th in header]
                    df.columns = headers

                    df['trip_id'] = trip_id  # attach trip ID to each row
                    all_dfs.append(df)
                
                combined_df = pd.concat(all_dfs, ignore_index=True)
                data = combined_df.to_dict(orient="records")
                
                print(f"[✓] Fetched data for vehicle {vehicle_id}")
                return data
        except Exception as e:
            print(f"[Error] Vehicle {vehicle_id}: {e}")
            return None
