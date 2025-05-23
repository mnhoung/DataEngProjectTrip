import csv
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd

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
                data = df.to_dict()
                
                print(f"[✓] Fetched data for vehicle {vehicle_id}")
                return data
        except Exception as e:
            print(f"[Error] Vehicle {vehicle_id}: {e}")
            return None
