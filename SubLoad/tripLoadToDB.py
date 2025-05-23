import psycopg2
import io
import os
from dotenv import load_dotenv
from tripValidate import DataValidator
from tripTransform import DataTransformer

load_dotenv()

class DBLoader:
    def __init__(self):
        self.dbname = 'postgres'
        self.dbuser = 'postgres'
        self.dbpass = os.getenv("DBPASS")
        self.conn = None
        self.trip_table = 'trip'
        self.breadcrumb_table = 'breadcrumb'
        self.validator = DataValidator()
        self.transformer = DataTransformer()

    def connect(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database=self.dbname,
            user=self.dbuser,
            password=self.dbpass,
        )
        self.conn.autocommit = True

    def load(self, df):
        self.connect()
        cursor = self.conn.cursor()

        trip_csv = io.StringIO()
        trip_df = df[['EVENT_NO_TRIP', 'VEHICLE_ID']].drop_duplicates()
        trip_df['route_id'] = None
        trip_df['service_key'] = None
        trip_df['direction'] = None
        trip_df[['EVENT_NO_TRIP', 'route_id', 'VEHICLE_ID', 'service_key', 'direction']].to_csv(trip_csv, index=False, header=False)
        trip_csv.seek(0)
        cursor.copy_from(trip_csv, self.trip_table, sep=",")

        breadcrumb_csv = io.StringIO()
        df[['tstamp', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].to_csv(breadcrumb_csv, index=False, header=False)
        breadcrumb_csv.seek(0)
        cursor.copy_from(breadcrumb_csv, self.breadcrumb_table, sep=",")

        cursor.close()

        print(f"Loaded {len(trip_df)} trip records.")
        print(f"Loaded {len(df)} breadcrumb records.")
        
    def run(self, df):
        valid_df = self.validator.run_validations(df)
        transformed_df = self.transformer.transform(valid_df)
        self.load(transformed_df)
