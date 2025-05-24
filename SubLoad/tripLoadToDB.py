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
        df.to_csv(trip_csv, index=False, header=False)
        trip_csv.seek(0)

        cursor.copy_from(trip_csv, self.trip_table, sep=",")
        cursor.close()

        print(f"Loaded {len(df)} trip records into `{self.trip_table}`.")

    def run(self, df):
        print("Running validation...")
        valid_df = self.validator.run_validations(df)
        print("Running transformation...")
        transformed_df = self.transformer.transform(valid_df)
        print("Loading to database...")
        self.load(transformed_df)
