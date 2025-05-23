import pandas as pd

class DataValidator:
    def __init__(self):
        pass

    def run_validations(self, df):
        df = df.copy()
        invalid_indices = set()

        # Convert columns to numeric types
        df['GPS_LATITUDE'] = pd.to_numeric(df['GPS_LATITUDE'], errors='coerce')
        df['GPS_LONGITUDE'] = pd.to_numeric(df['GPS_LONGITUDE'], errors='coerce')
        df['METERS'] = pd.to_numeric(df['METERS'], errors='coerce')
        df['GPS_SATELLITES'] = pd.to_numeric(df['GPS_SATELLITES'], errors='coerce')
        df['ACT_TIME'] = pd.to_numeric(df['ACT_TIME'], errors='coerce')
        df['GPS_HDOP'] = pd.to_numeric(df['GPS_HDOP'], errors='coerce')

        # Apply individual validations
        invalid_indices.update(self._required_fields_not_null(df))
        invalid_indices.update(self._latitude_limits(df))
        invalid_indices.update(self._longitude_limits(df))
        invalid_indices.update(self._meters_nonnegative(df))
        invalid_indices.update(self._satellites_range(df))
        invalid_indices.update(self._act_time(df))
        invalid_indices.update(self._hdop(df))
        invalid_indices.update(self._duplicates(df))

        if invalid_indices:
            print(f"Dropping {len(invalid_indices)} invalid rows.")
            df = df.drop(index=invalid_indices)

        return df

    def _required_fields_not_null(self, df):
        try:
            required = ['VEHICLE_ID', 'EVENT_NO_TRIP', 'ACT_TIME', 'OPD_DATE', 'GPS_LATITUDE', 'GPS_LONGITUDE']
            for col in required:
                assert df[col].notnull().all(), f"{col} has null values"
        except AssertionError as e:
            print(f"Required Fields Assertion Error: {e}")
            invalid = df[df[required].isnull().any(axis=1)].index
            return invalid
        return []

    def _latitude_limits(self, df):
        return df[~df['GPS_LATITUDE'].between(45, 46)].index

    def _longitude_limits(self, df):
        return df[~df['GPS_LONGITUDE'].between(-123, -122)].index

    def _meters_nonnegative(self, df):
        return df[df['METERS'] < 0].index

    def _satellites_range(self, df):
        return df[~df['GPS_SATELLITES'].between(3, 31)].index

    def _act_time(self, df):
        return df[df['ACT_TIME'] > 86400].index

    def _hdop(self, df):
        return df[(df['GPS_SATELLITES'].isna()) & (df['GPS_HDOP'].notna())].index

    def _duplicates(self, df):
        return df[df.duplicated(subset=['VEHICLE_ID', 'OPD_DATE', 'ACT_TIME'], keep=False)].index
