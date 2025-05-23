import pandas as pd

class DataTransformer:
    def __init__(self):
        pass

    def transform(self, df):
        df = df.copy()

        df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
        df['ACT_TIME'] = pd.to_timedelta(df['ACT_TIME'], unit='s', errors='coerce')
        df['tstamp'] = df['OPD_DATE'] + df['ACT_TIME']

        df = df.sort_values(by=['VEHICLE_ID', 'EVENT_NO_TRIP', 'tstamp'])

        df['PREV_METERS'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['METERS'].shift(1)
        df['PREV_TIME'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['tstamp'].shift(1)
        df['DELTA_METERS'] = df['METERS'] - df['PREV_METERS']
        df['DELTA_SECONDS'] = (df['tstamp'] - df['PREV_TIME']).dt.total_seconds()
        df['SPEED'] = df['DELTA_METERS'] / df['DELTA_SECONDS']
        df['SPEED'] = df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP'])['SPEED'].bfill(limit=1)

        # Filter unrealistic speed post-transform
        df = df[df['SPEED'] <= 40]

        # Validate timestamps are increasing
        invalid = set()
        for (vehicle_id, trip_id), group in df.groupby(['VEHICLE_ID', 'EVENT_NO_TRIP']):
            ts = group['tstamp'].values
            for i in range(1, len(ts)):
                if ts[i] <= ts[i - 1]:
                    invalid.add(group.index[i])
        if invalid:
            df = df.drop(index=invalid)

        return df
