import pandas as pd

class DataTransformer:
    def __init__(self):
        pass

    def transform(self, df):
        df = df.copy()

        # Only apply mapping and structural cleanup
        df['service_key'] = df['service_key'].map({
            'W': 'Weekday',
            'S': 'Saturday',
            'U': 'Sunday'
        })

        # Drop duplicate trip-vehicle pairs
        df = df.drop_duplicates(subset=['trip_number', 'vehicle_number'])

        # Rename columns to match DB schema
        df.rename(columns={
            'trip_number': 'trip_id',
            'route_number': 'route_id',
            'vehicle_number': 'vehicle_id'
        }, inplace=True)

        # Keep only relevant columns
        return df[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]
