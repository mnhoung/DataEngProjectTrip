import pandas as pd

class DataValidator:
    def __init__(self):
        pass

    def run_validations(self, df):
        df = df.copy()
        invalid_indices = set()

        # Clean string fields (remove leading/trailing whitespace)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Convert types
        df['trip_number'] = pd.to_numeric(df['trip_number'], errors='coerce')
        df['route_number'] = pd.to_numeric(df['route_number'], errors='coerce')
        df['vehicle_number'] = pd.to_numeric(df['vehicle_number'], errors='coerce')
        df['direction'] = pd.to_numeric(df['direction'], errors='coerce')

        # Apply validations
        invalid_indices.update(self._required_fields(df))
        invalid_indices.update(self._direction_values(df))
        invalid_indices.update(self._service_key_values(df))
        invalid_indices.update(self._trip_id_uniqueness(df))

        if invalid_indices:
            print(f"Dropping {len(invalid_indices)} invalid rows.")
            df = df.drop(index=invalid_indices)

        return df

    def _required_fields(self, df):
        required = ['trip_number', 'route_number', 'vehicle_number', 'service_key', 'direction']
        return df[df[required].isnull().any(axis=1)].index

    def _direction_values(self, df):
        return df[~df['direction'].isin([0, 1])].index

    def _service_key_values(self, df):
        valid_keys = ['W', 'S', 'U']
        return df[~df['service_key'].isin(valid_keys)].index

    def _trip_id_uniqueness(self, df):
        return df[df.duplicated(subset=['trip_number', 'vehicle_number'], keep=False)].index
