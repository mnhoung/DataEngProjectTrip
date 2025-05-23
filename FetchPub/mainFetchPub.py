from tripFetch import DataFetcher
from tripPublish import DataPublisher

def main():
    PROJECT_ID = "sp25-cs410-trimet-project"
    TOPIC_ID = "trip-topic"
    HOME_DIR = "/home/nhoung/DataEngProjectTrip/"
    SERVICE_ACCOUNT_FILE = HOME_DIR + "sp25-cs410-trimet-project-service-account.json"
    VEHICLE_ID_CSV = HOME_DIR + "vehicle_ids.csv"

    fetcher = DataFetcher(VEHICLE_ID_CSV)
    publisher = DataPublisher(PROJECT_ID, TOPIC_ID, SERVICE_ACCOUNT_FILE)

    vehicle_ids = fetcher.load_vehicle_ids()

    for vid in vehicle_ids:
        data = fetcher.fetch_trip_data(vid)
        if data:
            publisher.publish_data(data, vid)

if __name__ == "__main__":
    main()
