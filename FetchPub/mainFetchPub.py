from data_fetcher import DataFetcher
from data_publisher import DataPublisher

def main():
    PROJECT_ID = "sp25-cs410-trimet-project"
    TOPIC_ID = "trimet-topic"
    SERVICE_ACCOUNT_FILE = "/home/pjuyoung/term-project/sp25-cs410-trimet-project-service-account.json"
    VEHICLE_ID_CSV = "/home/pjuyoung/term-project/vehicle_ids.csv"

    fetcher = DataFetcher(vehicle_csv_path=VEHICLE_ID_CSV)
    publisher = DataPublisher(project_id=PROJECT_ID, topic_id=TOPIC_ID, service_account_file=SERVICE_ACCOUNT_FILE)

    vehicle_ids = fetcher.load_vehicle_ids()

    for vid in vehicle_ids:
        data = fetcher.fetch_breadcrumb_data(vid)
        if data:
            publisher.publish_data(data, vid)

if __name__ == "__main__":
    main()
