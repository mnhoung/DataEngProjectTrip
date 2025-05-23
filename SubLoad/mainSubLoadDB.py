from tripSubscribe import DataSubscriber

def main():
    PROJECT_ID = "sp25-cs410-trimet-project"
    SUBSCRIPTION_ID = "trip-sub"
    BASE_DIR = "/home/nhoung/DataEngProjectTrip/"
    SERVICE_ACCOUNT_FILE = BASE_DIR + "sp25-cs410-trimet-project-service-account.json"
    OUTPUT_DIR = "/home/nhoung/recieved_data/"
    BATCH_SIZE = 10000

    subscriber = DataSubscriber(
        project_id=PROJECT_ID,
        subscription_id=SUBSCRIPTION_ID,
        service_account_file=SERVICE_ACCOUNT_FILE,
        output_dir=OUTPUT_DIR,
        batch_size=BATCH_SIZE
    )
    subscriber.start()

if __name__ == "__main__":
    main()
