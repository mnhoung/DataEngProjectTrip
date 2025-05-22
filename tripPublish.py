import json
from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2 import service_account

class DataPublisher:
    def __init__(self, project_id, topic_id, service_account_file):
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = self.create_publisher(service_account_file)

    def create_publisher(self, service_account_file):
        creds = service_account.Credentials.from_service_account_file(service_account_file)
        return pubsub_v1.PublisherClient(credentials=creds)

    def future_callback(self, future):
        try:
            future.result()
        except Exception as e:
            print(f"An error occurred during publishing: {e}")

    def publish_data(self, data, vehicle_id):
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        future_list = []
        count = 0

        try:
            for obj in data:
                json_data = json.dumps(obj).encode()
                future = self.publisher.publish(topic_path, json_data)
                future.add_done_callback(self.future_callback)
                future_list.append(future)
                count += 1
        except Exception as e:
            print(f"[Error] Vehicle {vehicle_id} publishing failed: {e}")
            return

        for future in futures.as_completed(future_list):
            continue

        print(f"[✓] Vehicle {vehicle_id} published {count} messages to {topic_path}")
