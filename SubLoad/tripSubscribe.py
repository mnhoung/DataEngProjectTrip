from google.cloud import pubsub_v1
from google.oauth2 import service_account
from zoneinfo import ZoneInfo
from datetime import datetime
import os
import json
import threading
import pandas as pd
from tripLoadToDB import DBLoader

class DataSubscriber:
    def __init__(self, project_id, subscription_id, service_account_file, output_dir, batch_size=10000):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.service_account_file = service_account_file
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.count = 0
        self.message_list = []
        self.lock = threading.Lock()

        os.makedirs(self.output_dir, exist_ok=True)
        self.subscriber = pubsub_v1.SubscriberClient(
            credentials=service_account.Credentials.from_service_account_file(self.service_account_file)
        )
        self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)

    def start(self):
        self._start_timer()
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages on {self.subscription_path}..\n")

        with self.subscriber:
            try:
                streaming_pull_future.result()
            except:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
            # finally:
                # self._load_to_db()

        print(f"{self.count} messages received")

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        self.count += 1
        message_data = message.data.decode()
        if message_data:
            self._write_file(message_data)
            # with self.lock:
                # self.message_list.append(json.loads(message_data))
                # if len(self.message_list) >= self.batch_size:
                #     self._load_to_db()
        message.ack()

    def _write_file(self, message_data):
        timestamp = datetime.now(ZoneInfo("America/Los_Angeles")).strftime('%Y%m%d')
        filename = os.path.join(self.output_dir, f"recieved_data_{timestamp}.json")
        with open(filename, "a") as file:
            json.dump(json.loads(message_data), file)
            file.write("\n")

    def _load_to_db(self, triggered_by_timer=False):
        print("No Load yet")
        '''
        with self.lock:
            if self.message_list:
                df = pd.DataFrame(self.message_list)
                loader = DBLoader()
                loader.run(df)
                self.message_list = []
        if triggered_by_timer:
            self._start_timer()
            '''

    def _start_timer(self):
        print("Starting 20 minute timer...")
        threading.Timer(1200, lambda: self._load_to_db(triggered_by_timer=True)).start()
