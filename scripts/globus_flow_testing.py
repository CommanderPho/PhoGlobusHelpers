import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import globus_sdk

class GlobusFolderMonitor:
    """ Monitors a folder for changes, and uploads to an endpoint when found
    
    Usage:
    
        from scripts.globus_flow_testing import GlobusFolderMonitor

        # Configuration Variables
        SOURCE_ENDPOINT_ID = 'your-source-endpoint-id'
        DESTINATION_ENDPOINT_ID = 'your-destination-endpoint-id'
        SOURCE_PATH = '/path/to/monitor/'
        DESTINATION_PATH = '/destination/path/'
        CLIENT_ID = 'your-client-id'
        CLIENT_SECRET = 'your-client-secret'

        def main():
            monitor = GlobusFolderMonitor(
                source_endpoint_id=SOURCE_ENDPOINT_ID,
                dest_endpoint_id=DESTINATION_ENDPOINT_ID,
                source_path=SOURCE_PATH,
                dest_path=DESTINATION_PATH,
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET
            )
            monitor.run()

        if __name__ == "__main__":
            main()

    """
    def __init__(self, source_endpoint_id, dest_endpoint_id,
                 source_path, dest_path, client_id, client_secret):
        self.source_endpoint_id = source_endpoint_id
        self.dest_endpoint_id = dest_endpoint_id
        self.source_path = source_path
        self.dest_path = dest_path
        self.client_id = client_id
        self.client_secret = client_secret
        self.transfer_client = self._get_transfer_client()
        self.event_handler = self.LogFileChangeHandler(self)
        self.observer = Observer()

    class LogFileChangeHandler(FileSystemEventHandler):
        def __init__(self, monitor):
            self.monitor = monitor

        def on_modified(self, event):
            if not event.is_directory and event.src_path.endswith('.log'):
                self.transfer_file(event.src_path)

        def transfer_file(self, file_path):
            file_name = os.path.relpath(file_path, self.monitor.source_path)
            destination_file = os.path.join(self.monitor.dest_path, file_name)

            transfer_data = globus_sdk.TransferData(
                self.monitor.transfer_client,
                self.monitor.source_endpoint_id,
                self.monitor.dest_endpoint_id
            )
            transfer_data.add_item(file_path, destination_file)

            try:
                transfer_result = self.monitor.transfer_client.submit_transfer(transfer_data)
                print(f'Transfer submitted: Task ID {transfer_result["task_id"]}')
            except Exception as e:
                print(f'Error submitting transfer: {e}')

    def _get_transfer_client(self):
        confidential_client = globus_sdk.ConfidentialAppAuthClient(
            self.client_id, self.client_secret
        )
        scopes = 'urn:globus:auth:scope:transfer.api.globus.org:all'
        token_response = confidential_client.oauth2_client_credentials_tokens(
            requested_scopes=scopes
        )
        transfer_token = token_response.by_resource_server[
            'transfer.api.globus.org']['access_token']
        authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
        transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
        return transfer_client

    def start(self):
        self.observer.schedule(
            self.event_handler, path=self.source_path, recursive=True)
        self.observer.start()
        print(f"Monitoring started on {self.source_path}")

    def stop(self):
        self.observer.stop()
        self.observer.join()
        print("Monitoring stopped.")

    def run(self):
        try:
            self.start()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()


# from scripts.globus_flow_testing import GlobusFolderMonitor

# Configuration Variables
SOURCE_ENDPOINT_ID = '8c185a84-5c61-4bbc-b12b-11430e20010f'
DESTINATION_ENDPOINT_ID = 'cee0aaec-3e3b-11ef-9637-453c3ae125a5'
SOURCE_PATH = '/umms-kdiba/Data/Output/gen_scripts/'
DESTINATION_PATH = '/home/halechr/FastData/gen_scripts/'
CLIENT_ID = '769d24e1-d1cc-4198-9ff7-2626485da449'
CLIENT_SECRET = 'AgQ5950dl3Xnz7O4BoNnpd4M8Qq27lg5QbG0zjnPp0Xzd0zGo0hvC16yjVW19nlaDObxx7VBVr9pM2Sdg9lwgtK7QEW'

def main():
    monitor = GlobusFolderMonitor(
        source_endpoint_id=SOURCE_ENDPOINT_ID,
        dest_endpoint_id=DESTINATION_ENDPOINT_ID,
        source_path=SOURCE_PATH,
        dest_path=DESTINATION_PATH,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
    monitor.run()

if __name__ == "__main__":
    main()
    