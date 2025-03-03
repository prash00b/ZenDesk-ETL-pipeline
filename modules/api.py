import requests
import json
from logs.log import Logger
from datetime import datetime
import time

class APIManager:
    def __init__(self, api_url, auth_manager, log_prefix="api_logs"):
        self.api_url = api_url
        self.auth_manager = auth_manager
        self.token, self.token_expiry_time = self.auth_manager.get_bearer_token()
        self.logger = Logger(log_prefix)  # Initialize the Logger with a log prefix

    def send_data(self, data_batch):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'  
        }

        for record in data_batch:
            try:
                current_time = time.time()
                if current_time >= self.token_expiry_time:
                    self.token, self.token_expiry_time = self.auth_manager.get_bearer_token()
                    if not self.token:
                        print("Error: Failed to refresh authentication token.")
                        return
                    headers['Authorization'] = f'Bearer {self.token}'

                json_data = record
                # json_data = json.dumps(record, ensure_ascii=True)
                # print("from_apis", json_data)
                record_dict = json.loads(json_data)
                print("record_dict", record_dict)
                print(f"Type of record_dict: {type(record_dict)}")
                response = requests.post(self.api_url, data=json_data, headers=headers)
                print(response.text)
                response.raise_for_status()
                print(response.raise_for_status())
                # record_dict = json.dumps(json_data)
                print(f"Type of nextreor: {type(record_dict)}")
                identifier = record_dict.get('Content', {}).get('Identifier', 'N/A')
                print(f"Identifier: {identifier}")
                self.logger.log_success(identifier, response.text)

            except requests.exceptions.RequestException as e:
                print(f"Error sending data to API: {e}")
                error_message = str(e)
                if hasattr(e.response, 'text'):
                    print(f"Error response: {e.response.text}")
                    try:
                        error_details = e.response.text
                        error_message = json.dumps(error_details)
                    except ValueError:
                        error_message = e.response.text

                # Parse json_data back to dictionary to access the identifier
                try:
                    record_dict = json.loads(json_data)  # This incorrectly converts it to a string
                    identifier = record_dict.get('Content', {}).get('Identifier', 'N/A')  # Fails because strings don’t have `.get()`
                    self.logger.log_error(error_message, identifier, e.response.text)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON data: {e}")
                    self.logger.log_error(error_message, identifier, f"Unexpected Error: {str(e)}")
                except Exception as e:
                    print(f"Error in processing: {e}")
                    self.logger.log_error(error_message, identifier, f"Unexpected Error: {str(e)}")

        # After sending all data, flush the log buffers
        self.logger.flush()
