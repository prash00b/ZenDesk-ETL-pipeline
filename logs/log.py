import os
import csv
from datetime import datetime

class Logger:
    def __init__(self, log_prefix, log_directory="RE-IngestionLogs2"): #chaange the directory for reingestion
        self.log_prefix = log_prefix
        self.log_directory = log_directory  # Default directory for logs is 'logs'

        # Create the log directory if it doesn't exist
        os.makedirs(self.log_directory, exist_ok=True)

    def log_to_csv(self, filename, data, header):
        filepath = os.path.join(self.log_directory, filename)  # Create the full file path
        file_exists = os.path.isfile(filepath)
        with open(filepath, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(header)  # Write header if file doesn't exist
            writer.writerows(data)  # Write multiple records at once

    def log_success(self, identifier, response_text):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        success_filename = f'{timestamp[:10]}_{self.log_prefix}_success.csv'
        header = ['Timestamp', 'Identifier', 'Status', 'ResponseText']
        self.log_to_csv(success_filename, [[timestamp, identifier, 'Success', response_text]], header)

    def log_error(self, identifier, error_message, response_text):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_filename = f'{timestamp[:10]}_{self.log_prefix}_error.csv'
        header = ['Timestamp', 'Identifier', 'Status', 'ErrorMessage', 'ResponseText']
        self.log_to_csv(error_filename, [[timestamp, identifier, 'Error', error_message, response_text]], header)

    def log_info(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info_filename = f'{timestamp[:10]}_{self.log_prefix}_info.csv'
        header = ['Timestamp', 'Level', 'Message']
        self.log_to_csv(info_filename, [[timestamp, 'INFO', message]], header)

    def flush(self):
        """No longer needed as logs are written immediately."""
        pass
