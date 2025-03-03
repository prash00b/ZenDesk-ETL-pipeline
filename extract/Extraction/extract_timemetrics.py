import os
import json
import time
import requests
import shutil
import concurrent.futures
from log import setup_logging
from dotenv import load_dotenv

# Initialize loggers
fetch_logger_dir = "Logs/time_metric"
success_logger, error_logger, console_logger = setup_logging(fetch_logger_dir)

load_dotenv()

# Fetch from environment variables
ZENDESK_API_TOKEN = os.getenv('ZENDESK_API_TOKEN')
ZENDESK_SUBDOMAIN = os.getenv('ZENDESK_SUBDOMAIN')
ZENDESK_USER_MAIL = os.getenv('ZENDESK_USER_MAIL')

headers = {"Content-Type": "application/json"}
auth = (f'{ZENDESK_USER_MAIL}/token', ZENDESK_API_TOKEN)

# Global variables
request_count = 0
request_reset_time = None
BATCH_SIZE = 10000  # Save every 10,000 records

def monitor_rate_limit(response):
    global request_count, request_reset_time
    rate_limit_remaining = int(response.headers.get('X-Rate-Limit-Remaining', 1))

    if rate_limit_remaining == 0:
        reset_time = int(response.headers.get('X-Rate-Limit-Reset', time.time()))
        request_reset_time = reset_time
        wait_time = reset_time - time.time() + 1

        if wait_time > 0:
            console_logger.warning(f"Rate limit reached. Sleeping for {wait_time} seconds.")
            time.sleep(wait_time)
            request_count = 0
    else:
        request_count += 1

def fetch_time_metric(ticket_id, retry_count=0):
    """Fetch time_metric from Zendesk for a given ticket ID."""
    url = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets/{ticket_id}/metrics'

    try:
        response = requests.get(url, auth=auth, headers=headers, timeout=30)

        # Handle rate limits
        monitor_rate_limit(response)

        if response.status_code == 429:
            wait_time = 60 * (retry_count + 1)  # Exponential backoff
            error_logger.warning(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            return fetch_time_metric(ticket_id, retry_count + 1)

        if response.status_code != 200:
            error_logger.error(f"Error {response.status_code} for ticket {ticket_id}: {response.text}")
            return None, 0

        data = response.json()
        time_metric = data.get('ticket_metric', [])
        count = data.get('count', len(time_metric))  # Extract count (default to len if missing)

        return time_metric, count

    except requests.exceptions.Timeout:
        error_logger.error(f"Request timed out for ticket {ticket_id}. Retrying in 30 seconds...")
        time.sleep(30)
        return fetch_time_metric(ticket_id, retry_count + 1)

    except requests.exceptions.RequestException as e:
        error_logger.error(f"Request failed for ticket {ticket_id}: {e}")
        return None, 0

def save_time_metric_chunked(ticket_time_metric, batch_number):
    """Save 10,000 records per file."""
    filename = f"time_metric_{batch_number}.json"
    try:
        with open(filename, 'w', encoding='utf-8') as output_file:
            json.dump(ticket_time_metric, output_file, indent=4)
        console_logger.info(f"Saved {len(ticket_time_metric)} records to {filename}")
    except IOError as e:
        error_logger.error(f"Error saving to {filename}: {e}")

def process_tickets():
    """Process tickets by fetching time_metric and saving data in chunks of 10,000."""
    ticket_time_metric = {}
    batch_number = 1
    processed_count = 0

    # File path for tickets_processed.json
    file_path = r'C:\TigerPaw Ingestion\scripts\extract\tickets_processed.json'

    # Read the tickets_processed.json file to get ticket IDs
    try:
        with open(file_path, 'r', encoding='utf-8') as json_file:
            tickets_data = json.load(json_file)
            ticket_ids = [ticket.get('id') for ticket in tickets_data if 'id' in ticket]

        if not ticket_ids:
            console_logger.error("No ticket IDs found in the JSON file.")
            return

    except Exception as e:
        console_logger.error(f"Error reading the JSON file: {e}")
        return

    console_logger.info(f"Total tickets to process: {len(ticket_ids)}")

    # Use ThreadPoolExecutor to process multiple tickets concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_time_metric, ticket_id): ticket_id for ticket_id in ticket_ids}

        for future in concurrent.futures.as_completed(futures):
            ticket_id = futures[future]
            try:
                time_metric, count = future.result()
                if time_metric is not None:
                    ticket_time_metric[ticket_id] = time_metric
                    processed_count += 1

                # Save progress every BATCH_SIZE tickets
                if processed_count % BATCH_SIZE == 0:
                    save_time_metric_chunked(ticket_time_metric, batch_number)
                    batch_number += 1
                    ticket_time_metric.clear()  # Clear memory after saving

                # Log progress
                success_logger.info(f"Processed {processed_count}/{len(ticket_ids)} tickets")
                console_logger.info(f"Processed {processed_count}/{len(ticket_ids)} tickets")

            except Exception as exc:
                error_logger.error(f"Error processing ticket {ticket_id}: {exc}")
                continue

    # Save any remaining records
    if ticket_time_metric:
        save_time_metric_chunked(ticket_time_metric, batch_number)

def main():
    process_tickets()

if __name__ == "__main__":
    main()
