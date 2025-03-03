import requests
import os
import time
import json
from log import setup_logging  # Import logging setup
from dotenv import load_dotenv



load_dotenv()

fetch_logger_dir = "Logs/Tickets"

success_logger, error_logger, console_logger = setup_logging(fetch_logger_dir)

# Fetch from environment variables
ZENDESK_API_TOKEN = os.getenv('ZENDESK_API_TOKEN')
ZENDESK_SUBDOMAIN = os.getenv('ZENDESK_SUBDOMAIN')
ZENDESK_USER_MAIL = os.getenv('ZENDESK_USER_MAIL')

headers = {"Content-Type": "application/json"}
base_url = f'https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets.json?page[size]=100'
auth = (f'{ZENDESK_USER_MAIL}/token', ZENDESK_API_TOKEN)

ticket_count = 0
all_tickets = []

def fetch_tickets(url):
    global ticket_count
    try:
        response = requests.get(url, auth=auth, headers=headers, timeout=30)

        if response.status_code == 429:
            error_logger.warning("Rate limit exceeded. Waiting before retrying...")
            time.sleep(60)
            return fetch_tickets(url)

        if response.status_code != 200:
            error_logger.error(f"Error {response.status_code}: {response.text}")
            return None

        data = response.json()
        tickets = data.get('tickets', [])
        ticket_count += len(tickets)

        success_logger.info(f"Retrieved {len(tickets)} tickets (Total: {ticket_count})")
        console_logger.info(f"Retrieved {len(tickets)} tickets (Total: {ticket_count})")

        # Write tickets to file after each batch
        with open("tickets_raw.json", "a", encoding="utf-8") as f:
            json.dump(tickets, f, indent=4)

        next_url = data.get('links', {}).get('next')
        return tickets, next_url

    except requests.exceptions.Timeout:
        error_logger.error("Request timed out. Retrying in 30 seconds...")
        time.sleep(30)
        return fetch_tickets(url)

    except requests.exceptions.RequestException as e:
        error_logger.error(f"Request failed: {e}")
        return None

def main():
    global all_tickets
    url = base_url  

    while url:
        result = fetch_tickets(url)
        if result:
            tickets, next_url = result
            all_tickets.extend(tickets)
            url = next_url
        else:
            break

    success_logger.info(f"Total tickets retrieved: {ticket_count}")
    success_logger.info(f"Data saved to tickets_raw.json")
    console_logger.info(f"Total tickets retrieved: {ticket_count}")
    console_logger.info(f"Data saved to tickets_raw.json")

if __name__ == "__main__":
    main()
