from modules.auth import AuthManager
from modules.api import APIManager
from modules.data_transformation import transform_ticket
from config.config import AUTH_URL, CLIENT_ID, CLIENT_SECRET, TICKET_API_URL, BATCH_SIZE
import json
import time
import concurrent.futures
from datetime import datetime
import os

def find_batch_files(directory, target_identifiers):
    batch_files = set()
    for i in range(1, 110):
        filename = os.path.join(directory, f"batch_{i}_tickets.json")
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                tickets = json.load(json_file)
                for ticket in tickets:
                    if str(ticket.get("Identifier")) in target_identifiers:
                        batch_files.add(filename)
                        break  # No need to read further, move to next file
        except FileNotFoundError:
            print(f"File not found: {filename}")
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    return batch_files

def load_ticket_identifiers(directory="C:\\TigerPaw Ingestion\\Zendesk\\Transformation4\\output"):
    all_tickets = {}
    target_identifiers = {"683552", "683554", "668973", "194254", "685587"}  # Only these identifiers
    batch_files = find_batch_files(directory, target_identifiers)
    
    for filename in batch_files:
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                tickets = json.load(json_file)
                for ticket in tickets:
                    ticket_id = str(ticket.get("Identifier"))
                    if ticket_id in target_identifiers:
                        all_tickets[ticket_id] = ticket
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    
    print(f"Total tickets retrieved: {len(all_tickets)}")
    return all_tickets

def main():
    auth_manager = AuthManager(AUTH_URL, CLIENT_ID, CLIENT_SECRET)
    api_manager = APIManager(TICKET_API_URL, auth_manager)

    token, token_expiry_time = auth_manager.get_bearer_token()
    if not token:
        print("Error: Failed to fetch authentication token.")
        return
    print(f"Token received: {token}, expires on: {datetime.fromtimestamp(token_expiry_time)}")

    transformed_data = []
    ticket_data = load_ticket_identifiers()

    def process_ticket(ticket_id):
        nonlocal token, token_expiry_time
        if not token:
            return None
        
        ticket = ticket_data.get(ticket_id, {})
        if not ticket:
            print(f"Ticket with ID {ticket_id} not found in the loaded data.")
            return None
        
        return transform_ticket(ticket)

    ticket_identifiers = list(ticket_data.keys())
    print(f"Processing {len(ticket_identifiers)} ticket identifiers")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_ticket = {executor.submit(process_ticket, ticket_id): ticket_id for ticket_id in ticket_identifiers}
        
        for future in concurrent.futures.as_completed(future_to_ticket):
            transformed_ticket = future.result()
            if transformed_ticket:
                transformed_data.append(transformed_ticket)
                
                if len(transformed_data) >= BATCH_SIZE:
                    current_time = time.time()
                    if current_time >= token_expiry_time:
                        token, token_expiry_time = auth_manager.get_bearer_token()
                        if not token:
                            print("Error: Failed to refresh authentication token.")
                            return
                    print(f"Sending batch of {len(transformed_data)} tickets to API")
                    api_manager.send_data(transformed_data)
                    transformed_data = []

    if transformed_data:
        current_time = time.time()
        if current_time >= token_expiry_time:
            token, token_expiry_time = auth_manager.get_bearer_token()
            if not token:
                print("Error: Failed to refresh authentication token.")
                return
        print(f"Sending final batch of {len(transformed_data)} tickets to API")
        api_manager.send_data(transformed_data)

if __name__ == "__main__":
    main()