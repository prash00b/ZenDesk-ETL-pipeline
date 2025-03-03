import json
from datetime import datetime
from dateutil import tz
from config.config import INTEGRATION_UUID

def format_date(date_str):
    """Converts a date string to UTC format."""
    try:
        dt = datetime.fromisoformat(date_str)
        dt_utc = dt.astimezone(tz.UTC)
        return dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-3] + 'Z'
    except ValueError:
        return date_str  # Keep the original date format if it's invalid

def load_ticket_data(filename="test_tickets_batch_1.json"):
    """Loads ticket data from the provided JSON file."""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File {filename} not found.")
        return []
    except json.JSONDecodeError:
        print("Error: JSON decode error.")
        return []

# Load ticket data once to avoid reloading in each transformation call
external_tickets = load_ticket_data()

def transform_ticket(ticket):
    """Transforms a single ticket data and returns it in a structured format."""
    # Ensure the identifier is unique for each ticket
    identifier = ticket.get("Identifier", "Unknown Identifier")
    
    # Building the target map for transformation
    target_map = {
        "Context": {"IntegrationUuid": INTEGRATION_UUID},
        "Content": {
            "CreatedBy": ticket.get("CreatedBy", ""),
            "CreatedDate": format_date(ticket.get("CreatedDate", "")),
            "UpdatedBy": ticket.get("UpdatedBy", ""),
            "UpdatedDate": format_date(ticket.get("UpdatedDate", "")),
            "ClientName": ticket.get("ClientName", ""),
            "CompanySite": ticket.get("CompanySite", ""),
            "Identifier": str(identifier),
            # "Link": ticket.get("Link", ""),
            # "Title": ticket.get("Title", " "),
            "Title": ticket.get("Title") or "Default Title",
            "Description": ticket.get("Description", ""),
            "WorkNotes": ticket.get("WorkNotes", []),
            "TimeEntries": ticket.get("TimeEntries", [])
        },
        "Permissions": []
    }

    final_data = json.dumps(target_map, indent=4)
    
    # # Write final_data to sample.json
    # with open('sample.json', 'w') as json_file:
    #     json_file.write(final_data)
    
    # print(final_data)
    return final_data

# Call the transform_ticket function with the first ticket as a sample
# if external_tickets:
#     transform_ticket(external_tickets[0])  # Process one ticket at a time
# else:
#     print("No tickets available for transformation.")
