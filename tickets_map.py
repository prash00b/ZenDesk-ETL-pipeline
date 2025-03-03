import json
import csv
import os

DEFAULT_DATE = "2012-12-15T00:00:00Z"
DEFAULT_USER = "Default User"
DEFAULT_TITLE = "Default Title"
DEFAULT_DESC = "Default Description"
DEFAULT_NOTE = "Default Note Title"
LINK = "https://placeholder.com"
DEFAULT_SITE = "Default Site"
DEFAULT_CLIENT_NAME = "Default Client Name"


def load_csv_mapping_with_domain(csv_filename):
    mapping = {}
    with open(csv_filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            mapping[row['id']] = {
                'name': row['name'],
                'domain': row.get('domain_names', 'Unknown Domain')  
            }
    return mapping

def extract_comment_info(comment_content):
    try:
        # Print the type and content for debugging
        print(f"Type of comment_content: {type(comment_content)}")
        print(f"comment_content (raw): {comment_content[:100]}")  # print first 100 characters for inspection
        
        # Check if the comment_content is a non-empty string before attempting to parse
        if isinstance(comment_content, str):
            if not comment_content.strip():
                print("ERROR: comment_content is an empty string")
                return [{"Error": "Empty string, no content to process"}]
            
            print("comment_content is a string. Attempting to parse as JSON.")
            # Try parsing the string as JSON
            try:
                comment_content = json.loads(comment_content)
                if not isinstance(comment_content, list):
                    raise TypeError("Parsed comment_content is not a list")
            except json.JSONDecodeError as e:
                print(f"ERROR: Failed to parse comment_content as JSON - {str(e)}")
                return [{"Error": "Invalid JSON format"}]
        
        elif not isinstance(comment_content, list):
            raise TypeError("comment_content must be a list or a JSON string")

        # Process the comment content as a list
        extracted_comments = []
        for comment in comment_content:
            if isinstance(comment, dict):  # Ensure each comment is a dictionary
                extracted_comments.append({
                    "Description": comment.get('body', 'Unknown comment text'),
                    "date": comment.get('created_at', 'Unknown date'),
                    "Type": "External" if comment.get("public", False) else "Internal"
                })
            else:
                extracted_comments.append({
                    "Error": f"Invalid comment structure: {comment}"
                })

        return extracted_comments

    except Exception as e:
        print(f"ERROR: Unexpected issue in extract_comment_info - {str(e)}")
        return [{"Error": str(e)}]


# def extract_comment_info(comment_content):
#     try:

#         extracted_comments = []
#         for comment in comment_content:  
#             extracted_comments.append({
#                 "Description": comment.get('body', 'Unknown comment text'),
#                 "date": comment.get('created_at', 'Unknown date'),
#                 "Type": "External" if comment.get("public", False) else "Internal"
#             })
#         # print("comments",extracted_comments)
#         return extracted_comments  
#     except Exception as e:
#         print(f"ERROR: Unexpected issue in extract_comment_info - {str(e)}")
#         return [{"Error": str(e)}]
    
def extract_time_entry_info(time_entry_content):
    try:
        # print(f"DEBUG: Processing time entries - {json.dumps(time_entry_content, indent=4)}")
        # print(f"Type: {type(time_entry_content)}")

        extracted_time_entries = []

        # Check if time_entry_content is a list (multiple time entries)
        if isinstance(time_entry_content, list):
            for entries in time_entry_content:
                extracted_time_entries.append({
                    "Description": entries.get('description', DEFAULT_DESC),
                    "startDate": entries.get('created_at', DEFAULT_DATE),
                    "endDate": entries.get('updated_at', DEFAULT_DATE),
                    "Type": "External" if entries.get("public", False) else "Internal"
                })
        else:
            # If it's a single dictionary (a single time entry)
            extracted_time_entries.append({
                "Description": time_entry_content.get('description', DEFAULT_DATE),
                "startDate": time_entry_content.get('created_at', DEFAULT_DATE),
                "endDate": time_entry_content.get('updated_at', DEFAULT_DATE),
                "Type": "External" if time_entry_content.get("public", False) else "Internal"
            })

        # print("time_entries", extracted_time_entries)
        return extracted_time_entries  
    except Exception as e:
        print(f"ERROR: Unexpected issue in extract_time_entry_info - {str(e)}")
        return [{"Error": str(e)}]


# Load the CSV data into a dictionary (for submitter and assignee without domain)
def load_csv_mapping(csv_filename):
    mapping = {}
    with open(csv_filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            mapping[row['id']] = row['name']  
    return mapping

def load_comments_mapping(csv_filename):
    mapping = {}
    with open(csv_filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        print(reader)
        for row in reader:
            mapping[row['ticket_id']] = {
                            'comment_id': row['comment_id'],
                            'comment_files': row['file']
                        }
    return mapping

def load_time_entries_mapping(csv_filename):
    mapping = {}
    with open(csv_filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        print(reader)
        for row in reader:
            mapping[row['ticket_id']] = {
                            'time_metric_id': row['time_metric_id'],
                            'time_entry_files': row['file']
                        }
    return mapping


# Function to read the content from files
import json

def load_file_content(file_path, ticket_id):
    try:
        with open(file_path, 'r') as file:
            # Load the JSON file incrementally to avoid memory issues
            data = json.load(file)
            
            # Check if the ticket_id exists and return its associated data, 
            # otherwise return a message stating the ticket ID was not found
            if str(ticket_id) in data:
                return data[str(ticket_id)]
            else:
                return f'Ticket ID {ticket_id} not found in the file'
    
    except FileNotFoundError:
        return f'File not found: {file_path}'
    except json.JSONDecodeError:
        return 'Invalid JSON format in file'
    except Exception as e:
        return f'An unexpected error occurred: {str(e)}'


def load_json(filename):
    """Load JSON data from a file."""
    with open(filename, 'r') as file:
        return json.load(file)

def extract_ticket_data(ticket, field_mappings, organization_mapping, submitter_mapping, assignee_mapping):
    """Extract and transform ticket data based on mappings."""
    extracted_data = {}
    for field, new_field_name in field_mappings.items():
        if field == 'organization_id':
            extracted_data[new_field_name] = organization_mapping.get(
                str(ticket.get(field, 'Unknown')), {'name': 'Unknown Client', 'domain': 'Unknown Domain'}
            )['name']
        elif field == 'submitter_id':
            extracted_data[new_field_name] = submitter_mapping.get(str(ticket.get(field, 'Unknown')), 'Unknown Creator')
        elif field == 'assignee_id':
            extracted_data[new_field_name] = assignee_mapping.get(str(ticket.get(field, 'Unknown')), 'Unknown Updater')
        else:
            extracted_data[new_field_name] = ticket.get(field, 'Unknown')
    return extracted_data

def build_final_ticket(ticket, extracted_data, organization_mapping, comments_mapping,time_entries_mapping):
    """Construct the final structured ticket format."""
    ticket_id = ticket.get('id', 'Unknown ID')
    company_site = "https://" + organization_mapping.get(
        str(ticket.get('organization_id', 'Unknown')), {'domain': 'placeholder.com'}
    )['domain']
    
    final_ticket = {
        "CreatedBy": extracted_data.get('createdBy'),
        "Identifier": extracted_data.get('identifier'),
        "CreatedDate": extracted_data.get('createdDate'),
        "UpdatedBy": extracted_data.get('updatedBy'),
        "UpdatedDate": extracted_data.get('updatedDate'),
        "Title": extracted_data.get('title'),
        "Description": extracted_data.get('description'),
        "ClientName": extracted_data.get('clientName'),
        "CompanySite": company_site,
        "Properties": {
            "Priority": str(extracted_data.get('priority')),
            "Generated_timestamp": str(extracted_data.get('generated_timestamp')),
            "Status": extracted_data.get('status'),
            "Type": extracted_data.get('type')
        },
        "WorkNotes": process_comments(ticket_id, comments_mapping),
        "TimeEntries": process_time_entries(ticket_id, time_entries_mapping)
    }
    return final_ticket

def process_comments(ticket_id, comments_mapping):
    """Process comments from mapped files."""
    comment_file = comments_mapping.get(str(ticket_id), {}).get('comment_files', '')
    print(comment_file)
    if comment_file:
        comment_content = load_file_content(comment_file, ticket_id)  
        print("comment_content",comment_content)
        return extract_comment_info(comment_content)
    return[{"Title": DEFAULT_NOTE, "Description": DEFAULT_DESC, "Date": DEFAULT_DATE, "Type": "Internal"}]

def process_time_entries(ticket_id, time_entries_mapping):
    """Process time entries from mapped files."""
    time_entry_file = time_entries_mapping.get(str(ticket_id), {}).get('time_entry_files', '')
    print("time_entry_file",time_entry_file)
    if time_entry_file:
        time_entry_content = load_file_content(time_entry_file, ticket_id)  
        return extract_time_entry_info(time_entry_content)
    return [{"Title": DEFAULT_TITLE, "Description": DEFAULT_DESC, "StartDate": DEFAULT_DATE, "EndDate": DEFAULT_DATE, "Type": "Internal"}]

def main():
    """Main function to process and extract tickets."""
    data = load_json(r'extract\tickets_processed.json')
    
    # Load mappings from CSV files
    organization_mapping = load_csv_mapping_with_domain(r'extract\organizations.csv') 
    submitter_mapping = load_csv_mapping(r'extract\submitter_ids.csv')  
    assignee_mapping = load_csv_mapping(r'extract\assignee_idssecond.csv')
    comments_mapping = load_comments_mapping(r'comments_extracted.csv')
    time_entries_mapping = load_time_entries_mapping(r'time_metrics_extracted.csv')
    # comments_timenetries_mapping = load_comments_timenetries_mapping('ticket_file_mapping.csv')
    
    field_rename_mapping = {
        'id': 'identifier', 'created_at': 'createdDate', 'updated_at': 'updatedDate',
        'generated_timestamp': 'generated_timestamp', 'subject': 'title', 'description': 'description',
        'status': 'status', 'organization_id': 'clientName', 'submitter_id': 'createdBy',
        'assignee_id': 'updatedBy', 'type': 'type'
    }
    
    sorted_tickets = sorted(data, key=lambda x: x['id'])[:100]
    print(len(sorted_tickets))
    extracted_tickets = []
    
    for ticket in sorted_tickets:
        extracted_data = extract_ticket_data(ticket, field_rename_mapping, organization_mapping, submitter_mapping, assignee_mapping)
        final_ticket = build_final_ticket(ticket, extracted_data, organization_mapping, comments_mapping, time_entries_mapping)
        extracted_tickets.append(final_ticket)
    
    with open("TestOneTickets.json", "w", encoding="utf-8") as json_file:
        json.dump(extracted_tickets, json_file, indent=4)
    
    print("Extracted Tickets Data written to JSON.")

if __name__ == "__main__":
    main()
