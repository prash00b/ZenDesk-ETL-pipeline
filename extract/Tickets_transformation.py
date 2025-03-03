import json,csv
import multiprocessing
import logging
from itertools import islice

DEFAULT_DATE = "2012-12-15T00:00:00Z"
DEFAULT_USER = "Default User"
DEFAULT_TITLE = "Default Title"
DEFAULT_DESC = "Default Description"
DEFAULT_NOTE = "Default Note Title"
LINK = "https://placeholder.com"
DEFAULT_SITE = "Default Site"
DEFAULT_CLIENT_NAME = "Default Client Name"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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
        # print(f"DEBUG: Processing comments - {json.dumps(comment_content, indent=4)}")
        # print(f"Type: {type(comment_content)}")

        extracted_comments = []
        for comment in comment_content:  
            extracted_comments.append({
                "Description": comment.get('body',DEFAULT_DESC),
                "date": comment.get('created_at', DEFAULT_DATE),
                "Type": "External" if comment.get("public", False) else "Internal"
            })
        # print("comments",extracted_comments)
        return extracted_comments  
    except Exception as e:
        print(f"ERROR: Unexpected issue in extract_comment_info - {str(e)}")
        return [{"Error": str(e)}]
    
def extract_time_entry_info(time_entry_content):
    try:
        # print(f"DEBUG: Processing time entries - {json.dumps(time_entry_content, indent=4)}")
        # print(f"Type: {type(time_entry_content)}")

        extracted_time_entries = []

        # Check if time_entry_content is a list (multiple time entries)
        if isinstance(time_entry_content, list):
            for entries in time_entry_content:
                extracted_time_entries.append({
                    "Description": entries.get('description',DEFAULT_DESC),
                    "startDate": entries.get('created_at', DEFAULT_DATE),
                    "endDate": entries.get('updated_at',DEFAULT_DATE),
                    "Type": "External" if entries.get("public", False) else "Internal"
                })
        else:
            # If it's a single dictionary (a single time entry)
            extracted_time_entries.append({
                "Description": time_entry_content.get('description', DEFAULT_DESC),
                "startDate": time_entry_content.get('created_at',DEFAULT_DATE),
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

def load_comments_timenetries_mapping(csv_filename):
    mapping = {}
    with open(csv_filename, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            mapping[row['ticket_id']] = {
                            'comment_files': row['comment_files'],
                            'time_entry_files': row['time_entry_files']
                        }
    return mapping

# Function to read the content from files
def load_file_content(file_path, ticket_id):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            return data.get(str(ticket_id), f'Ticket ID {ticket_id} not found in the file')
    except FileNotFoundError:
        return 'File not found'
    except json.JSONDecodeError:
        return 'Invalid JSON format in file'
    
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

def build_final_ticket(ticket, extracted_data, organization_mapping, comments_timenetries_mapping):
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
        "WorkNotes": process_comments(ticket_id, comments_timenetries_mapping),
        "TimeEntries": process_time_entries(ticket_id, comments_timenetries_mapping)
    }
    return final_ticket

def process_comments(ticket_id, comments_timenetries_mapping):
    """Process comments from mapped files."""
    comment_file = comments_timenetries_mapping.get(str(ticket_id), {}).get('comment_files', '')
    if comment_file:
        comment_content = load_file_content(comment_file, ticket_id)  
        return extract_comment_info(comment_content)
    return  [{"Title": DEFAULT_TITLE, "Description": DEFAULT_DESC, "StartDate": DEFAULT_DATE, "EndDate": DEFAULT_DATE, "Type": "External"}]
def process_time_entries(ticket_id, comments_timenetries_mapping):
    """Process time entries from mapped files."""
    time_entry_file = comments_timenetries_mapping.get(str(ticket_id), {}).get('time_entry_files', '')
    if time_entry_file:
        time_entry_content = load_file_content(time_entry_file, ticket_id)  
        return extract_time_entry_info(time_entry_content)
    return [{"Title": DEFAULT_NOTE, "Description": DEFAULT_DESC, "Date": DEFAULT_DATE, "Type": "Internal"}],

# Configure logging

def process_batch(ticket_batch, batch_index, field_rename_mapping, organization_mapping, submitter_mapping, assignee_mapping, comments_timenetries_mapping):
    """Process a batch of tickets and save it to a JSON file."""
    logging.info(f"Processing batch {batch_index} with {len(ticket_batch)} records.")
    
    extracted_tickets = []
    for i, ticket in enumerate(ticket_batch, start=1):
        extracted_data = extract_ticket_data(ticket, field_rename_mapping, organization_mapping, submitter_mapping, assignee_mapping)
        final_ticket = build_final_ticket(ticket, extracted_data, organization_mapping, comments_timenetries_mapping)
        extracted_tickets.append(final_ticket)
        print(f"Ticket processed: {ticket.get('id', 'Unknown ID')}")
        
        if i % 1000 == 0:
            logging.info(f"Batch {batch_index}: Processed {i} tickets...")

    output_filename = f"test_tickets_batch_{batch_index}.json"
    with open(output_filename, "w", encoding="utf-8") as json_file:
        json.dump(extracted_tickets, json_file, indent=4)
    
    logging.info(f"Batch {batch_index} completed. Data written to {output_filename}.")

def chunkify(iterable, chunk_size):
    """Yield successive chunk_size-sized chunks from iterable."""
    iterator = iter(iterable)
    batch_index = 1
    while chunk := list(islice(iterator, chunk_size)):
        logging.info(f"Creating batch {batch_index} with {len(chunk)} records.")
        yield batch_index, chunk
        batch_index += 1

def main():
    """Main function to process and extract tickets concurrently."""
    logging.info("Loading ticket data...")
    data = load_json('tickets_processed.json')
    logging.info(f"Loaded {len(data)} tickets.")

    # Load mappings from CSV files
    logging.info("Loading mapping files...")
    organization_mapping = load_csv_mapping_with_domain('organizations.csv') 
    submitter_mapping = load_csv_mapping('submitter_ids.csv')  
    assignee_mapping = load_csv_mapping('assignee_idssecond.csv')
    comments_timenetries_mapping = load_comments_timenetries_mapping('ticket_file_mapping.csv')
    logging.info("All mapping files loaded successfully.")

    field_rename_mapping = {
        'id': 'identifier', 'created_at': 'createdDate', 'updated_at': 'updatedDate',
        'generated_timestamp': 'generated_timestamp', 'subject': 'title', 'description': 'description',
        'status': 'status', 'organization_id': 'clientName', 'submitter_id': 'createdBy',
        'assignee_id': 'updatedBy', 'type': 'type'
    }

    logging.info("Sorting tickets by ID...")
    sorted_tickets = sorted(data, key=lambda x: x['id'])
    logging.info("Sorting completed.")

    chunk_size = 5000  # Process 5000 records per batch
    num_batches = (len(sorted_tickets) + chunk_size - 1) // chunk_size
    logging.info(f"Total batches to process: {num_batches}")

    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        pool.starmap(
            process_batch,
            [(chunk, batch_index, field_rename_mapping, organization_mapping, submitter_mapping, assignee_mapping, comments_timenetries_mapping)
             for batch_index, chunk in chunkify(sorted_tickets, chunk_size)]
        )

    logging.info("All batches processed successfully.")

if __name__ == "__main__":
    main()