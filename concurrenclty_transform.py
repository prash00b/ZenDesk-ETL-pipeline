import json
import csv
import os
import logging
import concurrent.futures
import math
from datetime import datetime
from tqdm import tqdm

# Set up logging
log_dir = "Transformation4/logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_DATE = "2012-12-15T00:00:00Z"
DEFAULT_USER = "Default User"
DEFAULT_TITLE = "Default Title"
DEFAULT_DESC = "Default Description"
DEFAULT_NOTE = "Default Note Title"
LINK = "https://placeholder.com"
DEFAULT_SITE = "Default Site"
DEFAULT_CLIENT_NAME = "Default Client Name"
BATCH_SIZE = 500
MAX_WORKERS = 2

# Create output directories
output_dir = "Transformation4/output"
error_dir = "Transformation4/errors"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(error_dir, exist_ok=True)

def load_csv_mapping_with_domain(csv_filename):
    mapping = {}
    try:
        with open(csv_filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                mapping[row['id']] = {
                    'name': row['name'],
                    'domain': row.get('domain_names', 'Unknown Domain')  
                }
        logger.info(f"Successfully loaded {len(mapping)} records from {csv_filename}")
        return mapping
    except Exception as e:
        logger.error(f"Error loading CSV mapping with domain from {csv_filename}: {str(e)}")
        return mapping
    

def extract_comment_info(comment_content):
    try:
        # print(f"DEBUG: Processing time entries - {json.dumps(time_entry_content, indent=4)}")
        # print(f"Type: {type(time_entry_content)}")

        extracted_comments = []

        # Check if time_entry_content is a list (multiple time entries)
        if isinstance(comment_content, list):
            for comment in comment_content:
                extracted_comments.append({
                    "Description": comment.get('body', 'Unknown comment text'),
                    "date": comment.get('created_at', 'Unknown date'),
                    "Type": "External" if comment.get("public", False) else "Internal"
                })
        else:
            # If it's a single dictionary (a single time entry)
                extracted_comments.append({
                    "Description": comment_content.get('body', 'Unknown comment text'),
                    "date": comment_content.get('created_at', 'Unknown date'),
                    "Type": "External" if comment_content.get("public", False) else "Internal"
                })

        # print("time_entries", extracted_time_entries)
        return extracted_comments  
    except Exception as e:
        print(f"ERROR: Unexpected issue in extract_time_entry_info - {str(e)}")
        return [{"Error": str(e)}]

    
# def extract_time_entry_info(time_entry_content):
#     try:
#         # print(f"DEBUG: Processing time entries - {json.dumps(time_entry_content, indent=4)}")
#         # print(f"Type: {type(time_entry_content)}")

#         extracted_time_entries = []

#         # Check if time_entry_content is a list (multiple time entries)
#         if isinstance(time_entry_content, list):
#             for entries in time_entry_content:
#                 extracted_time_entries.append({
#                     "Description": entries.get('description', DEFAULT_DESC),
#                     "startDate": entries.get('created_at', DEFAULT_DATE),
#                     "endDate": entries.get('updated_at', DEFAULT_DATE),
#                     "Type": "External" if entries.get("public", False) else "Internal"
#                 })
#         else:
#             # If it's a single dictionary (a single time entry)
#             extracted_time_entries.append({
#                 "Description": time_entry_content.get('description', DEFAULT_DATE),
#                 "startDate": time_entry_content.get('created_at', DEFAULT_DATE),
#                 "endDate": time_entry_content.get('updated_at', DEFAULT_DATE),
#                 "Type": "External" if time_entry_content.get("public", False) else "Internal"
#             })

#         # print("time_entries", extracted_time_entries)
#         return extracted_time_entries  
#     except Exception as e:
#         print(f"ERROR: Unexpected issue in extract_time_entry_info - {str(e)}")
#         return [{"Error": str(e)}]


# def extract_comment_info(comment_content):
#     try:
#         # print(f"DEBUG: Processing time entries - {json.dumps(time_entry_content, indent=4)}")
#         # print(f"Type: {type(time_entry_content)}")

#         extracted_comments = []

#         # Check if time_entry_content is a list (multiple time entries)
#         if isinstance(comment_content, list):
#             for comment in comment_content:
#                 extracted_comments.append({
#                     "Description": comment.get('body', 'Unknown comment text'),
#                     "date": comment.get('created_at', 'Unknown date'),
#                     "Type": "External" if comment.get("public", False) else "Internal"
#                 })
#         else:
#             # If it's a single dictionary (a single time entry)
#                 extracted_comments.append({
#                     "Description": comment.get('body', 'Unknown comment text'),
#                     "date": comment.get('created_at', 'Unknown date'),
#                     "Type": "External" if comment.get("public", False) else "Internal"
#                 })

#         # print("time_entries", extracted_time_entries)
#         return extracted_comments  
#     except Exception as e:
#         print(f"ERROR: Unexpected issue in extract_time_entry_info - {str(e)}")
#         return [{"Error": str(e)}]


def extract_time_entry_info(time_entry_content):
    try:
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
                "Description": time_entry_content.get('description', DEFAULT_DESC),
                "startDate": time_entry_content.get('created_at', DEFAULT_DATE),
                "endDate": time_entry_content.get('updated_at', DEFAULT_DATE),
                "Type": "External" if time_entry_content.get("public", False) else "Internal"
            })

        return extracted_time_entries  
    except Exception as e:
        logger.error(f"Unexpected issue in extract_time_entry_info - {str(e)}")
        return [{"Error": str(e)}]

# Load the CSV data into a dictionary (for submitter and assignee without domain)
def load_csv_mapping(csv_filename):
    mapping = {}
    try:
        with open(csv_filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                mapping[row['id']] = row['name']
        logger.info(f"Successfully loaded {len(mapping)} records from {csv_filename}")
        return mapping
    except Exception as e:
        logger.error(f"Error loading CSV mapping from {csv_filename}: {str(e)}")
        return mapping

def load_comments_mapping(csv_filename):
    mapping = {}
    try:
        with open(csv_filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                mapping[row['ticket_id']] = {
                    'comment_id': row['comment_id'],
                    'comment_files': row['file']
                }
        logger.info(f"Successfully loaded {len(mapping)} comment mappings from {csv_filename}")
        return mapping
    except Exception as e:
        logger.error(f"Error loading comments mapping from {csv_filename}: {str(e)}")
        return mapping

def load_time_entries_mapping(csv_filename):
    mapping = {}
    try:
        with open(csv_filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                mapping[row['ticket_id']] = {
                    'time_metric_id': row['time_metric_id'],
                    'time_entry_files': row['file']
                }
        logger.info(f"Successfully loaded {len(mapping)} time entry mappings from {csv_filename}")
        return mapping
    except Exception as e:
        logger.error(f"Error loading time entries mapping from {csv_filename}: {str(e)}")
        return mapping

def load_file_content(file_path, ticket_id):
    try:
        with open(file_path, 'r') as file:
            # Load the JSON file
            data = json.load(file)
            
            # Check if the ticket_id exists and return its associated data
            if str(ticket_id) in data:
                return data[str(ticket_id)]
            else:
                logger.warning(f'Ticket ID {ticket_id} not found in the file {file_path}')
                return f'Ticket ID {ticket_id} not found in the file'
    
    except FileNotFoundError:
        logger.error(f'File not found: {file_path}')
        return f'File not found: {file_path}'
    except json.JSONDecodeError:
        logger.error(f'Invalid JSON format in file: {file_path}')
        return 'Invalid JSON format in file'
    except Exception as e:
        logger.error(f'An unexpected error occurred while loading {file_path}: {str(e)}')
        return f'An unexpected error occurred: {str(e)}'

def load_json(filename):
    """Load JSON data from a file."""
    try:
        with open(filename, 'r') as file:
            data = json.load(file)
        logger.info(f"Successfully loaded JSON data from {filename} with {len(data)} records")
        return data
    except Exception as e:
        logger.error(f"Error loading JSON from {filename}: {str(e)}")
        return []

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

def build_final_ticket(ticket, extracted_data, organization_mapping, comments_mapping, time_entries_mapping):
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
    logger.debug(f"Processing comments for ticket {ticket_id} from file {comment_file}")
    if comment_file:
        comment_content = load_file_content(comment_file, ticket_id)
        logger.debug(f"Comment content loaded for ticket {ticket_id}")
        return extract_comment_info(comment_content)
    return[{"Title": DEFAULT_NOTE, "Description": DEFAULT_DESC, "Date": DEFAULT_DATE, "Type": "Internal"}]

def process_time_entries(ticket_id, time_entries_mapping):
    """Process time entries from mapped files."""
    time_entry_file = time_entries_mapping.get(str(ticket_id), {}).get('time_entry_files', '')
    logger.debug(f"Processing time entries for ticket {ticket_id} from file {time_entry_file}")
    if time_entry_file:
        time_entry_content = load_file_content(time_entry_file, ticket_id)
        logger.debug(f"Time entry content loaded for ticket {ticket_id}")
        return extract_time_entry_info(time_entry_content)
    return [{"Title": DEFAULT_TITLE, "Description": DEFAULT_DESC, "StartDate": DEFAULT_DATE, "EndDate": DEFAULT_DATE, "Type": "Internal"}]

def process_batch(batch_id, tickets_batch, field_rename_mapping, 
                  organization_mapping, submitter_mapping, assignee_mapping, 
                  comments_mapping, time_entries_mapping):
    """Process a batch of tickets concurrently."""
    try:
        logger.info(f"Starting to process batch {batch_id} with {len(tickets_batch)} tickets")
        start_time = datetime.now()
        
        extracted_tickets = []
        errors = []
        
        for ticket in tickets_batch:
            try:
                extracted_data = extract_ticket_data(
                    ticket, field_rename_mapping, 
                    organization_mapping, submitter_mapping, assignee_mapping
                )
                final_ticket = build_final_ticket(
                    ticket, extracted_data, 
                    organization_mapping, comments_mapping, time_entries_mapping
                )
                extracted_tickets.append(final_ticket)
            except Exception as e:
                logger.error(f"Error processing ticket {ticket.get('id', 'Unknown')}: {str(e)}")
                errors.append({
                    "ticket_id": ticket.get('id', 'Unknown'),
                    "error": str(e)
                })
        
        # Write the batch to a file
        output_file = f"{output_dir}/batch_{batch_id}_tickets.json"
        with open(output_file, "w", encoding="utf-8") as json_file:
            json.dump(extracted_tickets, json_file, indent=4)
        
        # Write errors if any
        if errors:
            error_file = f"{error_dir}/batch_{batch_id}_errors.json"
            with open(error_file, "w", encoding="utf-8") as error_file:
                json.dump(errors, error_file, indent=4)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Completed batch {batch_id} in {duration:.2f} seconds. "
                    f"Processed {len(extracted_tickets)} tickets with {len(errors)} errors")
        
        return len(extracted_tickets), len(errors)
    
    except Exception as e:
        logger.error(f"Batch {batch_id} failed with error: {str(e)}")
        return 0, 0

def main():
    """Main function to process and extract tickets concurrently."""
    start_time = datetime.now()
    logger.info("Starting ticket processing")
    
    try:
        # Load all data
        data = load_json(r'extract\tickets_processed.json')
        
        # Load mappings from CSV files
        organization_mapping = load_csv_mapping_with_domain(r'extract\organizations.csv') 
        submitter_mapping = load_csv_mapping(r'extract\submitter_ids.csv')  
        assignee_mapping = load_csv_mapping(r'extract\assignee_idssecond.csv')
        comments_mapping = load_comments_mapping(r'C:\TigerPaw Ingestion\Zendesk\comments_extracted.csv')
        time_entries_mapping = load_time_entries_mapping(r'C:\TigerPaw Ingestion\Zendesk\time_metrics_extracted.csv')
        
        field_rename_mapping = {
            'id': 'identifier', 'created_at': 'createdDate', 'updated_at': 'updatedDate',
            'generated_timestamp': 'generated_timestamp', 'subject': 'title', 'description': 'description',
            'status': 'status', 'organization_id': 'clientName', 'submitter_id': 'createdBy',
            'assignee_id': 'updatedBy', 'type': 'type'
        }
        
        # Sort all tickets by id for consistency
        sorted_tickets = sorted(data, key=lambda x: x['id'])
        total_tickets = len(sorted_tickets)
        logger.info(f"Total tickets to process: {total_tickets}")
        
        # Calculate number of batches
        num_batches = math.ceil(total_tickets / BATCH_SIZE)
        logger.info(f"Processing will be done in {num_batches} batches of {BATCH_SIZE} tickets each")
        
        # Create batches
        batches = []
        for i in range(num_batches):
            start_idx = i * BATCH_SIZE
            end_idx = min((i + 1) * BATCH_SIZE, total_tickets)
            batches.append((i+1, sorted_tickets[start_idx:end_idx]))
        
        # Process batches concurrently
        total_processed = 0
        total_errors = 0
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a list of futures
            futures = []
            for batch_id, batch_data in batches:
                future = executor.submit(
                    process_batch, 
                    batch_id, 
                    batch_data, 
                    field_rename_mapping,
                    organization_mapping, 
                    submitter_mapping, 
                    assignee_mapping, 
                    comments_mapping, 
                    time_entries_mapping
                )
                futures.append(future)
            
            # Process results as they complete
            for future in tqdm(concurrent.futures.as_completed(futures), 
                              total=len(futures), 
                              desc="Processing batches"):
                processed, errors = future.result()
                total_processed += processed
                total_errors += errors
        
        # Create a summary file
        summary = {
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": (datetime.now() - start_time).total_seconds(),
            "total_tickets": total_tickets,
            "processed_tickets": total_processed,
            "errors": total_errors,
            "batches": num_batches
        }
        
        with open(f"{output_dir}/processing_summary.json", "w", encoding="utf-8") as summary_file:
            json.dump(summary, summary_file, indent=4)
        
        logger.info(f"Processing completed. Total tickets processed: {total_processed}")
        logger.info(f"Total errors: {total_errors}")
        logger.info(f"Total time: {(datetime.now() - start_time).total_seconds():.2f} seconds")
        
    except Exception as e:
        logger.error(f"Main process failed with error: {str(e)}")

if __name__ == "__main__":
    main()