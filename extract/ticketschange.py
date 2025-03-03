import json

ticket_ids = []  # Initialize an empty list to store ticket ids

try:
    # Read the content of the JSON file
    with open(r'C:\TigerPaw Ingestion\scripts\extract\tickets_raw.json', 'r', encoding='utf-8') as json_file:
        content = json_file.read()
        
        # Replace '][' with ',' to correctly format the JSON
        content = content.replace('][', ',')
        
        # Remove any trailing commas before closing brackets
        if content.endswith(','):
            content = content.rstrip(',')
        
        # Now the content is a valid JSON array or multiple objects
        tickets_data = json.loads(content)
        
        # Extract 'id' from each ticket in the JSON array
        for ticket in tickets_data:
            ticket_ids.append(ticket.get('id'))
    
    # Print the number of ticket ids
    print(len(ticket_ids))

    # Save the properly formatted JSON content back to a new file
    with open(r'C:\TigerPaw Ingestion\scripts\extract\tickets_processed.json', 'w', encoding='utf-8') as json_file:
        json.dump(tickets_data, json_file, indent=4)
        
except Exception as e:
    print(f"An error occurred: {e}")
