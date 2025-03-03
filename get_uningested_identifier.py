import pandas as pd
import json

# Read the CSV file and extract unique identifiers
ingested_df = pd.read_csv(r'IngestionLogs2\2025-03-01_api_logs_success.csv')
ingested_identifiers = set(ingested_df['Identifier'])

# Read the JSON file and extract unique identifiers
with open(r'extract/tickets_processed.json', 'r', encoding='utf-8') as file:
    all_data = json.load(file)

all_identifiers = {item['id'] for item in all_data}  # Extract 'id' from JSON

# Compute the difference: items in JSON that are NOT in CSV
missing_in_csv = all_identifiers - ingested_identifiers

# Print the counts and differences
print(f"Total unique identifiers in CSV: {len(ingested_identifiers)}")
print(f"Total unique identifiers in JSON: {len(all_identifiers)}")
print(f"Total identifiers in JSON but not in CSV: {len(missing_in_csv)}")
print("Identifiers missing in CSV:", missing_in_csv)
