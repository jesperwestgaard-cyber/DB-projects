import json
import re

input_path = "/tmp/clean_movies_enriched_fixed.jsonl"  # Adjust this path as necessary
output_path = "/tmp/clean_movies_enriched_cleaned.jsonl"

def clean_json_line(line):
    # Remove unwanted escape characters and fix formatting issues
    line = line.strip()  # Remove leading/trailing whitespace

    # Remove unnecessary escape characters
    line = line.replace('\\"', '"')  # Remove backslashes before quotes
    line = line.replace("\\'", "'")   # Remove backslashes before single quotes

    # Fix keys and values
    line = re.sub(r'\"([a-zA-Z_]+)\":', r'"\1":', line)  # Ensure keys are wrapped in quotes
    line = re.sub(r':\s*\"', r': "', line)  # Fix spacing after colons for string values
    line = re.sub(r':\s*([0-9]+)', r': \1', line)  # Fix numeric values
    line = re.sub(r',(\s*[}\]])', r'\1', line)  # Remove trailing commas

    # Remove any leading/trailing commas and whitespace
    line = re.sub(r'\s*,\s*', ',', line)  # Remove spaces around commas
    line = re.sub(r',\s*([}\]])', r'\1', line)  # Remove trailing commas before closing braces/brackets

    # Attempt to load the cleaned line as JSON
    try:
        if line:  # Ensure the line is not empty
            doc = json.loads(line)
            return doc
    except json.JSONDecodeError as e:
        print(f"Error parsing line: {e} (Line content: {line})")
    return None

with open(input_path, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8") as outfile:
    for line_number, line in enumerate(infile, start=1):
        cleaned_doc = clean_json_line(line)
        print(f"Processing line {line_number}")  # Indicate processing

        if cleaned_doc:
            json.dump(cleaned_doc, outfile)
            outfile.write("\n")
            print(f"Successfully cleaned line {line_number}")  # Print success
        else:
            print(f"Failed to clean line {line_number}.")

print("Cleaning complete. Check the cleaned file at:", output_path)