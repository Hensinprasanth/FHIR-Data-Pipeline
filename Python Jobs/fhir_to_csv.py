# -*- coding: utf-8 -*-
"""
Created on Fri 23 13:01:00 2024

@author: Hensin Prasanth M
"""
import pandas as pd
import json
import os

def flatten_dict(d, parent_key='', sep='.'):
    items = []
    if isinstance(d, dict):
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    items.extend(flatten_dict(item, f"{new_key}", sep=sep).items())  #_{i}
            else:
                items.append((new_key, v))
    else:
        # Handle case where d is not a dictionary
        # You can choose to raise an error, ignore it, or handle it differently
        pass
    return dict(items)

def normalize_fhir_data(fhir_data):
    normalized_data = {}
    
    for entry in fhir_data.get('entry', []):
        resource = entry.get('resource', {})
        resource_type = resource.get('resourceType', None)
        
        if resource_type:
            normalized_resource = flatten_dict(resource)

            if resource_type not in normalized_data:
                normalized_data[resource_type] = []
            normalized_data[resource_type].append(normalized_resource)

    return {resource_type: pd.DataFrame(resource_data) for resource_type, resource_data in normalized_data.items()}

# Folder containing JSON files
json_folder = r'C:\Users\HensinPrasanthMony\Documents\Interview\Emis-SDE\Python_Program\data'
    
# Loop through each JSON file in the folder
for filename in os.listdir(json_folder):
    if filename.endswith('.json'):
        # Load FHIR data from a JSON file
        file_path = json_folder + '\\' + filename  
        with open(file_path, encoding='utf-8') as f:
            fhir_data = json.load(f)

    # Normalize FHIR data
    normalized_dfs = normalize_fhir_data(fhir_data)

    # Save each DataFrame to a separate CSV file
    for resource_type, df in normalized_dfs.items():
        csv_file = f'{resource_type}.csv'
        if os.path.isfile(csv_file):
            df.to_csv(csv_file,  mode='a', header=False, index=False)
        else:
            df.to_csv(csv_file, index=False)
