# -*- coding: utf-8 -*-
"""
Created on Fri 23 08:01:00 2024

@author: Hensin Prasanth M
"""
import time
job_start_time = time.time()
import pandas as pd
import json
import os
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from collections import deque
import numpy as np
import hashlib
from config_reader import es_timeout,data_index,index_suffix,index_suffix
from config_reader import current_time,json_folder,pii_column
import deidentification
import sys

# Function to create unique identifier
def create_unique_identifier(value):
    unique_identifier = hashlib.sha256(value.encode()).hexdigest()
    return unique_identifier

# Function to Ingest the Data into Elastic
def ingest_data_into_elastic(df,resource_type):
    #elastic client to connect with elasticsearch db
    es = Elasticsearch(hosts=['http://localhost:9200'])
    chunk_size = 1000

    #Preparing the data to ingest into elastic index
    df ["ingestion_time"] = current_time
    df["_id"] = df["id"] + resource_type
    df["_id"] = df["_id"].apply(create_unique_identifier)
    df.fillna(np.nan)
    df = df.where(pd.notnull(df), None)

    #Preparing the index
    if index_suffix == "na":
        index = data_index
    else:
        index = data_index + index_suffix
    if not es.indices.exists(index=index):
        es.indices.create(index=index)

    try:
       # Bulk Parallel Indexing  
        #helpers.bulk(es, df.to_dict(orient='records'), index=index, chunk_size=chunk_size, request_timeout=es_timeout)
        deque(helpers.parallel_bulk(es, df.to_dict(orient='records'), index=index, chunk_size=chunk_size, thread_count=8, queue_size=8, ignore=400))
        pass
    except BulkIndexError as e:
        # Print the number of failed documents
        print("%i document(s) failed to index." % len(e.errors))
        # Print the errors for each failed document
        for error in e.errors:
            print("Failed document:", error)


# Flatten the json data
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
        pass
    return dict(items)


# normalize the FHIR data
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
        
        #didentifying the data
        deidentified_df = deidentification.deidentifycolumns(df, set(pii_column))        
        #Re-dentifying the data
        #reidentified_df = deidentification.reidentifycolumns(deidentified_df, df, pii_column)
        ingest_data_into_elastic(df,resource_type)

print("Successfull Execution: ", time.time() - job_start_time)
