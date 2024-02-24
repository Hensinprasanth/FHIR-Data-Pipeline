# -*- coding: utf-8 -*-
"""
Created on Fri 23 10:01:00 2024

@author: Hensin Prasanth M
"""
import time
job_start_time = time.time()
import logging
import pandas as pd
import json
import os
import boto3
from botocore.exceptions import NoCredentialsError
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from collections import deque
import numpy as np
import hashlib
from ssl import create_default_context
from config_reader import es_timeout, data_index, index_suffix, username
from config_reader import current_time, pii_column,bucket_name
import deidentification
import sys
import keyring

# Configure logging
logging.basicConfig(filename='example.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to create unique identifier
def create_unique_identifier(value):
    try:
        unique_identifier = hashlib.sha256(value.encode()).hexdigest()
        return unique_identifier
    except Exception as e:
        # Handle the exception here
        print(f"Error occurred while creating unique identifier: {e}")
        sys.exit(1)

def es_connection():
    ssl_context = create_default_context(cafile=keyring.get_password('elastic','ca_pem'))
    try:
        es = Elasticsearch(keyring.get_password('elastic','host'), port=keyring.get_password('elastic','port'), scheme="https", http_auth=(username, keyring.get_password('elastic','password')), timeout=es_timeout, ssl_context=ssl_context)
        print("Elasticsearch client initialized successfully.")
        response = "Elasticsearch client initialized successfully."
    except Exception as e:
        response = f"Error initializing Elasticsearch client: {e}"
        print(response)
    return es

# Function to Ingest the Data into Elastic
def ingest_data_into_elastic(df,resource_type):
    #elastic client to connect with elasticsearch db
    es = es_connection()
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
                    items.extend(flatten_dict(item, f"{new_key}", sep=sep).items())
            else:
                items.append((new_key, v))
    else:
        items.append((parent_key, d))
    return dict(items)


# normalize the FHIR data
def normalize_fhir_data(fhir_data):
    normalized_data = {}

    if fhir_data != None:
        for entry in fhir_data.get('entry', []):
            resource = entry.get('resource', {})
            resource_type = resource.get('resourceType', None)
            
            if resource_type:
                normalized_resource = flatten_dict(resource)

                if resource_type not in normalized_data:
                    normalized_data[resource_type] = []
                normalized_data[resource_type].append(normalized_resource)
    else:
        normalized_data = {}

    return {resource_type: pd.DataFrame(resource_data) for resource_type, resource_data in normalized_data.items()}


# Connect to AWS S3
def s3_connect(bucket_name):
    try:
        access_key_id = keyring.get_password('aws', 'access_key_id')
        secret_access_key = keyring.get_password('aws', 'secret_access_key')
        s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        print("Connected to AWS S3 successfully.")
        return s3
    except Exception as e:
        print(f"Error connecting to AWS S3: {e}")
        sys.exit(1)

# Download FHIR files from S3 bucket
def download_fhir_files_from_s3(s3, bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        folder_names = [obj['Key'] for obj in response.get('CommonPrefixes', [])]
        folder_names.sort(key=lambda x: int(x.split('/')[-2]))  # Sort folders by creation date
        latest_folder = folder_names[-1]
        fhir_data = []
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=latest_folder)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.json'):
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                fhir_data.append(json.loads(file_obj['Body'].read().decode('utf-8')))
        print("FHIR files downloaded from S3 successfully.")
        return fhir_data
    except Exception as e:
        print(f"Error downloading FHIR files from S3: {e}")
        sys.exit(1)


if __name__ == '__main__':
    # Connect to AWS S3
    s3 = s3_connect(bucket_name)

    # Download FHIR files from the most recently created folder in S3 bucket
    fhir_data_from_s3 = download_fhir_files_from_s3(s3, bucket_name)

    # Normalize fetched FHIR data
    normalized_dfs = normalize_fhir_data(fhir_data_from_s3)

    # Ingest normalized data into Elasticsearch
    for resource_type, df in normalized_dfs.items():
        #didentifying the data
        deidentified_df = deidentification.deidentifycolumns(df, set(pii_column))
        print (deidentified_df)     
        #Re-dentifying the data
        #reidentified_df = deidentification.reidentifycolumns(deidentified_df, df, pii_column)
        ingest_data_into_elastic(df,resource_type)

    print("Successfull Execution: ", time.time() - job_start_time)
