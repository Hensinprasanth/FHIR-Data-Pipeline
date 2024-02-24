# -*- coding: utf-8 -*-
"""
Created on Fri 23 02:11:00 2024

@author: Hensin Prasanth M
"""
import pandas as pd
from faker import Faker

# Initialize Faker for generating fake data
fake = Faker()

# De-identification function
def deidentifycolumns(df, columns_to_deidentify):
    deidentified_df = df.copy()
    for col in deidentified_df.columns:
        for col_to_deidentify in columns_to_deidentify:
            if col_to_deidentify in col:
                deidentified_df[col] = deidentified_df[col].apply(lambda x: fake.name())
                break  # Stop searching for matches once a column is de-identified
    return deidentified_df

# Re-identification function
def reidentifycolumns(deidentified_df, original_df, columns_to_deidentify):
    reidentified_df = deidentified_df.copy()
    for col in reidentified_df.columns:
        for col_to_deidentify in columns_to_deidentify:
            if col_to_deidentify in col:
                reidentified_df[col] = original_df[col]
                break  # Stop searching for matches once a column is re-identified
    return reidentified_df
