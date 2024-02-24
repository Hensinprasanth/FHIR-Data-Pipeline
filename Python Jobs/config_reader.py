# -*- coding: utf-8 -*-
"""
Created on Fri 23 10:01:00 2024

@author: Hensin Prasanth M
"""

from datetime import datetime
from dateutil import tz
import pytz 
from configparser import ConfigParser 
from dateutil.relativedelta import relativedelta
import numpy as np
import datetime
from datetime import datetime


########################################################CONFIG PARSER########################################################################

config_path = r'C:\Users\HensinPrasanthMony\Documents\Interview\Emis-SDE\Python_Program\configuration_files\general_configurations.ini'
config_parser = ConfigParser() 
config_parser.read(config_path)

#######################################################AWS BUCKET CONIG############################################################


json_folder =config_parser.get('path','json_folder')
bucket_name = config_parser.get('path','bucket_name')

#######################################################FHIR DATA###################################################################
pii_column =config_parser.get('fhir','pii_columns').split(',')

#######################################################ELASTICSEARCH CONN VARIABLES###########################################################

es_timeout = config_parser.getint('timeouts', 'general_timeout')
username = config_parser.get('es_configurations', 'username')

#######################################################ELASTICSEARCH VARIABLES################################################################

chunk_size = config_parser.getint('chunk_sizes', 'ingest_chunk_size') 
data_timezone = config_parser.get('timezone', 'timezone')
current_time = datetime.now(pytz.timezone(data_timezone))
month = current_time.strftime("%m")
year = current_time.strftime("%Y")

########################################################ELASTIC OUPUT INDEX###################################################################

data_index = config_parser.get('es_fhir_output_indices', 'data_index')
index_suffix = config_parser.get('es_fhir_output_indices', 'index_suffix')

if index_suffix == "MM-YYYY":
    index_suffix = "-" + month + "-" + year
elif index_suffix == "na":
    index_suffix = np.nan
else:
    index_suffix = np.nan

########################################################********END********###################################################################






