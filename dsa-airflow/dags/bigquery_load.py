from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import yaml
import os
from datetime import datetime


#------------------------------------------------
#SETUP config and FileSensor data dir path
#------------------------------------------------
_default_config_path = '/opt/airflow/dags/config.yml'
CONF_PATH = Variable.get('config_file', default_var=_default_config_path)
config: dict = {}
with open(CONF_PATH) as open_yaml:
    config: dict =  yaml.full_load(open_yaml)
    
#data_fs = FSHook(conn_id='data_fs')     # get airflow connection for data_fs
#data_dir = data_fs.get_path()

data_dir = '../data/'

#------------------------------------------------
#Initialize spark for ETL to parquet files
#------------------------------------------------

#creates directory within the data directory for transformed parquet files
def create_data_outputs():
    try:
        os.mkdir(os.path.join(data_dir,'outputs'))
    except:
        pass

#------------------------------------------------
#Create project info and table schemas for load into BigQuery
#------------------------------------------------
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

#create bigquery client
client = bigquery.Client()

#create dataset_id and table_ids
dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
bitcoin_table_id = f"{PROJECT_NAME}.{DATASET_NAME}.bitcoin_pricing"

#schemas for tables to be loaded
TABLE_SCHEMA = [
    bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('symbol', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('open', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('close', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('volume', 'FLOAT', mode='NULLABLE'),
    ]

#------------------------------------------------
#function to create dataset in BigQuery
#------------------------------------------------
def create_dataset():
    if client.get_dataset(dataset_id) == NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
    else:
        pass