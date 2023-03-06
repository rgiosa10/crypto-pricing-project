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
#def create_data_outputs():
    #try:
        #os.mkdir(os.path.join(data_dir,'outputs'))
    #except:
        #pass

#------------------------------------------------
#Create project info and table schemas for load into BigQuery
#------------------------------------------------
PROJECT_NAME = config['project']
DATASET_NAME = config['dataset']

#create bigquery client
client = bigquery.Client()

#create dataset_id and table_ids
dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
table_id = f"{PROJECT_NAME}.{DATASET_NAME}.bitcoin_pricing"

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

#------------------------------------------------
#functions to create and load tables in BigQuery
#------------------------------------------------
def create_table():
    job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CVS,
            autodetect=True,
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            ignore_unknown_values=True,
        )
    table = bigquery.Table(table_id, schema=TABLE_SCHEMA)
    table = client.create_table(table, exists_ok=True)

    with open(os.path.join(data_dir, config['bitcoin_consolidated']), "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()