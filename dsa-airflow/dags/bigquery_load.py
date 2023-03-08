from airflow.models import Variable
#from airflow.hooks.filesystem import FSHook
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
#from google.oauth2 import service_account
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

data_dir = './data/'

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
TABLE_NAME = config['table']

#create bigquery client
client = bigquery.Client()
s_client = storage.Client()

#create dataset_id and table_ids
dataset_id = f"{PROJECT_NAME}.{DATASET_NAME}"
table_id = f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}"

#schema for tables to be loaded
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
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)

#------------------------------------------------
#functions to create and load tables in BigQuery
#------------------------------------------------
def create_table():
    job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
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

def gcs_upload():
    s_client = storage.Client()
    
    export_bucket = s_client.get_bucket('bitcoin_pricing')
    blob = export_bucket.blob(config['bitcoin_consolidated'])
    blob.upload_from_filename(os.path.join(data_dir,config['bitcoin_consolidated']))
    
    
    #from pydrive.auth import GoogleAuth
    #from pydrive.drive import GoogleDrive
    #import os

    #data_dir = './data/'

    #gauth = GoogleAuth()
    #gauth.LoadCredentialsFile('/opt/airflow/dags/client_secrets.json')
    #drive = GoogleDrive(gauth)

    #file = drive.CreateFile({'parents': [{'id': '1DDqEwMd87_SP5iLse-Luw3TMwJVLCy3W'}]})
    #file.SetContentFile(os.path.join(data_dir,'combined_BTC_hist_pricing.csv'))
    #file.Upload()
    
    #delete file
    #os.remove(os.path.join(data_dir, config['bitcoin_consolidated']))