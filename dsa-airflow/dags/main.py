import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable
from airflow.utils import timezone
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor, BigQueryTablePartitionExistenceSensor
import time
import yaml

# local imports
from webscrape import scrape_yahoo, open_raw_yahoo_transform, close_raw_yahoo_transform, stg_file_setup
from history import hist_transf

def sleep_delay():
    time.sleep(23400)

# DAG definition
# -----------------------------------------

with DAG(
    dag_id='bitcoin_prediction',
    schedule_interval='30 14 * * *',
    start_date=datetime(2022, 3, 6, 13, 0),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['dsa', 'data-loaders'],
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    }
) as dag:
    # dag's doc in markdown
    # setting it to this module's docstring defined at the very top of this file
    dag.doc_md = __doc__

    print(__file__)
    # pre-check task

    history_data_task = PythonOperator(
        task_id='get_transform_historical_data',
        python_callable = hist_transf,
        doc_md = hist_transf.__doc__        
    )

    webscrape_task_1 = PythonOperator(
        task_id='scrape_open_bitcoin_price',
        python_callable = scrape_yahoo,
        doc_md = scrape_yahoo.__doc__        
    )

    transform_open_pricing_task = PythonOperator(
        task_id='transform_open_webscrape_pricing',
        python_callable = open_raw_yahoo_transform,
        doc_md = open_raw_yahoo_transform.__doc__        
    )

    sleep_minutes = 390
    sleep_task = TimeDeltaSensor(
        task_id='sleep_until_2nd_scrape',
        delta= timedelta(minutes=sleep_minutes),
        target_time=(datetime.utcnow()+timedelta(minutes=sleep_minutes)).time(),
        mode = 'reschedule'
    )

    webscrape_task_2 = PythonOperator(
        task_id='scrape_close_bitcoin_price',
        python_callable = scrape_yahoo,
        doc_md = scrape_yahoo.__doc__        
    )

    transform_close_pricing = PythonOperator(
        task_id='transform_close_webscrape_pricing',
        python_callable = close_raw_yahoo_transform,
        doc_md = close_raw_yahoo_transform.__doc__        
    )

    stg_file_creation = PythonOperator(
        task_id='combine_open_close_pricing',
        python_callable = stg_file_setup,
        doc_md = stg_file_setup.__doc__
    )

    #create_bq_dataset_task

    #load_table_bq_task

#task flow
history_data_task >> webscrape_task_1 >> transform_open_pricing_task >> sleep_task >> webscrape_task_2 >> transform_close_pricing >> stg_file_creation

    