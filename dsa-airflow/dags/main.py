import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable
from airflow.utils import timezone
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor, BigQueryTablePartitionExistenceSensor
import time
import yaml
from dateutil.tz import tzlocal

# local imports
from webscrape import scrape_yahoo, open_raw_yahoo_transform, close_raw_yahoo_transform, stg_file_setup
from history import hist_transf
from bigquery_load import create_dataset, create_table, gcs_upload
from openai_predict import chat_gpt_prediction

sleep_minutes = 390

# first DAG definition
# -----------------------------------------

with DAG(
    dag_id='bitcoin_first_webscrape',
    schedule_interval='30 6 * * *',
    start_date=datetime(2022, 3, 6),
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

with DAG(
    dag_id='bitcoin_second_webscrape',
    schedule_interval='00 13 * * *',
    start_date=datetime(2022, 3, 6),
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

    #create_outputs_data_dir_task
    #create_outputs_data_dir_task = PythonOperator(
        #task_id='create_outputs_data_dir_task',
        #python_callable = create_data_outputs,
        #doc_md = create_data_outputs.__doc__        
    #)

    #create_bq_dataset_task
    create_dataset_task = PythonOperator(
        task_id='create_dataset',
        python_callable = create_dataset,
        doc_md = create_dataset.__doc__        
    )

    #load_table_bq_task
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable = create_table,
        doc_md = create_table.__doc__        
    )

    gcs_upload_task = PythonOperator(
        task_id='gcs_csv_file_upload',
        python_callable = gcs_upload,
        doc_md = gcs_upload.__doc__        
    )
    
    #PythonVirtualenvOperator(
       # task_id='google_drive_upload',
       # python_callable = google_drive_upload,
       # requirements=[
                    #'selenium', 
                   # 'pandas',
                   # 'gcsfs',
                   # 'fsspec',
                   # 'google-cloud-storage',
                   # 'google-cloud-bigquery',
                   # 'google-auth',
                   # 'beautifulsoup4',
                    #'lxml',
                    #'html5lib',
                    #'pytz',
                    #'tzwhere',
                   # 'apache-airflow-providers-google',
                   # 'apache-airflow[google]',
                   # 'openai',
                   # 'pydrive'
                   # ],
        #doc_md = google_drive_upload.__doc__        
    #)

    chatgpt_prediction_task = PythonVirtualenvOperator(
        task_id='chatgpt_prediction',
        python_callable = chat_gpt_prediction,
        requirements=['selenium', 
                   'pandas',
                   'gcsfs',
                   'fsspec',
                   'google-cloud-storage',
                   'google-cloud-bigquery',
                   'google-auth',
                   'apache-airflow-providers-google',
                   'apache-airflow[google]',
                   'openai',
                   ],
        doc_md = chat_gpt_prediction.__doc__        
    )

#task flow
history_data_task >> webscrape_task_1 >> transform_open_pricing_task

webscrape_task_2 >> transform_close_pricing >> stg_file_creation >> create_dataset_task >> create_table_task >> gcs_upload_task >> chatgpt_prediction_task

    