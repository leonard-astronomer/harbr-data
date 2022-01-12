from airflow import DAG
from airflow.models.taskinstance import TR
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from datetime import datetime, timedelta


from include.harbr_utilities import indicate_initial_load_completed
from include.Tracking import Tracking
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 

import urllib

PRODUCT_NAME="sample_data_product"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2000, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
  

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('indicate_successful_initial_load',
         start_date=datetime(2000, 1, 1),
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:


    t0 = PythonOperator(
        task_id='add_indicator_key_to_endpoint',
        python_callable=indicate_initial_load_completed,
        op_kwargs={'product_name':PRODUCT_NAME},
        provide_context=True
    )

    t1 = DummyOperator(
        task_id='No_Op'
    )

    t0 >> t1 