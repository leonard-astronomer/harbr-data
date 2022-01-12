from subprocess import CompletedProcess
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

from sqlalchemy.sql.elements import False_

from include.harbr_utilities import  check_if_initial_load, load_from_url_to_endpoint, write_tnf
from include.Tracking import Tracking
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.models.variable import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


PRODUCT_NAME="sample_data_product"
URLS = ['https://api.mockaroo.com/api/511cd950?count=1000&key=a8034b60',
        'https://api.mockaroo.com/api/55e57ce0?count=1000&key=a8034b60',
        'https://api.mockaroo.com/api/f51b19d0?count=1000&key=a8034b60'
]
ASSET_NAMES = ['names', 'ip', 'app_info']



@task.python
def load_t1(folder='required', url='required', asset_name='required', product_name='required'):
    print("folder:"+folder)
    load_from_url_to_endpoint(folder=folder, the_url=url, product_name=product_name, 
                                asset_name=asset_name)
    print("--------COMPLETE--------")
    return

@task.python
def load_t2(folder='required', url='required', asset_name='required', product_name='required'):
    print("folder:"+folder)
    load_from_url_to_endpoint(folder=folder, the_url=url, product_name=product_name, 
                                asset_name=asset_name)
    print("--------COMPLETE--------")
    return

@task.python
def load_t3(folder='required', url='required', asset_name='required', product_name='required'):
    print("folder:"+folder)
    load_from_url_to_endpoint(folder=folder, the_url=url, product_name=product_name, 
                                asset_name=asset_name)
    print("--------COMPLETE--------")
    return

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2000, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

def _initial_or_repeating():
    f_name='_initial_or_repeating'
    context = get_current_context()
    task_instance = context['task_instance']
    is_initial = task_instance.xcom_pull(task_ids='check_if_initial_load',key='initial_load')
    print("{}: initial_load?: {}".format(f_name, is_initial))

    if(is_initial):
        print("{}:  COMPLETED ".format(f_name)) 
        return 'completed'
    else:
        print("{}:  NOTIFY OF TRANSFER ".format(f_name))
        return 'notify_of_transfer'



DAG_NAME = 'load_to_harbr'
# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(    description="load to harbr",
         start_date=datetime(2000, 1, 1),
         schedule_interval=None,
         catchup=False,
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=1,
         default_args=default_args
         )
def load_to_harbr():
    
    routing = check_if_initial_load(dag_name=DAG_NAME, product_name=PRODUCT_NAME)

    t1_1 = load_t1(
        folder=routing['folder'],
        url=URLS[0],
        asset_name=ASSET_NAMES[0], 
        product_name=PRODUCT_NAME
    )
    t1_2 = load_t2(
        folder=routing['folder'],
        url=URLS[1],
        asset_name=ASSET_NAMES[1],  
        product_name=PRODUCT_NAME
    )
    t1_3 = load_t3(
        folder=routing['folder'],
        url=URLS[2],
        asset_name=ASSET_NAMES[2],  
        product_name=PRODUCT_NAME
    )

    branch = BranchPythonOperator(
        task_id='initial_load_or_not',
        python_callable=_initial_or_repeating
    )

    completed = DummyOperator(task_id='completed')
    op_kwargs={'product_name':PRODUCT_NAME}
    write_transfer_notification_file = PythonOperator(
            task_id='notify_of_transfer', python_callable=write_tnf, op_kwargs=op_kwargs)

    routing >> [t1_1, t1_2, t1_3] >> branch >> [completed, write_transfer_notification_file ]
    
  

dag = load_to_harbr()
