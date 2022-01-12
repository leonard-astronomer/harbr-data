from airflow import DAG
from airflow.models.taskinstance import TR
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from datetime import datetime, timedelta


from include.Tracking import Tracking
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 

PRODUCT_NAME="sample_data_product"


def list_and_delete_from_singleload( product_name='required'):

    params =Variable.get("harbr_data."+product_name, deserialize_json=True)
    aws_conn_id=params['connection_id']
    bucket=params['bucket']
    endpoint=params['endpoint']

    print("list_and_delete_from_singleload: aws_conn_id: {}".format(aws_conn_id))
    print("list_and_delete_from_singleload: bucket: {}".format(bucket))
    print("list_and_delete_from_singleload: endpoint: {}".format(endpoint))
    s3 = S3Hook(aws_conn_id)

    prefix=endpoint+"/singleload/"
    print("list_and_delete_from_singleload: The prefix for the keys being listed is '{}'".format(prefix))
    try:
        the_keys = s3.list_keys(bucket_name=bucket, prefix=prefix)
    except BaseException as err:
        msg=f"list_and_delete_from_singleload: Error while listing keys in singleload folder: {err=}, {type(err)=}"
        raise Exception(msg)

    print("The keys: {}".format(the_keys))
    delete_list=[]
    for key in the_keys:
        print(key)
        if(key != prefix):
            delete_list.append(key)
    
    key=endpoint+"/initial_load_complete.indicator"
    print("list_and_delete_from_singleload: Delete indicate named '{}' if it exists".format(key))
    try:
        indicatore_exists = s3.check_for_key(key=key, bucket_name=bucket)
        print(indicatore_exists)
        if(indicatore_exists):
            delete_list.append(key)
    except BaseException as err:
        msg=f"list_and_delete_from_singleload: Error while checking for existence of {err=}, {type(err)=}"
        raise Exception(msg)

    try:
        delete_result=s3.delete_objects(bucket=bucket, keys=delete_list)
    except BaseException as err:
        msg=f"list_and_delete_from_singleload: Error while opening url: {err=}, {type(err)=}"
        raise Exception(msg)
    print("list_and_delete_from_singleload: delete result: {}".format(delete_result))



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

  
# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('remove_initial_load',
         start_date=datetime(2000, 1, 1),
         schedule_interval=None,
         catchup=False,
         max_active_runs=1,
         default_args=default_args
         ) as dag:


    t0 = PythonOperator(
        task_id='list_and_delete_from_singleload',
        python_callable=list_and_delete_from_singleload,
        op_kwargs={'product_name':PRODUCT_NAME},
        provide_context=True
    )

    t1 = DummyOperator(
        task_id='No_Op'
    )

    t0 >> t1 