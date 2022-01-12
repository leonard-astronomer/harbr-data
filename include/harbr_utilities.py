# from dags.initial_load_to_harbr import AWS_CONN_ID, BUCKET, ENDPOINT
from include.Tracking import Tracking
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.models.variable import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
import urllib
import boto3

def indicate_initial_load_completed(product_name, **op_kwargs):
    fname='indicate_initial_load_completed'
    context=op_kwargs
    
    params =Variable.get("harbr_data."+product_name, deserialize_json=True)
    aws_conn_id=params['connection_id']
    bucket=params['bucket']
    endpoint=params['endpoint']
    run_id=context['dag_run'].run_id

    print(fname+": aws_conn_id: {}".format(aws_conn_id))
    print(fname+": bucket:      {}".format(bucket))
    print(fname+": endpoint:    {}".format(endpoint))
    print(fname+": run_id:      {}".format(run_id))

    s3 = S3Hook(aws_conn_id)

    
        
    track = Tracking(": indicate that the initial load has been completed")
    singleload_folder = endpoint+ "/singleload/"
    indicator_key=endpoint+"/initial_load_complete.indicator"
    print(fname+": source key:      {}".format(singleload_folder))
    print(fname+": dest key:        {}".format(indicator_key))

    try:
        s3.copy_object( source_bucket_key=singleload_folder,
                        dest_bucket_key=indicator_key,
                        source_bucket_name=bucket, 
                        dest_bucket_name=bucket)
        print("{}: Indicator key: '{}' created.".format(fname,indicator_key))
    except BaseException as err:
        msg = f"Unknown error: {err=}, {type(err)=}"
        raise err
    
    return

@task.python(multiple_outputs=True)
def check_if_initial_load(dag_name='required', product_name='required' ):
    fname='check_if_initial_load'
    context=get_current_context()
    
    params =Variable.get("harbr_data."+product_name, deserialize_json=True)
    aws_conn_id=params['connection_id']
    bucket=params['bucket']
    endpoint=params['endpoint']
    run_id=context['dag_run'].run_id

    print(fname+": aws_conn_id: {}".format(aws_conn_id))
    print(fname+": bucket:      {}".format(bucket))
    print(fname+": endpoint:    {}".format(endpoint))
    print(fname+": run_id:      {}".format(run_id))

    s3 = S3Hook(aws_conn_id)
        
    track = Tracking(": determine where to load")
    indicator_key=endpoint+"/initial_load_complete.indicator"
    try:
        initial_load=not s3.check_for_key(indicator_key, bucket)
        print("{}: Indicator key: {}, Initial load: {}".format(fname,indicator_key, initial_load))
    except BaseException as err:
        msg = f"Unknown error: {err=}, {type(err)=}"
        raise err
    
    
    if not initial_load:
        print("{}: Not initial load.".format(fname))
        folder=run_id
    else:
 
        folder = "singleload"

        must_be_empty=True
        list_singleload_folder = list_harbr_endpoint_folder(s3, bucket, endpoint, folder,  track)
        if(not track.success):
            msg="Operation: '{}' did not succeed. Mesage is '{}.'".format(track.name, track.error_message)
            print(msg)
            raise Exception(msg)
        
        if(len(list_singleload_folder)==0):
            msg='Singleload folder does not exist.  Check bucket and endpoint names.'
            raise Exception(msg)
        
        if(len(list_singleload_folder)>1):
            msg='Singleload folder is not empty.  Must be empty to proceed.\n=== Contents of singleload folder ==='
            for key in list_singleload_folder:
                msg+="\n    {}".format(key)
            raise Exception(msg)
    
    return {'initial_load':initial_load, 'folder':folder}

def list_key_prefix(s3, bucket, key, track):
    try:
        print("list_key_prefix: Bucket: {}, key: {}".format(bucket,key))
        key_list = s3.list_keys(bucket_name=bucket, prefix=key, delimiter='/')
    except BaseException as err:
        msg = f"Unknown error: {err=}, {type(err)=}"
        print(msg)
        track.failed(msg)
        key_list = None
    else:
        track.succeeded()

    return key_list

def list_harbr_endpoint_folder(s3, bucket, endpoint, folder, track) -> object:
    folder = endpoint + "/" + folder + "/"
    keys=list_key_prefix(s3, bucket, folder, track)
    return keys

def load_from_url_to_endpoint(
                folder='required',
                the_url='required', 
                product_name='required',
                asset_name='required'):

    params =Variable.get("harbr_data."+product_name, deserialize_json=True)
    aws_conn_id=params['connection_id']
    bucket=params['bucket']
    endpoint=params['endpoint']


    print("load_from_url_to_s3: aws_conn_id: {}".format(aws_conn_id))
    print("load_from_url_to_s3: bucket: {}".format(bucket))
    print("load_from_url_to_s3: endpoint: {}".format(endpoint))
    s3 = S3Hook(aws_conn_id)

    print("load_from_url_to_s3: the_url: {}".format(the_url))
    print("load_from_url_to_s3: the_table_name: {}".format(asset_name))
    


    try:
        f = urllib.request.urlopen(the_url)
    except BaseException as err:
        msg=f"Error while opening url: {err=}, {type(err)=}"
        raise Exception(msg)

    the_key= endpoint + "/" + folder +"/"+asset_name +"/table.csv"
    print("load_from_url_to_s3: the_file_name: {}".format(the_key))
    try:
        s3.load_file_obj(f,key=the_key, bucket_name=bucket, replace=False, encrypt=False, acl_policy=None)
    except BaseException as err:
        msg=f"Error while loading from url: {err=}, {type(err)=}"
        raise Exception(msg)

def write_tnf(**op_kwargs):
    print('op_kwargs: {}'.format(op_kwargs))
    context = get_current_context()
    task_instance = context['task_instance']
    f_name='write_tnf'
    product_name = op_kwargs['product_name']
    print(f_name + ":product_name: {}".format(product_name))
    folder = task_instance.xcom_pull(task_ids='check_if_initial_load',key='folder')
    print(f_name + ":      folder: {}".format(folder))

    params =Variable.get("harbr_data."+product_name, deserialize_json=True)
    aws_conn_id=params['connection_id']
    bucket=params['bucket']
    endpoint=params['endpoint']
    run_id=context['dag_run'].run_id


    print(f_name + ": aws_conn_id: {}".format(aws_conn_id))
    print(f_name + ":      bucket: {}".format(bucket))
    print(f_name + ":    endpoint: {}".format(endpoint))
    print(f_name + ":      run_id: {}".format(run_id))
    key=endpoint+"/tnfs/"+run_id+".tnf"
    print(f_name + ":         key: {}".format(key))
    tnf_file_contents='{{"DataFolder":"{}"}}'.format(run_id)
    print(f_name + ":TNF Contents: {}".format(tnf_file_contents))
    the_tnf=tnf_file_contents.encode('ASCII')

    s3 = S3Hook(aws_conn_id)
  

    s3.load_bytes(bytes_data=the_tnf, key=key, bucket_name=bucket)

# {"DataFolder":"the_folder_within_the_endpoint_that_contains_your_files"}


