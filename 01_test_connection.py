from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pendulum
from datetime import timedelta
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource

import io

# MinIO configuration
MINIO_CONN_ID = "minio-local-connection"
RAW_BUCKET = "raw-bucket"
STAGING_BUCKET = "staging-bucket"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_minio_client():
    """Get MinIO client using Airflow connection"""
    connection = BaseHook.get_connection(MINIO_CONN_ID)
    
    return Minio(
        endpoint=f"minio:9000",
        access_key=connection.login,
        secret_key=connection.password,
        secure=connection.schema == 'https'
    )

def create_buckets():
    """Create raw and staging buckets if they don't exist"""
    client = get_minio_client()
    
    for bucket in [RAW_BUCKET, STAGING_BUCKET]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Created bucket: {bucket}")
        else:
            print(f"Bucket already exists: {bucket}")

def upload_sample_data():
    """Upload sample data to raw bucket"""
    client = get_minio_client()
    
    sample_data = "id,name,value\n1,test1,100\n2,test2,200\n3,test3,300"
    data_bytes = sample_data.encode('utf-8')
    data_stream = io.BytesIO(data_bytes)
    
    client.put_object(
        RAW_BUCKET,
        "sample_data.csv",
        data_stream,
        length=len(data_bytes),
        content_type='text/csv'
    )
    print("Uploaded sample data to raw bucket")

def list_raw_files(**context):
    """List all files in raw bucket"""
    client = get_minio_client()
    
    objects = client.list_objects(RAW_BUCKET)
    file_list = [obj.object_name for obj in objects]
    print(f"Files in raw bucket: {file_list}")
    
    # Push file list to XCom for next task
    context['ti'].xcom_push(key='raw_files', value=file_list)
    return file_list

def move_to_staging(**context):
    """Move files from raw to staging bucket"""
    client = get_minio_client()
    
    # Get file list from previous task
    file_list = context['ti'].xcom_pull(key='raw_files', task_ids='list_raw_files')
    
    if not file_list:
        print("No files to move")
        return
    
    for filename in file_list:
        try:

            source = CopySource(RAW_BUCKET, filename)

            # Copy file to staging
            client.copy_object(
                STAGING_BUCKET,
                filename,
                source
            )
            print(f"Copied {filename} to staging bucket")
            
            # Optional: Remove from raw bucket after successful copy
            # client.remove_object(RAW_BUCKET, filename)
            # print(f"Removed {filename} from raw bucket")
            
        except S3Error as e:
            print(f"Error moving {filename}: {e}")
            raise

def verify_staging(**context):
    """Verify files are in staging bucket"""
    client = get_minio_client()
    
    objects = client.list_objects(STAGING_BUCKET)
    staging_files = [obj.object_name for obj in objects]
    print(f"Files in staging bucket: {staging_files}")
    
    # Get original file list
    raw_files = context['ti'].xcom_pull(key='raw_files', task_ids='list_raw_files')
    
    if set(raw_files) == set(staging_files):
        print("✓ All files successfully moved to staging!")
    else:
        print(f"⚠ Missing files: {set(raw_files) - set(staging_files)}")

with DAG(
    'minio_raw_to_staging',
    default_args=default_args,
    description='Test pipeline to move data from raw to staging bucket in MinIO',
    schedule=None, 
    start_date=pendulum.today("UTC").add(days=-2),
    catchup=False,
    tags=['test', 'minio'],
) as dag:

    task_create_buckets = PythonOperator(
        task_id='create_buckets',
        python_callable=create_buckets,
    )

    task_upload_sample = PythonOperator(
        task_id='upload_sample_data',
        python_callable=upload_sample_data,
    )

    task_list_raw = PythonOperator(
        task_id='list_raw_files',
        python_callable=list_raw_files,
    )

    task_move_to_staging = PythonOperator(
        task_id='move_to_staging',
        python_callable=move_to_staging,
    )

    task_verify = PythonOperator(
        task_id='verify_staging',
        python_callable=verify_staging,
    )

    # Define task dependencies
    task_create_buckets >> task_upload_sample >> task_list_raw >> task_move_to_staging >> task_verify