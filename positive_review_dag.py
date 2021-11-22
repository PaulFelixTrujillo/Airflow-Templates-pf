from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import timedelta
from datetime import datetime
import os


#default arguments 

default_args = {
    'owner': 'paul.felix',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 15),
    'email': ['felix-p@hotmail.es'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=2),
}

#name the DAG and configuration
dag = DAG('positive_review_spark',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)


def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path


bucket = 'de-bucket-terraform-pf'
REGION = "us-central1"
PROJECT_ID = "gcp-terraform-pf"
file_name = "postgresql-42.3.1.jar"


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": "cluster-db-pf"},
    "pyspark_job": {"main_python_file_uri": "gs://data-bootcamp-terraforms-us/positive_review_job.py", "jar_file_uris": ["file://" + file_path(file_name)]}
    
}


#Task 
task_download_jar = GCSToLocalFilesystemOperator(task_id="download_jar",
        object_name=file_name,
        bucket=bucket,
        filename=file_name,
        gcp_conn_id = "google_cloud_default",
        dag = dag
    )

submit_spark = DataprocSubmitJobOperator(task_id="review_spark_job",
                                            job=PYSPARK_JOB,
                                            region=REGION,
                                            project_id=PROJECT_ID,
                                            dag = dag
                                        )







task_download_jar >> submit_spark