import airflow
import os
import psycopg2
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime

"""
Load CSV > Postgres in GCP Cloud SQL Instance
"""

#default arguments 

default_args = {
    'owner': 'paul.felix',
    'depends_on_past': False,    
    'start_date': datetime(2021, 10, 15),
    'email': ['felix-p@hotmail.es'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=1),
}

#name the DAG and configuration
dag = DAG('insert_movie_review_postgres',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

def file_path(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    split_path = relative_path.split("/")
    new_path = os.path.join(dir, *split_path)
    return new_path

file_name = "movie_review.csv"
table_name = "movie_review_dt"
bucket = 'de-bucket-terraform-pf'

COPY_QUERY = f""" COPY {table_name} from stdin WITH CSV HEADER DELIMITER ',' ESCAPE '"' """

def csv_to_postgres():
    #Open Postgres Connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    get_postgres_conn = PostgresHook(postgres_conn_id='postgres_default').get_conn()
    curr = get_postgres_conn.cursor()    
    # CSV loading to table
    with open(file_name, "r") as f:
        next(f)
        #curr.copy_from(f, table_name, sep=",")
        curr.copy_expert(COPY_QUERY, file = f)
        get_postgres_conn.commit()
        curr.close()

def delete_file():
    os.remove(file_name)

#Task 

task_create_table = PostgresOperator(task_id = 'create_table',
                        sql=f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (    
                            cid INTEGER,
                            review_str VARCHAR);
                            """,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_download_file = GCSToLocalFilesystemOperator(task_id="download_file",
        object_name=file_name,
        bucket=bucket,
        filename=file_name,
        gcp_conn_id = "google_cloud_default",
        dag = dag
    )

task_load_csv = PythonOperator(task_id='csv_to_database',
                   provide_context=True,
                   python_callable=csv_to_postgres,
                   dag=dag)

task_delete_file = PythonOperator(task_id='delete_file',
                   provide_context=True,
                   python_callable=delete_file,
                   dag=dag)



task_create_table >> task_download_file >> task_load_csv >> task_delete_file