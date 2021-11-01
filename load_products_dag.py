import airflow.utils.dates

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_modules.dag_gcs_postgres import GoogleCloudStorageToPostgresTransfer

default_args = {
    'owner': 'paul.felix',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_insert_data', default_args = default_args, schedule_interval = '@daily')

process_dag = GoogleCloudStorageToPostgresTransfer(
    task_id = 'dag_gcs_postgres',
    schema = 'bootcampdb_pf',
    table= 'user_purchase',
    bucket = 'bootcamp-wl-de-airflow-pf',
    key =  'user_purchase.csv',
    gcs_conn_postgres_id = 'postgres_default',
    google_cloud_storage_conn_id ='gcp_default',   
)

process_dag