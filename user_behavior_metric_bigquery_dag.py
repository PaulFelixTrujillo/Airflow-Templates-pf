from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
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
    'retry_delay': timedelta(seconds=1),
}

#Name the DAG and configuration

dag = DAG('user_behavior_metric_bigquery',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

bucket = 'de-bucket-terraform-pf'
region = "us-central1"



PROJECT_ID = "gcp-terraform-pf"
TABLE_NAME = "user_behavior_metric"
DATASET_NAME = "metrics"


insert_into_table_query = (
        f"TRUNCATE TABLE {DATASET_NAME}.{TABLE_NAME}; "
        f"INSERT INTO {DATASET_NAME}.{TABLE_NAME} "
        f"SELECT * FROM EXTERNAL_QUERY(\"projects/gcp-terraform-pf/locations/us-central1/connections/postgres-conn-pf\", "
        f"\"\"\"SELECT u.customer_id AS customer_id, CAST(SUM(u.quantity * u.unit_price) AS DECIMAL(18, 5)) AS amount_spent, SUM(r.positive_review) AS positive_review, COUNT(r.cid) AS review_count, CURRENT_DATE AS insert_date "
        f"FROM public.reviews r "
        f"JOIN public.user_purchase_dt u ON r.cid = u.customer_id "
        f"GROUP BY u.customer_id;\"\"\"); "
    )


#Task

create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id = "create-dataset",
            dataset_id = DATASET_NAME,
            project_id = PROJECT_ID,
            location = region,
            exists_ok = True,
            dag = dag
        )


create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id = DATASET_NAME,
    project_id = PROJECT_ID,
    table_id = TABLE_NAME,
    exists_ok = True,
    schema_fields=[
        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "amount_spent", "type": "DECIMAL", "mode": "REQUIRED"},
        {"name": "review_score", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "insert_date", "type": "DATE", "mode": "REQUIRED"},
    ],
    dag = dag
)

insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": insert_into_table_query,
            "useLegacySql": False,
        }
    },
    location=region,
    project_id = PROJECT_ID,
    dag = dag
)



create_dataset >> create_table >> insert_query_job