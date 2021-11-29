from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

dag = DAG('user_behavior_metric',
          default_args=default_args,
          schedule_interval='@once',
          catchup=False)

bucket = 'de-bucket-terraform-pf'
schema_name = "metrics"
table_name = "user_behavior_metric"

create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name} ;"

create_table_query = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (    
                    customer_id INTEGER,
                    amount_spent DECIMAL(18, 5),
                    review_score INTEGER,
                    review_count INTEGER,
                    insert_date DATE);"""
                            
create_insert_into_table = f"""TRUNCATE TABLE {schema_name}.{table_name};
                    INSERT INTO {schema_name}.{table_name}
                    SELECT u.customer_id, CAST(SUM(u.quantity * u.unit_price) AS DECIMAL(18, 5)), SUM(r.positive_review), COUNT(r.cid), CURRENT_DATE                      
                    FROM public.reviews r
                    JOIN public.user_purchase u ON r.cid = u.customer_id
                    GROUP BY u.customer_id;
                    """


#Task

task_create_schema = PostgresOperator(task_id = 'create_schema',
                        sql=create_schema_query,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_create_table = PostgresOperator(task_id = 'create_table',
                        sql=create_table_query,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)

task_insert_into_table = PostgresOperator(task_id = 'insert_into_table',
                        sql=create_insert_into_table,
                            postgres_conn_id= 'postgres_default', 
                            autocommit=True,
                            dag= dag)


task_create_schema >> task_create_table >> task_insert_into_table