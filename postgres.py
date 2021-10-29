import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_user_table = PostgresOperator(
        task_id="create_user_table",
        sql=""" 
            CREATE TABLE user_purchase (
                invoice_number varchar (10),
                stock_code varchar (20),
                detail varchar (1000),
                quality int,
                invoice_date timestamp,
                unit_price numeric(8,3)
                customer_id int,
                country varchar(20));
          """,
    )