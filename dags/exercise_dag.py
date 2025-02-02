from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

with DAG(
    dag_id="user_processing",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    # add the task here
    # Task to create the users table in Postgres
    create_table = PostgresOperator(
        task_id='create_table',  # Name of the task
        postgres_conn_id='postgres',  # Connection ID to use from Airflow UI
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        """,  # SQL query to create the table
    )
    
    is_api_available = HttpSensor(  # Task to check if the API is available
        task_id="is_api_available",
        http_conn_id="user_api",
        endpoint="api/"
    )
    
    extract_user = SimpleHttpOperator(  # Task to extract the user data from the API
        task_id="extract_user",
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )