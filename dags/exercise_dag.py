from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from pandas import json_normalize

def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user") # fetch data pushed by the previous task extract_user
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)  
    
def _store_user():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.copy_expert(
        sql="COPY users FROM STDIN WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )

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
    
    process_user = PythonOperator(  # Task to process the user data
        task_id="process_user",
        python_callable=_process_user        
    )
    
    store_user = PythonOperator(  # Task to store the user data in Postgres
        task_id="store_user",
        python_callable=_store_user
    )
    
    create_table >> is_api_available >> extract_user >> process_user >> store_user # Set the task dependencies