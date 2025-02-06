from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

with DAG (
    dag_id="producer",
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False
):
    
    @task(outlets=[my_file]) # This tasks updates the dataset
    def update_dataset():
        with open("/tmp/my_file.txt", "a+") as f:
            f.write("producer update")

    @task(outlets=[my_file_2]) # This tasks updates the dataset
    def update_dataset_2():
        with open("/tmp/my_file_2.txt", "a+") as f:
            f.write("producer update")
            
    update_dataset() >> update_dataset_2()