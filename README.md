# airflow-hands-on
Repo for the-complete-hands-on-course-to-master-apache-airflow

```sh
docker ps
docker exec -it airflow-hands-on-airflow-scheduler-1 /bin/bash

airflow -h
```

## Execute concrete tasks
### airflow tasks test dag_id task_id past_date
```sh
airflow tasks test user_processing create_table 2022-01-01
```