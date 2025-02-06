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

## Check extract_user and process_user tasks
```sh
docker ps
docker exec -it airflow-hands-on-airflow-worker-1 /bin/bash (only in powershell, not in git bash)
ls /tmp (CSV file should be there)

control + d

docker ps
docker exec -it airflow-hands-on-postgres-1 /bin/bash
psql -Uairflow
select * from users; 
```

---------

## Executors

# Copy the airflow conf to our local machine
docker cp airflow-hands-on-airflow-scheduler-1:/opt/airflow/airflow.cfg .

# Vars starting like AIRFLOW__CORE__EXECUTOR: CeleryExecutor overrides values from airflow.cfg



