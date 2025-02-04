import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id="02_download_rocket_launches",
    description="Download rocket launches from NASA Launch Library.",
    start_date=airflow.utils.dates.days_ago(15),
    # At what interval the DAG should run
    schedule_interval="@daily",
)

download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)