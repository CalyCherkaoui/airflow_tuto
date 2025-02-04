#!/bin/bash
# This script runs an Apache Airflow container via Docker,
# initializing the Airflow database, creating an admin user,
# and then starting the webserver and scheduler.

set -x

SCRIPT_DIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

docker run \
-ti \
-p 8080:8080 \
-v ${SCRIPT_DIR}/../dags/download_rocket_launches.py:/opt/airflow/dags/download_rocket_launches.py \
--name airflow
--entrypoint=/bin/bash \
apache/airflow:2.0.0-python3.8 \
-c '( \
airflow db init && \
airflow users create --username admin --password admin --firstname Anonymous --lastname Admin --role Admin --email admin@example.org \
); \
airflow webserver & \
airflow scheduler \
'
