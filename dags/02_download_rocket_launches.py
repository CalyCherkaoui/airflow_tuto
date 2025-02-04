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

# PYTHON function that downloads all images from the launches.json file
def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                # Extracts image URLs for every rocket launch and downloads the images to /tmp/images
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

# PYTHON operator that downloads all images from the launches.json file
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

# Set dependencies between all tasks
download_launches >> get_pictures >> notify

# __________________________________________________________________________
# In Python, the rshift operator (>>) is used to set dependencies between tasks. 
#
# There is a difference, though. Tasks in Airflow manage the execution of an operator;
# they can be thought of as a small wrapper or manager around an operator that
# ensures the operator executes correctly. The user can focus on the work to be done
# by using operators, while Airflow ensures correct execution of the work via tasks
# __________________________________________________________________________
