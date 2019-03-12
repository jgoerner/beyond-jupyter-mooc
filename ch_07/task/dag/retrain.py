from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def retrain():
    logging.info("Your Code Here")

### DAG ###
default_args={
        "owner":"airflow",
        "depends_on_past":"false",
        "start_date":datetime.today()-timedelta(days=1),
        }

dag = DAG(
    dag_id="retrain-dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=30))

### TASK ###
task = PythonOperator(
    task_id="model-retraining",
    python_callable=retrain,
    dag=dag,
)
