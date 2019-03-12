from datetime import datetime, timedelta
import logging
from pathlib import Path
import re
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from minio import Minio
import pandas as pd
from sklearn.externals import joblib
from sqlalchemy import create_engine

def retrain():
    # fetching the data
    logging.info("fetching model from object store")

    # create a connection to the object store
    minio_client = Minio(
            endpoint="minio:9000",
            access_key="12345678",
            secret_key="password",
            secure=False
    )

    # load the model from object store
    with NamedTemporaryFile() as tmp:
        # dump the classifier
        minio_client.fget_object("trained-models", "rf.pkl", tmp.name)
        # import the model
        clf = joblib.load(tmp.name)

    # load the data
    logging.info("fetching data")
    con = create_engine("postgres://shared:password@postgres")
    df = pd.read_sql(
            sql="t_wine",
            con=con,
    )

    logging.info("fitting new data")
    clf.fit(
        X=df.drop("target", axis=1),
        y=df["target"]
    )

    # persist the model
    logging.info("storing new model")
    now = str(datetime.now())
    now_formatted = re.sub(r"[ .:]", "-", now)
    with NamedTemporaryFile() as tmp:
        # dump the classifer
        joblib.dump(value=clf, filename=tmp.name)
        # upload the model to minio
        minio_client.fput_object(
                bucket_name="trained-models",
                object_name="rf-{now_formatted}.pkl".format(**locals()),
                file_path=Path(tmp.name)
        )

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
