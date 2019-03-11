from datetime import datetime
from pathlib import Path
import re
from tempfile import NamedTemporaryFile

from apistar import App, Route
from minio import Minio
import numpy as np
import pandas as pd
from sklearn.externals import joblib
from sqlalchemy import create_engine

SECRET_TOKEN = "python"

minio_client = Minio(
        endpoint="minio:9000",
        access_key="12345678",
        secret_key="password",
        secure=False
)

# Retrain Handler
def retrain(token=None):
    # security check
    if not token or token != SECRET_TOKEN:
        return {"message": "No retraining allowed"}

    # get model
    with NamedTemporaryFile() as tmp:
        # dump the classifier to the local file system
        minio_client.fget_object("trained-models", "rf.pkl", tmp.name)
        # import the model
        clf = joblib.load(tmp.name)

    # load the data
    con = create_engine("postgres://shared:password@postgres")
    df = pd.read_sql(
            sql="t_wine",
            con=con,
    )

    clf.fit(
        X=df.drop("target", axis=1),
        y=df["target"]
    )

    # persist the model
    now = str(datetime.now())
    now_formatted = re.sub(r"[ .:]", "-", now)
    with NamedTemporaryFile() as tmp:
        # dump the classifier to local file system
        joblib.dump(value=clf, filename=tmp.name)
        # upload the local file to minio
        minio_client.fput_object(
            bucket_name="trained-models",
            object_name="rf-{now_formatted}.pkl".format(**locals()),
            file_path=Path(tmp.name)
        )

    return {"message": "Model successfully retrained"}

def predict(
    alcohol: str="0",
    malic_acid: str="0",
    ash: str="0",
    alcalinity_of_ash: str="0",
    magnesium: str="0",
    total_phenols: str="0",
    flavanoids: str="0",
    nonflavanoid_phenols: str="0",
    proanthocyanins: str="0",
    color_intensity: str="0",
    hue: str="0",
    diluted_wines: str="0",
    proline: str="0"):

    # convert query params to list of ints
    features = np.array([
        alcohol,
        malic_acid,
        ash,
        alcalinity_of_ash,
        magnesium,
        total_phenols,
        flavanoids,
        nonflavanoid_phenols,
        proanthocyanins,
        color_intensity,
        hue,
        diluted_wines,
        proline],
        dtype=int).reshape(1, -1)

    # load the model
    with NamedTemporaryFile() as tmp:
        # dump model to local file system
        minio_client.fget_object("trained-models", "rf.pkl", tmp.name)
        clf = joblib.load(tmp.name)

    # make classification
    probas = clf.predict_proba(features)[0]

    # return results
    return {
            "probas": {
                "class_0": probas[0],
                "class_1": probas[1],
                "class_2": probas[2],
            }
           }

# Routes
routes = [
        Route("/api/retrain", method="GET", handler=retrain),
        Route("/api/predict", method="GET", handler=predict),
]

# App
app = App(routes=routes)

if __name__ == "__main__":
    app.serve("0.0.0.0", 5000, debug=True)  # serve on 0.0.0.0 for docker use
