FROM python:3.6-alpine
WORKDIR /usr/src/app

RUN apk add --update \
    postgresql-dev \
    build-base \
    openblas-dev \
    gfortran \
    && rm -rf /var/cache/apk/*

RUN pip install --no-cache-dir \
    numpy===1.14.3 \
    apistar==0.5.41 \
    gunicorn==19.8.1 \
    dill===0.2.7.1 \
    minio===4.0.0 \
    psycopg2-binary===2.7.4 \
    scipy \
    scikit-learn \
    sqlalchemy

RUN pip install --no-cache-dir --no-deps pandas

EXPOSE 8000
