FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.3-python3.11}


RUN pip install --upgrade pip
RUN pip install --upgrade pip setuptools wheel


USER root

RUN apt-get update && apt-get install -y gosu

RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt requirements.txt
COPY data   data
RUN pip3 install -r ./requirements.txt
