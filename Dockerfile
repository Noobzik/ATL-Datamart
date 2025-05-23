FROM apache/airflow:2.8.1-python3.11

USER root

# Installer minio
RUN pip install minio

USER airflow

