FROM apache/airflow:2.11.0-python3.11

RUN pip install --no-cache-dir apache-airflow-providers-postgres minio

# (Optional) Ensure correct permissions for mounted volumes
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins \
    && chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins
