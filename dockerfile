FROM apache/airflow:3.0.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
COPY setup.py .
COPY plugins/ ./plugins/
COPY dags/ ./dags/

USER root

RUN chown -R airflow:0 /opt/airflow && \
    chmod -R 775 /opt/airflow

USER airflow

RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install -e .

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/plugins:/opt/airflow/dags:/opt/airflow/include"

WORKDIR /opt/airflow