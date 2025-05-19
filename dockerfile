FROM apache/airflow:3.0.0

USER root

# Instala dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
        libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copia os arquivos necessários
COPY requirements.txt .
COPY setup.py .
COPY plugins/ ./plugins/
COPY dags/ ./dags/

USER root

# Corrige permissões
RUN chown -R airflow:0 /opt/airflow && \
    chmod -R 775 /opt/airflow

USER airflow

# Instala dependências Python (incluindo requirements.txt)
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install -e .

# Configura paths
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/plugins:/opt/airflow/dags:/opt/airflow/include"

# Define o diretório de trabalho
WORKDIR /opt/airflow