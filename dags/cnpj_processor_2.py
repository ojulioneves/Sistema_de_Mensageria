import os
import zipfile
import glob
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BASE_DIR = '/tmp'

class FileProcessor:
    def __init__(self, directory: str):
        self.directory = directory

    def extract_and_rename(self):
        zip_files = [f for f in glob.glob(os.path.join(self.directory, '*.zip')) if zipfile.is_zipfile(f)]
        print(f"Encontrados {len(zip_files)} arquivos .zip para processar.")

        renamed_files = []

        try:
            for zip_path in zip_files:
                print(f"Processando: {zip_path}")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    files = zip_ref.namelist()
                    zip_ref.extractall(self.directory)
                    print(f"Arquivos extraídos: {files}")

                    base = os.path.splitext(os.path.basename(zip_path))[0]
                    for f in files:
                        old = os.path.join(self.directory, f)
                        new = os.path.join(self.directory, f"{base}.csv")
                        if os.path.exists(old):
                            os.rename(old, new)
                            renamed_files.append(new)
                            print(f"Renomeado: {old} -> {new}")
                        else:
                            print(f"Arquivo não encontrado para renomear: {old}")

                os.remove(zip_path)
                print(f"Removido arquivo zip: {zip_path}")

            print("Processamento concluído com sucesso.")
        except Exception as e:
            print(f"Erro durante o processamento: {e}")

        return renamed_files

@task()
def extract_files():
    period = datetime.today().replace(day=1).strftime('%Y-%m')
    target_dir = os.path.join(BASE_DIR, period)

    processor = FileProcessor(target_dir)
    extracted_files = processor.extract_and_rename()
    return extracted_files


@task(task_id="...", executor_config={"KubernetesExecutor": {"request_memory": "4Gi"}})
def process_single_file(file_path: str):
    try:
        print(f"Processando arquivo: {file_path}")
        for chunk in pd.read_csv(file_path, sep=';', encoding='latin1', header=None, chunksize=100_000):
            print(f"Chunk com {len(chunk)} linhas")
        
    except Exception as e:
        print(f"Erro ao processar {file_path}: {e}")
        raise e

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1), "retries": 0}

with DAG(
    dag_id='processa_receita_federal_dinamico',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:


    extracted = extract_files()
    process_tasks = process_single_file.expand(file_path=extracted)

    trigger_dag3 = TriggerDagRunOperator(
        task_id='trigger_processa_dag3',
        trigger_dag_id='cnpj_bronze_pipeline',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )    

    extracted >> process_tasks >> trigger_dag3