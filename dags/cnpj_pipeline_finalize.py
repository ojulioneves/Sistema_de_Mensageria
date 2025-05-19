from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import os
import shutil
import math

MONGO_URI = "mongodb://root:example@mongo:27017/admin"
PERIOD = datetime.today().replace(day=1).strftime("%Y-%m")
DATA_DIR = os.path.join("/tmp", PERIOD)

COLECOES_POR_BANCO = {
    "bronze": ["cnaes", "empresas", "estabelecimentos", "motivos", "municipios", "naturezas", "paises", "qualificacoes", "simples", "socios"],
    "silver": ["empresas", "estabelecimentos", "socios"]
}

class SnapshotManager:
    def __init__(self, mongo_uri, period):
        self.client = MongoClient(mongo_uri)
        self.bronze_db = self.client["bronze"]
        self.snapshot_collection = self.bronze_db["snapshot"]
        self.period = period
        self.now = datetime.now()
        self.snapshot_data = []

    def gerar_snapshot(self):
        empresas = self.bronze_db["empresas"].find({}, {"_id": 1, "cnpj_basico": 1})
        for empresa in empresas:
            for k, v in empresa.items():
                if isinstance(v, float) and math.isnan(v):
                    empresa[k] = None
            self.snapshot_data.append({
                "_origem": str(empresa["_id"]),
                "cnpj_basico": empresa.get("cnpj_basico"),
                "data_snapshot": self.now
            })
        if self.snapshot_data:
            self.snapshot_collection.delete_many({})
            self.snapshot_collection.insert_many(self.snapshot_data)
            print(f"Criado com {len(self.snapshot_data)} registros.")
        else:
            print("Nenhum dado encontrado.")
        return self.snapshot_data

def criar_snapshot():
    manager = SnapshotManager(MONGO_URI, PERIOD)
    manager.gerar_snapshot()

def deletar_dados():
    client = MongoClient(MONGO_URI)

    for banco_nome, colecoes in COLECOES_POR_BANCO.items():
        db = client[banco_nome]
        for nome_colecao in colecoes:
            resultado = db[nome_colecao].delete_many({})
            print(f"{resultado.deleted_count} registros deletados de {banco_nome}.{nome_colecao}")

def limpar_diretorio():
    if os.path.exists(DATA_DIR):
        for filename in os.listdir(DATA_DIR):
            file_path = os.path.join(DATA_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Erro ao deletar {file_path}: {e}")
        print("Diretório limpo.")
    else:
        print("Diretório não existe.")

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1), "retries": 0}

with DAG(
    dag_id='snapshot_cleanup',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='criar_snapshot',
        python_callable=criar_snapshot
    )

    t2 = PythonOperator(
        task_id='deletar_dados_staging',
        python_callable=deletar_dados
    )

    t3 = PythonOperator(
        task_id='limpar_diretorio_temporario',
        python_callable=limpar_diretorio
    )

    t1 >> t2 >> t3