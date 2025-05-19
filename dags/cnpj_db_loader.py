from datetime import datetime
import os
import glob
import pandas as pd
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PERIOD = datetime.today().replace(day=1).strftime("%Y-%m")
DATA_DIR = os.path.join("/tmp", PERIOD)
CNAE_FILTERS = { "5250803", "5231102", "5250804" }
CHUNKSIZE = 100_000

EXECUTOR_CONFIG = {
    "KubernetesExecutor": {"request_memory": "2Gi", "limit_memory": "4Gi"}
}

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1), "retries": 0}

with DAG(
    dag_id="cnpj_bronze_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "cnpj"],
) as dag:

    @task(executor_config=EXECUTOR_CONFIG)
    def load_estabelecimentos():
        logger = logging.getLogger("airflow.task")
        hook = MongoHook(conn_id="mongo_default", mongo_db="bronze")
        coll = hook.get_collection("estabelecimentos")
        coll.delete_many({})

        cnpjs = set()
        total_ins = 0
        pattern = os.path.join(DATA_DIR, "*Estabelecimentos*.csv")
        for file in glob.glob(pattern):
            for chunk in pd.read_csv(
                file,
                sep=';',
                names=["cnpj_basico", "cnpj_ordem", "cnpj_dv", "matriz_filial", "nome_fantasia",
                       "situacao_cadastral", "data_situacao", "motivo_situacao", "nome_cidade_exterior",
                       "pais", "data_inicio", "cnae_principal", "cnae_secundario", "tipo_logradouro",
                       "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio",
                       "ddd1", "telefone1", "ddd2", "telefone2", "ddd_fax", "fax", "email",
                       "situacao_especial", "data_situacao_especial"],
                header=None,
                dtype=str,
                chunksize=CHUNKSIZE,
                low_memory=False,
                encoding='latin1'
            ):
                chunk = chunk.apply(lambda c: c.str.strip() if c.dtype == 'object' else c)
                filt = chunk[chunk['cnae_principal'].isin(CNAE_FILTERS)]
                if not filt.empty:
                    records = filt.to_dict(orient='records')
                    result = coll.insert_many(records, ordered=False)
                    count = len(result.inserted_ids)
                    total_ins += count
                    cnpjs.update(filt['cnpj_basico'].unique())
        logger.info(f"Inserted {total_ins} estabelecimento records.")
        return list(cnpjs)

    @task(executor_config=EXECUTOR_CONFIG)
    def load_related(cnpjs: list, tipo: str):
        logger = logging.getLogger("airflow.task")
        hook = MongoHook(conn_id="mongo_default", mongo_db="bronze")
        coll_name = tipo.lower()
        coll = hook.get_collection(coll_name)
        coll.delete_many({})

        configs = {
            "Empresas": ["cnpj_basico", "razao_social", "natureza_juridica", "qualificacao_responsavel",
                          "capital_social", "porte_empresa", "ente_federativo"],
            "Simples": ["cnpj_basico", "opcao_simples", "data_opcao_simples", "data_exclusao_simples",
                         "opcao_mei", "data_opcao_mei", "data_exclusao_mei"],
            "Socios": ["cnpj_basico", "identificador_socio", "nome_socio", "carteira_socio",
                        "qualificacao_socio", "data_entrada_sociedade", "pais",
                        "representante_legal", "nome_representante", "qualificacao_representante", "faixa_etaria"]
        }
        cols = configs[tipo]
        total_ins = 0
        pattern = os.path.join(DATA_DIR, f"*{tipo}*.csv")
        for file in glob.glob(pattern):
            for chunk in pd.read_csv(
                file,
                sep=';',
                names=cols,
                header=None,
                dtype=str,
                chunksize=CHUNKSIZE,
                low_memory=False,
                encoding='latin1'
            ):
                chunk = chunk.apply(lambda c: c.str.strip() if c.dtype == 'object' else c)
                filt = chunk[chunk['cnpj_basico'].isin(cnpjs)]
                if not filt.empty:
                    recs = filt.to_dict(orient='records')
                    res = coll.insert_many(recs, ordered=False)
                    total_ins += len(res.inserted_ids)
        logger.info(f"[{tipo}] Inserted {total_ins} records.")
        return True

    @task(executor_config=EXECUTOR_CONFIG)
    def load_lookup(name: str, filename: str, columns: list):
        logger = logging.getLogger("airflow.task")
        hook = MongoHook(conn_id="mongo_default", mongo_db="bronze")
        coll = hook.get_collection(name)
        coll.delete_many({})

        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            logger.warning(f"Lookup file not found: {path}")
            return False

        df = pd.read_csv(path, sep=';', names=columns, header=None, dtype=str, encoding='latin1')
        df = df.apply(lambda c: c.str.strip() if c.dtype == 'object' else c)
        df.dropna(subset=columns, inplace=True)
        recs = df.to_dict(orient='records')
        if recs:
            result = coll.insert_many(recs, ordered=False)
            logger.info(f"[{name}] Inserted {len(result.inserted_ids)} lookup records.")
        else:
            logger.info(f"[{name}] No lookup records to insert.")
        return True

    cnpjs = load_estabelecimentos()
    t_emp = load_related(cnpjs, "Empresas")
    t_sim = load_related(cnpjs, "Simples")
    t_soc = load_related(cnpjs, "Socios")

    lookups = []
    for nm, fn, cols in [
        ("cnaes", "Cnaes.csv", ["codigo", "descricao"]),
        ("motivos", "Motivos.csv", ["codigo", "descricao"]),
        ("municipios", "Municipios.csv", ["codigo", "descricao"]),
        ("naturezas", "Naturezas.csv", ["codigo", "descricao"]),
        ("paises", "Paises.csv", ["codigo", "descricao"]),
        ("qualificacoes", "Qualificacoes.csv", ["codigo", "descricao"]),
    ]:
        lookups.append(load_lookup(nm, fn, cols))

    trigger_dag4 = TriggerDagRunOperator(
        task_id='trigger__gold_dag4',
        trigger_dag_id='gold_pipeline',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
)    


    cnpjs >> [t_emp, t_sim, t_soc, *lookups] >> trigger_dag4