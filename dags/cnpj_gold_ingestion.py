from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Configurações do MongoDB (mantidas iguais)
client = MongoClient('mongodb://root:example@mongo:27017/admin')

db_bronze = client['bronze']
db_silver = client['silver']
db_gold = client['gold']

tbl_staging_b = db_bronze['empresas']
tbl_staging_s = db_silver['empresas']

tbl_socios = db_silver['socios']
tbl_estabelecimentos = db_silver['estabelecimentos']
tbl_empresas_s = db_silver['empresas']
tbl_empresas_g = db_gold['empresas']

# Pipelines (mantidos iguais)
pipeline_empresas_s = [
    {
        "$lookup": {
            "from": "naturezas",
            "localField": "natureza_juridica",
            "foreignField": "codigo",
            "as": "natureza"
        }
    },
    {
        "$unwind": {
            "path": "$natureza",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "simples",
            "localField": "cnpj_basico",
            "foreignField": "cnpj_basico",
            "as": "simples_info"
        }
    },
    {
        "$unwind": {
            "path": "$simples_info",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$lookup": {
            "from": "snapshot",
            "localField": "cnpj_basico",
            "foreignField": "cnpj_basico",
            "as": "gold_match"
        }
    },
    {
        "$addFields": {
            "data_opcao_simples": {
                "$cond": [
                    {
                        "$and": [
                            {
                                "$regexMatch": {
                                    "input": "$simples_info.data_opcao_simples",
                                    "regex": "^[0-9]{8}$"
                                }
                            },
                            { "$ne": ["$simples_info.data_opcao_simples", "00000000"] }
                        ]
                    },
                    {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {
                                "$dateFromString": {
                                    "dateString": {
                                        "$concat": [
                                            { "$substr": ["$simples_info.data_opcao_simples", 0, 4] }, "-",
                                            { "$substr": ["$simples_info.data_opcao_simples", 4, 2] }, "-",
                                            { "$substr": ["$simples_info.data_opcao_simples", 6, 2] }
                                        ]
                                    },
                                    "format": "%Y-%m-%d"
                                }
                            }
                        }
                    },
                    None
                ]
            },
            "data_exclusao_simples": {
                "$cond": [
                    {
                        "$and": [
                            {
                                "$regexMatch": {
                                    "input": "$simples_info.data_exclusao_simples",
                                    "regex": "^[0-9]{8}$"
                                }
                            },
                            { "$ne": ["$simples_info.data_exclusao_simples", "00000000"] }
                        ]
                    },
                    {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {
                                "$dateFromString": {
                                    "dateString": {
                                        "$concat": [
                                            { "$substr": ["$simples_info.data_exclusao_simples", 0, 4] }, "-",
                                            { "$substr": ["$simples_info.data_exclusao_simples", 4, 2] }, "-",
                                            { "$substr": ["$simples_info.data_exclusao_simples", 6, 2] }
                                        ]
                                    },
                                    "format": "%Y-%m-%d"
                                }
                            }
                        }
                    },
                    None
                ]
            },
            "data_opcao_mei": {
                "$cond": [
                    {
                        "$and": [
                            {
                                "$regexMatch": {
                                    "input": "$simples_info.data_opcao_mei",
                                    "regex": "^[0-9]{8}$"
                                }
                            },
                            { "$ne": ["$simples_info.data_opcao_mei", "00000000"] }
                        ]
                    },
                    {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {
                                "$dateFromString": {
                                    "dateString": {
                                        "$concat": [
                                            { "$substr": ["$simples_info.data_opcao_mei", 0, 4] }, "-",
                                            { "$substr": ["$simples_info.data_opcao_mei", 4, 2] }, "-",
                                            { "$substr": ["$simples_info.data_opcao_mei", 6, 2] }
                                        ]
                                    },
                                    "format": "%Y-%m-%d"
                                }
                            }
                        }
                    },
                    None
                ]
            },
            "data_exclusao_mei": {
                "$cond": [
                    {
                        "$and": [
                            {
                                "$regexMatch": {
                                    "input": "$simples_info.data_exclusao_mei",
                                    "regex": "^[0-9]{8}$"
                                }
                            },
                            { "$ne": ["$simples_info.data_exclusao_mei", "00000000"] }
                        ]
                    },
                    {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {
                                "$dateFromString": {
                                    "dateString": {
                                        "$concat": [
                                            { "$substr": ["$simples_info.data_exclusao_mei", 0, 4] }, "-",
                                            { "$substr": ["$simples_info.data_exclusao_mei", 4, 2] }, "-",
                                            { "$substr": ["$simples_info.data_exclusao_mei", 6, 2] }
                                        ]
                                    },
                                    "format": "%Y-%m-%d"
                                }
                            }
                        }
                    },
                    None
                ]
            },
            "nova": {
                "$eq": [{ "$size": "$gold_match" }, 0]
            }
        }
    },
    {
        "$group": {
            "_id": "$_id",
            "cnpj_basico": { "$first": "$cnpj_basico" },
            "razao_social": { "$first": { "$toUpper": "$razao_social" } },
            "natureza_juridica": { "$first": { "$toUpper": "$natureza.descricao" } },
            "empresa_nova": { "$first": "$nova" },
            "porte_empresa": {
                "$first": {
                    "$switch": {
                        "branches": [
                            { "case": { "$eq": ["$porte_empresa", "00"] }, "then": "NÃO INFORMADO" },
                            { "case": { "$eq": ["$porte_empresa", "01"] }, "then": "MICRO EMPRESA" },
                            { "case": { "$eq": ["$porte_empresa", "03"] }, "then": "EMPRESA DE PEQUENO PORTE" },
                            { "case": { "$eq": ["$porte_empresa", "05"] }, "then": "DEMAIS" }
                        ],
                        "default": "DESCONHECIDO"
                    }
                }
            },
            "simples": {
                "$first": {
                    "opcao_simples": {
                        "$cond": [
                            { "$eq": ["$simples_info.opcao_simples", "S"] },
                            True,
                            False
                        ]
                    },
                    "data_opcao_simples": "$data_opcao_simples",
                    "data_exclusao_simples": "$data_exclusao_simples",
                    "opcao_mei": {
                        "$cond": [
                            { "$eq": ["$simples_info.opcao_mei", "S"] },
                            True,
                            False
                        ]
                    },
                    "data_opcao_mei": "$data_opcao_mei",
                    "data_exclusao_mei": "$data_exclusao_mei"
                }
            }
        }
    }
]
pipeline_socios = [
    {"$lookup": {"from": "socios", "localField": "cnpj_basico", "foreignField": "cnpj_basico", "as": "socios"}},
    {"$unwind": {"path": "$socios", "preserveNullAndEmptyArrays": True}},
    {"$lookup": {"from": "qualificacoes", "localField": "socios.qualificacao_socio", "foreignField": "codigo", "as": "qualificacao_socio_info"}},
    {"$unwind": {"path": "$qualificacao_socio_info", "preserveNullAndEmptyArrays": True}},
    {"$lookup": {"from": "qualificacoes", "localField": "socios.qualificacao_representante", "foreignField": "codigo", "as": "qualificacao_representante_info"}},
    {"$unwind": {"path": "$qualificacao_representante_info", "preserveNullAndEmptyArrays": True}},
    {"$lookup": {"from": "snapshot", "localField": "cnpj_basico", "foreignField": "cnpj_basico", "as": "gold_match"}},
    {"$addFields": {
        "data_entrada_sociedade": {
            "$cond": [
                {"$and": [
                    {"$regexMatch": {"input": "$socios.data_entrada_sociedade", "regex": "^[0-9]{8}$"}},
                    {"$ne": ["$socios.data_entrada_sociedade", "00000000"]}
                ]},
                {"$dateToString": {"format": "%Y-%m-%d", "date": {"$dateFromString": {"dateString": {"$concat": [
                    {"$substr": ["$socios.data_entrada_sociedade", 0, 4]}, "-",
                    {"$substr": ["$socios.data_entrada_sociedade", 4, 2]}, "-",
                    {"$substr": ["$socios.data_entrada_sociedade", 6, 2]}
                ]}, "format": "%Y-%m-%d"}}}},
                None
            ]
        }
    }},
    {"$group": {
        "_id": "$_id",
        "cnpj_basico": {"$first": "$cnpj_basico"},
        "socios": {"$push": {
            "identificador_socio": {"$switch": {"branches": [
                {"case": {"$eq": ["$socios.identificador_socio", "1"]}, "then": "PESSOA JURÍDICA"},
                {"case": {"$eq": ["$socios.identificador_socio", "2"]}, "then": "PESSOA FÍSICA"},
                {"case": {"$eq": ["$socios.identificador_socio", "3"]}, "then": "ESTRANGEIRO"}
            ], "default": "DESCONHECIDO"}},
            "nome_socio": {"$toUpper": "$socios.nome_socio"},
            "carteira_socio": {"$toUpper": "$socios.carteira_socio"},
            "qualificacao_socio": {"$toUpper": "$qualificacao_socio_info.descricao"},
            "data_entrada_sociedade": "$data_entrada_sociedade",
            "nome_representante": {"$cond": [
                {"$regexMatch": {"input": {"$toString": "$socios.nome_representante"}, "regex": "^nan$", "options": "i"}},
                None,
                {"$toUpper": "$socios.nome_representante"}
            ]},
            "carteira_representante": {"$toUpper": "$socios.representante_legal"},
            "qualificacao_representante": {"$toUpper": "$qualificacao_representante_info.descricao"},
            "faixa_etaria": {"$switch": {"branches": [
                {"case": {"$eq": ["$socios.faixa_etaria", "0"]}, "then": "NÃO SE APLICA"},
                {"case": {"$eq": ["$socios.faixa_etaria", "1"]}, "then": "0 A 12 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "2"]}, "then": "13 A 20 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "3"]}, "then": "21 A 30 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "4"]}, "then": "31 A 40 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "5"]}, "then": "41 A 50 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "6"]}, "then": "51 A 60 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "7"]}, "then": "61 A 70 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "8"]}, "then": "71 A 80 ANOS"},
                {"case": {"$eq": ["$socios.faixa_etaria", "9"]}, "then": "MAIORES DE 80 ANOS"}
            ], "default": "DESCONHECIDO"}}
        }}
    }}
]
pipeline_estabelecimentos = [
    {"$lookup": {"from": "estabelecimentos", "localField": "cnpj_basico", "foreignField": "cnpj_basico", "as": "estabelecimentos"}},
    {"$unwind": "$estabelecimentos"},
    {"$lookup": {"from": "municipios", "localField": "estabelecimentos.municipio", "foreignField": "codigo", "as": "municipio"}},
    {"$lookup": {"from": "paises", "localField": "estabelecimentos.pais", "foreignField": "codigo", "as": "pais_info"}},
    {"$unwind": {"path": "$pais_info", "preserveNullAndEmptyArrays": True}},
    {"$lookup": {"from": "motivos", "localField": "estabelecimentos.motivo_situacao", "foreignField": "codigo", "as": "motivo"}},
    {"$lookup": {"from": "cnaes", "localField": "estabelecimentos.cnae_principal", "foreignField": "codigo", "as": "cnae_principal_info"}},
    {"$addFields": {
        "cnae_secundario_array": {
            "$filter": {
                "input": {"$split": [{"$toString": "$estabelecimentos.cnae_secundario"}, ","]},
                "as": "cnae",
                "cond": {"$ne": ["$$cnae", ""]}
            }
        }
    }},
    {"$lookup": {
        "from": "cnaes",
        "let": {"cnaes": "$cnae_secundario_array"},
        "pipeline": [
            {"$match": {"$expr": {"$in": ["$codigo", "$$cnaes"]}}},
            {"$project": {"_id": 0, "codigo": 1, "descricao": 1}}
        ],
        "as": "cnaes_secundarios_info"
    }},
    {"$addFields": {
        "cnaes_combinados": {
            "$concatArrays": [
                [{
                    "codigo": {"$cond": [
                        {"$isArray": "$cnae_principal_info"},
                        {"$let": {"vars": {"raw": {"$arrayElemAt": ["$cnae_principal_info.codigo", 0]}},
                                  "in": {"$concat": [
                                    {"$substr": ["$$raw", 0, 4]}, "-",
                                    {"$substr": ["$$raw", 4, 1]}, "/",
                                    {"$substr": ["$$raw", 5, 2]}
                                  ]}}},
                        None
                    ]},
                    "descricao": {"$cond": [
                        {"$isArray": "$cnae_principal_info"},
                        {"$toUpper": {"$arrayElemAt": ["$cnae_principal_info.descricao", 0]}},
                        None
                    ]},
                    "principal": True
                }],
                {"$map": {"input": "$cnaes_secundarios_info", "as": "cnae", "in": {
                    "codigo": {"$concat": [
                        {"$substr": ["$$cnae.codigo", 0, 4]}, "-",
                        {"$substr": ["$$cnae.codigo", 4, 1]}, "/",
                        {"$substr": ["$$cnae.codigo", 5, 2]}
                    ]},
                    "descricao": {"$toUpper": "$$cnae.descricao"},
                    "principal": False
                }}}
            ]
        }
    }},
    {"$addFields": {
        "data_situacao_formatada": {
            "$cond": [
                {"$and": [
                    {"$regexMatch": {"input": "$estabelecimentos.data_situacao", "regex": "^[0-9]{8}$"}},
                    {"$ne": ["$estabelecimentos.data_situacao", "00000000"]}
                ]},
                {"$dateToString": {"format": "%Y-%m-%d", "date": {"$dateFromString": {"dateString": {"$concat": [
                    {"$substr": ["$estabelecimentos.data_situacao", 0, 4]}, "-",
                    {"$substr": ["$estabelecimentos.data_situacao", 4, 2]}, "-",
                    {"$substr": ["$estabelecimentos.data_situacao", 6, 2]}
                ]}, "format": "%Y-%m-%d"}}}},
                None
            ]
        },
        "data_inicio_formatada": {
            "$cond": [
                {"$and": [
                    {"$regexMatch": {"input": "$estabelecimentos.data_inicio", "regex": "^[0-9]{8}$"}},
                    {"$ne": ["$estabelecimentos.data_inicio", "00000000"]}
                ]},
                {"$dateToString": {"format": "%Y-%m-%d", "date": {"$dateFromString": {"dateString": {"$concat": [
                    {"$substr": ["$estabelecimentos.data_inicio", 0, 4]}, "-",
                    {"$substr": ["$estabelecimentos.data_inicio", 4, 2]}, "-",
                    {"$substr": ["$estabelecimentos.data_inicio", 6, 2]}
                ]}, "format": "%Y-%m-%d"}}}},
                None
            ]
        }
    }},
    {
    "$group": {
        "_id": "$_id",
        "cnpj_basico": {"$first": "$cnpj_basico"},
        "estabelecimentos": {
            "$push": {
                "inscricao_federal": {
                    "$concat": [
                        {"$substr": ["$estabelecimentos.cnpj_basico", 0, 2]}, ".",
                        {"$substr": ["$estabelecimentos.cnpj_basico", 2, 3]}, ".",
                        {"$substr": ["$estabelecimentos.cnpj_basico", 5, 3]}, "/",
                        "$estabelecimentos.cnpj_ordem", "-",
                        "$estabelecimentos.cnpj_dv"
                    ]
                },
                "matriz_filial": "$estabelecimentos.matriz_filial",
                "nome_fantasia": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": {"$toString": "$estabelecimentos.nome_fantasia"},
                                "regex": "^nan$",
                                "options": "i"
                            }
                        },
                        None,
                        {"$toUpper": "$estabelecimentos.nome_fantasia"}
                    ]
                },
                "situacao_cadastral": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$estabelecimentos.situacao_cadastral", "01"]}, "then": "NULA"},
                            {"case": {"$eq": ["$estabelecimentos.situacao_cadastral", "02"]}, "then": "ATIVA"},
                            {"case": {"$eq": ["$estabelecimentos.situacao_cadastral", "03"]}, "then": "SUSPENSA"},
                            {"case": {"$eq": ["$estabelecimentos.situacao_cadastral", "04"]}, "then": "INAPTA"},
                            {"case": {"$eq": ["$estabelecimentos.situacao_cadastral", "08"]}, "then": "BAIXADA"}
                        ],
                        "default": "DESCONHECIDO"
                    }
                },
                "data_situacao": "$data_situacao_formatada",
                "motivo": {
                    "$cond": [
                        {"$isArray": "$motivo.descricao"},
                        {"$toUpper": {"$arrayElemAt": ["$motivo.descricao", 0]}},
                        None
                    ]
                },
                "pais_info": {
                    "$cond": [
                        {"$isArray": "$pais_info.descricao"},
                        {"$toUpper": {"$arrayElemAt": ["$pais_info.descricao", 0]}},
                        None
                    ]
                },
                "data_inicio": "$data_inicio_formatada",
                "cnaes": "$cnaes_combinados",
                "tipo_logradouro": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": {"$toString": "$estabelecimentos.tipo_logradouro"},
                                "regex": "^nan$",
                                "options": "i"
                            }
                        },
                        None,
                        {"$toUpper": "$estabelecimentos.tipo_logradouro"}
                    ]
                },
                "logradouro": {"$toUpper": "$estabelecimentos.logradouro"},
                "numero": "$estabelecimentos.numero",
                "complemento": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": {"$toString": "$estabelecimentos.complemento"},
                                "regex": "^nan$",
                                "options": "i"
                            }
                        },
                        None,
                        {"$toUpper": "$estabelecimentos.complemento"}
                    ]
                },
                "bairro": {"$toUpper": "$estabelecimentos.bairro"},
                "cep": "$estabelecimentos.cep",
                "uf": {"$toUpper": "$estabelecimentos.uf"},
                "municipio": {"$toUpper": {"$arrayElemAt": ["$municipio.descricao", 0]}},
                "telefone1": {
                    "$cond": [
                        {
                            "$or": [
                                {
                                    "$regexMatch": {
                                        "input": {"$toString": "$estabelecimentos.ddd1"},
                                        "regex": "^nan$",
                                        "options": "i"
                                    }
                                },
                                {
                                    "$regexMatch": {
                                        "input": {"$toString": "$estabelecimentos.telefone1"},
                                        "regex": "^nan$",
                                        "options": "i"
                                    }
                                }
                            ]
                        },
                        None,
                        {
                            "$concat": [
                                {"$toString": "$estabelecimentos.ddd1"},
                                {"$toString": "$estabelecimentos.telefone1"}
                            ]
                        }
                    ]
                },
                "fax": {
                    "$cond": [
                        {
                            "$or": [
                                {
                                    "$regexMatch": {
                                        "input": {"$toString": "$estabelecimentos.ddd_fax"},
                                        "regex": "^nan$",
                                        "options": "i"
                                    }
                                },
                                {
                                    "$regexMatch": {
                                        "input": {"$toString": "$estabelecimentos.fax"},
                                        "regex": "^nan$",
                                        "options": "i"
                                    }
                                }
                            ]
                        },
                        None,
                        {
                            "$concat": [
                                {"$toString": "$estabelecimentos.ddd_fax"},
                                {"$toString": "$estabelecimentos.fax"}
                            ]
                        }
                    ]
                },
                "email": {
                    "$cond": [
                        {
                            "$regexMatch": {
                                "input": {"$toString": "$estabelecimentos.email"},
                                "regex": "^nan$",
                                "options": "i"
                            }
                        },
                        None,
                        "$estabelecimentos.email"
                    ]
                }
            }
        }
    }
}
]
pipeline_empresas_g = [
    {
        "$lookup": {
            "from": "socios",
            "localField": "cnpj_basico",
            "foreignField": "cnpj_basico",
            "as": "socios"
        }
    },
    {
        "$lookup": {
            "from": "estabelecimentos",
            "localField": "cnpj_basico",
            "foreignField": "cnpj_basico",
            "as": "estabelecimentos"
        }
    },
    {
        "$addFields": {
            "estabelecimentos": {
                "$reduce": {
                    "input": {
                        "$map": {
                            "input": "$estabelecimentos",
                            "as": "e",
                            "in": {
                                "$ifNull": ["$$e.estabelecimentos", []]
                            }
                        }
                    },
                    "initialValue": [],
                    "in": { "$concatArrays": ["$$value", "$$this"] }
                }
            },
            "socios": {
                "$reduce": {
                    "input": {
                        "$map": {
                            "input": "$socios",
                            "as": "s",
                            "in": {
                                "$ifNull": ["$$s.socios", []]
                            }
                        }
                    },
                    "initialValue": [],
                    "in": { "$concatArrays": ["$$value", "$$this"] }
                }
            }
        }
    }
]

# Funções auxiliares (mantidas iguais)
def truncate_collection(collection):
    collection.delete_many({})
    print(f"Coleção '{collection.name}' truncada.")

# Tarefas da DAG
def truncate_tables():
    truncate_collection(tbl_socios)
    truncate_collection(tbl_estabelecimentos)
    truncate_collection(tbl_empresas_s)
    truncate_collection(tbl_empresas_g)

def process_socios():
    print("Iniciando pipeline de sócios...")
    for doc in tbl_staging_b.aggregate(pipeline_socios):
        tbl_socios.replace_one({'_id': doc['_id']}, doc, upsert=True)
    print("Pipeline de sócios concluído.")

def process_estabelecimentos():
    print("Iniciando pipeline de estabelecimentos...")
    for doc in tbl_staging_b.aggregate(pipeline_estabelecimentos):
        tbl_estabelecimentos.replace_one({'_id': doc['_id']}, doc, upsert=True)
    print("Pipeline de estabelecimentos concluído.")

def process_empresas_silver():
    print("Iniciando pipeline de empresas (silver)...")
    for doc in tbl_staging_b.aggregate(pipeline_empresas_s):
        tbl_empresas_s.replace_one({'_id': doc['_id']}, doc, upsert=True)
    print("Pipeline de empresas (silver) concluído.")

def process_empresas_gold():
    print("Iniciando pipeline de empresas (gold)...")
    for doc in tbl_staging_s.aggregate(pipeline_empresas_g):
        tbl_empresas_g.replace_one({'_id': doc['_id']}, doc, upsert=True)
    print("Pipeline de empresas (gold) concluído.")

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'gold_pipeline',
    default_args=default_args,
    description='ETL pipeline para processamento de dados de CNPJ',
    schedule=None,
    catchup=False,
)

# Definindo as tarefas
task_truncate = PythonOperator(
    task_id='truncate_tables',
    python_callable=truncate_tables,
    dag=dag,
)

task_socios = PythonOperator(
    task_id='process_socios',
    python_callable=process_socios,
    dag=dag,
)

task_estabelecimentos = PythonOperator(
    task_id='process_estabelecimentos',
    python_callable=process_estabelecimentos,
    dag=dag,
)

task_empresas_silver = PythonOperator(
    task_id='process_empresas_silver',
    python_callable=process_empresas_silver,
    dag=dag,
)

task_empresas_gold = PythonOperator(
    task_id='process_empresas_gold',
    python_callable=process_empresas_gold,
    dag=dag,
)

trigger_dag5 = TriggerDagRunOperator(
        task_id='trigger__gold_dag4',
        trigger_dag_id='publisher_dag',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
)

# Definindo a ordem de execução
task_truncate >> [task_socios, task_estabelecimentos] >> task_empresas_silver >> task_empresas_gold >> trigger_dag5