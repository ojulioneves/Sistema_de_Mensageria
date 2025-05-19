from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pika
import json
from pymongo import MongoClient

def publicar_mensagens():
    mongo_client = MongoClient("mongodb://root:example@mongo:27017/admin")
    db = mongo_client["gold"]
    colecao = db["empresas"]

    connection_parameters = pika.ConnectionParameters(
        host="rabbitmq",
        port=5672,
        credentials=pika.PlainCredentials(
            username="user",
            password="password"
        )
    )

    channel = pika.BlockingConnection(connection_parameters).channel()

    for doc in colecao.find():
        doc.pop("_id", None)

        mensagem = json.dumps(doc, ensure_ascii=False)

        channel.basic_publish(
            exchange="data_exchange",
            routing_key='',
            body=mensagem.encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

        print(f"Mensagem enviada: {mensagem}")

# Definição da DAG
with DAG(
    dag_id="publisher_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  
    catchup=False,
    tags=["rabbitmq", "mongo", "empresas"]
) as dag:

    publicar_task = PythonOperator(
        task_id="publicar_mensagens_rabbitmq",
        python_callable=publicar_mensagens
    )

    publicar_task
