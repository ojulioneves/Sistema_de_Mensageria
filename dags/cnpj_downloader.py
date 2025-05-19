import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

class ReceitaFederalDownloader:
    def __init__(self, period: str, base_url="https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj"):
        self.period = period
        self.url = f"{base_url}/{period}/"
        self.destination = os.path.join('/tmp', period)
        os.makedirs(self.destination, exist_ok=True)

    def fetch_zip_urls(self):
        response = requests.get(self.url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        return [self.url + a['href'] for a in soup.find_all('a') if a['href'].endswith('.zip')]

    def download_all_files(self):
        urls = self.fetch_zip_urls()
        results = []

        def _download(url):
            filename = os.path.join(self.destination, url.split('/')[-1])
            if os.path.exists(filename):
                return f'skip: {os.path.basename(filename)}'
            try:
                with requests.get(url, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(filename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                return f'success: {filename}'
            except Exception as e:
                return f'error: {url} - {e}'

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_download, url): url for url in urls}
            for future in as_completed(futures):
                results.append(future.result())
        return
    
def download_receita(**kwargs):
    period = datetime.today().replace(day=1).strftime('%Y-%m')
    downloader = ReceitaFederalDownloader(period)
    downloader.download_all_files()

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1), "retries": 0}

with DAG(
    dag_id='download_receita_federal',
    default_args=default_args,
    schedule='@monthly',
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id='download_files',
        python_callable=download_receita
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_processa_dag2',
        trigger_dag_id='processa_receita_federal_dinamico',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )    


    download_task >> trigger_dag2