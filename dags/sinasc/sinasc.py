from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def download_sinasc_csv():
    import requests
    from bs4 import BeautifulSoup
    import os
    try:
        url = 'https://opendatasus.saude.gov.br/dataset/sistema-de-informacao-sobre-nascidos-vivos-sinasc'
        download_path = '/opt/airflow/data/sinasc'

        if os.path.exists(download_path):
            for file in os.listdir(download_path):
                os.remove(os.path.join(download_path, file))
            os.rmdir(download_path)
        os.makedirs(download_path, exist_ok=True)

        response = requests.get(url, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')

        for link in links:
            href = link.get('href')
            if href and 'csv' in href:
                download_url = href if 'http' in href else url + href
                filename = download_url.split('/')[-1]
                file_path = os.path.join(download_path, filename)
                with requests.get(download_url, stream=True, timeout=30) as r:
                    with open(file_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                print(f"Downloaded {filename} to {download_path}")

    except requests.RequestException as e:
        print(f"Request failed: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sinasc_1_download_csv',
    default_args=default_args,
    description='Download SINASC CSV - OpenDataSUS',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='sinasc_1_download_csv',
    python_callable=download_sinasc_csv,
    dag=dag
)

