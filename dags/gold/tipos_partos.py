from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def comparativo_tipos_parto():
    import pandas as pd
    import os

    save_path = '/opt/airflow/data/gold/'
    os.makedirs(save_path, exist_ok=True)

    sinasc_path = '/opt/airflow/data/silver/sinasc.parquet'
    auxiliares_path = '/opt/airflow/data/gold/auxiliares/'

    sinasc_df = pd.read_parquet(sinasc_path, engine='pyarrow', columns=['ANONASC','PARTO'])

    sinasc_parto = sinasc_df.groupby(['ANONASC', 'PARTO']).size().reset_index().rename(columns={0: 'COUNT'})
    parto_df = pd.read_parquet(auxiliares_path + 'parto.parquet', engine='pyarrow')
    sinasc_parto['PARTO'] = sinasc_parto['PARTO'].astype(int).fillna(9)
    parto_df['Codigo'] = parto_df['Codigo'].astype(int)

    sinasc_parto = sinasc_parto.merge(parto_df, left_on='PARTO', right_on='Codigo')

    sinasc_parto = sinasc_parto.drop(columns=['PARTO', 'Codigo'])

    sinasc_parto.to_parquet(f'{save_path}/comparativo_tipos_parto.parquet', engine='pyarrow', compression='snappy')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

dag = DAG(
    'Analises_de_Dados_2_Tipos_Parto',
    default_args=default_args,
    description='Analises de Dados - Tipos de parto',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='comparativo_tipos_parto',
    python_callable=comparativo_tipos_parto,
    dag=dag
)

