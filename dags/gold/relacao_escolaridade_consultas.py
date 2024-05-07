from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def comparativo_escolaridade_consultas():
    import pandas as pd
    import os

    save_path = '/opt/airflow/data/gold/'
    os.makedirs(save_path, exist_ok=True)

    sinasc_path = '/opt/airflow/data/silver/sinasc.parquet'
    auxiliares_path = '/opt/airflow/data/gold/auxiliares/'

    sinasc_df = pd.read_parquet(sinasc_path, engine='pyarrow', columns=['ESCMAE','CONSULTAS','ESCMAE2010'])
    #print(sinasc_df.head())
    # para cada escolaridade, calcular a quantidade de consultas

    sinasc_df['ESCMAE2010'] = sinasc_df['ESCMAE2010'].fillna('9').astype(int)
    sinasc_df['ESCMAE'] = sinasc_df['ESCMAE'].fillna('9').astype(int)
    sinasc_df['CONSULTAS'] = sinasc_df['CONSULTAS'].fillna('0').astype(int)

    sinasc_tempo_estudo = sinasc_df.groupby('ESCMAE').CONSULTAS.mean().rename('CONSULTAS').reset_index()
    sinasc_escolaridade = sinasc_df.groupby('ESCMAE2010').CONSULTAS.mean().rename('CONSULTAS').reset_index()

    escmae = pd.read_parquet(auxiliares_path + 'escmae.parquet', engine='pyarrow')
    escmae2010 = pd.read_parquet(auxiliares_path + 'escmae2010.parquet', engine='pyarrow')

    escmae['Codigo'] = escmae['Codigo'].astype(int)
    escmae2010['Codigo'] = escmae2010['Codigo'].astype(int)

    sinasc_tempo_estudo = sinasc_tempo_estudo.merge(escmae, left_on='ESCMAE', right_on='Codigo')
    sinasc_escolaridade = sinasc_escolaridade.merge(escmae2010, left_on='ESCMAE2010', right_on='Codigo')

    sinasc_tempo_estudo = sinasc_tempo_estudo.drop(columns=['ESCMAE', 'Codigo']).rename(columns={'Descricao':'ESCMAE'})
    sinasc_escolaridade = sinasc_escolaridade.drop(columns=['ESCMAE2010', 'Codigo']).rename(columns={'Descricao':'ESCMAE2010'})

    sinasc_tempo_estudo.to_parquet(f'{save_path}/comparativo_tempo_estudo_consultas.parquet', engine='pyarrow', compression='snappy')
    sinasc_escolaridade.to_parquet(f'{save_path}/comparativo_escolaridade_consultas.parquet', engine='pyarrow', compression='snappy')
    

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

dag = DAG(
    'Analises_de_Dados_3_Relação_Escolaridade_Consultas',
    default_args=default_args,
    description='Analises de Dados - Relação entre escolaridade e consultas',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='comparativo_escolaridade_consultas',
    python_callable=comparativo_escolaridade_consultas,
    dag=dag
)

