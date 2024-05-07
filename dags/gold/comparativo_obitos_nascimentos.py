from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def comparativo_obitos_nascimentos():
    import pandas as pd
    import os

    save_path = '/opt/airflow/data/gold/'
    os.makedirs(save_path, exist_ok=True)

    import pandas as pd
    import dask.dataframe as dd

    # Carregar os dados das origens Parquet
    sim_path = '/opt/airflow/data/silver/sim.parquet'
    sinasc_path = '/opt/airflow/data/silver/sinasc.parquet'

    sim_df = dd.read_parquet(sim_path, engine='pyarrow', columns=['ANOOBITO','DTOBITO'])
    sinasc_df = dd.read_parquet(sinasc_path, engine='pyarrow', columns=['ANONASC','DTNASC'])

    
    # groupby ano com dask
    sim_annual = sim_df.groupby('ANOOBITO').size().compute().reset_index(name='Obitos')
    sinasc_annual = sinasc_df.groupby('ANONASC').size().compute().reset_index(name='Nascimentos')

    # Unir os DataFrames no eixo do ano
    annual_comparison = pd.merge(sim_annual, sinasc_annual, left_on='ANOOBITO', right_on='ANONASC', how='outer').fillna('0')

    # Excluir registros com anos faltantes
    annual_comparison = annual_comparison[annual_comparison['ANOOBITO'] != '0']
    annual_comparison = annual_comparison[annual_comparison['ANONASC'] != '0']

    # Convertendo para Int64
    annual_comparison['Obitos'] = annual_comparison['Obitos'].astype('int64')
    annual_comparison['Nascimentos'] = annual_comparison['Nascimentos'].astype('int64')
    annual_comparison['ANOOBITO'] = annual_comparison['ANOOBITO'].astype('int64')
    annual_comparison['ANONASC'] = annual_comparison['ANONASC'].astype('int64')

    annual_comparison = annual_comparison.rename(columns={'ANOOBITO': 'Ano'})
    annual_comparison = annual_comparison[['Ano', 'Obitos', 'Nascimentos']]
    annual_comparison['Ano'] = annual_comparison['Ano'].astype(int)

    annual_comparison.sort_values(by='Ano', inplace=True)

    # Salvar o DataFrame
    annual_comparison.to_parquet(f'{save_path}/comparativo_obitos_nascimentos.parquet', engine='pyarrow', compression='snappy')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

dag = DAG(
    'Analises_de_Dados_1_Comparativo_Obitos_Nascimentos',
    default_args=default_args,
    description='Analises de Dados - Comparativo Obitos e Nascimentos',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='comparativo_obitos_nascimentos',
    python_callable=comparativo_obitos_nascimentos,
    dag=dag
)

