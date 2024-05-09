from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def principais_causabasica_gravidez():
    import pandas as pd
    import dask.dataframe as dd
    import os

    save_path = '/opt/airflow/data/gold/'
    os.makedirs(save_path, exist_ok=True)

    sim_path = '/opt/airflow/data/silver/sim.parquet'
    auxiliares_path = '/opt/airflow/data/gold/auxiliares/'


    sim_df = dd.read_parquet(sim_path)

    obitopuerp_df = dd.read_parquet(auxiliares_path + 'obitopuerp.parquet')
    cid10 = dd.read_parquet(auxiliares_path + 'cid10.parquet')
    tpmorteoco_df = dd.read_parquet(auxiliares_path + 'tpmorteoco.parquet')

    obitopuerp_df['Codigo'] = obitopuerp_df['Codigo'].astype(int)
    tpmorteoco_df['Codigo'] = tpmorteoco_df['Codigo'].astype(int)

    sim_obitos_maternos = sim_df[['CAUSABAS','CAUSAMAT','OBITOPUERP','TPMORTEOCO']]

    sim_obitos_maternos['OBITOPUERP'] = sim_obitos_maternos['OBITOPUERP'].fillna('-1').astype(int)
    sim_obitos_maternos['TPMORTEOCO'] = sim_obitos_maternos['TPMORTEOCO'].fillna('-1').astype(int)

    sim_obitos_maternos = sim_obitos_maternos[sim_obitos_maternos['TPMORTEOCO'].notnull()]
    sim_obitos_maternos = sim_obitos_maternos[sim_obitos_maternos['TPMORTEOCO'] > 0]

    sim_obitos_maternos = sim_obitos_maternos.merge(obitopuerp_df, left_on='OBITOPUERP', right_on='Codigo', how='left')
    sim_obitos_maternos = sim_obitos_maternos.rename(columns={'Descricao':'DESC_OBITOPUERP'})
    sim_obitos_maternos = sim_obitos_maternos.drop(columns=['Codigo'], axis=1)

    sim_obitos_maternos = sim_obitos_maternos.merge(tpmorteoco_df, left_on='TPMORTEOCO', right_on='Codigo', how='left')
    sim_obitos_maternos = sim_obitos_maternos.rename(columns={'Descricao':'DESC_TPMORTEOCO'})
    sim_obitos_maternos = sim_obitos_maternos.drop(columns=['Codigo'], axis=1)

    sim_obitos_maternos = sim_obitos_maternos[['CAUSABAS','OBITOPUERP','DESC_OBITOPUERP','TPMORTEOCO','DESC_TPMORTEOCO']]

    sim_obitos_maternos = sim_obitos_maternos.merge(cid10, left_on='CAUSABAS', right_on='SUBCAT', how='left')

    sim_obitos_maternos = sim_obitos_maternos[sim_obitos_maternos['TPMORTEOCO']!=8]
    sim_obitos_maternos = sim_obitos_maternos[sim_obitos_maternos['TPMORTEOCO']!=9]
    sim_obitos_maternos = sim_obitos_maternos.compute()

    agrupado_tpmorteoco = sim_obitos_maternos[["CAUSABAS","DESCRICAO_CAT", "TPMORTEOCO", "DESC_TPMORTEOCO"]].groupby(["CAUSABAS","DESCRICAO_CAT", "TPMORTEOCO", "DESC_TPMORTEOCO"]).size().reset_index(name="COUNT")

    agrupado_tpmorteoco = agrupado_tpmorteoco.sort_values(by=['COUNT'], ascending=False)

    agrupado_tpmorteoco = agrupado_tpmorteoco.sort_values(by=['TPMORTEOCO','COUNT'], ascending=[True,False])

    agrupado_tpmorteoco = agrupado_tpmorteoco.groupby('TPMORTEOCO').head(10)

    # Salvar o DataFrame
    agrupado_tpmorteoco.to_parquet(f'{save_path}/principais_causabasica_gravidez.parquet', engine='pyarrow', compression='snappy')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
}

dag = DAG(
    'Analises_de_Dados_4_Principais_CausaBasica_Gravidez',
    default_args=default_args,
    description='Analises de Dados - 10 - Principais Causa Basica Gravidez',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='principais_causabasica_gravidez',
    python_callable=principais_causabasica_gravidez,
    dag=dag
)

