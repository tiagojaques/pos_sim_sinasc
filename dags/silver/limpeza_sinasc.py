from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def sinasc_4_limpeza_dados():
    import dask.dataframe as dd
    import pandas as pd

    # Carregar o arquivo Parquet
    sinasc_df = dd.read_parquet('/opt/airflow/data/bronze/sinasc.parquet', engine='pyarrow')
    print('Parquet carregado')
    sinasc_df = sinasc_df.drop(columns=['ORIGEM','NUMEROLOTE','VERSAOSIST','CONTADOR','HORANASC','DTCADASTRO',
                                'DTRECEBIM','DIFDATA','DTRECORIGA','DTDECLARAC','STDNEPIDEM','STDNNOVA'])
    print('Colunas removidas')
    columns_to_convert = ['DTULTMENST','DTNASCMAE','DTNASC']

    sinasc_df['ANONASC'] = sinasc_df['DTNASC'].str[4:8]

    for col in columns_to_convert:
            if col in sinasc_df.columns:
                print(col, sinasc_df[col].dtype)
                sinasc_df[f'{col}'] = dd.to_datetime(sinasc_df[col], format='%d%m%Y', errors='coerce')
                print(col, sinasc_df[col].dtype)
    print('Colunas convertidas')
    print('Linhas sem DTNASC removidas')

    #columns_to_convert = ['ORIGEM','LOCNASC','IDATEMAE','ESTCIVMAE','ESCMAE','QTDFILVIVO','QTDFILMOT',
    #                        'GESTACAO','GRAVIDEZ','PARTO','CONSULTAS','SEXO','RACACOR','CODUFNATU','ESCMAE2010',
    #                        'SERIESCMAE','RACACORMAE','QTDGESTANT','QTDPARTNOR','QTDPARTCES','IDADEPAI','SEMAGESTAC',
    #                        'TPMESTIM','CONSPRENAT','MESPRENAT','TPAPRESENT','STTRABPART','STCESPARTO','TPNASCASSI',
    #                        'TPFUNCRESP','TPDOCRESP','ESCMAEAGR1','STDNEPIDEM','STDNNOVA','CODPAISRES','PARIDADE',
    #                        'TPROBSON','KOTELCHUCK']

    #for col in columns_to_convert:
    #    if col in sinasc_df.columns:
    #        print(col, sinasc_df[col].dtype)
    #        sinasc_df[col] = pd.to_numeric(sinasc_df[col], errors='coerce').fillna(-1).astype(int)
    #        print(col, sinasc_df[col].dtype)

    #print('Colunas convertidas para int')

    sinasc_df.to_parquet('/opt/airflow/data/silver/sinasc.parquet', engine='pyarrow', compression='snappy')
    #dd.to_parquet(sinasc_df, '/opt/airflow/data/silver/sinasc.parquet', engine='pyarrow', compression='snappy')
    


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sinasc_4_limpeza_dados',
    default_args=default_args,
    description='Limpeza SINASC - OpenDataSUS',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='sinasc_4_limpeza_dados',
    python_callable=sinasc_4_limpeza_dados,
    dag=dag
)

