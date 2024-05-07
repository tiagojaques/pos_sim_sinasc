from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def sim_4_limpeza_dados():
    import dask.dataframe as dd
    import pandas as pd

    # Carregar o arquivo Parquet
    sim_df = dd.read_parquet('/opt/airflow/data/bronze/sim.parquet', engine='auto')
    print('Parquet carregado')
    sim_df = sim_df.drop(columns=['NUMEROLOTE','VERSAOSIST','VERSAOSCB','CODIFICADO','DTRECEBIM',
                                'CONTADOR','ORIGEM','HORAOBITO', 'ESTABDESCR','CB_PRE','DTATESTADO',
                                'STDOEPIDEM','STDONOVA','DIFDATA','NUDIASOBCO','NUDIASOBIN','DTCADINV',
                                'DTCONINV','FONTES','TPRESGINFO','TPNIVELINV','NUDIASINF','DTCADINF',
                                'DTCONCASO','FONTESINF','ALTCAUSA','DTINVESTIG'])
    print('Colunas removidas')
    sim_df['ANOOBITO'] = sim_df['DTOBITO'].str[4:8]
    columns_to_convert = ['DTOBITO','DTCADASTRO','DTNASC','DTRECEBIM']
    print (sim_df.columns)
    for col in columns_to_convert:
            if col in sim_df.columns:
                print(col, sim_df[col].dtype)
                sim_df[f'{col}'] = dd.to_datetime(sim_df[col], format='%d%m%Y', errors='coerce')
                print(col, sim_df[col].dtype)
    print('Colunas convertidas')

    sim_df = sim_df.dropna(subset=['DTNASC'])
    print('Linhas sem DTNASC removidas')

    columns_to_convert = [
        'ACIDTRAB', 'ALTCAUSA', 'ASSISTMED', 'ATESTANTE', 'CIRCOBITO',
        'CIRURGIA', 'ESC', 'ESC2010', 'ESCFALAGR1', 'ESCMAE', 'ESCMAE2010',
        'ESCMAEAGR1', 'ESTCIV', 'ESTCIVIL', 'EXAME', 'FONTE', 'FONTEINV',
        'GESTACAO', 'GRAVIDEZ', 'INSTRUCAO', 'LOCACID', 'LOCOCOR', 'MORTEPARTO',
        'NECROPSIA', 'OBITOFE1', 'OBITOFE2', 'OBITOGRAV', 'OBITOPARTO', 'OBITOPUERP',
        'ORIGEM', 'PARTO', 'RACACOR', 'SEMANGEST', 'STDOEPIDEM', 'STDONOVA',
        'TIPOACID', 'TIPOBITO', 'TIPOGRAV', 'TIPOPARTO', 'TIPOVIOL', 'TPMORTEOCO',
        'TPOBITOCOR', 'TPPOS', 'TPRESGINFO'
    ]

    
    #for col in columns_to_convert:
    #    if col in sim_df.columns:
    #        print(col, sim_df[col].dtype)
    #        sim_df[col] = pd.to_numeric(sim_df[col], errors='coerce').fillna(-1).astype(int)
    #        print(col, sim_df[col].dtype)
    #print('Colunas convertidas para int')

    sim_df.to_parquet('/opt/airflow/data/silver/sim.parquet', engine='pyarrow', compression='snappy')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sim_4_limpeza_dados',
    default_args=default_args,
    description='Limpeza SIM - OpenDataSUS',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='sim_4_limpeza_dados',
    python_callable=sim_4_limpeza_dados,
    dag=dag
)

