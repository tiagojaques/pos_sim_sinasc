from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

def convert_sim_to_parquet():
    import os
    import pandas as pd
    source_dir = '/opt/airflow/data/sim'
    target_dir = '/opt/airflow/data/parquet'
    #convert_csv_to_parquet(source_dir, target_dir, 'sinasc.parquet')
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Iterar sobre cada arquivo no diretório
    for file_name in os.listdir(source_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(source_dir, file_name)
            # Ler o arquivo CSV
            print(f"Lendo arquivo: {file_path}")
            df = pd.read_csv(file_path, delimiter=';', encoding='latin1', low_memory=False, dtype=str)
            # convert to parquet and overwrite the file if it already exists

            if os.path.exists(os.path.join(target_dir, file_name.replace('.csv', '.parquet'))) :             
                os.remove(os.path.join(target_dir, file_name.replace('.csv', '.parquet')))

            # replace value ".." in any column to ""
            df = df.replace(r'\.\.', '', regex=True)

            df.to_parquet(os.path.join(target_dir, file_name.replace('.csv', '.parquet')), engine='pyarrow', compression='snappy')
    print("Conversão concluída.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sim_2_convert_to_parquet',
    default_args=default_args,
    description='Convert CSV files to Parquet format',
    schedule_interval='@daily',
    catchup=False
)

conversion_task = PythonOperator(
    task_id='sim_2_convert_to_parquet',
    python_callable=convert_sim_to_parquet,
    dag=dag
)
