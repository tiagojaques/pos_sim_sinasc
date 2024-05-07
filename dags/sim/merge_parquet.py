from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

def merge_sim_parquet():
    import os
    import pandas as pd
    import pyarrow.parquet as pq
    import pyarrow as pa

    source_dir = '/opt/airflow/data/parquet'
    target_dir = '/opt/airflow/data/bronze'

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    files = [file for file in os.listdir(source_dir) if file.endswith('.parquet') and (file.startswith('DO') or file.startswith('Mortalidade'))]

    table = pq.ParquetFile(os.path.join(source_dir,files[0]))
    new_fields = [pa.field(field.name, pa.string()) for field in table.schema]
    schema = pa.schema(new_fields)

    with pq.ParquetWriter(f"{target_dir}/sim.parquet", schema=schema) as writer:
        for file in files:
            print(f"Reading file: {file}")
            if file.endswith('.parquet') and (file.startswith('DO') or file.startswith('Mortalidade')):
                writer.write_table(pq.read_table(os.path.join(source_dir,file), schema=schema))

    print("Conversão concluída.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sim_3_merge_parquet',
    default_args=default_args,
    description='Merge SIM parquet files',
    schedule_interval='@daily',
    catchup=False
)

conversion_task = PythonOperator(
    task_id='sim_3_merge_parquet',
    python_callable=merge_sim_parquet,
    dag=dag
)
