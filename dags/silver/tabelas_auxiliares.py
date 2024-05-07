from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path


# Adicionar o diretório da DAG ao path
sys.path.append(str(Path(__file__).parent.parent))

def atualizacao_tabelas_auxiliares():
    import pandas as pd
    import os

    save_path = '/opt/airflow/data/gold/auxiliares'
    os.makedirs(save_path, exist_ok=True)

    def create_and_save_auxiliary(df_dict, filename, save_path): 
        df = pd.DataFrame(df_dict)
        full_path = os.path.join(save_path, f"{filename}.parquet")
        df.to_parquet(full_path, engine='pyarrow', compression='snappy')
        print(f"Saved {filename} to {full_path}")

    estcivmae_dict = {'Codigo': [1, 2, 3, 4, 5, 9], 'Descricao': ['Solteira', 'Casada', 'Viúva', 'Separada judicialmente/divorciada', 'União estável', 'Ignorada']}
    create_and_save_auxiliary(estcivmae_dict, 'estcivmae', save_path)

    escmae_dict = {'Codigo': [1, 2, 3, 4, 5, 9], 'Descricao': ['Nenhuma', '1 a 3 anos', '4 a 7 anos', '8 a 11 anos', '12 e mais', 'Ignorado']}
    create_and_save_auxiliary(escmae_dict, 'escmae', save_path)

    gestacao_dict = {'Codigo': [1, 2, 3, 4, 5, 6, 9], 'Descricao': ['Menos de 22 semanas', '22 a 27 semanas', '28 a 31 semanas', '32 a 36 semanas', '37 a 41 semanas', '42 semanas e mais', 'Ignorado']}
    create_and_save_auxiliary(gestacao_dict, 'gestacao', save_path)

    gravidez_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Única', 'Dupla', 'Tripla ou mais', 'Ignorado']}
    create_and_save_auxiliary(gravidez_dict, 'gravidez', save_path)

    parto_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Vaginal', 'Cesário', 'Ignorado']}
    create_and_save_auxiliary(parto_dict, 'parto', save_path)

    consultas_dict = {'Codigo': [1, 2, 3, 4, 9], 'Descricao': ['Nenhuma', 'de 1 a 3', 'de 4 a 6', '7 e mais', 'Ignorado']}
    create_and_save_auxiliary(consultas_dict, 'consultas', save_path)

    sexo_dict = {'Codigo': [1, 2, 0], 'Descricao': ['M – Masculino', 'F – Feminino', 'I – Ignorado']}
    create_and_save_auxiliary(sexo_dict, 'sexo', save_path)

    racacor_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['Branca', 'Preta', 'Amarela', 'Parda', 'Indígena']}
    create_and_save_auxiliary(racacor_dict, 'racacor', save_path)

    idanomal_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Sim', 'Não', 'Ignorado']}
    create_and_save_auxiliary(idanomal_dict, 'idanomal', save_path)

    stdnnova_dict = {'Codigo': [1, 0], 'Descricao': ['Sim', 'Não']}
    create_and_save_auxiliary(stdnnova_dict, 'stdnnova', save_path)

    racacor_rn_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['Branca', 'Preta', 'Amarela', 'Parda', 'Indígena']}
    create_and_save_auxiliary(racacor_rn_dict, 'racacor_rn', save_path)

    racacorn_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['Branca', 'Preta', 'Amarela', 'Parda', 'Indígena']}
    create_and_save_auxiliary(racacorn_dict, 'racacorn', save_path)

    escmae2010_dict = {'Codigo': [0, 1, 2, 3, 4, 5, 9], 'Descricao': ['Sem escolaridade', 'Fundamental I (1ª a 4ª série', 'Fundamental II (5ª a 8ª série', 'Médio (antigo 2º Grau', 'Superior incompleto', 'Superior completo', 'Ignorado']}
    create_and_save_auxiliary(escmae2010_dict, 'escmae2010', save_path)

    tpnascassi_dict = {'Codigo': [1, 2, 3, 4, 9], 'Descricao': ['Médico', 'Enfermagem ou Obstetriz', 'Parteira', 'Outros', 'Ignorado']}
    create_and_save_auxiliary(tpnascassi_dict, 'tpnascassi', save_path)

    escmaeagr1_dict = {'Codigo': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 'Descricao': ['Sem Escolaridade', 'Fundamental I Incompleto', 'Fundamental I Completo', 'Fundamental II Incompleto', 'Fundamental II Completo', 'Ensino Médio Incompleto', 'Ensino Médio Completo', 'Superior Incompleto', 'Superior Completo', 'Ignorado', 'Fundamental I Incompleto ou Inespecífico', 'Fundamental II Incompleto ou Inespecífico', 'Ensino Médio Incompleto ou Inespecífico']}
    create_and_save_auxiliary(escmaeagr1_dict, 'escmaeagr1', save_path)

    tpfuncresp_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['Médico', 'Enfermeiro', 'Parteira', 'Funcionário do cartório', 'Outros']}
    create_and_save_auxiliary(tpfuncresp_dict, 'tpfuncresp', save_path)

    tpdocresp_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['CNES','CRM','COREN','RG','CPF']}
    create_and_save_auxiliary(tpdocresp_dict, 'tpdocresp', save_path)

    paridade_dict = {'Codigo': [1, 0], 'Descricao': ['Multípara', 'Nulípara']}
    create_and_save_auxiliary(paridade_dict, 'paridade', save_path)

    tpapresent_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Cefálico', 'Pélvica ou podálica', 'Transversa', 'Ignorado.']}
    create_and_save_auxiliary(tpapresent_dict, 'tpapresent', save_path)

    sttrabpart_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Sim', 'Não', 'Não se aplica', 'Ignorado.']}
    create_and_save_auxiliary(sttrabpart_dict, 'sttrabpart', save_path)

    tpmetestim_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Exame físico', 'Outro método', 'Ignorado.']}
    create_and_save_auxiliary(tpmetestim_dict, 'tpmetestim', save_path)

    racacormae_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['Branca', 'Preta', 'Amarela', 'Parda', 'Indígena.']}
    create_and_save_auxiliary(racacormae_dict, 'racacormae', save_path)

    origem_dict = {'Codigo': [1], 'Descricao': ['Oracle, 2- FTP, 3- SEAD']}
    create_and_save_auxiliary(origem_dict, 'origem', save_path)

    altcausa_dict = {'Codigo': [1, 2], 'Descricao': ['Sim', 'Não']}
    create_and_save_auxiliary(altcausa_dict, 'altcausa', save_path)

    assistmed_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Sim', 'Não', 'Ignorado']}
    create_and_save_auxiliary(assistmed_dict, 'assistmed', save_path)

    circobito_dict = {'Codigo': [1, 2, 3, 4, 9], 'Descricao': ['Acidente', 'Suicídio', 'Homicídio', 'Outros', 'Ignorado']}
    create_and_save_auxiliary(circobito_dict, 'circobito', save_path)

    cirurgia_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Sim', 'Não', 'Ignorado']}
    create_and_save_auxiliary(cirurgia_dict, 'cirurgia', save_path)

    esc_dict = {'Codigo': [1, 2, 3, 4, 5, 9], 'Descricao': ['Nenhuma', 'de 1 a 3 anos', 'de 4 a 7 anos', 'de 8 a 11 anos', '12 anos e mais', 'Ignorado']}
    create_and_save_auxiliary(esc_dict, 'esc', save_path)

    esc2010_dict = {'Codigo': [0, 1, 2, 3, 4, 5, 9], 'Descricao': ['Sem escolaridade', 'Fundamental I (1ª a 4ª série', 'Fundamental II (5ª a 8ª série', 'Médio  (antigo  2º  Grau', 'Superior  incompleto', 'Superior completo', 'Ignorado']}
    create_and_save_auxiliary(esc2010_dict, 'esc2010', save_path)

    tpmorteoco_dict = {'Codigo': [1, 2, 3, 4, 5, 8, 9], 'Descricao': ['Na gravidez', 'No parto', 'No abortamento', 'Até 42 dias após o término do parto', 'de 43 dias a 1 ano após o término da gestação', 'Não ocorreu nestes períodos', 'Ignorado']}
    create_and_save_auxiliary(tpmorteoco_dict, 'tpmorteoco', save_path)

    tpobitocor_dict = {'Codigo': [1], 'Descricao': ['Durante a gestação, 2- Durante o abortamento, 3- Após o abortamento, 4- No parto ou até 1 hora após o parto, 5- No puerpério - até 42 dias após o parto, 6- Entre 43 dias e até 1 ano após o parto, 7- A investigação Não identificou o momento do óbito, 8- Mais de um ano após o parto, 9- O óbito Não ocorreu nas circunstâncias anteriores']}
    create_and_save_auxiliary(tpobitocor_dict, 'tpobitocor', save_path)

    tppos_dict = {'Codigo': [1, 2], 'Descricao': ['Sim', 'Não']}
    create_and_save_auxiliary(tppos_dict, 'tppos', save_path)

    tpresginfo_dict = {'Codigo': [1], 'Descricao': ['Não acrescentou']}
    create_and_save_auxiliary(tpresginfo_dict, 'tpresginfo', save_path)

    atestante_dict = {'Codigo': [1, 2, 3, 4, 5], 'Descricao': ['Sim','Substituto','IML','SVO','Outros']}
    create_and_save_auxiliary(atestante_dict, 'atestante', save_path)

    escmaeagr1_dict = {'Codigo': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 'Descricao': ['Sem Escolaridade', 'Fundamental I Incompleto', 'Fundamental I Completo', 'Fundamental II Incompleto', 'Fundamental II Completo', 'Ensino Médio Incompleto', 'Ensino Médio Completo', 'Superior Incompleto', 'Superior Completo', 'Ignorado', 'Fundamental I Incompleto ou Inespecífico', 'Fundamental II Incompleto ou Inespecífico', 'Ensino Médio Incompleto ou Inespecífico']}
    create_and_save_auxiliary(escmaeagr1_dict, 'escmaeagr1', save_path)

    estciv_dict = {'Codigo': [1, 2, 3, 4, 5, 9], 'Descricao': ['Solteiro', 'Casado', 'Viúvo', 'Separado judicialmente/divorciado', 'União estável', 'Ignorado']}
    create_and_save_auxiliary(estciv_dict, 'estciv', save_path)

    exame_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Sim', 'Não', 'Ignorado']}
    create_and_save_auxiliary(exame_dict, 'exame', save_path)

    fonte_dict = {'Codigo': [1, 2, 3, 4, 9], 'Descricao': ['Ocorrência policial', 'Hospital', 'Hamília', 'Outra', 'Ignorado']}
    create_and_save_auxiliary(fonte_dict, 'fonte', save_path)

    gestacao_dict = {'Codigo': [1, 2, 3, 4, 5, 6], 'Descricao': ['Menos de 22 semanas', '22 a 27 semanas', '28 a 31 semanas', '32 a 36 semanas', '37 a 41 semanas', '42 e + semanas']}
    create_and_save_auxiliary(gestacao_dict, 'gestacao', save_path)

    gravidez_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Única', 'Dupla', 'Tripla e mais', 'Ignorada']}
    create_and_save_auxiliary(gravidez_dict, 'gravidez', save_path)

    obitoparto_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Antes', 'Durante', 'Depois', 'Ignorado']}
    create_and_save_auxiliary(obitoparto_dict, 'obitoparto', save_path)

    obitopuerp_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Sim, até 42 dias após o parto', 'Sim, de 43 dias a 1 ano', 'Não', 'Ignorado']}
    create_and_save_auxiliary(obitopuerp_dict, 'obitopuerp', save_path)

    origem_dict = {'Codigo': [1, 2, 3, 9], 'Descricao': ['Oracle', 'Banco estadual diponibilizado via FTP', 'Banco SEADE', 'Ignorado']}
    create_and_save_auxiliary(origem_dict, 'origem', save_path)

    parto_dict = {'Codigo': [1, 2, 9], 'Descricao': ['Vaginal', 'Cesáreo', 'Ignorado']}
    create_and_save_auxiliary(parto_dict, 'parto', save_path)

    stdoepidem_dict = {'Codigo': [1, 0], 'Descricao': ['Sim', 'Não']}
    create_and_save_auxiliary(stdoepidem_dict, 'stdoepidem', save_path)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'atualizacao_tabelas_auxiliares',
    default_args=default_args,
    description='Tabelas Auxiliares - OpenDataSUS',
    schedule_interval='@daily',
    catchup=False
)

download_task = PythonOperator(
    task_id='atualizacao_tabelas_auxiliares',
    python_callable=atualizacao_tabelas_auxiliares,
    dag=dag
)

