import os
import pandas as pd

def convert_csv_to_parquet(source_dir, target_dir, target_file):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # Lista para armazenar os dataframes
    df_list = []

    # Iterar sobre cada arquivo no diretório
    for file_name in os.listdir(source_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(source_dir, file_name)
            # Ler o arquivo CSV
            print(f"Lendo arquivo: {file_path}")
            df = pd.read_csv(file_path, delimiter=';', encoding='latin1', low_memory=False)
            # Adicionar o dataframe à lista
            df_list.append(df)

    # Concatenar todos os dataframes em um único dataframe
    if df_list:
        print("Concatenando dataframes...")
        
        full_df = pd.DataFrame()

        for df in df_list:
            print(f"Dataframe com {len(df)} linhas")
            full_df = pd.concat([full_df, df], ignore_index=True)

        # Inferir os tipos de dados
        print("Inferindo tipos de dados...")
        full_df.infer_objects()
        # Salvar o dataframe concatenado em um arquivo Parquet
        print(f"Salvando dados em: {os.path.join(target_dir,target_file)}")
        full_df.to_parquet(os.path.join(target_dir,target_file), engine='pyarrow', compression='snappy',)
        print(f"Dados Salvos: {target_file}")
    else:
        print("Nenhum arquivo CSV encontrado para processar.")