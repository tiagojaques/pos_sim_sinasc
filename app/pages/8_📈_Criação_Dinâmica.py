import streamlit as st
import pandas as pd
import plotly.express as px
import dask.dataframe as dd
import os

st.set_page_config(
    page_title="Tabelas Auxiliares", 
    page_icon=":bar_chart:",
    layout="wide")

##### DADOS ######
gold_path = '../data/gold/'
auxiliares = '../data/gold/auxiliares/'
silver_path = '../data/silver/'

colunas_agrupadas = ""

def valida_x(variavel_x):
    if variavel_x:
        return variavel_x
    else:
        if campos_selecionados:
            return campos_selecionados[0]
   
def valida_y(variavel_y):
    if variavel_y:
        return variavel_y
    else:
        if len(campos_selecionados) > 1:
            return campos_selecionados[1]
    
def carrega_parquet(nomes, df):
    filenames = os.listdir(auxiliares)
    #st.write('nomes 1: ',nomes)
    final_df = df
    if nomes:
        for nome in nomes:
            #st.write('nome: ',nome)
            if nome:
                if nome.startswith('CODMUN'):
                    df_aux = pd.read_parquet(f'{auxiliares}/municipio.parquet').astype('str')
                    df_aux['codigo_municipio_completo'] = df_aux['codigo_municipio_completo'].str[:6]
                    df[nome] = df[nome].str[:6]
                    final_df = df.merge(df_aux, left_on=nome, right_on='codigo_municipio_completo', how='left')
                    final_df = final_df.drop(
                            columns=['codigo_municipio_completo','uf','codigo_municipio']
                            ).rename(
                                columns={
                                    'nome_municipio': 'CODMUNRES_DESC',
                                    'nome_uf': 'UF',
                                })
                    df = final_df
                else:
                    #st.write('nome else: ',nome)
                    for i in range(len(filenames)):
                        if filenames[i].lower().startswith(nome.lower()):
                            #st.write(filenames[i].lower())
                            df_aux = pd.read_parquet(f'{auxiliares}/{filenames[i]}').astype('str')
                            final_df = df.merge(df_aux, left_on=nome, right_on='Codigo', how='left').dropna(subset=['Descricao'])
                            final_df = final_df.drop(columns=['Codigo']).rename(columns={'Descricao': f'{nome}_DESC'})
                            df = final_df
                            break

    return final_df

st.write("# Analise de Dados - OpenDatasus 📊")
base_principal = st.sidebar.selectbox("Selecione a Base de Dados desejada:", ["","SIM", "SINASC"])

if base_principal == "SIM":
    df = dd.read_parquet(f'{silver_path}/sim.parquet')
elif base_principal == "SINASC":
    df = dd.read_parquet(f'{silver_path}/sinasc.parquet')

if base_principal or base_principal != "":
    st.write(f"Base selecionada: {base_principal}")

    graficos = ["Tipo de Gráfico", "Bar", "Line", "Scatter", "Pie", "Histogram", "Box"]

    campos_selecionados = st.sidebar.multiselect("Selecione as colunas", df.columns)
    if campos_selecionados:
        colunas_agrupadas = st.sidebar.multiselect("Selecione o agrupamento", campos_selecionados)
        df = df[campos_selecionados]
    else:
        df = df

    if colunas_agrupadas != "" and colunas_agrupadas:
        #st.write("Agrupando por: ", colunas_agrupadas)
        df = df.groupby(colunas_agrupadas).size().compute().reset_index(name='CONTADOR')

    tipos_graficos = st.sidebar.selectbox("Selecione o tipo de gráfico", graficos)

    variavel_x = st.sidebar.multiselect("Selecione as variaveis X", campos_selecionados)
    variavel_y = st.sidebar.multiselect("Selecione as variaveis Y", campos_selecionados)

    var_x = valida_x(variavel_x)
    var_y = valida_y(variavel_y)

    print('var_x: ',var_x)
    print('var_y: ',var_y)

    df_aux = carrega_parquet([var_x, var_y], df)

    if tipos_graficos != "Tipo de Gráfico":
        if tipos_graficos == "Bar":
                var = f'{var_y}_DESC'
                fig = px.bar(df_aux, x=var_x, y='CONTADOR', color=var, barmode="group")
        elif tipos_graficos == "Line":
            # busca a coluna com _DESC para o agrupamento
            var = f'{var_y}_DESC'
            df_aux = df_aux.sort_values(by=[var_x])
            
            fig = px.line(df_aux, markers=True, x=campos_selecionados[0], y='CONTADOR', color=var)
            fig.update_xaxes(
                dtick="M1",
                tickformat="%b\n%Y"
            )
            #st.plotly_chart(fig, use_container_width=True)
        elif tipos_graficos == "Pie":
            fig = px.pie(df_aux, values='CONTADOR', names=df_aux['Descricao'])
        elif tipos_graficos == "Histogram":
            var = f'{var_y}_DESC'
            fig = px.histogram(df_aux, x=campos_selecionados[0], y='CONTADOR', color=var)
        elif tipos_graficos == "Box":
            fig = px.box(df_aux, x=campos_selecionados[0])
        else:
            fig = px.scatter(df, x=campos_selecionados[0], y=campos_selecionados[1])
        st.plotly_chart(fig, use_container_width=True)

    with st.container():
        if len(df_aux.index) != 0:
            st.write(df_aux.head(1000), use_container_width=True )
        else:
            st.write(df.head(1000), use_container_width=True )