import streamlit as st
import pandas as pd
import plotly.express as px
import dask.dataframe as dd
import os

gold_path = '../data/gold/'
auxiliares = '../data/gold/auxiliares/'

st.set_page_config(
    page_title="Análises de Dados - OpenDatasus", 
    page_icon=":bar_chart:",
    layout="wide")

st.title("Avaliação entre escolaridade e numero de consulta pré-natais - OpenDatasus")

st.markdown("""
    ##### **Objetivo:**
            Avaliar a relação entre a escolaridade da mãe e o número de consultas pré-natais realizadas.
    ##### **Visualização:** 
            Um box plot ou gráfico de barras agrupadas pode ser utilizado para representar a distribuição do número de consultas pré-natais por diferentes níveis de escolaridade.
    ##### **Insight:** 
            Fornecer insights sobre possíveis disparidades no acesso ao atendimento pré-natal, o que pode influenciar a saúde materno-infantil.
""")


st.write("Comparativo entre tempo de estudo em anos concluido e número de consultas pré-natais")
comparacao_tempo_estudo = pd.read_parquet(f'{gold_path}/comparativo_tempo_estudo_consultas.parquet')

fig = px.bar(
    comparacao_tempo_estudo,
    x="ESCMAE",
    y="CONSULTAS",
    title="Média de Consultas Pré-Natais por Tempo de estudo em anos",
    labels={"ESCMAE": "Escolaridade, em anos de estudo concluido", "CONSULTAS": "Média de Consultas pré-natais"},
    text_auto=True
)
fig.update_layout(xaxis_title="Anos de estudo concluido", yaxis_title="Média de Consultas Pré-Natais")
st.plotly_chart(fig, use_container_width=True)

st.write("Comparativo entre escolaridade e número de consultas pré-natais")
comparacao_escolaridade = pd.read_parquet(f'{gold_path}/comparativo_escolaridade_consultas.parquet')

fig = px.bar(
    comparacao_escolaridade,
    x="ESCMAE2010",
    y="CONSULTAS",
    title="Média de Consultas Pré-Natais por Escolaridade",
    labels={"ESCMAE2010": "Escolaridade", "CONSULTAS": "Média de Consultas"},
    text_auto=True
)
fig.update_layout(xaxis_title="Nível de Escolaridade (Código)", yaxis_title="Média de Consultas Pré-Natais")
st.plotly_chart(fig, use_container_width=True)

st.write("Fonte: OpenDatasus")

col1, col2 = st.columns(2)
col1.write(comparacao_tempo_estudo, use_container_width=True)
col2.write(comparacao_escolaridade, use_container_width=True)   
