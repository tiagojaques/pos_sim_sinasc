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

st.title("Comparativo Tipos de Parto ao longo dos anos- OpenDatasus")

st.markdown("""
    ##### **Objetivo:**
            Mostrar a proporção de partos normais, cesáreos e ignorados ao longo dos anos.
    ##### **Insight:** 
            Ajudar a identificar possíveis variações nos métodos de parto, como aumento de cesarianas ou mudança de práticas médicas.
""")

comparacao_partos = pd.read_parquet(f'{gold_path}/comparativo_tipos_partos.parquet')

fig = px.bar(
    comparacao_partos,
    x="ANONASC",
    y="COUNT",
    color="Descricao",
    text_auto=True,
    title="Distribuição de Tipos de Parto ao Longo do Tempo",
    labels={"ANONASC": "Ano", "COUNT": "Número de Partos", "Descricao": "Tipo de Parto"}
)

# Configurações adicionais
fig.update_layout(barmode='stack', xaxis_title="Ano", yaxis_title="Número de Partos")
fig.update_xaxes(
    dtick="M1",
    tickformat="%b\n%Y"
)
# Exibindo o gráfico
st.plotly_chart(fig, use_container_width=True)

st.write("Fonte: OpenDatasus")
st.write(comparacao_partos, use_container_width=True)
