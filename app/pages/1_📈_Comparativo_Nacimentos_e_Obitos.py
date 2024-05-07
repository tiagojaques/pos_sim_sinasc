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

st.title("Comparativo Histórico Motalidade e Nacimentos - OpenDatasus")

annual_comparison = pd.read_parquet(f'{gold_path}/comparativo_obitos_nascimentos.parquet')
fig = px.line(
    annual_comparison, x='Ano', 
    y=['Obitos', 'Nascimentos'], 
    markers=True, 
    labels={'value': 'Quantidade', 'variable': 'Tipo'}, 
    )
fig.update_xaxes(
    dtick="M1",
    tickformat="%b\n%Y"
)
st.plotly_chart(fig, use_container_width=True)

st.write("Fonte: OpenDatasus")
st.write(annual_comparison, use_container_width=True)
