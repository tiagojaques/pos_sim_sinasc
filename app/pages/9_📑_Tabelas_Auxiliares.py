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
filenames = os.listdir(auxiliares)
lista = []
lista.append("Selecione um arquivo")
for i in range(len(filenames)):
    lista.append(filenames[i].replace(".parquet","").upper())

st.sidebar.write("Escolhar uma tabela auxiliar na lista abaixo:")
selected_file = st.sidebar.selectbox('Selecione um arquivo:', lista)

col1 = st.columns(1)

# Título e descrição principal
with st.container():
    st.title("Análises de Dados - OpenDatasus")
    # read the selected file
    if selected_file != "Selecione um arquivo":
        df = pd.read_parquet(f'{auxiliares}/{selected_file}.parquet')
        st.write(df.set_index(df.columns[0]))