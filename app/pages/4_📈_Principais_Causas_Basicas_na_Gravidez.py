import streamlit as st
import pandas as pd
import plotly.express as px

gold_path = '../data/gold/'
auxiliares = '../data/gold/auxiliares/'

st.set_page_config(
    page_title="Análises de Dados - OpenDatasus", 
    page_icon=":bar_chart:",
    layout="wide")

st.title("Principais causas básicas na Gravidez - OpenDatasus")

st.markdown("""
    ##### **Objetivo:**
            Identificar as principais causas de óbito materno e correlacioná-las com o período pós-parto.
    ##### **Insight:** 
            Entender como as causas básicas variam de acordo com o período pós-parto e a situação gestacional pode ajudar a identificar padrões de risco específicos, fornecendo subsídios para melhorar os cuidados obstétricos e reduzir a mortalidade materna.
""")

agrupado_tpmorteoco = pd.read_parquet(f'{gold_path}/principais_causabasica_gravidez.parquet')

tpmorteoco_array = pd.read_parquet(auxiliares + 'tpmorteoco.parquet')
tpmorteoco_array = tpmorteoco_array[tpmorteoco_array['Codigo'] != 8]
tpmorteoco_array = tpmorteoco_array[tpmorteoco_array['Codigo'] != 9]
tpmorteoco_array = tpmorteoco_array.drop(columns=['Codigo'], axis=1)
#print(tpmorteoco_array.head())
# convert dataframe to array
tpmorteocorrencia = tpmorteoco_array['Descricao'].values
st.write('')
st.write("<h3 style='text-align: center;'>**Distribuição das principais Causas Básicas por Situação Gestacional** </h3> ", unsafe_allow_html=True)

#col1, col2 = st.columns(2)
#col3, col4 = st.columns(2)
#col5 = st.columns(1)
contador = 1
col1, col2 = st.columns(2)
for tpmarteoco in tpmorteocorrencia:

    fig1 = px.histogram(
                    agrupado_tpmorteoco.loc[agrupado_tpmorteoco['DESC_TPMORTEOCO'] == tpmarteoco],
                    x="DESC_TPMORTEOCO",
                    y="COUNT",
                    color="CAUSABAS",
                    labels={"DESC_TPMORTEOCO": "Situação Gestacional ou Pós-Gestacional", "CAUSABAS": "Causa Básica", "COUNT": "Número de Casos"},
                    barmode='group',
                    text_auto=True)
    exec(f"col{contador}.plotly_chart(fig1, use_container_width=True)")
    contador += 1
    if contador > 2:
        col1, col2 = st.columns(2)
        contador = 1
