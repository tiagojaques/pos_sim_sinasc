# Usando a imagem oficial do Python 3.12.3 como base
FROM python:3.12.3

# Instalar o Streamlit
RUN pip install streamlit pandas pyarrow plotly.express dask[complete]

# Copiar as configurações desejadas para o container
COPY config.toml /root/.streamlit/config.toml

# Definindo o comando padrão como iniciar o Streamlit
CMD ["streamlit", "run", "/app/🏠_Home.py"]