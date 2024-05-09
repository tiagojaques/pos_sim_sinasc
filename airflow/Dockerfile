FROM apache/airflow:2.9.0

USER root

RUN apt-get update
RUN apt-get install -y curl 
RUN apt-get install -y vim

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    requests \
    xlrd \
    "dask[complete]"

# COPY my_config_file.cfg /path/to/destination/