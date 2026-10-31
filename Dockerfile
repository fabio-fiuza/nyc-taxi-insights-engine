FROM apache/airflow:2.10.2-python3.11

USER root

# Instala ferramentas de sistema essenciais
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Instala versões travadas e compatíveis do dbt, Snowflake e utilitários
RUN pip install --no-cache-dir \
    dbt-core==1.8.2 \
    dbt-snowflake==1.8.2 \
    apache-airflow-providers-snowflake \
    loguru \
    astronomer-cosmos>=1.7.0