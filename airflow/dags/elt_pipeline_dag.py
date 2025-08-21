# -*- coding: utf-8 -*-
"""
DAG de ELT com Airflow + MinIO + dbt + DuckDB
Pipeline:
1. Cria buckets no MinIO (raw, processed, curated)
2. Envia CSVs locais para o bucket raw
3. Converte CSVs para Parquet e envia para processed
4. Executa dbt (clean, run, test, docs generate)
5. Faz upload do banco DuckDB gerado para curated
6. Serve dbt docs (com cwd garantido, sem duplicidade)
"""

import os
import logging
from datetime import datetime, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ------------------------
# CONFIGURAÇÕES
# ------------------------
MINIO_BUCKET_RAW = "raw"
MINIO_BUCKET_PROCESSED = "processed"
MINIO_BUCKET_CURATED = "curated"

LOCAL_DATA_PATH = "/opt/airflow/data"

# ------------------------
# FUNÇÕES PYTHON
# ------------------------

def create_minio_buckets(**context):
    hook = S3Hook(aws_conn_id="minio_conn", verify=False)
    for bucket in [MINIO_BUCKET_RAW, MINIO_BUCKET_PROCESSED, MINIO_BUCKET_CURATED]:
        try:
            if not hook.check_for_bucket(bucket_name=bucket):
                logging.info(f"Criando bucket {bucket}...")
                hook.create_bucket(bucket_name=bucket, region_name="")
            else:
                logging.info(f"Bucket {bucket} já existe.")
        except Exception as e:
            logging.warning(f"Erro ao verificar/criar bucket {bucket}: {e}")

def upload_csvs_to_minio(**context):
    hook = S3Hook(aws_conn_id="minio_conn", verify=False)
    if not os.path.exists(LOCAL_DATA_PATH):
        logging.warning(f"Pasta {LOCAL_DATA_PATH} não existe.")
        return
    for filename in os.listdir(LOCAL_DATA_PATH):
        if filename.lower().endswith(".csv"):
            local_filepath = os.path.join(LOCAL_DATA_PATH, filename)
            logging.info(f"Enviando {filename} para bucket {MINIO_BUCKET_RAW}...")
            hook.load_file(
                filename=local_filepath,
                key=filename,
                bucket_name=MINIO_BUCKET_RAW,
                replace=True
            )

def convert_csvs_to_parquet(**context):
    hook = S3Hook(aws_conn_id="minio_conn", verify=False)
    try:
        raw_objects = hook.list_keys(bucket_name=MINIO_BUCKET_RAW) or []
    except Exception as e:
        logging.warning(f"Falha ao listar objetos em {MINIO_BUCKET_RAW}: {e}")
        raw_objects = []

    for key in raw_objects:
        if key.lower().endswith(".csv"):
            local_csv = os.path.join("/tmp", os.path.basename(key))
            hook.get_key(key=key, bucket_name=MINIO_BUCKET_RAW).download_file(local_csv)
            df = pd.read_csv(local_csv)
            parquet_file = local_csv.replace(".csv", ".parquet")
            df.to_parquet(parquet_file, index=False)
            hook.load_file(
                filename=parquet_file,
                key=os.path.basename(parquet_file),
                bucket_name=MINIO_BUCKET_PROCESSED,
                replace=True
            )
            logging.info(f"{key} convertido e enviado para {MINIO_BUCKET_PROCESSED}.")

def upload_duckdb_to_curated(**context):
    hook = S3Hook(aws_conn_id="minio_conn", verify=False)
    local_db_path = os.path.join(LOCAL_DATA_PATH, "duckdb_pipeline.duckdb")
    if not os.path.exists(local_db_path):
        logging.warning(f"Banco {local_db_path} não encontrado.")
        return
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_id = context.get("run_id", "unknown_run")
    object_key = f"duckdb_pipeline_{now}_{run_id}.duckdb"
    hook.load_file(
        filename=local_db_path,
        key=object_key,
        bucket_name=MINIO_BUCKET_CURATED,
        replace=True
    )
    logging.info(f"DuckDB enviado para {MINIO_BUCKET_CURATED}/{object_key}.")

# ------------------------
# DAG
# ------------------------
with DAG(
    dag_id="elt_duckdb_minio_pipeline_separado",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "duckdb", "minio", "docker"],
) as dag:

    create_buckets = PythonOperator(
        task_id="create_minio_buckets",
        python_callable=create_minio_buckets,
    )

    upload_csvs = PythonOperator(
        task_id="upload_csvs_to_minio",
        python_callable=upload_csvs_to_minio,
    )

    convert_parquet = PythonOperator(
        task_id="convert_csvs_to_parquet",
        python_callable=convert_csvs_to_parquet,
    )

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "docker exec dbt_service dbt run "
            "--profiles-dir /dbt_project --project-dir /dbt_project"
        ),
    )

    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=(
            "docker exec dbt_service dbt test "
            "--profiles-dir /dbt_project --project-dir /dbt_project"
        ),
    )

    upload_duckdb = PythonOperator(
        task_id="upload_duckdb_to_curated",
        python_callable=upload_duckdb_to_curated,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            "docker exec dbt_service dbt docs generate "
            "--profiles-dir /dbt_project --project-dir /dbt_project"
        ),
    )

    dbt_docs_serve = BashOperator(
        task_id="dbt_docs_serve",
        bash_command=(
            "docker exec -d -w /dbt_project dbt_service bash -c "
            "'dbt docs serve --profiles-dir /root/.dbt "
            "--host 0.0.0.0 --port 8080 --no-browser'"
        ),
    )



    # ------------------------
    # ORQUESTRAÇÃO
    # ------------------------
    (
        create_buckets
        >> upload_csvs
        >> convert_parquet
        >> dbt_run
        >> dbt_tests
        >> upload_duckdb
        >> dbt_docs
        >> dbt_docs_serve
    )
