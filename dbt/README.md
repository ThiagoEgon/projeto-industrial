# Pipeline de Manutenção (Airflow + dbt + DuckDB + MinIO)

Este projeto implementa um pipeline ELT para dados de manutenção industrial.

Fluxo de dados:
- Ingestão: arquivos CSV (eventos_manutencao.csv, maquinas.csv, sensores.csv) são enviados para o bucket s3://raw no MinIO.
- Processamento: os CSVs são convertidos para Parquet em s3://processed.
- Modelagem: o dbt lê Parquet via DuckDB/httpfs e cria camadas staging e marts (dimensões e fatos).
- Artefatos: o banco DuckDB é versionado e enviado para s3://curated.

Modelos principais:
- Dimensões: dim_tempo, dim_maquina
- Fatos: fato_eventos, fato_sensores
- Staging: stg_eventos_manutencao, stg_maquinas, stg_sensores

Como reconstruir a documentação:
1. dbt run
2. dbt docs generate
3. dbt docs serve

Ambiente:
- DuckDB com extensão httpfs para ler Parquet do MinIO
- MinIO (s3 compatível) como data lake
- Airflow para orquestração (DAG elt_duckdb_minio_pipeline_separado)
