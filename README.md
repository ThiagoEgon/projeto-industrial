# Pipeline de Manutenção Industrial (Airflow + dbt + DuckDB + MinIO)

Pipeline ELT completo para dados de manutenção industrial: ingestão de CSVs, conversão para Parquet, modelagem com dbt sobre DuckDB lendo MinIO (S3-compatível), testes e documentação. Orquestrado por Airflow e com dbt Docs servido em um container dedicado.

---

### Sumário

* [Arquitetura](#arquitetura)
* [Pré-requisitos](#pré-requisitos)
* [Variáveis de ambiente](#variáveis-de-ambiente)
* [Estrutura do repositório](#estrutura-do-repositório)
* [Passo a passo (execução completa)](#passo-a-passo-execução-completa)
* [Operações frequentes](#operações-frequentes)
* [Modelagem (camadas e modelos)](#modelagem-camadas-e-modelos)
* [Conexões e credenciais](#conexões-e-credenciais)
* [Solução de problemas](#solução-de-problemas)
* [Opcionais (recomendações e extensões)](#opcionais-recomendações-e-extensões)
* [Licença](#licença)

---

### Arquitetura

* **Data Lake**: MinIO
    * **raw**: CSVs originais.
    * **processed**: Parquet padronizado (gerado pela DAG).
    * **curated**: artefatos finais (ex.: `duckdb_pipeline.duckdb`).
* **Engine/Query**: DuckDB com extensão `httpfs` (leitura S3/MinIO).
* **Transformações**: dbt (staging e marts).
* **Orquestração**: Apache Airflow (DAG `elt_duckdb_minio_pipeline_separado`).
* **Documentação**: dbt Docs (`generate`/`serve`) no container `dbt_service`.

---

### Pré-requisitos

* Docker e Docker Compose instalados.
* **Portas livres**:
    * `8080` (Airflow Webserver)
    * `8081` (dbt Docs — mapeado para 8080 no container `dbt_service`)
    * `9000` (MinIO API) e `9001` (MinIO Console)
* Sistema operacional compatível com Docker (Linux/macOS/WSL).

---

### Variáveis de ambiente

Criar um arquivo `.env` na raiz (opcional, recomendado):

```ini
MINIO_ROOT_USER=seu_usuario
MINIO_ROOT_PASSWORD=sua_senha
AIRFLOW_UID=50000 (ou UID do usuário local)
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
AIRFLOW_SECRET_KEY=uma_chave_secreta_qualquer
Observação: As credenciais do MinIO são reutilizadas no dbt/duckdb via variáveis.

Estrutura do repositório
.
├── docker-compose.yml
├── airflow/
│   ├── dags/
│   │   └── elt_pipeline_dag.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── requirements.txt
│   └── README.md
└── data/
    ├── eventos_manutencao.csv
    ├── maquinas.csv
    └── sensores.csv
Passo a passo (execução completa)
Subir a stack

Bash

docker compose up -d --build
Serviços expostos:

MinIO Console: http://localhost:9001

Airflow Webserver: http://localhost:8080

dbt Docs: http://localhost:8081

Configurar a Connection no Airflow (minio_conn)

Acesse Airflow > Admin > Connections > +.

Connection Id: minio_conn

Conn Type: Amazon Web Services

Login: MINIO_ROOT_USER

Password: MINIO_ROOT_PASSWORD

Extra (JSON): {"host": "http://minio:9000", "aws_access_key_id": "MINIO_ROOT_USER", "aws_secret_access_key": "MINIO_ROOT_PASSWORD", "verify": false}

Preparar dados

Coloque os CSVs em ./data/: eventos_manutencao.csv, maquinas.csv, sensores.csv.

A pasta ./data é montada como /opt/airflow/data nos containers.

Executar a DAG

No Airflow, ative e rode a DAG elt_duckdb_minio_pipeline_separado.

Ordem das tasks: create_minio_buckets > upload_csvs_to_minio > convert_csvs_to_parquet > run_dbt_models > dbt_tests > upload_duckdb_to_curated > dbt_docs_generate e dbt_docs_serve.

Verificar resultados

MinIO Console: verifique arquivos em raw, processed e curated.

dbt Docs: acesse http://localhost:8081 (overview, modelos, lineage).

DuckDB: o arquivo ./data/duckdb_pipeline.duckdb é atualizado e também enviado para curated.