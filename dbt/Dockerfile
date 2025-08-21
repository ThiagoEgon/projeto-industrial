# Dockerfile do container dbt_service
# Objetivo: fornecer ambiente dbt + duckdb com extensão httpfs para ler S3/MinIO e servir dbt docs.

# Baseado em python:3.9-slim para manter o tamanho do container pequeno
FROM python:3.9-slim

# Diretório de trabalho do projeto dbt dentro do container
WORKDIR /dbt_project

# Copia primeiro requirements para otimizar cache de build
COPY ./requirements.txt .

# Instala dependências (dbt-duckdb, duckdb). Em produção, fixe versões (já fixadas aqui)
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do projeto (dbt_project.yml, profiles.yml, models, etc.)
COPY . .

# CMD padrão mantém o container ativo; Airflow usará 'docker exec' para rodar comandos dbt
CMD ["tail", "-f", "/dev/null"]
