
  
  create view "duckdb_pipeline"."main"."stg_sensores__dbt_tmp" as (
    -- stg_sensores.sql
-- Objetivo: expor leituras de sensores a partir de Parquet no bucket 'processed'.
-- Grão: 1 linha por leitura (timestamp, id_maquina).
-- Origem: s3://processed/sensores.parquet



select *
from parquet_scan('s3://processed/sensores.parquet')
  );
