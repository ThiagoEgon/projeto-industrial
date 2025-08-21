
  
  create view "duckdb_pipeline"."main"."stg_eventos_manutencao__dbt_tmp" as (
    -- stg_eventos_manutencao.sql
-- Objetivo: expor eventos de manutenção a partir de Parquet no bucket 'processed'.
-- Grão: 1 linha por evento (id_evento).
-- Origem: s3://processed/eventos_manutencao.parquet



select *
from parquet_scan('s3://processed/eventos_manutencao.parquet')
  );
