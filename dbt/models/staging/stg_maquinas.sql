-- stg_maquinas.sql
-- Objetivo: expor cadastro de máquinas a partir de Parquet no bucket 'processed'.
-- Grão: 1 linha por máquina (id_maquina).
-- Origem: s3://processed/maquinas.parquet

{{ config(materialized='view') }}

select *
from parquet_scan('s3://processed/maquinas.parquet')
