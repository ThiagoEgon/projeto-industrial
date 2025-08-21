
  
    
    

    create  table
      "duckdb_pipeline"."main"."dim_maquina__dbt_tmp"
  
    as (
      -- dim_maquina.sql
-- Objetivo: dimensão de máquinas com atributos descritivos.
-- Grão: 1 linha por id_maquina (SCD Tipo 0 no escopo atual).



select
  id_maquina,
  tipo,
  localizacao
from "duckdb_pipeline"."main"."stg_maquinas"
    );
  
  