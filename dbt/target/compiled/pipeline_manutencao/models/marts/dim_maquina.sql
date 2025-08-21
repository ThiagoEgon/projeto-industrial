-- dim_maquina.sql
-- Objetivo: dimensão de máquinas com atributos descritivos.
-- Grão: 1 linha por id_maquina (SCD Tipo 0 no escopo atual).



select
  id_maquina,
  tipo,
  localizacao
from "duckdb_pipeline"."main"."stg_maquinas"