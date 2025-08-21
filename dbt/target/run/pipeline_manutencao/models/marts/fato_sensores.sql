
  
    
    

    create  table
      "duckdb_pipeline"."main"."fato_sensores__dbt_tmp"
  
    as (
      -- fato_sensores.sql
-- Objetivo: fato de sensores, associada a tempo por hora.
-- Grão: 1 linha por leitura (após trunc para hora podem existir múltiplas linhas por máquina-hora).

select
  t.timestamp_hour,
  s.id_maquina,
  s.temperatura_c,
  s.pressao_psi,
  s.vibracao_mm_s
from "duckdb_pipeline"."main"."dim_tempo" t
join "duckdb_pipeline"."main"."stg_sensores" s
  on date_trunc('hour', cast(s.timestamp as timestamp)) = t.timestamp_hour
    );
  
  