-- agg_sensores_hora.sql
-- Objetivo: agregações de sensores por máquina-hora (reduz granularidade para consumo em dashboards).
-- Grão: 1 linha por id_maquina e timestamp_hour.

{{ config(materialized='table') }}

with base as (
  select
    s.id_maquina,
    date_trunc('hour', cast(s.timestamp as timestamp)) as timestamp_hour,
    s.temperatura_c,
    s.pressao_psi,
    s.vibracao_mm_s
  from {{ ref('stg_sensores') }} s
)

select
  id_maquina,
  timestamp_hour,
  avg(temperatura_c) as temp_avg,
  max(temperatura_c) as temp_max,
  avg(pressao_psi) as pressao_avg,
  quantile(pressao_psi, 0.95) as pressao_p95,
  avg(vibracao_mm_s) as vibracao_avg,
  max(vibracao_mm_s) as vibracao_max
from base
group by 1,2
