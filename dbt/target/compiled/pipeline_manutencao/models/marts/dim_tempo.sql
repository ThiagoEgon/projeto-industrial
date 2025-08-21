-- dim_tempo.sql
-- Objetivo: dimensão de tempo por hora derivada dos timestamps de eventos.
-- Observação: horas sem eventos podem não aparecer (sensores podem ter horas extras).

with eventos as (
  select distinct
    date_trunc('hour', cast(data_evento as timestamp)) as timestamp_hour,
    cast(data_evento as timestamp) as data_evento
  from "duckdb_pipeline"."main"."stg_eventos_manutencao"
)

select
  timestamp_hour,
  extract(year  from timestamp_hour) as ano,
  extract(month from timestamp_hour) as mes,
  extract(day   from timestamp_hour) as dia,
  extract(hour  from timestamp_hour) as hora
from eventos