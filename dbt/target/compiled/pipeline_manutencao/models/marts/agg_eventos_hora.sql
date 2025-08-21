-- agg_eventos_hora.sql
-- Objetivo: agregações de eventos por máquina-hora (contagens e flags).
-- Grão: 1 linha por id_maquina e timestamp_hour.



with base as (
  select
    e.id_maquina,
    date_trunc('hour', cast(e.data_evento as timestamp)) as timestamp_hour,
    e.tipo_evento
  from "duckdb_pipeline"."main"."stg_eventos_manutencao" e
)

select
  id_maquina,
  timestamp_hour,
  count(*) as eventos_total,
  sum(case when lower(tipo_evento) = 'preventiva' then 1 else 0 end) as eventos_preventiva,
  sum(case when lower(tipo_evento) = 'corretiva' then 1 else 0 end)   as eventos_corretiva,
  sum(case when lower(tipo_evento) in ('inspeção','inspecao') then 1 else 0 end) as eventos_inspecao,
  case when count(*) > 0 then 1 else 0 end as flag_evento
from base
group by 1,2