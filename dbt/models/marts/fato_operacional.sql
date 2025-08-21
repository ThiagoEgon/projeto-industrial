-- fato_operacional.sql
-- Objetivo: fato derivada por máquina-hora, combinando métricas agregadas de sensores e eventos.
-- Grão: 1 linha por id_maquina e timestamp_hour (machine-hour).
-- Observação: dim_tempo deriva de eventos (pode faltar hora só com sensores). Usado FULL JOIN entre agregações e LEFT nas dimensões.

{{ config(materialized='table') }}

with tempo as (
  select
    t.timestamp_hour,
    t.ano,
    t.mes,
    t.dia,
    t.hora
  from {{ ref('dim_tempo') }} t
),
maquina as (
  select
    m.id_maquina,
    m.tipo,
    m.localizacao
  from {{ ref('dim_maquina') }} m
),
s as (select * from {{ ref('agg_sensores_hora') }}),
e as (select * from {{ ref('agg_eventos_hora') }}),

joined as (
  select
    coalesce(s.timestamp_hour, e.timestamp_hour) as timestamp_hour,
    coalesce(s.id_maquina, e.id_maquina) as id_maquina,
    s.temp_avg, s.temp_max, s.pressao_avg, s.pressao_p95, s.vibracao_avg, s.vibracao_max,
    e.eventos_total, e.eventos_preventiva, e.eventos_corretiva, e.eventos_inspecao, e.flag_evento
  from s
  full outer join e
    on s.id_maquina = e.id_maquina
   and s.timestamp_hour = e.timestamp_hour
)

select
  j.timestamp_hour,
  j.id_maquina,
  -- chaves de navegação
  j.timestamp_hour as fk_timestamp_hour,
  j.id_maquina     as fk_id_maquina,

  -- métricas sensores
  j.temp_avg,
  j.temp_max,
  j.pressao_avg,
  j.pressao_p95,
  j.vibracao_avg,
  j.vibracao_max,

  -- métricas eventos (com default 0)
  coalesce(j.eventos_total, 0) as eventos_total,
  coalesce(j.eventos_preventiva, 0) as eventos_preventiva,
  coalesce(j.eventos_corretiva, 0) as eventos_corretiva,
  coalesce(j.eventos_inspecao, 0) as eventos_inspecao,
  coalesce(j.flag_evento, 0) as flag_evento,

  -- atributos de dimensões (opcionalmente desnormalizados)
  t.ano, t.mes, t.dia, t.hora,
  m.tipo as tipo_maquina,
  m.localizacao as localizacao_maquina

from joined j
left join tempo t
  on j.timestamp_hour = t.timestamp_hour
left join maquina m
  on j.id_maquina = m.id_maquina
