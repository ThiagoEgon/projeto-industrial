-- fato_eventos.sql
-- Objetivo: fato de eventos, juntando tempo (hora) e máquina.
-- Grão: 1 linha por evento (id_evento).

select
  t.timestamp_hour,
  e.id_evento,
  m.id_maquina,
  e.tipo_evento,
  e.descricao
from {{ ref('dim_tempo') }} t
join {{ ref('stg_eventos_manutencao') }} e
  on date_trunc('hour', cast(e.data_evento as timestamp)) = t.timestamp_hour
join {{ ref('dim_maquina') }} m
  on e.id_maquina = m.id_maquina
