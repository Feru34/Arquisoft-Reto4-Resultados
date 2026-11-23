-- Sin índices ni restricciones de unicidad: se quiere poder CONTAR duplicados y no frenar la ingesta.
CREATE TABLE telemetria (
  truck_id int, seq int, part smallint, ts_origin bigint, ts_sent bigint, ts_recv bigint,
  receptor smallint, late boolean,
  saved_at bigint DEFAULT (extract(epoch from clock_timestamp()) * 1000000)::bigint
);
CREATE TABLE panico (
  truck_id int, seq int, part smallint, ts_origin bigint, ts_sent bigint, ts_recv bigint,
  receptor smallint, writer text,
  saved_at bigint DEFAULT (extract(epoch from clock_timestamp()) * 1000000)::bigint
);
CREATE TABLE deteccion (
  truck_id int, seq int, part smallint, ts_origin bigint, ts_sent bigint,
  detected_at bigint, anomaly smallint, ooo smallint
);
-- Registro del generador (se carga al final de la corrida para verificar).
CREATE TABLE enviado (
  truck_id int, seq int, type smallint, ts_origin bigint, ts_sent bigint, ts_ack bigint,
  attempts int, late boolean
);
