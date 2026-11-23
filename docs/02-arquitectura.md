# 2. Arquitectura del prototipo

Código en [`prototipo/`](../prototipo). Toda la infraestructura se levanta con
[`docker-compose.yml`](../prototipo/docker-compose.yml) (proyecto `lsa`).

## Componentes

| Componente | Tecnología | Responsabilidad |
|---|---|---|
| **Generador** (`go/cmd/generador`) | Go | Simula 18.000 camiones. Cada camión envía en orden y espera el acuse; reintentos con backoff 100 ms → 2 s y timeout de 5 s por intento. Genera ráfagas de pánico (con prioridad sobre la telemetría pendiente) y pérdidas de cobertura (señales atrasadas). Registra cada envío en `gen.csv.gz`. |
| **HAProxy** | HAProxy 3.0 | Balancea entre receptores; health check cada 1 s, `fall 2`. |
| **Receptor** ×3 (`go/cmd/receptor`) | Go + franz-go | Recibe las señales por HTTP y las publica en Redpanda. Particionado manual FNV-1a(`truck_id`) mod *n*, de modo que cada camión siempre cae en la misma partición. |
| **Redpanda** ×3 | Redpanda 24.2.7 (API Kafka) | Broker de mensajes. `fsync` activado (`unsafe-bypass-fsync=false`), `write_caching` desactivado. |
| **Despachador** ×2 (`go/cmd/despachador`) | Go + franz-go + pgx | Consume el topic de pánicos como grupo de consumidores y guarda cada caso en PostgreSQL. |
| **Procesador** (`go/cmd/procesador`) | Go + pgx | 24 pipelines, uno por partición: escritor (COPY a PostgreSQL) → cola en memoria → worker de detección (costo simulado de 2 ms por señal; reglas de temperatura, puerta abierta en movimiento y exceso de velocidad). Marca duplicados y fuera de orden por camión. Expone `/metrics`. |
| **PostgreSQL 16** | `postgres:16-alpine` | Tablas `telemetria`, `panico`, `deteccion`, `enviado` **sin índices ni unicidad**, para poder contar duplicados. `saved_at` con `clock_timestamp()` en microsegundos. |

## Tácticas

| Táctica | Dónde se implementa | Escenario |
|---|---|---|
| Redundancia activa | 3 receptores, 2 despachadores, 3 brokers con factor de replicación 3 | DIS-1 |
| Heartbeat (detección de fallas) | Orquestador cada 1 s + latido del grupo de consumidores | DIS-1 |
| Acuse después de replicar | Receptor `ACK_MODE=replicated`: `acks=all`, timeout 4 s → HTTP 503 (el camión reintenta) | DIS-1 |
| Commit después de persistir | Despachador `COMMIT_MODE=after_write`: el offset se confirma solo tras guardar en PostgreSQL | DIS-1 |
| Canal prioritario de pánicos | Topic propio para pánicos, separado de la telemetría | DIS-1 / LAT |
| Particionado y concurrencia | 24 particiones y 24 workers; orden garantizado por camión | LAT-1, LAT-2 |

## Variantes comparadas

| | Táctica | Línea base EXP-1 / EXP-1b | Línea base EXP-2 / EXP-3 |
|---|---|---|---|
| Brokers / RF | 3 / RF=3 | 1 / RF=1 | 3 / RF=3 |
| Receptores | 3 | 1 | 3 |
| Despachadores | 2 | 1 | — (sin canal de pánicos) |
| Acuse al camión | Tras replicar (`acks=all`) | Inmediato, publicación asíncrona `acks=1` | Tras replicar |
| Commit del offset | Tras guardar | Auto-commit cada 1 s, escritura en lotes cada 500 ms | — |
| Colas de telemetría | 24 | 24 | **1 cola, 1 worker** |

La configuración exacta de cada variante está en `TACTICA`, `BASE_EXP1` y `BASE_COLA_UNICA` de
[`orquestador/run.py`](../prototipo/orquestador/run.py). La configuración efectiva de cada corrida
quedó registrada en su `docker_inspect.json`.

## Decisiones de diseño relevantes

- **Particionado manual por camión** en el receptor: garantiza el orden por camión sin depender del
  particionador del cliente.
- **Sin índices en la base de datos**: la verificación necesita contar duplicados; una restricción de
  unicidad los ocultaría.
- **Rebalanceo automático de líderes desactivado** en Redpanda y `MetadataMinAge` en 250 ms: el
  rebalanceo provocaba pausas de más de 4 s al producir (ver [bitácora](05-bitacora.md)).
- **Costo de detección simulado (2 ms/señal)**: hace que el procesamiento, y no el guardado, sea el
  recurso escaso, como ocurriría con reglas reales de detección.
