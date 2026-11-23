# 3. Método experimental

## Carga

Definida en [`orquestador/run.py`](../prototipo/orquestador/run.py) (`LOAD_EXP1`, `LOAD_EXP2`, `LOAD_EXP3`).

| Experimento | Perfil de carga | Pánicos | Otros |
|---|---|---|---|
| EXP-1, 1b, 1c | 150 s a 5.000 señales/s | Ráfagas de 400 pánicos cada 5 s (11.600 por corrida), repartidas en 2 s | Falla inyectada en t = 60,5 s, dentro de la ráfaga que empieza en t = 60 s |
| EXP-2 | 60 s a 1.500/s → 300 s a 5.000/s → 30 s a 1.500/s | Igual que EXP-1 | 5,5 % de los camiones pierden cobertura 120 s entre t = 60 y 240 s (señales atrasadas) |
| EXP-3 | 30 s a 1.500/s → 300 s a 5.000/s → cola a 1.500/s | — | La corrida termina cuando el backlog es ≤ 1.500 sostenido 30 s (mínimo 60 s, máximo 600 s) |

Cada escenario se repitió **5 veces**: 75 corridas en total (50 del plan y 25 extra).

## Ejecución de una corrida

1. Estado limpio: `docker compose down -v` y se levanta la variante.
2. Configuración de Redpanda: `write_caching_default=false`, `enable_leader_balancer=false`.
3. Hilos de monitoreo: línea de tiempo cada 1 s (`timeline.csv`), `docker stats`
   (`docker_stats.jsonl`) y heartbeat que detecta caídas y relanza el contenedor.
4. Se lanza el generador; en EXP-1 se envía `SIGKILL` al componente en t = 60,5 s.
5. Al terminar: verificación SQL, histogramas, exportación de tablas y logs, estado de Redpanda y
   HAProxy, y `done.json`.

Todos los eventos de la falla (inyección, detección, relanzamiento, instancia viva) quedan con
marca de tiempo en microsegundos en `events.json`.

## Verificación

La verificación **no depende de las métricas que reportan los propios servicios**. Se cruzan dos
fuentes independientes por (`truck_id`, `seq`):

- **Lo que se envió**: `gen.csv.gz`, escrito por el generador (cada señal, si fue acusada y cuántos intentos tomó).
- **Lo que se guardó**: las tablas de PostgreSQL (`tablas/*.csv.gz`).

De ese cruce salen, en `verificacion.json`:

| Métrica | Definición |
|---|---|
| `perdidas` | Señales emitidas que no están en la base |
| `perdidas_acusadas` | Señales que el camión recibió como acusadas pero no están en la base (la falla más grave) |
| `perdidas_panico` | Pánicos que no están en la base |
| `duplicados` | Señales guardadas más de una vez |
| `fuera_de_orden_worker` | Señales que el worker procesó fuera del orden del camión |
| `panicos_guardados_pct` | Porcentaje de pánicos guardados (criterio DIS-1) |

## Métricas de latencia

Calculadas desde los histogramas de cada corrida (`histogramas/*.csv`):

| Histograma | Mide |
|---|---|
| `acuse_desde_envio` | Tiempo hasta que el camión recibe el acuse |
| `guardado_desde_origen` | Desde que se genera la señal hasta que queda en PostgreSQL (LAT-1) |
| `panico_guardado_desde_origen` | Desde el botón de pánico hasta que el caso queda guardado |
| `deteccion_desde_origen_pico` | Desde la generación hasta la detección, solo señales del pico (LAT-2) |

## Agregación

[`orquestador/analyze.py`](../prototipo/orquestador/analyze.py) agrupa las repeticiones, evalúa cada
criterio y genera `resultados/resumen.json`, `resultados/RESUMEN.md` y `resultados/informe.html`.
Los percentiles agregados se calculan sobre la unión de los histogramas de las 5 repeticiones;
también se reporta la mediana de los p99 por repetición.
