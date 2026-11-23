# Resultados

Evidencia de las 75 corridas. Estructura: `EXP-n/<escenario>/<variante>/repN/`.

## Archivos agregados

| Archivo | Contenido |
|---|---|
| `RESUMEN.md` | Veredicto (cumple / no cumple) por grupo |
| `resumen.json` | Métricas agregadas por grupo y por corrida |
| `informe.html` | Informe completo con gráficos (descargar y abrir en el navegador) |
| `entorno.json` | Hardware, sistema operativo, versiones de Docker e imágenes |
| `orquestador.log`, `stdout.log`, `stdout_extras.log` | Registro con hora de toda la ejecución |

## Archivos por corrida

| Archivo | Contenido |
|---|---|
| `verificacion.json` | Cruce de lo enviado contra lo guardado: pérdidas, duplicados, fuera de orden, % de pánicos guardados |
| `done.json` | Metadatos de la corrida y resultado de la verificación |
| `events.json` | Eventos con marca de tiempo en µs: inicio, inyección de la falla, detección, relanzamiento |
| `gen_summary.json` | Resumen del generador (enviadas, acusadas, reintentos) |
| `timeline.csv` | Backlog, lag y throughput cada segundo |
| `docker_stats.jsonl` | CPU y memoria de cada contenedor durante la corrida |
| `docker_inspect.json` | Configuración efectiva de cada contenedor |
| `redpanda_estado.json` | Estado del clúster, topics y particiones al terminar |
| `haproxy_stats.csv` | Estadísticas del balanceador |
| `metricas_*.json` | Métricas expuestas por cada servicio |
| `histogramas/*.csv` | Distribuciones de latencia (acuse, guardado, pánico, detección) |
| `logs/*.log` | Logs de los contenedores |

## Datos crudos

Los archivos `gen.csv.gz` (cada señal enviada) y `tablas/{telemetria,panico,deteccion}.csv.gz`
(volcado de PostgreSQL) suman ~3,1 GB y no están en el árbol del repositorio por tamaño.
Son la entrada de la verificación; los resultados de esa verificación sí están aquí
(`verificacion.json`, `histogramas/`).
