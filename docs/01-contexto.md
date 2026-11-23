# 1. Contexto: escenarios de calidad e hipótesis

LogiSur Andina opera una flota de camiones que envían telemetría continua (posición, temperatura,
estado de puertas, velocidad) y, ocasionalmente, **señales de pánico** cuando el conductor oprime el
botón de emergencia. La plataforma debe recibir, guardar y procesar estas señales incluso en picos de
carga y ante fallas de sus componentes.

## Escenarios evaluados

| Id | Atributo | Estímulo | Medida de respuesta |
|---|---|---|---|
| **DIS-1** | Disponibilidad | Un componente (receptor, despachador o broker) se cae **mientras procesa una ráfaga de pánicos** | ≥ 99,99 % de los pánicos quedan guardados; ningún pánico acusado al camión se pierde. Meta complementaria: pánico guardado en < 2 s desde el botón |
| **LAT-1** | Latencia de guardado | Pico de carga (5.000 señales/s), incluidas señales atrasadas por pérdida de cobertura | p99 de guardado < 1 min, sin pérdidas |
| **LAT-2** | Latencia de procesamiento | Pico de carga sostenido 5 minutos | p99 de procesamiento (detección de anomalías) < 5 s, sin desorden por camión, y la cola vuelve a la normalidad tras el pico |

## Experimentos

| Experimento | Escenario | Hipótesis |
|---|---|---|
| **EXP-1** | DIS-1 | Con réplicas (RF=3), acuse solo después de replicar y commit del offset solo después de persistir, ningún pánico se pierde ante la caída de cualquier componente; la línea base (acuse inmediato, auto-commit, sin réplicas) sí pierde. |
| **EXP-2** | LAT-1 | Con particionado en 24 colas el guardado cumple p99 < 1 min en el pico. |
| **EXP-3** | LAT-2 | Con 24 colas y un worker por cola el procesamiento cumple p99 < 5 s y la cola se normaliza; con una sola cola no. |

### Pruebas adicionales (agregadas tras ver los resultados)

| Experimento | Motivo |
|---|---|
| **EXP-1b** | En EXP-1 la línea base no perdió con falla de despachador o broker porque el contenedor se relanzaba en ~1 s con su disco intacto. EXP-1b simula la **pérdida de la máquina**: el heartbeat detecta la caída pero no relanza. |
| **EXP-1c** | En EXP-1 el p99 del pánico durante la falla (~4,7 s) no cumplía la meta de 2 s por la sesión de 6 s del grupo de consumidores. EXP-1c repite la falla de despachador con sesión de 2 s y latido de 0,5 s. |
