# 4. Resultados

Cifras agregadas sobre 5 repeticiones por escenario. Fuente: [`resultados/resumen.json`](../resultados/resumen.json),
[`resultados/RESUMEN.md`](../resultados/RESUMEN.md) y la evidencia de cada corrida en `resultados/EXP-*/`.

## EXP-1 — Disponibilidad (DIS-1): falla en plena ráfaga de pánicos

Duración real: ≈ 1,6 h. 174.000 pánicos enviados con la táctica (15 corridas con falla).

| Falla | Táctica | Línea base |
|---|---|---|
| Despachador | 0 pánicos perdidos | 0 perdidos (128 duplicados) |
| Receptor | 0 pánicos perdidos | **30 pánicos y 645 señales acusadas perdidas**; peor repetición 99,914 % → **no cumple** |
| Broker | 0 pánicos perdidos | 0 perdidos |

- Detección de la caída por el heartbeat: mediana **0,55 s**.
- Táctica con falla de broker: 2 señales de **telemetría** acusadas se perdieron (caso borde de
  Redpanda: error `producer_state` en el mismo milisegundo del acuse). Ningún pánico afectado.
- Latencia del pánico durante la falla (táctica): p99 ≈ 4,7 s (despachador) y ≈ 3,9 s (broker),
  por la sesión de 6 s del grupo de consumidores y la elección de líder. Esto motivó EXP-1c.
- En la línea base, las fallas de despachador y broker no perdieron datos porque el proceso se
  relanza en ~1 s con su disco intacto. Esto motivó EXP-1b.

**Conclusión:** se confirma la hipótesis. La pérdida de la línea base se debe a que acusa antes de persistir.

Evidencia directa (ejemplo): `resultados/EXP-1/falla-receptor/base/rep1/verificacion.json`
→ `perdidas_panico_acusadas: 5`; `events.json` → SIGKILL a `receptor-1` en t = 60,500 s, detectado
0,6 s después y relanzado.

## EXP-1b — La máquina no vuelve

El heartbeat detecta la caída pero no relanza: solo sobreviven los datos replicados.

| Pérdida de… | Táctica (58.000 pánicos) | Línea base (58.000 pánicos) |
|---|---|---|
| Despachador | **0 pánicos perdidos** (3 duplicados) | **35.697 pánicos perdidos** (~61 %) |
| Broker | **0 pánicos perdidos** (4 señales de telemetría, 659 duplicados) | **35.494 pánicos y 2.272.394 señales perdidas** |

- Cifras muy estables entre repeticiones (~7.100 pánicos perdidos por corrida en la línea base).
- La línea base pierde porque confirma el offset antes de guardar y no tiene réplicas.

**Conclusión:** es el argumento más fuerte a favor de la redundancia con replicación.

## EXP-1c — Falla del despachador con sesión de 2 s

Solo táctica; sesión del grupo de 2 s y latido de 0,5 s.

| Métrica | Sesión 6 s (EXP-1) | Sesión 2 s (EXP-1c) |
|---|---|---|
| p99 pánico guardado desde el botón | 4,73 s | **1,08 s** |
| Mediana de p99 por repetición | — | 1,19 s |
| p99,9 / máximo | — | 1,93 s / 2,06 s |
| Pánicos perdidos | 0 | 0 (1 duplicado) |
| Detección de la caída (mediana) | 0,55 s | 0,37 s |

**Conclusión:** con sesión corta la meta de 2 s se cumple en p99; el máximo absoluto queda justo en el límite.

## EXP-2 — Latencia de guardado (LAT-1)

Duración real: ≈ 1,3 h.

| | Táctica (24 colas) | Línea base (1 cola) |
|---|---|---|
| p99 de guardado | **0,345 s** | 0,03 s |
| Pérdidas | 0 en 8,33 M señales | 0 |
| Señales atrasadas | 1,98 % | — |

- Ambas variantes cumplen con amplio margen: guardar no es el cuello de botella frente a una meta de 1 min.
- La línea base guarda más rápido porque un solo escritor arma lotes más grandes; su costo aparece
  al procesar (detección p99 ≈ 325 s).

**Conclusión:** se cumple con holgura; el experimento no discrimina entre variantes. Las colas se
justifican por LAT-2.

## EXP-3 — Latencia de procesamiento (LAT-2)

Duración real: ≈ 2 h.

| | Táctica (24 colas) | Línea base (1 cola) |
|---|---|---|
| p99 de procesamiento en el pico | **0,749 s** (mediana de p99 por rep. 0,62 s) | > 833 s observado |
| Fuera de orden | 0 | 0 |
| Vuelta a la normalidad tras el pico | 0,8 – 1,4 s | **Nunca** (backlog ~2,05 M señales) |
| Señales del pico procesadas | 7,5 M (100 %) | 24 % diez minutos después |

- Reparto por hash parejo entre las 24 colas (desviación ~3 %); cada camión siempre en la misma cola.
- ~480 señales/s por worker → 24 colas ≈ 11.500 señales/s, suficiente para el pico proyectado de
  40.000 camiones (ESC-2).

**Conclusión:** se confirma la hipótesis.

## Resumen de criterios

| Grupo | Veredicto | Valor |
|---|---|---|
| EXP-1 falla-despachador táctica / base | Cumple / Cumple | 100 % / 100 % |
| EXP-1 falla-receptor táctica / base | Cumple / **No cumple** | 100 % / 99,914 % |
| EXP-1 falla-broker táctica / base | Cumple / Cumple | 100 % / 100 % |
| EXP-1b máquina perdida despachador táctica / base | Cumple / **No cumple** | 100 % / 37,99 % |
| EXP-1b máquina perdida broker táctica / base | Cumple / **No cumple** | 100 % / 38,75 % |
| EXP-1c sesión 2 s táctica | Cumple | 100 % |
| EXP-2 pico táctica / base | Cumple / Cumple | p99 0,345 s / 0,03 s |
| EXP-3 pico táctica / base | Cumple / **No cumple** | p99 0,749 s / no normaliza |
