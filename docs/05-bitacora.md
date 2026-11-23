# 5. Bitácora: problemas encontrados y correcciones

Cambios hechos al plan durante la ejecución. Los registros con hora de toda la ejecución están en
[`resultados/orquestador.log`](../resultados/orquestador.log), [`resultados/stdout.log`](../resultados/stdout.log)
y [`resultados/stdout_extras.log`](../resultados/stdout_extras.log).

| # | Problema | Efecto | Corrección |
|---|---|---|---|
| 1 | Redpanda venía con el bypass de `fsync` activado (modo desarrollo) | La persistencia no era real: un acuse no garantizaba que el dato estuviera en disco | Se desactivó (`unsafe-bypass-fsync=false`) y `write_caching_default=false` |
| 2 | Pausas de más de 4 s al producir (`context deadline exceeded`) | Picos de latencia y reintentos ajenos a la táctica evaluada | Se desactivó `enable_leader_balancer` y se bajó `MetadataMinAge` a 250 ms |
| 3 | Las primeras corridas de EXP-1 mataban el componente fuera de la ráfaga de pánicos | No probaban DIS-1, que exige la caída *mientras se procesan pánicos* | Se sincronizó el `SIGKILL` con la ráfaga (t = 60,5 s) y se reinició EXP-1 desde cero; las corridas anteriores se descartaron |
| 4 | El cálculo de fuera de orden por timestamp daba falsos positivos | El reloj de la VM de Docker retrocede algunos milisegundos | Se usa la marca del worker (`fuera_de_orden_worker`); el cálculo por timestamp se reporta aparte |
| 5 | Las primeras corridas extra coincidieron con una videollamada en el equipo | Acuse p99 de 13 ms a 0,86 s y miles de reintentos | Se detuvieron, se descartaron las 4 corridas contaminadas y se repitieron con el equipo libre |
| 6 | Tras EXP-1, la línea base no perdía con falla de despachador o broker | El relanzamiento en ~1 s con disco intacto ocultaba la falta de réplicas | Se agregó EXP-1b (la máquina no vuelve) |
| 7 | Tras EXP-1, el p99 del pánico durante la falla era ~4,7 s | No se cumplía la meta de 2 s | Se agregó EXP-1c (sesión de 2 s, latido de 0,5 s) |

## Cronología

| Fecha y hora | Hito |
|---|---|
| 13-nov 00:17 | Inicio del plan principal (10 escenarios × 5 repeticiones) |
| 14-nov 05:07 | Fin del plan principal: 50/50 corridas correctas |
| 14-nov 10:20 | Inicio de las pruebas extra (EXP-1b, EXP-1c) |
| 14-nov 11:43 | Fin de las pruebas extra: 25 corridas |
