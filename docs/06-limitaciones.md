# 6. Limitaciones y amenazas a la validez

- **Una sola máquina.** Todo corrió en un MacBook (Apple M5, 10 núcleos, 16 GB RAM; Docker Desktop
  con 10 CPU y 7,7 GB). Las cifras absolutas son indicativas; lo relevante es la **comparación entre
  táctica y línea base bajo la misma carga**. Detalle del entorno en
  [`resultados/entorno.json`](../resultados/entorno.json).
- **Red local.** No hay latencia de red real entre camiones, receptores y brokers; en producción las
  latencias de acuse serían mayores.
- **Caída de "máquina" simulada.** EXP-1b simula la pérdida del nodo deteniendo el contenedor sin
  relanzarlo; no hay pérdida física de disco ni partición de red.
- **Costo de detección simulado.** El worker usa 2 ms fijos por señal; reglas reales podrían tener
  costo variable.
- **Sensibilidad a la carga del equipo.** Una videollamada degradó el acuse p99 de 13 ms a 0,86 s.
  Esas corridas se descartaron. En EXP-1b, dos repeticiones se hicieron con uso liviano del equipo y
  el acuse p99 subió a ~0,6 s: las **latencias** de EXP-1b son indicativas, pero los **conteos de
  pérdida** no dependen de eso.
- **Reloj de la VM.** El reloj de la VM de Docker retrocede algunos milisegundos; por eso el orden se
  verifica con la marca del worker y no por timestamp.
- **Caso borde de Redpanda.** Con falla de broker, la táctica perdió 2 señales de telemetría acusadas
  (EXP-1) y 4 (EXP-1b) por un error `producer_state` en el mismo milisegundo del acuse. No afectó
  ningún pánico, pero muestra que `acks=all` no es una garantía absoluta.
