# Arquisoft — Reto 4: experimentos de arquitectura para LogiSur Andina

Prototipo, experimentos y evidencia del Reto 4 del curso **ARTI 4109 — Arquitectura de Software**
(caso *LogiSur Andina*). Se construyó un prototipo de la plataforma de monitoreo de flota y se
ejecutaron experimentos controlados que comparan, bajo la misma carga, la **arquitectura con
tácticas** contra una **línea base** sin ellas.

- **75 corridas** (5 repeticiones por escenario), ejecutadas del 13 al 14 de noviembre de 2026.
- **18.000 camiones simulados**, picos de **5.000 señales/s**, fallas inyectadas con `SIGKILL`.
- Cada resultado se **verifica cruzando** lo que envió cada camión contra lo que quedó en la base de datos.

> Informe completo con gráficos: [`resultados/informe.html`](resultados/informe.html)
> (descargarlo y abrirlo en el navegador).

---

## Resultados en una tabla

| Experimento | Atributo / escenario | Táctica | Línea base | Veredicto |
|---|---|---|---|---|
| **EXP-1** — falla en plena ráfaga de pánicos | DIS-1: ≥ 99,99 % de pánicos guardados | **0 pánicos perdidos** en las 3 fallas (receptor, despachador, broker) | Falla de receptor: 30 pánicos y 645 señales acusadas perdidas (peor rep. 99,914 %) | Táctica cumple · base **no cumple** |
| **EXP-1b** — la máquina no vuelve | DIS-1 con pérdida de nodo | **0 pánicos perdidos** | ~35.600 pánicos perdidos (~61 %) | Táctica cumple · base **no cumple** |
| **EXP-1c** — sesión del grupo de 2 s | Pánico guardado en < 2 s durante la falla | p99 **1,08 s** (vs. 4,73 s con sesión de 6 s) | — | Cumple |
| **EXP-2** — pico de carga | LAT-1: guardado < 1 min (p99) | p99 **0,345 s**, 0 pérdidas en 8,33 M señales | p99 0,03 s, 0 pérdidas | Ambas cumplen (no discrimina) |
| **EXP-3** — pico de carga | LAT-2: procesamiento < 5 s (p99) y cola que se normaliza | p99 **0,749 s**, 0 fuera de orden, normaliza en ≤ 1,4 s | Backlog ~2,05 M, nunca se normaliza, p99 > 833 s | Táctica cumple · base **no cumple** |

Detalle por experimento: [`docs/04-resultados.md`](docs/04-resultados.md).

---

## Arquitectura del prototipo

```
 Generador (18.000 camiones)
        │  HTTP
        ▼
     HAProxy ──► Receptores ×3 ──► Redpanda (Kafka) ×3 brokers, RF=3
                                     │                         │
                         topic de pánicos              topic de telemetría
                          (1 partición)                 (24 particiones)
                                     │                         │
                                     ▼                         ▼
                          Despachadores ×2              Procesador
                         (grupo de consumo)          (24 pipelines: guardar
                                     │                 → detectar anomalías)
                                     └──────────► PostgreSQL 16 ◄──┘
```

**Stack:** Go 1.23 (franz-go, pgx) · Redpanda 24.2 · PostgreSQL 16 · HAProxy 3.0 · Docker Compose ·
Python 3 (solo biblioteca estándar) para orquestar y analizar.

Tácticas evaluadas: redundancia activa (réplicas), heartbeat, acuse solo después de replicar,
commit del offset solo después de persistir y particionado por camión en colas paralelas.
Ver [`docs/02-arquitectura.md`](docs/02-arquitectura.md).

---

## Estructura del repositorio

```
├── docs/
│   ├── 01-contexto.md            escenarios de calidad e hipótesis
│   ├── 02-arquitectura.md        componentes, tácticas, variantes y decisiones
│   ├── 03-metodo.md              carga, inyección de fallas, verificación y métricas
│   ├── 04-resultados.md          resultados por experimento
│   ├── 05-bitacora.md            problemas encontrados y correcciones
│   ├── 06-limitaciones.md        amenazas a la validez
│   └── PROCESO_Y_RESULTADOS.txt  bitácora completa del proceso
├── prototipo/
│   ├── docker-compose.yml        infraestructura completa
│   ├── go/                       servicios: receptor, despachador, procesador, generador
│   ├── postgres/init.sql         esquema
│   ├── haproxy/                  balanceador (3 o 1 receptor)
│   └── orquestador/              run.py (ejecuta), analyze.py (verifica y agrega)
├── resultados/                   evidencia de las 75 corridas (ver resultados/README.md)
└── scripts/reproducir.sh         vuelve a ejecutar todo
```

---

## Reproducir

Requisitos: Docker Desktop (≥ 8 GB asignados a la VM) y Python 3. Go no es necesario: se compila
dentro de un contenedor.

```bash
./scripts/reproducir.sh            # plan completo + pruebas extra (~6,5 h)
./scripts/reproducir.sh --quick    # versión corta para comprobar que todo funciona
```

Recomendación: equipo conectado a la corriente, sin videollamadas ni procesos pesados
(ver [`docs/06-limitaciones.md`](docs/06-limitaciones.md)).

---

**Autor:** Felipe Rueda — Maestría en Arquitecturas de TI, Universidad de los Andes.
