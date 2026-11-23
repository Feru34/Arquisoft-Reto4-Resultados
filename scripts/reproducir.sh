#!/usr/bin/env bash
# Vuelve a ejecutar los experimentos y regenera resumen e informe.
#   ./scripts/reproducir.sh           plan completo + pruebas extra (~6,5 h)
#   ./scripts/reproducir.sh --quick   versión corta para comprobar el entorno
# Los resultados se escriben en resultados/ (el orquestador es reanudable).
set -euo pipefail
cd "$(dirname "$0")/../prototipo/orquestador"

command -v docker >/dev/null || { echo "Falta Docker"; exit 1; }
command -v python3 >/dev/null || { echo "Falta Python 3"; exit 1; }

# Evita que macOS suspenda el equipo durante las corridas.
if command -v caffeinate >/dev/null; then caffeinate -dimsu & CAF=$!; trap 'kill $CAF 2>/dev/null' EXIT; fi

if [[ "${1:-}" == "--quick" ]]; then
  # Carpeta aparte para no mezclarse con los resultados oficiales.
  OUT="$(cd ../.. && pwd)/resultados_quick"
  python3 -u run.py --quick --reps 1 --results "$OUT"
  python3 -W ignore analyze.py "$OUT"
  echo "Listo: $OUT/RESUMEN.md"
else
  python3 -u run.py --reps 5
  python3 -u run.py --extras --reps 5
  python3 -W ignore analyze.py
  echo "Listo: resultados/RESUMEN.md y resultados/informe.html"
fi
