#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results/local
if [[ -f results/local/run.json ]]; then
    echo "Local run already exists in results/local. Use docker compose start to open the existing lakehouse."
    exit 1
fi
docker info >/dev/null
docker compose up -d --build trino
python3 scripts/bootstrap.py
python3 - <<'PY'
import json
import time
import urllib.request
for attempt in range(120):
    try:
        with urllib.request.urlopen('http://127.0.0.1:18080/v1/info', timeout=5) as r:
            if r.status == 200 and not json.load(r).get('starting', True):
                break
    except OSError:
        pass
    time.sleep(2)
else:
    raise SystemExit('Trino is not ready; inspect docker compose logs trino')
PY
python3 scripts/run.py --scale 1 --write-repeats 3 --query-repeats 5 | tee results/local/run.log
docker compose up -d --build superset
python3 - <<'PY'
import time
import urllib.request
for attempt in range(180):
    try:
        with urllib.request.urlopen('http://127.0.0.1:18088/health', timeout=5) as r:
            if r.status == 200:
                break
    except OSError:
        time.sleep(2)
else:
    raise SystemExit('Superset is not ready; inspect docker compose logs superset')
PY
docker compose exec -T superset python /app/superset_bronze.py | tee results/local/superset.log
docker compose ps > results/local/docker-ps.txt
docker compose images --format json > results/local/images.json
python3 scripts/report.py results/local
