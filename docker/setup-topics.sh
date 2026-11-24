#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi
for topic in "${ORDERS_TOPIC:-orders}" "${RETRY_TOPIC:-orders.retry}" "${DLQ_TOPIC:-orders.dlq}"; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions "${TOPIC_PARTITIONS:-3}" --replication-factor 1
done