#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

printf "Waiting for Kafka at %s...\n" "$BOOTSTRAP_SERVER"
until kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; do
  sleep 2
done

create_topic() {
  local topic="$1"
  local partitions="$2"
  kafka-topics \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms=3600000
  printf "Ensured topic exists: %s (partitions=%s)\n" "$topic" "$partitions"
}

# Keep healthy with more partitions, while slow/stopped/bursty use one partition
# for clearer Burrow state transitions.
create_topic demo.healthy 3
create_topic demo.slow 1
create_topic demo.stopped 1
create_topic demo.bursty 1

printf "Created topics:\n"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep ^demo\. || true
