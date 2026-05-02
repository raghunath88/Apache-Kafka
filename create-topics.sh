#!/bin/bash
# create-topics.sh — idempotent Kafka topic setup
# Run once after docker-compose up, or in CI before integration tests

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
REPLICATION="${REPLICATION_FACTOR:-1}"  # 1 for local, 3 for production

echo "Creating Kafka topics on broker: $BROKER"
echo "Replication factor: $REPLICATION"

create_topic() {
  local TOPIC="$1"
  local PARTITIONS="${2:-3}"
  local RETENTION_MS="${3:-604800000}"  # 7 days default

  echo "→ Creating topic: $TOPIC (partitions=$PARTITIONS, replication=$REPLICATION)"

  docker exec dwp-kafka kafka-topics \
    --bootstrap-server "$BROKER" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --config retention.ms="$RETENTION_MS" \
    --config compression.type=lz4 \
    --config cleanup.policy=delete \
    --config segment.bytes=1073741824

  echo "  ✓ $TOPIC created"
}

# Main event topic
create_topic "dwp.orders" 3 604800000

# Dead-letter topic — receives events that failed all retries
create_topic "dwp.orders.DLT" 1 2592000000  # 30 days retention for investigation

# Audit topic — append-only, long retention for compliance
create_topic "dwp.audit" 3 31536000000  # 1 year

echo ""
echo "Listing all topics:"
docker exec dwp-kafka kafka-topics --bootstrap-server "$BROKER" --list

echo ""
echo "Done. Topics ready for use."
