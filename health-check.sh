#!/bin/bash
# health-check.sh — validates the Kafka cluster is healthy before deployment
# Used as a pre-deploy gate in GitLab CI

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
REQUIRED_TOPICS=("dwp.orders" "dwp.orders.DLT" "dwp.audit")
MAX_UNDER_REPLICATED=0
MAX_OFFLINE_PARTITIONS=0

echo "=== Kafka Cluster Health Check ==="
echo "Broker: $BROKER"
echo ""

# 1. Check broker is reachable
echo "→ Checking broker connectivity..."
if ! docker exec dwp-kafka kafka-broker-api-versions --bootstrap-server "$BROKER" > /dev/null 2>&1; then
  echo "FAIL: Cannot reach broker at $BROKER"
  exit 1
fi
echo "  ✓ Broker reachable"

# 2. Verify required topics exist
echo "→ Checking required topics..."
for TOPIC in "${REQUIRED_TOPICS[@]}"; do
  if docker exec dwp-kafka kafka-topics --bootstrap-server "$BROKER" --describe --topic "$TOPIC" > /dev/null 2>&1; then
    echo "  ✓ $TOPIC exists"
  else
    echo "  FAIL: Topic $TOPIC does not exist"
    exit 1
  fi
done

# 3. Check under-replicated partitions
echo "→ Checking under-replicated partitions..."
UNDER_REP=$(docker exec dwp-kafka kafka-topics \
  --bootstrap-server "$BROKER" \
  --describe \
  --under-replicated-partitions 2>/dev/null | wc -l | tr -d ' ')

if [ "$UNDER_REP" -gt "$MAX_UNDER_REPLICATED" ]; then
  echo "  WARN: $UNDER_REP under-replicated partition(s) found"
else
  echo "  ✓ No under-replicated partitions"
fi

# 4. Check consumer group lag
echo "→ Checking consumer group lag..."
docker exec dwp-kafka kafka-consumer-groups \
  --bootstrap-server "$BROKER" \
  --describe \
  --group dwp-consumers 2>/dev/null || echo "  INFO: Consumer group not yet registered (expected before first deploy)"

echo ""
echo "=== Health check passed ==="
