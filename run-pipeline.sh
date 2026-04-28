#!/bin/bash
set -e

NETWORK="kafka-net"
KAFKA_CONTAINER="kafka"
DATA_VOL="$(pwd)/data"
mkdir -p "$DATA_VOL"

echo "══════════════════════════════════════════"
echo " Step 0: Network & Kafka"
echo "══════════════════════════════════════════"
docker network create $NETWORK 2>/dev/null || true

# Start Kafka (KRaft mode, no Zookeeper)
docker rm -f $KAFKA_CONTAINER 2>/dev/null || true
docker run -d --name $KAFKA_CONTAINER --network $NETWORK \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  apache/kafka:3.7.0

echo "Waiting for Kafka to be ready..."
sleep 10

# Build images
echo "══════════════════════════════════════════"
echo " Step 1: Build images"
echo "══════════════════════════════════════════"
docker build -t kafka-base ./base          # ← add this line
docker build -t kafka-producer ./producer
docker build -t kafka-consumer ./consumer
docker build -t kafka-analyzer ./analyser

collect_scenario() {
  local FAULT=$1
  local DURATION=$2
  echo ""
  echo "══════════════════════════════════════════"
  echo " Collecting: $FAULT (${DURATION}s)"
  echo "══════════════════════════════════════════"

  # Start producer
  docker rm -f producer 2>/dev/null || true
  docker run -d --name producer --network $NETWORK \
    -e FAULT_MODE=$FAULT kafka-producer

  # Start consumer
  docker rm -f consumer 2>/dev/null || true
  docker run -d --name consumer --network $NETWORK \
    -e FAULT_MODE=$FAULT kafka-consumer

  # Let them run a moment before collecting
  sleep 5

  # Run analyser in collect mode (blocks until done)
  docker rm -f analyser 2>/dev/null || true
  docker run --rm --name analyser --network $NETWORK \
    -v "$DATA_VOL":/data \
    -e FAULT_MODE=$FAULT \
    -e ANALYZER_MODE=collect \
    -e COLLECT_SECONDS=$DURATION \
    kafka-analyser

  # Tear down producer/consumer
  docker rm -f producer consumer 2>/dev/null || true
  echo "Done: $FAULT"
  sleep 3
}

echo "══════════════════════════════════════════"
echo " Step 2: Collect labeled scenarios"
echo "══════════════════════════════════════════"
collect_scenario "healthy"             90
collect_scenario "slow_consumer"       90
collect_scenario "partition_imbalance" 90
collect_scenario "commit_failure"      90
collect_scenario "high_throughput"     90

# rebalance_loop — consumer crashes repeatedly, we collect from outside
echo ""
echo "══ Collecting: rebalance_loop ══"
docker rm -f producer 2>/dev/null || true
docker run -d --name producer --network $NETWORK \
  -e FAULT_MODE=rebalance_loop kafka-producer

# Run consumer with restart policy so it keeps crashing and restarting
docker rm -f consumer 2>/dev/null || true
docker run -d --name consumer --network $NETWORK \
  --restart=always \
  -e FAULT_MODE=rebalance_loop kafka-consumer

sleep 5
docker run --rm --name analyser --network $NETWORK \
  -v "$DATA_VOL":/data \
  -e FAULT_MODE=rebalance_loop \
  -e ANALYZER_MODE=collect \
  -e COLLECT_SECONDS=90 \
  kafka-analyser
docker rm -f producer consumer 2>/dev/null || true

echo ""
echo "══════════════════════════════════════════"
echo " Step 3: Train model"
echo "══════════════════════════════════════════"
docker run --rm --name analyser --network $NETWORK \
  -v "$DATA_VOL":/data \
  -e ANALYZER_MODE=train \
  kafka-analyser

echo ""
echo "══════════════════════════════════════════"
echo " Step 4: Live inference"
echo "══════════════════════════════════════════"
echo "Starting producer + consumer in HEALTHY mode..."
docker rm -f producer consumer 2>/dev/null || true
docker run -d --name producer --network $NETWORK -e FAULT_MODE=healthy kafka-producer
docker run -d --name consumer --network $NETWORK -e FAULT_MODE=healthy kafka-consumer

sleep 5
echo "Running live analyser (Ctrl+C to stop)..."
docker run --rm --name analyser --network $NETWORK \
  -v "$DATA_VOL":/data \
  -e ANALYZER_MODE=infer \
  kafka-analyser