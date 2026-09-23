#!/bin/bash
# Create required Kafka topics

KAFKA_BIN="/opt/kafka/bin"

echo "Creating Kafka topics..."

# Create orders topic
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Create notifications topic
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --topic notifications \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Create demo-topic
$KAFKA_BIN/kafka-topics.sh --create --if-not-exists \
  --topic demo-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo ""
echo "Listing all topics:"
$KAFKA_BIN/kafka-topics.sh --list --bootstrap-server localhost:9092

echo ""
echo "Done!"
