#!/bin/bash
# Start Kafka streaming infrastructure

set -e

echo "Starting Kafka infrastructure..."

docker-compose -f docker-compose.kafka.yml up -d

echo "Waiting for Kafka to be ready..."
sleep 10

if docker exec budgetbox-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo "Kafka is ready"
else
    echo "ERROR: Kafka failed to start. Check logs with: docker-compose -f docker-compose.kafka.yml logs"
    exit 1
fi

echo ""
echo "Kafka UI available at: http://localhost:8081"
echo ""
echo "To start producer: python -m streaming.producer --num-transactions 100"
echo "To start consumer: python -m streaming.consumer"
echo ""
echo "To stop: docker-compose -f docker-compose.kafka.yml down"
