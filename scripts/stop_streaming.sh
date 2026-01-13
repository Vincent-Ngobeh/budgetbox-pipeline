#!/bin/bash
# Stop Kafka streaming infrastructure

echo "Stopping Kafka infrastructure..."
docker-compose -f docker-compose.kafka.yml down

echo "Kafka stopped"
