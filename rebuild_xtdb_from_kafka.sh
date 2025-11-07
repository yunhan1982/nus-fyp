#!/bin/bash

# rebuild_xtdb_from_kafka.sh
# Script to rebuild XTDB from Kafka transaction log with backdated entries

set -e

echo "=== XTDB Kafka Rebuild Script ==="
echo "This script will:"
echo "1. Stop XTDB container"
echo "2. Backup and remove existing XTDB index"
echo "3. Start XTDB to rebuild from Kafka tx-log"
echo "4. Monitor rebuild progress"
echo ""

# Configuration
XTDB_CONTAINER="xtdb-v2"
XTDB_DATA_PATH="/var/lib/xtdb"
KAFKA_CONTAINER="kafka"
KAFKA_TOPIC="xtdb-tx-log"
BACKUP_SUFFIX=$(date +%s)

# Function to check if container exists
check_container() {
    if ! docker ps -a --format "table {{.Names}}" | grep -q "^${XTDB_CONTAINER}$"; then
        echo "Error: Container '${XTDB_CONTAINER}' not found"
        echo "Available containers:"
        docker ps -a --format "table {{.Names}}\t{{.Status}}"
        exit 1
    fi
}

# Function to stop XTDB container
stop_xtdb() {
    echo "Stopping XTDB container..."
    docker-compose stop xtdb || docker stop ${XTDB_CONTAINER} || true
    echo "XTDB container stopped"
}

# Function to backup and remove index
backup_and_remove_index() {
    echo "Backing up and removing XTDB index..."
    
    # Try docker-compose approach first
    if docker-compose ps xtdb &>/dev/null; then
        echo "Using docker-compose to manage XTDB data..."
        docker-compose run --rm xtdb bash -c "mv ${XTDB_DATA_PATH}/index ${XTDB_DATA_PATH}/index.bak.${BACKUP_SUFFIX} 2>/dev/null || echo 'No existing index found'"
        docker-compose run --rm xtdb bash -c "rm -rf ${XTDB_DATA_PATH}/index"
    else
        echo "Using direct docker commands..."
        docker run --rm --volumes-from ${XTDB_CONTAINER} alpine sh -c "mv ${XTDB_DATA_PATH}/index ${XTDB_DATA_PATH}/index.bak.${BACKUP_SUFFIX} 2>/dev/null || echo 'No existing index found'"
        docker run --rm --volumes-from ${XTDB_CONTAINER} alpine sh -c "rm -rf ${XTDB_DATA_PATH}/index"
    fi
    
    echo "Index backup created: index.bak.${BACKUP_SUFFIX}"
    echo "Existing index removed"
}

# Function to start XTDB
start_xtdb() {
    echo "Starting XTDB container..."
    
    if docker-compose ps xtdb &>/dev/null; then
        docker-compose up -d xtdb
    else
        docker start ${XTDB_CONTAINER}
    fi
    
    echo "XTDB container started"
}

# Function to monitor logs
monitor_logs() {
    echo "Monitoring XTDB logs for rebuild progress..."
    echo "Press Ctrl+C to stop monitoring (XTDB will continue running)"
    echo "Look for messages about index replay and completion"
    echo ""
    
    if docker-compose ps xtdb &>/dev/null; then
        docker-compose logs -f xtdb
    else
        docker logs -f ${XTDB_CONTAINER}
    fi
}

# Function to verify Kafka messages (optional)
verify_kafka_messages() {
    echo "Verifying Kafka messages in topic 'xtdb-tx-log'..."
    echo "This will show the first 10 messages with timestamps:"
    echo ""
    
    # Try to consume messages from Kafka
    if command -v kafka-console-consumer &> /dev/null; then
        kafka-console-consumer --bootstrap-server localhost:9092 --topic xtdb-tx-log --from-beginning --property print.timestamp=true --max-messages 10
    else
        echo "kafka-console-consumer not found. You can verify messages manually:"
        echo "kafka-console-consumer --bootstrap-server localhost:9092 --topic xtdb-tx-log --from-beginning --property print.timestamp=true --max-messages 10"
    fi
}

# Main execution
echo "Checking for XTDB container..."
check_container

echo ""
read -p "Do you want to proceed with XTDB rebuild? This will remove existing index data. (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation cancelled"
    exit 0
fi

echo ""
read -p "Do you want to verify Kafka messages first? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    verify_kafka_messages
    echo ""
    read -p "Continue with rebuild? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Operation cancelled"
        exit 0
    fi
fi

# Execute rebuild steps
stop_xtdb
backup_and_remove_index
start_xtdb

echo ""
echo "=== Rebuild initiated ==="
echo "XTDB is now rebuilding from Kafka transaction log"
echo "Backup created: index.bak.${BACKUP_SUFFIX}"
echo ""
echo "To restore backup if needed:"
echo "docker-compose run --rm xtdb bash -c 'rm -rf ${XTDB_DATA_PATH}/index && mv ${XTDB_DATA_PATH}/index.bak.${BACKUP_SUFFIX} ${XTDB_DATA_PATH}/index'"
echo ""

read -p "Do you want to monitor the rebuild logs now? (Y/n): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    monitor_logs
else
    echo "You can monitor logs later with:"
    echo "docker-compose logs -f xtdb"
fi

echo ""
echo "Rebuild process completed!"
echo "Check XTDB HTTP API at http://localhost:3000 to verify the backdated transactions"