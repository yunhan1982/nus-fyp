# XTDB Replay Solution

The `XTDBReplaySolution` is a bitemporal database solution that integrates with XTDB using Kafka log replay to accurately populate transaction times. This solution leverages the original valid time and transaction time timestamps from the bitemporal space to ensure data equivalence across different database systems.

## Features

- **Kafka Log Replay**: Uses Kafka as the transaction log backend for XTDB
- **Accurate Bitemporal Timestamps**: Preserves original valid time and transaction time from the bitemporal space
- **EDN Transaction Format**: Produces properly formatted EDN transactions for XTDB
- **Automatic Index Rebuild**: Forces XTDB to rebuild from the transaction log with correct timestamps
- **Full Bitemporal Queries**: Supports all bitemporal query operations with proper timestamp handling

## Architecture

```
Bitemporal Space → XTDBReplaySolution → Kafka → XTDB
     (VT, TT)           (EDN Txs)      (Log)   (Rebuild)
```

1. **Data Input**: Rectangles from bitemporal space with original VT/TT timestamps
2. **Transaction Creation**: Convert to EDN format with proper XTDB transaction structure
3. **Kafka Production**: Send transactions to Kafka with original transaction timestamps
4. **XTDB Replay**: XTDB rebuilds from Kafka log, preserving original timestamps

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

Key dependencies:
- `kafka-python>=2.0.2` - Kafka client for Python
- `edn-format>=0.7.5` - EDN format support for XTDB transactions
- `asyncpg>=0.30.0` - PostgreSQL async driver for XTDB queries

### 2. Start Services with Docker Compose

```bash
# Start all services (Zookeeper, Kafka, XTDB, MongoDB, MarkLogic)
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs xtdb
docker-compose logs kafka
```

### 3. Verify Kafka Setup

```bash
# Check if Kafka topic is created
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Monitor Kafka messages (optional)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic xtdb-tx-log --from-beginning
```

## Usage

### Basic Usage

```python
import asyncio
from datetime import datetime, timezone
from core.xtdb_replay_solution import XTDBReplaySolution
from core.bitemporal_space import Rectangle

async def main():
    # Initialize solution
    solution = XTDBReplaySolution(
        host="localhost",
        port=5433,
        kafka_bootstrap="localhost:9092",
        kafka_topic="xtdb-tx-log"
    )
    
    # Connect to XTDB and Kafka
    await solution.connect()
    
    # Create sample rectangles with bitemporal data
    rectangles = [
        Rectangle(
            data={"id": 1, "name": "Alice", "age": 25},
            vt_from=datetime(2023, 1, 1, tzinfo=timezone.utc),
            vt_to=datetime(2023, 12, 31, tzinfo=timezone.utc),
            tt_from=datetime(2023, 1, 15, tzinfo=timezone.utc),
            tt_to=datetime(2023, 6, 15, tzinfo=timezone.utc),
            index=1
        )
    ]
    
    # Insert data using Kafka replay
    await solution.insert_rectangle_to_collections(rectangles, "Student")
    
    # Query data at specific bitemporal coordinates
    results = await solution.query_by_name_and_age(
        name="Alice",
        age=25,
        tt=datetime(2023, 2, 1, tzinfo=timezone.utc),
        vt=datetime(2023, 6, 1, tzinfo=timezone.utc)
    )
    
    print(f"Query results: {results}")
    
    # Cleanup
    await solution.cleanup()

# Run the example
asyncio.run(main())
```

### Advanced Queries

```python
# Range query across bitemporal dimensions
results = await solution.range_query_by_attribute(
    attribute_name="age",
    attribute_value=25,
    vt_from=datetime(2023, 1, 1, tzinfo=timezone.utc),
    vt_to=datetime(2023, 12, 31, tzinfo=timezone.utc),
    tt_from=datetime(2023, 1, 1, tzinfo=timezone.utc),
    tt_to=datetime(2023, 6, 30, tzinfo=timezone.utc)
)

# Delta queries for change tracking
vt_changes = await solution.delta_since_vt_range(
    attribute_name="name",
    attribute_value="Alice",
    vt_from=datetime(2023, 1, 1, tzinfo=timezone.utc),
    vt_to=datetime(2023, 6, 1, tzinfo=timezone.utc),
    tt=datetime(2023, 3, 1, tzinfo=timezone.utc)
)

tt_changes = await solution.delta_since_tt_range(
    attribute_name="name",
    attribute_value="Alice",
    tt_from=datetime(2023, 1, 1, tzinfo=timezone.utc),
    tt_to=datetime(2023, 3, 1, tzinfo=timezone.utc),
    vt=datetime(2023, 6, 1, tzinfo=timezone.utc)
)
```

## Replay Process

### Manual Replay (for testing)

1. **Stop XTDB**:
   ```bash
   docker-compose stop xtdb
   ```

2. **Clear XTDB Index** (forces rebuild):
   ```bash
   docker volume rm fyp_xtdb_data
   # Or backup and remove: docker run --rm -v fyp_xtdb_data:/data alpine rm -rf /data/*
   ```

3. **Produce Backdated Transactions**:
   ```python
   # Use the XTDBReplaySolution to send transactions to Kafka
   await solution.insert_rectangle_to_collections(rectangles)
   ```

4. **Restart XTDB** (rebuilds from Kafka):
   ```bash
   docker-compose up -d xtdb
   ```

5. **Verify Replay**:
   ```bash
   # Check XTDB logs for rebuild progress
   docker-compose logs -f xtdb
   
   # Query XTDB to verify timestamps
   curl -X POST http://localhost:3000/_xtdb/submit-q \
     -H "Content-Type: application/json" \
     -d '{"q": ["find", ["?tx", "?e"], ["pull", "?e", ["*"]], ["tx-time", "?tx"]], "args": []}'
   ```

### Automated Replay Script

Use the provided script for automated replay:

```bash
# Make script executable
chmod +x rebuild_xtdb_from_kafka.sh

# Run automated rebuild
./rebuild_xtdb_from_kafka.sh
```

## Configuration

### Docker Compose Configuration

The `docker-compose.yml` includes:

- **Zookeeper**: Kafka coordination service
- **Kafka**: Transaction log backend with topic `xtdb-tx-log`
- **XTDB**: Configured to use Kafka for transaction log
- **MongoDB**: For comparison with other solutions
- **MarkLogic**: Additional bitemporal database option

### XTDB Environment Variables

```yaml
environment:
  XTDB_NODE_ID: "node1"
  KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
  XTDB_LOG_TOPIC: "xtdb-tx-log"
  XTDB_TX_LOG_KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
  XTDB_TX_LOG_KAFKA_TOPIC: "xtdb-tx-log"
```

### Kafka Configuration

- **Topic**: `xtdb-tx-log`
- **Partitions**: Single partition for ordered replay
- **Retention**: 168 hours (7 days)
- **Auto-create**: Enabled for convenience

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**:
   ```bash
   # Check Kafka status
   docker-compose logs kafka
   
   # Verify Kafka is accessible
   docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

2. **XTDB Not Rebuilding**:
   ```bash
   # Ensure XTDB index is cleared
   docker volume ls | grep xtdb
   docker volume rm fyp_xtdb_data
   
   # Check XTDB configuration
   docker-compose logs xtdb | grep -i kafka
   ```

3. **Transaction Format Errors**:
   ```bash
   # Monitor Kafka messages for format issues
   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic xtdb-tx-log
   ```

4. **Timestamp Issues**:
   - Ensure all timestamps are timezone-aware (UTC recommended)
   - Verify EDN format: `#inst "2023-01-01T00:00:00.000Z"`
   - Check Kafka message timestamps match transaction times

### Performance Considerations

- **Batch Size**: Process rectangles in batches for better throughput
- **Kafka Partitioning**: Use single partition for ordered replay
- **Memory**: Monitor XTDB memory usage during rebuild
- **Network**: Ensure stable network between Kafka and XTDB

## Comparison with Other Solutions

| Feature | XTDBReplaySolution | XTDBSolution | MongoDB Solutions |
|---------|-------------------|--------------|------------------|
| Bitemporal Support | Native | Manual fields | Manual fields |
| Transaction Time | Kafka replay | Current time | Manual |
| Query Performance | High | High | Variable |
| Data Consistency | Strong | Eventual | Eventual |
| Complexity | Medium | Low | Low |

## API Reference

### Core Methods

- `insert_rectangle_to_collections(rectangles, entity)`: Insert bitemporal data via Kafka
- `query_by_name_and_age(name, age, tt, vt, entity)`: Point query at bitemporal coordinates
- `range_query_by_attribute(attr_name, attr_value, vt_from, vt_to, tt_from, tt_to, entity)`: Range query
- `delta_since_vt_range(attr_name, attr_value, vt_from, vt_to, tt, entity)`: Valid time changes
- `delta_since_tt_range(attr_name, attr_value, tt_from, tt_to, vt, entity)`: Transaction time changes

### Utility Methods

- `_datetime_to_iso(dt)`: Convert datetime to XTDB ISO format
- `_iso_to_epoch_ms(iso)`: Convert ISO string to epoch milliseconds
- `_make_edn_tx(tx_id, tx_time_iso, ops)`: Create EDN transaction payload
- `_send_tx_to_kafka(tx_id, tx_time, ops)`: Send transaction to Kafka

## License

This solution is part of the bitemporal database comparison project.