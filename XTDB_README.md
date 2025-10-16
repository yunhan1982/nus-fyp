# XTDB Solution for Bitemporal Data Storage

This document describes the XTDB implementation for bitemporal data storage in the FYP project.

## Overview

The XTDB solution (`core/xtdb_solution.py`) implements bitemporal data storage using XTDB v2, a general-purpose database with built-in temporal capabilities. XTDB v2 provides native support for bitemporal queries and PostgreSQL wire protocol compatibility, making it an ideal choice for applications requiring both transaction time and valid time tracking.

## Features

- **Native Bitemporal Support**: XTDB v2 inherently supports both transaction time and valid time
- **PostgreSQL Compatibility**: Uses PostgreSQL wire protocol for familiar SQL-based interactions
- **Schema-less Design**: Flexible document storage with JSONB support
- **SQL Queries**: Standard SQL interface with temporal extensions
- **Immutable Data**: All data is immutable with full audit trail

## Architecture

The XTDB v2 solution uses a dual-table approach with PostgreSQL-compatible SQL:

1. **temporal_data Table**: Stores temporal metadata (entity references, time bounds)
2. **payload_data Table**: Stores actual payload data as JSONB with version references

This separation allows for efficient temporal queries while maintaining data integrity through immutable references.

### Table Structure

**temporal_data Table:**

```sql
CREATE TABLE temporal_data (
    id TEXT PRIMARY KEY,
    entity_id INTEGER,
    eref TEXT,
    vref TEXT,
    tt_from TIMESTAMPTZ,
    tt_to TIMESTAMPTZ,
    vt_from TIMESTAMPTZ,
    vt_to TIMESTAMPTZ
);
```

**payload_data Table:**

```sql
CREATE TABLE payload_data (
    vref TEXT PRIMARY KEY,
    payload JSONB
);
```

**Sample Data:**

```sql
-- Temporal record
INSERT INTO temporal_data VALUES (
    'temporal-abc123-1',
    123,
    'entity_hash',
    'version_hash',
    '2024-01-01T00:00:00Z',
    '2024-12-31T23:59:59Z',
    '2024-01-01T00:00:00Z',
    '2024-12-31T23:59:59Z'
);

-- Payload record
INSERT INTO payload_data VALUES (
    'version_hash',
    '{"name": "John Doe", "age": 25, "grade": "A"}'
);
```

## Setup

### 1. Start XTDB v2 Server

```bash
# Using Docker Compose (recommended)
docker-compose up xtdb-v2

# Or manually with Docker
docker run -p 5432:5432 -p 8080:8080 ghcr.io/xtdb/xtdb:2.0.0
```

### 2. Install Dependencies

```bash
pip install asyncpg
```

### 3. Test Connection

```bash
# Test XTDB v2 connection only
python test_xtdb_solution.py --connection-only

# Test PostgreSQL wire protocol connection
psql -h localhost -p 5432 -U xtdb -d xtdb -c "SELECT 1;"

# Full functionality test
python test_xtdb_solution.py
```

## Usage

### Basic Usage

```python
import asyncio
from core.xtdb_solution import XTDBSolution
from datetime import datetime, timezone

async def example():
    # Initialize with PostgreSQL connection parameters
    solution = XTDBSolution(
        host="localhost",
        port=5432,
        database="xtdb",
        user="xtdb",
        password="xtdb"
    )

    try:
        # Connect to XTDB v2 via PostgreSQL wire protocol
        await solution.connect()

        # Initialize tables
        await solution.initialize_collections()

        # Insert data (rectangles from generate_rectangles)
        await solution.insert_rectangle_to_collections(rectangles)

        # Query data using SQL
        results = await solution.query_by_name_and_age(
            "John", 25,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc)
        )

        # Get all entities
        entities = await solution.get_all_entities()

        # Get data history
        history = await solution.get_data_history(entity_id)

    finally:
        await solution.cleanup()

asyncio.run(example())
```

### Integration with Mock Data Generation

```python
# In generate_mock_data.py, uncomment the XTDB solution:
solutions = [
    SolutionE(),
    XTDBSolution()  # Enable XTDB solution
]
```

## API Reference

### Core Methods

- `__init__(host="localhost", port=5432, database="xtdb", user="xtdb", password="xtdb")`: Initialize with PostgreSQL connection parameters
- `connect()`: Establish asyncpg connection to XTDB v2
- `cleanup()`: Close connection and cleanup resources
- `initialize_collections()`: Create temporal_data and payload_data tables

### Data Operations

- `insert_rectangle_to_collections(rectangles)`: Insert bitemporal data using SQL batch operations
- `query_by_name_and_age(name, age, tt, vt)`: Query using SQL with temporal constraints and JSONB operations
- `get_all_entities()`: Retrieve all unique entity IDs using SQL
- `get_current_data(entity_id, tt, vt)`: Get current data for specific entity using SQL joins
- `get_all_current_data(tt, vt)`: Get all current data at specific times using SQL
- `delete_data(entity_id, tt, vt)`: Logically delete data by updating temporal bounds
- `get_data_history(entity_id)`: Retrieve complete history for entity using SQL joins

### Helper Methods

- `_generate_vref(data)`: Generate version reference using MD5 hash
- `_generate_eref(entity_id)`: Generate entity reference using MD5 hash

## Data Model

### Temporal Document Structure

```json
{
  "xt/id": "temporal-{hash}",
  "type": "temporal",
  "entity_id": "entity-123",
  "eref": "entity-ref-hash",
  "vref": "version-ref-hash",
  "tt_from": "2023-01-01T00:00:00Z",
  "tt_to": "2023-12-31T23:59:59Z",
  "vt_from": "2023-01-01T00:00:00Z",
  "vt_to": "2023-12-31T23:59:59Z"
}
```

### Data Document Structure

```json
{
  "xt/id": "data-{hash}",
  "type": "data",
  "payload": {
    "name": "John Doe",
    "age": 25,
    "grade": "A",
    "course": "Computer Science"
  }
}
```

## Performance Considerations

- **Batch Operations**: Use batch inserts for better performance
- **Query Optimization**: Leverage XTDB's built-in temporal indexes
- **Memory Usage**: XTDB in-memory mode for development, persistent storage for production
- **Connection Pooling**: Reuse HTTP connections for better performance

## Troubleshooting

### Common Issues

1. **Connection Failed**

   ```
   Error: Cannot connect to XTDB v2 at localhost:5433
   ```

   - Ensure XTDB v2 is running: `docker-compose up xtdb-v2`
   - Check if port 5432 is available
   - Verify XTDB v2 health: `curl http://localhost:8080/healthz/alive`
   - Test PostgreSQL connection: `psql -h localhost -p 5432 -U xtdb -d xtdb`

2. **SQL Execution Errors**

   ```
   Error: SQL execution failed
   ```

   - Check table structure exists (run initialize_collections())
   - Ensure JSONB data is properly formatted
   - Verify temporal constraints use correct timestamp format

3. **Query Errors**

   ```
   Error: Query returned no results
   ```

   - Validate SQL query syntax
   - Check temporal constraints are properly formatted
   - Ensure JSONB field paths are correct (e.g., payload->>'name')
   - Verify data exists within the specified time ranges

4. **Docker Issues**
   ```
   Error: XTDB v2 container fails to start
   ```
   - Check Docker logs: `docker-compose logs xtdb-v2`
   - Ensure sufficient memory allocation
   - Verify port conflicts (5432, 8080)

### Performance Tips

- Use batch insertions with executemany() for large datasets
- Create indexes on frequently queried JSONB fields
- Optimize temporal query ranges to reduce scan time
- Monitor XTDB v2 memory usage via HTTP endpoint
- Use EXPLAIN ANALYZE to optimize complex queries

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Test with smaller datasets
rectangles = generate_rectangles(num_ids=10, num_points_per_id=5)
```

## Comparison with Other Solutions

| Feature            | XTDB    | MongoDB       | DuckDB | PostgreSQL |
| ------------------ | ------- | ------------- | ------ | ---------- |
| Native Bitemporal  | ✅      | ❌            | ❌     | ❌         |
| Schema Flexibility | ✅      | ✅            | ❌     | ❌         |
| Query Language     | Datalog | MongoDB Query | SQL    | SQL        |
| ACID Compliance    | ✅      | ✅            | ✅     | ✅         |
| Horizontal Scaling | ✅      | ✅            | ❌     | Limited    |

## Future Enhancements

- **Persistent Storage**: Configure XTDB with RocksDB backend
- **Clustering**: Set up XTDB cluster for high availability
- **Advanced Queries**: Implement more complex temporal query patterns
- **Performance Monitoring**: Add metrics and monitoring
- **Data Migration**: Tools for migrating from other solutions

## References

- [XTDB Documentation](https://docs.xtdb.com/)
- [XTDB HTTP API](https://docs.xtdb.com/reference/http/)
- [Datalog Query Reference](https://docs.xtdb.com/reference/queries/)
- [Bitemporal Data Modeling](https://docs.xtdb.com/concepts/bitemporality/)
