# Bitemporal Data Storage Solutions

This project implements and compares various database solutions for bitemporal data storage, focusing on performance, scalability, and query capabilities for time-series data with both transaction time and valid time dimensions.

## Overview

Bitemporal data requires tracking two time dimensions:

- **Transaction Time (TT)**: When the data was stored in the database
- **Valid Time (VT)**: When the data was valid in the real world

This project implements six different solutions using various database technologies to handle bitemporal data storage and querying.

## Solutions Implemented

### 1. SolutionA - MongoDB (Basic)

- **Technology**: MongoDB with Motor (async driver)
- **Approach**: Single collection with embedded temporal metadata
- **File**: `core/solutionA.py`
- **Features**: Basic bitemporal storage, simple queries

### 2. SolutionB - MongoDB (Optimized)

- **Technology**: MongoDB with Motor
- **Approach**: Optimized indexing and query patterns
- **File**: `core/solutionB.py`
- **Features**: Enhanced performance, better indexing strategy

### 3. SolutionD - DuckDB (Basic)

- **Technology**: DuckDB (analytical database)
- **Approach**: Relational tables with temporal columns
- **File**: `core/solutionD.py`
- **Features**: SQL-based queries, columnar storage

### 4. SolutionC - MongoDB (Advanced)

- **Technology**: MongoDB with Motor
- **Approach**: Advanced aggregation pipelines and indexing
- **File**: `core/solutionC.py`
- **Features**: Complex temporal queries, aggregation optimization

### 5. SolutionE - DuckDB (Advanced)

- **Technology**: DuckDB with advanced features
- **Approach**: Optimized schema design and query patterns
- **File**: `core/solutionE.py`
- **Features**: Advanced SQL queries, performance optimization

### 6. XTDB Solution - Native Bitemporal

- **Technology**: XTDB v2 (purpose-built bitemporal database)
- **Approach**: Native bitemporal support with SQL queries via PostgreSQL wire protocol
- **File**: `core/xtdb_solution.py`
- **Features**: Built-in bitemporal capabilities, immutable data, SQL queries, PostgreSQL compatibility

### 7. MarkLogic Solution - Enterprise Document Database

- **Technology**: MarkLogic Server (multi-model database)
- **Approach**: Two-collection architecture using MarkLogic REST API and XQuery
- **File**: `core/marklogic_solution.py`
- **Features**: Document database, XQuery support, REST API, enterprise-grade security

## Project Structure

```
fyp/
├── core/
│   ├── solutionA.py          # MongoDB basic solution
│   ├── solutionB.py          # MongoDB optimized solution
│   ├── solutionD.py          # DuckDB basic solution
│   ├── solutionC.py          # MongoDB advanced solution
│   ├── solutionE.py          # DuckDB advanced solution
│   ├── xtdb_solution.py      # XTDB native bitemporal solution
│   ├── marklogic_solution.py # MarkLogic enterprise solution
│   ├── bitemporal_space.py   # Core bitemporal data structures
│   └── utils/
│       ├── generate_rectangles.py    # Data generation utilities
│       └── generate_student_data.py  # Student data generator
├── generate_mock_data.py     # Main data generation script
├── test_xtdb_solution.py     # XTDB solution test script
├── test_marklogic_solution.py # MarkLogic solution test script
├── docker-compose.yml        # Database services configuration
├── requirements.txt          # Python dependencies
├── README.md                 # This file
└── XTDB_README.md           # Detailed XTDB documentation
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Database Services

```bash
# Start all services
docker-compose up -d

# Or start specific services
docker-compose up -d xtdb      # For XTDB solution
docker-compose up -d marklogic # For MarkLogic solution
docker-compose up -d duckdb    # For DuckDB solutions
```

### 3. Generate and Test Data

```bash
# Generate mock data using all solutions
python generate_mock_data.py

# Test XTDB solution specifically
python test_xtdb_solution.py

# Test MarkLogic solution specifically
python test_marklogic_solution.py

# Test XTDB connection only
python test_xtdb_solution.py --connection-only
```

## Data Model

### Rectangle Data Structure

The project uses a "rectangle" abstraction for bitemporal data:

```python
class Rectangle:
    entity_id: str          # Unique entity identifier
    tt_from: datetime       # Transaction time start
    tt_to: datetime         # Transaction time end
    vt_from: datetime       # Valid time start
    vt_to: datetime         # Valid time end
    data: dict             # Payload data
```

### Student Data Example

```json
{
  "entity_id": "student-123",
  "tt_from": "2023-01-01T00:00:00Z",
  "tt_to": "2023-12-31T23:59:59Z",
  "vt_from": "2023-01-01T00:00:00Z",
  "vt_to": "2023-12-31T23:59:59Z",
  "data": {
    "payload": {
      "name": "John Doe",
      "age": 25,
      "grade": "A",
      "course": "Computer Science"
    }
  }
}
```

## Performance Testing

### Data Generation Parameters

- **Batch Size**: 500 entities per batch
- **Total Entities**: 200 (configurable)
- **Points per Entity**: 500 temporal points
- **Time Range**: 2018-2025 (7 years)
- **Data Type**: Student records with name, age, grade, course

### Benchmarking

Each solution is tested with:

1. **Insert Performance**: Batch insertion of temporal data
2. **Query Performance**: Various temporal query patterns
3. **Memory Usage**: Resource consumption analysis
4. **Scalability**: Performance with increasing data volumes

## Database Services

### XTDB v2 (Port 5432)

- **Image**: `ghcr.io/xtdb/xtdb:latest`
- **Purpose**: Native bitemporal database with PostgreSQL wire protocol
- **API**: PostgreSQL wire protocol
- **Query Language**: SQL

### DuckDB (Port 4000)

- **Image**: `qldrsc/duckdb:latest`
- **Purpose**: Analytical database for SQL-based solutions
- **API**: HTTP API
- **Query Language**: SQL

### MongoDB

- **Setup**: Local installation or MongoDB Atlas
- **Purpose**: Document database for NoSQL solutions
- **API**: Native MongoDB protocol
- **Query Language**: MongoDB Query Language

### MarkLogic Server (Ports 8000-8002)

- **Image**: `progressofficial/marklogic-db:latest`
- **Purpose**: Enterprise multi-model database for document-based solutions
- **API**: REST API
- **Query Language**: XQuery
- **Admin Interface**: http://localhost:8001 (admin/admin123)
- **App Server**: http://localhost:8000

## Common Operations

### Query Patterns

1. **Point-in-Time Query**: Get data valid at specific TT and VT
2. **Range Query**: Get data valid within time ranges
3. **History Query**: Get all versions of an entity
4. **Current Data Query**: Get latest valid data
5. **Temporal Join**: Join data across time dimensions

### Example Queries

```python
# Point-in-time query
results = await solution.query_by_name_and_age(
    "John", 25,
    datetime(2023, 6, 1),
    datetime(2023, 6, 1)
)

# Get entity history
history = await solution.get_data_history("student-123")

# Get all current data
current = await solution.get_all_current_data(
    datetime.now(),
    datetime.now()
)
```

## Development

### Adding New Solutions

1. Create new solution file in `core/`
2. Implement required interface methods:

   - `__init__()`
   - `connect()` (if needed)
   - `initialize_collections()`
   - `insert_rectangle_to_collections()`
   - `query_by_name_and_age()`
   - `get_all_entities()`
   - `get_current_data()`
   - `delete_data()`
   - `get_data_history()`

3. Add to `generate_mock_data.py` solutions list
4. Create tests and documentation

### Testing

```bash
# Test individual solutions
python -c "import asyncio; from core.solutionA import SolutionA; asyncio.run(SolutionA().test())"

# Test XTDB solution
python test_xtdb_solution.py

# Test MarkLogic solution
python test_marklogic_solution.py

# Run performance benchmarks
python generate_mock_data.py
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**

   - Ensure Docker services are running
   - Check port availability
   - Verify network connectivity

2. **Memory Issues**

   - Reduce batch sizes in data generation
   - Monitor Docker container memory usage
   - Consider persistent storage for large datasets

3. **Performance Issues**
   - Check database indexes
   - Optimize query patterns
   - Monitor resource usage

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Use smaller datasets for testing
rectangles = generate_rectangles(num_ids=10, num_points_per_id=5)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Update documentation
5. Submit a pull request

## License

This project is part of a Final Year Project (FYP) for academic research purposes.

## References

- [XTDB Documentation](https://docs.xtdb.com/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [MarkLogic Documentation](https://docs.marklogic.com/)
- [MarkLogic Docker](https://github.com/marklogic/marklogic-docker)
- [Bitemporal Data Modeling](https://en.wikipedia.org/wiki/Temporal_database)
