# Setting Up XTDB and MarkLogic for Bitemporal Data Storage

Both XTDB and MarkLogic can be easily deployed using the provided `docker-compose.yml` configuration. **XTDB v2** runs as a containerized service exposing PostgreSQL wire protocol on port 5433, enabling SQL-based interactions with native bitemporal capabilities. The `XTDBSolution` class leverages asyncpg to connect via PostgreSQL protocol, creating temporal_data and payload_data tables dynamically for efficient bitemporal queries. **MarkLogic Server** deploys as an enterprise-grade multi-model database with REST API endpoints on ports 8000-8002, providing document storage with XQuery capabilities. The `MarkLogicSolution` implementation uses MarkLogic's REST API with HTTP Digest authentication, mirroring the two-collection approach of MongoDB solutions but leveraging MarkLogic's enterprise features. Both solutions can be started with `docker-compose up -d xtdb marklogic` and integrate seamlessly with the project's bitemporal Rectangle data model, offering different architectural approaches—XTDB for native temporal support and MarkLogic for enterprise document management with temporal extensions.

## XTDB v2 Setup

### Docker Configuration
```yaml
xtdb:
  image: ghcr.io/xtdb/xtdb:latest
  container_name: xtdb-v2
  ports:
    - "5432:5432" # Postgres wire protocol (primary API)
```

### Key Features
- **Native Bitemporal Support**: Built-in transaction time and valid time handling
- **PostgreSQL Compatibility**: Standard SQL interface with temporal extensions
- **Dynamic Table Creation**: Tables created automatically during INSERT operations
- **Immutable Data**: Full audit trail with version control

### Connection Example
```python
from core.xtdb_solution import XTDBSolution

solution = XTDBSolution(
    host="localhost",
    port=5432,
    database="xtdb",
    user="xtdb",
    password=""
)

await solution.connect()
await solution.initialize_collections()
```

## MarkLogic Setup

### Docker Configuration
```yaml
marklogic:
  image: progressofficial/marklogic-db:latest
  container_name: marklogic-server
  ports:
    - "8000:8000" # App Server port
    - "8001:8001" # Admin interface
    - "8002:8002" # Manage interface
  environment:
    - MARKLOGIC_INIT=true
    - MARKLOGIC_ADMIN_USERNAME=admin
    - MARKLOGIC_ADMIN_PASSWORD=admin123
```

### Key Features
- **Multi-Model Database**: Document, graph, and relational data support
- **REST API**: HTTP-based document operations
- **XQuery Support**: Powerful query language for complex operations
- **Enterprise Security**: Built-in authentication and authorization

### Connection Example
```python
from core.marklogic_solution import MarkLogicSolution

solution = MarkLogicSolution(
    host="localhost",
    port=8000,
    username="admin",
    password="admin123"
)

solution.initialize_collections()
```

## Quick Start Commands

```bash
# Start both services
docker-compose up -d xtdb marklogic

# Test XTDB connection
python test_xtdb_solution.py --connection-only

# Test MarkLogic connection
python test_marklogic_solution.py

# Access MarkLogic Admin Interface
open http://localhost:8001
```

## Integration with Project

Both solutions integrate with the project's bitemporal Rectangle data model:

```python
from core.utils.generate_rectangles import generate_rectangles
from datetime import datetime, timezone

# Generate test data
rectangles = generate_rectangles(
    start_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    num_ids=100,
    num_points_per_id=50,
    generate_data=True
)

# Insert into XTDB
xtdb_solution = XTDBSolution()
await xtdb_solution.insert_rectangle_to_collections(rectangles)

# Insert into MarkLogic
marklogic_solution = MarkLogicSolution()
marklogic_solution.insert_rectangle_to_collections(rectangles)
```

## Architecture Comparison

| Aspect | XTDB v2 | MarkLogic |
|--------|---------|----------|
| **Protocol** | PostgreSQL Wire | REST API |
| **Query Language** | SQL + Temporal Extensions | XQuery |
| **Data Model** | Relational + JSONB | Document-based |
| **Temporal Support** | Native Bitemporal | Application-level |
| **Scalability** | Horizontal | Enterprise Clustering |
| **Use Case** | Temporal Analytics | Enterprise Content Management |