# XTDB Transaction Log Generation and Replay Prototype

This prototype demonstrates how to programmatically generate XTDB transaction logs with custom system times and replay them on new nodes. This is essential for testing bitemporal scenarios and creating reproducible datasets.

## Overview

The prototype consists of:

1. **`generate_tx_log_prototype.clj`** - Core Clojure implementation
2. **`run_tx_log_prototype.py`** - Python wrapper for easy execution
3. **This README** - Documentation and usage guide

## Key Features

### 🕒 Custom System Time Control
- Generate transactions with specific system timestamps
- Create historical data scenarios for testing
- Simulate different data ingestion timelines

### 🔄 Transaction Log Replay
- Replay generated logs on fresh XTDB nodes
- Preserve bitemporal relationships
- Verify data consistency across replays

### 🔍 Bitemporal Querying
- Query data at specific system and valid times
- Demonstrate temporal data evolution
- Test complex bitemporal scenarios

## Prerequisites

### Required Software
1. **Clojure CLI Tools** - [Installation Guide](https://clojure.org/guides/getting_started)
2. **Java 8+** - Required for XTDB
3. **Python 3.6+** - For the wrapper script

### Project Setup
Ensure you're in a directory with a valid `deps.edn` file containing XTDB dependencies:

```clojure
{:deps {com.xtdb/xtdb-core {:mvn/version "1.24.1"}
        com.xtdb/xtdb-rocksdb {:mvn/version "1.24.1"}}}
```

## Usage

### Quick Start

```bash
# Run the complete prototype
python3 run_tx_log_prototype.py
```

### Manual Clojure Execution

```bash
# Start Clojure REPL
clojure

# Load and run the prototype
(load-file "generate_tx_log_prototype.clj")
(in-ns 'generate-tx-log-prototype)
(run-prototype)
```

## How It Works

### 1. Transaction Log Generation

The prototype creates transactions with specific system times:

```clojure
;; Sample transaction with custom system time
{:tx-ops [[:xtdb.api/put {:xt/id :student-1
                          :name "Alice"
                          :age 20}]]
 :valid-time #inst "2024-01-01T10:00:00.000-00:00"
 :system-time #inst "2024-01-01T10:00:00.000-00:00"}
```

### 2. Log Storage

Transactions are stored in RocksDB format:
- **TX Log**: `/tmp/xtdb-custom-tx-log`
- **Document Store**: `/tmp/xtdb-custom-doc-store`

### 3. Log Replay

A new XTDB node reads the existing transaction log:

```clojure
(def replay-node 
  (xt/start-node
    {:xtdb/tx-log {:xtdb/module 'xtdb.rocksdb/->kv-store
                   :db-dir "/tmp/xtdb-custom-tx-log"
                   :read-only? true}
     :xtdb/document-store {:xtdb/module 'xtdb.rocksdb/->kv-store
                           :db-dir "/tmp/xtdb-replay-doc-store"}}))
```

## Example Scenarios

### Scenario 1: Student Data Evolution

```clojure
;; Day 1: Alice enrolled
{:tx-ops [[:xtdb.api/put {:xt/id :student-1, :name "Alice", :age 20}]]
 :valid-time #inst "2024-01-01T10:00:00.000-00:00"
 :system-time #inst "2024-01-01T10:00:00.000-00:00"}

;; Day 2: Bob enrolled  
{:tx-ops [[:xtdb.api/put {:xt/id :student-2, :name "Bob", :age 21}]]
 :valid-time #inst "2024-01-02T10:00:00.000-00:00"
 :system-time #inst "2024-01-02T10:00:00.000-00:00"}

;; Day 3: Alice's data updated
{:tx-ops [[:xtdb.api/put {:xt/id :student-1, :name "Alice Updated", :age 21}]]
 :valid-time #inst "2024-01-03T10:00:00.000-00:00"
 :system-time #inst "2024-01-03T10:00:00.000-00:00"}

;; Day 4: Bob deleted
{:tx-ops [[:xtdb.api/delete :student-2]]
 :valid-time #inst "2024-01-04T10:00:00.000-00:00"
 :system-time #inst "2024-01-04T10:00:00.000-00:00"}
```

### Scenario 2: Bitemporal Queries

```clojure
;; Query as of different system times
(xt/db replay-node #inst "2024-01-02T12:00:00.000-00:00")
;; Returns: Alice and Bob

(xt/db replay-node #inst "2024-01-04T12:00:00.000-00:00")
;; Returns: Alice Updated (Bob deleted)

;; Query with valid time constraint
(xt/db replay-node 
       #inst "2024-01-04T12:00:00.000-00:00"  ;; system-time
       #inst "2024-01-02T12:00:00.000-00:00") ;; valid-time
;; Returns: Alice (original) and Bob (as they were on day 2)
```

## Output Example

```
=== XTDB Transaction Log Generation and Replay Prototype ===

1. Generating transaction log with custom system times...
Submitting transaction at system-time: 2024-01-01T10:00:00.000Z, valid-time: 2024-01-01T10:00:00.000Z
Submitting transaction at system-time: 2024-01-02T10:00:00.000Z, valid-time: 2024-01-02T10:00:00.000Z
Submitting transaction at system-time: 2024-01-03T10:00:00.000Z, valid-time: 2024-01-03T10:00:00.000Z
Submitting transaction at system-time: 2024-01-04T10:00:00.000Z, valid-time: 2024-01-04T10:00:00.000Z
Transaction log generated successfully!

2. Replaying transaction log...
Transaction log replayed successfully!
Total documents after replay: 1

3. Querying at different bitemporal points...

As of system-time 2024-01-01T12:00:00.000Z:
  :student-1: Alice (age 20)

As of system-time 2024-01-02T12:00:00.000Z:
  :student-1: Alice (age 20)
  :student-2: Bob (age 21)

As of system-time 2024-01-03T12:00:00.000Z:
  :student-1: Alice Updated (age 21)
  :student-2: Bob (age 21)

As of system-time 2024-01-04T12:00:00.000Z:
  :student-1: Alice Updated (age 21)

4. Querying with valid time constraints...
Valid time as of 2024-01-02 (system time 2024-01-04):
  :student-1: Alice (age 20)
  :student-2: Bob (age 21)

=== Prototype completed successfully! ===
```

## Advanced Usage

### Custom Transaction IDs

```clojure
(generate-advanced-tx-log 
  "/tmp/custom-log"
  "/tmp/custom-docs"
  [{:tx-ops [[:xtdb.api/put {:xt/id :custom-1}]]
    :valid-time #inst "2024-01-01T10:00:00.000-00:00"
    :system-time #inst "2024-01-01T10:00:00.000-00:00"
    :tx-id 1000}])
```

### Log Inspection

```clojure
(inspect-tx-log "/tmp/xtdb-custom-tx-log")
```

### Read-Only Replay

```clojure
;; Prevent modifications to original log
{:xtdb/tx-log {:xtdb/module 'xtdb.rocksdb/->kv-store
               :db-dir "/tmp/xtdb-custom-tx-log"
               :read-only? true}}
```

## Use Cases

### 🧪 Testing Scenarios
- Create reproducible test datasets
- Test bitemporal query logic
- Validate data migration procedures

### 📊 Data Analysis
- Analyze temporal data patterns
- Study data evolution over time
- Compare different temporal perspectives

### 🔄 System Integration
- Replay production scenarios in development
- Create training datasets
- Benchmark temporal queries

### 🐛 Debugging
- Reproduce temporal bugs
- Isolate specific time periods
- Test edge cases in temporal logic

## Limitations and Notes

### XTDB Version Compatibility
- Designed for XTDB 1.x
- May require modifications for XTDB 2.x
- Internal APIs may change between versions

### System Time Control
- Uses XTDB's transaction submission API
- System time is typically set when transaction is submitted
- For precise control, may need to access internal APIs

### Performance Considerations
- RocksDB backend recommended for local development
- Large transaction logs may require significant disk space
- Replay time depends on transaction volume

## Troubleshooting

### Common Issues

1. **Clojure CLI not found**
   ```bash
   # Install Clojure CLI tools
   # macOS: brew install clojure/tools/clojure
   # Linux: Follow official installation guide
   ```

2. **deps.edn missing**
   ```clojure
   ;; Create deps.edn with XTDB dependencies
   {:deps {com.xtdb/xtdb-core {:mvn/version "1.24.1"}
           com.xtdb/xtdb-rocksdb {:mvn/version "1.24.1"}}}
   ```

3. **Permission errors on /tmp**
   ```clojure
   ;; Change directory paths in the code
   (def tx-log-dir "./local-tx-log")
   ```

4. **RocksDB native library issues**
   ```bash
   # Ensure Java and native libraries are compatible
   java -version
   ```

### Debug Mode

```clojure
;; Enable debug logging
(require '[clojure.tools.logging :as log])
(log/info "Debug information here")
```

## Integration with Existing Solutions

This prototype can be integrated with the existing bitemporal solutions:

### SolutionD Integration
```python
# Generate XTDB log, then import into DuckDB
from core.solutionD import SolutionD

# Use XTDB replay data as source for DuckDB
solution_d = SolutionD()
# ... populate from XTDB replay results
```

### Native Solution Integration
```python
# Use generated logs with xtdb_native_solution.py
from core.xtdb_native_solution import XTDBNativeSolution

# Point to replayed transaction log
native_solution = XTDBNativeSolution()
# ... configure to use replayed data
```

## Next Steps

1. **Extend for XTDB 2.x** - Update for latest XTDB version
2. **Add more backends** - Support Kafka, JDBC transaction logs
3. **Performance optimization** - Batch transaction submission
4. **Integration testing** - Automated test suite
5. **Production deployment** - Docker containerization

## Contributing

To extend this prototype:

1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Submit pull request

## License

This prototype is part of the FYP project and follows the same license terms.