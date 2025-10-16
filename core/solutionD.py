import hashlib
import json
import uuid
from typing import Dict, Any, List
import duckdb

# Assuming this is imported from the existing code
from .bitemporal_space import Rectangle

class SolutionD:
    def __init__(self) -> None:
        self.name = "SolutionD"
        self.connection = None
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]
        self.memory_limit = '8GB'
    
    async def connect(self):
        """Connect to DuckDB instance"""
        self.connection = duckdb.connect(f'duckdb_data/{self.name}.db')
        # Enable JSON extension for handling JSON data
        self.connection.execute("INSTALL httpfs; LOAD httpfs;")
        self.connection.execute("INSTALL json; LOAD json;")
        self.connection.execute(f"PRAGMA memory_limit='{self.memory_limit}';")
        print(f"{self.name}: Set memory limit to {self.memory_limit}")

        print(f"{self.name}: Connected to DuckDB database")
    
    async def close(self):
        """Close the DuckDB connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print(f"{self.name}: Disconnected from DuckDB database")
    
    async def initialize_collections(self):
        """Initialize tables with proper schema and indexes"""
        if not self.connection:
            await self.connect()
        
        # Drop existing tables if they exist
        self.connection.execute("DROP TABLE IF EXISTS Index_ts")
        self.connection.execute("DROP TABLE IF EXISTS Index_data")
        # Note: Payload table removed
        
        # Create Index_data table with dynamic columns based on indices,
        # and add a 'data' column to store the full JSON payload.
        create_index_data_sql = """
        CREATE TABLE Index_data (
            vref UUID PRIMARY KEY,
            data JSON
        """
        
        # Add columns dynamically based on self.indices
        for field in self.indices:
            create_index_data_sql += f",\n{field} VARCHAR"
        
        create_index_data_sql += "\n)"
        self.connection.execute(create_index_data_sql)
        
        # Create indexes for efficient querying on Index_data
        for field in self.indices:
            self.connection.execute(f"""
            CREATE INDEX idx_{field}_tt_vt_from ON Index_data ({field})
            """)
        
        # Create Index_ts table
        self.connection.execute("""
        CREATE TABLE Index_ts (
            vref UUID,
            eref UUID,
            tt_from TIMESTAMP,
            tt_to TIMESTAMP,
            vt_from TIMESTAMP,
            vt_to TIMESTAMP,
            entity VARCHAR,
            FOREIGN KEY (vref) REFERENCES Index_data(vref)
        )
        """)
   
    async def initialize_indices(self):
        self.connection.execute("CREATE INDEX idx_tt_from ON Index_ts (tt_from)")
        self.connection.execute("CREATE INDEX idx_tt_to ON Index_ts (tt_to)")
        self.connection.execute("CREATE INDEX idx_vt_from ON Index_ts (vt_from)")
        self.connection.execute("CREATE INDEX idx_vt_to ON Index_ts (vt_to)")
        print(f"{self.name}: Tables initialized with appropriate indexes")

    @staticmethod
    def compute_md5(data_dict: Dict[str, Any]) -> bytes:
        """Compute MD5 hash of a dictionary, ensuring JSON-like string format."""
        data_str = json.dumps(data_dict, sort_keys=True)
        return hashlib.md5(data_str.encode('utf-8')).digest()
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles into tables using batch insertion for atomicity,
        ignoring unique constraint violations."""
        if not self.connection:
            await self.connect()

        # Prepare lists to hold batch parameters
        index_data_values = []
        index_ts_values = []

        for rect in rectangles:
            # Generate UUID for the given payload based on its md5 hash
            data_str = json.dumps(rect.data["payload"], sort_keys=True)
            vref = uuid.UUID(bytes=hashlib.md5(data_str.encode()).digest())
            
            # Generate eref: convert integer ID to UUID
            entity_id = rect.data.get("id", 0)
            # Convert integer to UUID by padding with zeros
            eref = uuid.UUID(int=entity_id)
            
            # Build values for Index_data (vref, data, plus dynamic fields)
            row_values = [vref, json.dumps(rect.data["payload"])]
            for field in self.indices:
                row_values.append(rect.data["payload"].get(field))
            index_data_values.append(tuple(row_values))
            
            # Build values for Index_ts
            index_ts_values.append((
                vref,
                eref,
                rect.tt_from,
                rect.tt_to,
                rect.vt_from,
                rect.vt_to,
                entity
            ))
        
        try:
            # Start a transaction
            self.connection.execute("BEGIN TRANSACTION")
            
            # Insert batch rows into Index_data with conflict handling (ignore duplicates)
            columns = ["vref", "data"] + self.indices
            placeholders = ", ".join(["?"] * len(columns))
            insert_index_data_sql = (
                f"INSERT INTO Index_data ({', '.join(columns)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT DO NOTHING"
            )
            self.connection.executemany(insert_index_data_sql, index_data_values)
            
            # Insert batch rows into Index_ts with conflict handling (if applicable)
            # (If no unique constraint exists on Index_ts then this clause is harmless.)
            insert_index_ts_sql = (
                """
                INSERT INTO Index_ts (vref, eref, tt_from, tt_to, vt_from, vt_to, entity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            )
            self.connection.executemany(insert_index_ts_sql, index_ts_values)
            
            # Commit the transaction
            self.connection.execute("COMMIT")
            print(f"{self.name}: Successfully inserted {len(rectangles)} rectangles in batch")
            
        except Exception as e:
            self.connection.execute("ROLLBACK")
            print(f"{self.name}: Error inserting rectangles in batch: {e}")
            raise

        
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Query data with bitemporal filtering using SQL join."""
        if not self.connection:
            await self.connect()
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # Adjusted query to select from Index_data now that Payload has been removed
        query = """
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE i_data.name = ?
            AND i_data.age = ?
            AND i_ts.entity = ?
            AND i_ts.tt_from <= ?
            AND i_ts.tt_to > ?
            AND i_ts.vt_from <= ?
            AND i_ts.vt_to > ?
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Get query plan for analysis
        explain_query = "EXPLAIN " + query
        explain_result = self.connection.execute(explain_query, (
            name,
            age,
            entity,
            tt,
            tt,
            vt,
            vt
        )).fetchall()
        
        results = self.connection.execute(query, (
            name,
            age,
            entity,
            tt,
            tt,
            vt,
            vt
        )).fetchall()
        
        # Log memory usage information
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Memory Usage: {memory_used:.2f} MB")
        
        # Log query execution info
        self._log_query_stats("SQL Query", explain_result)
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]
    
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Range query records by name and age within VT and TT intervals."""
        if not self.connection:
            await self.connect()
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # SQL query with interval-based time constraints
        query = """
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE i_data.name = ?
            AND i_data.age = ?
            AND i_ts.entity = ?
            AND i_ts.tt_from < ?
            AND i_ts.tt_to > ?
            AND i_ts.vt_from < ?
            AND i_ts.vt_to > ?
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Get query plan for analysis
        explain_query = "EXPLAIN " + query
        explain_result = self.connection.execute(explain_query, (
            name,
            age,
            entity,
            tt_to,
            tt_from,
            vt_to,
            vt_from
        )).fetchall()
        
        results = self.connection.execute(query, (
            name,
            age,
            entity,
            tt_to,
            tt_from,
            vt_to,
            vt_from
        )).fetchall()
        
        # Log memory usage information
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Log query execution info
        self._log_query_stats("SQL Range Query", explain_result)
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]

    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Range query records by a single attribute within VT and TT intervals."""
        if not self.connection:
            await self.connect()
        
        # Track memory usage
        initial_memory = self._get_memory_usage()
        
        # SQL query with interval-based time constraints for a single attribute
        query = f"""
        WITH matched_indices AS (
            SELECT i_ts.vref
            FROM Index_ts i_ts
            JOIN Index_data i_data ON i_ts.vref = i_data.vref
            WHERE i_data.{attribute_name} = ?
            AND i_ts.entity = ?
            AND i_ts.tt_from < ?
            AND i_ts.tt_to > ?
            AND i_ts.vt_from < ?
            AND i_ts.vt_to > ?
        )
        SELECT i_data.data
        FROM Index_data i_data
        JOIN matched_indices m ON i_data.vref = m.vref
        """
        
        # Get query plan for analysis
        explain_query = "EXPLAIN " + query
        explain_result = self.connection.execute(explain_query, (
            attribute_value,
            entity,
            tt_to,
            tt_from,
            vt_to,
            vt_from
        )).fetchall()
        
        results = self.connection.execute(query, (
            attribute_value,
            entity,
            tt_to,
            tt_from,
            vt_to,
            vt_from
        )).fetchall()
        
        # Log memory usage information
        final_memory = self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Log query execution info
        self._log_query_stats(f"SQL {attribute_name} Range Query", explain_result)
        
        # Parse JSON data from results
        return [json.loads(row[0]) for row in results]

    async def delta_since_vt_range(self, name, age, vt_from, vt_to, tt, entity="Student"):
        """Find entities at two VT points (vt_from, tt) and (vt_to, tt).
        Returns a tuple (entity_at_start, entity_at_end) where each can be None if no entity exists."""
        from .timer import Timer
        timer = Timer()
        timer.start()
        
        # Query for entity at (vt_from, tt)
        entity_at_start = await self._query_at_point(name, age, vt_from, tt, entity)
        entity_at_end = await self._query_at_point(name, age, vt_to, tt, entity)
        
        timer.stop()
        memory_usage = self._get_memory_usage()
        
        return {
            "result": (entity_at_start, entity_at_end),
            "execution_time": timer.get_elapsed_time(),
            "memory_usage": memory_usage
        }
    
    async def delta_since_tt_range(self, name, age, tt_from, tt_to, vt, entity="Student"):
        """Find entities at two TT points (vt, tt_from) and (vt, tt_to).
        Returns a tuple (entity_at_start, entity_at_end) where each can be None if no entity exists.
        If entities are identical, returns (None, None)."""
        from .timer import Timer
        timer = Timer()
        timer.start()
        
        # Query for entity at (vt, tt_from) and (vt, tt_to)
        entity_at_start = await self._query_at_point(name, age, vt, tt_from, entity)
        entity_at_end = await self._query_at_point(name, age, vt, tt_to, entity)
        
        # If both entities exist and are identical, return (None, None)
        if entity_at_start and entity_at_end and entity_at_start == entity_at_end:
            entity_at_start = None
            entity_at_end = None
        
        timer.stop()
        memory_usage = self._get_memory_usage()
        
        return {
            "result": (entity_at_start, entity_at_end),
            "execution_time": timer.get_elapsed_time(),
            "memory_usage": memory_usage
        }
    
    async def _query_at_point(self, name, age, vt, tt, entity="Student"):
        """Helper method to query entity at a specific bitemporal point using SQL."""
        if not self.connection:
            await self.connect()
        
        try:
            query = """
            WITH matched_indices AS (
                SELECT i_ts.vref
                FROM Index_ts i_ts
                JOIN Index_data i_data ON i_ts.vref = i_data.vref
                WHERE i_data.name = ?
                AND i_data.age = ?
                AND i_ts.entity = ?
                AND i_ts.tt_from <= ?
                AND i_ts.tt_to > ?
                AND i_ts.vt_from <= ?
                AND i_ts.vt_to > ?
                LIMIT 1
            )
            SELECT i_data.data
            FROM Index_data i_data
            JOIN matched_indices m ON i_data.vref = m.vref
            """
            
            results = self.connection.execute(query, (
                name,
                age,
                entity,
                tt,
                tt,
                vt,
                vt
            )).fetchall()
            
            if results:
                return json.loads(results[0][0])
            return None
            
        except Exception:
            return None

    def _get_memory_usage(self):
        """Get current memory usage from DuckDB."""
        try:
            result = self.connection.execute("PRAGMA memory_usage").fetchone()
            # Convert bytes to MB
            return result[0] / (1024 * 1024) if result else 0
        except Exception:
            return 0
    
    def _log_query_stats(self, query_name, explain_result):
        """Log query execution statistics."""
        try:
            if explain_result:
                print(f"{self.name} {query_name} Plan: {len(explain_result)} steps")
                # Log first few steps of the execution plan
                for i, step in enumerate(explain_result[:3]):
                    print(f"{self.name} Step {i+1}: {step[0]}")
        except Exception:
            pass
