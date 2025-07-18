import hashlib
import json
import uuid
from typing import Dict, Any, List
from collections import defaultdict
import duckdb
import copy

from .bitemporal_space import Rectangle
from .utils.timing import Timer
from .utils.constants import DATETIME_MAX

class Solution5:
    def __init__(self) -> None:
        self.name = "Solution5"
        self.connection = None
        self.memory_limit = '4GB'
        self.collections = ["Name", "Age", "Attr1", "Attr2", "Attr3", "Attr4"]

    async def connect(self):
        """Connect to DuckDB instance"""
        self.connection = duckdb.connect(database='duckdb_data/solution5.db')
        self.connection.execute("INSTALL httpfs; LOAD httpfs;")
        self.connection.execute("INSTALL json; LOAD json;")
        self.connection.execute(f"SET memory_limit='{self.memory_limit}';")
        print(f"{self.name}: Set memory limit to {self.memory_limit}")
        print(f"{self.name}: Connected to DuckDB database")

    async def close(self):
        """Close the DuckDB connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print(f"{self.name}: Disconnected from DuckDB database")

    async def initialize_collections(self):
        """Initialize tables with proper schema and indexes - mimicking Solution4's collection structure"""
        if not self.connection:
            await self.connect()
        
        # Drop existing tables if they exist
        for collection in self.collections:
            self.connection.execute(f"DROP TABLE IF EXISTS {collection}")
        
        # Create tables for each collection (similar to Solution4's MongoDB collections)
        for collection in self.collections:
            create_table_sql = f"""
            CREATE TABLE {collection} (
                eref VARCHAR,
                value VARCHAR,
                tt_from TIMESTAMP,
                tt_to TIMESTAMP,
                vt_from TIMESTAMP,
                vt_to TIMESTAMP,
                entity VARCHAR
            )
            """
            self.connection.execute(create_table_sql)
            
            # Create indexes similar to Solution4's MongoDB indexes
            self.connection.execute(f"CREATE INDEX idx_{collection}_value_tt_vt ON {collection} (value, tt_from, vt_from)")
            self.connection.execute(f"CREATE INDEX idx_{collection}_value_tt_to_vt_to ON {collection} (value, tt_to, vt_to)")
            self.connection.execute(f"CREATE INDEX idx_{collection}_eref ON {collection} (eref)")
        
        print(f"{self.name}: Tables initialized with appropriate indexes")

    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles into tables using the same logic as Solution4"""
        if not self.connection:
            await self.connect()

        records_map = defaultdict(list)

        latest_vt = DATETIME_MAX
        for rect in rectangles:
            eref = str(rect.data.get("id", 0))
            for collection in self.collections:
                value = rect.data["payload"].get(collection.lower())
                if value is None:
                    continue
                    
                # Convert value to string for consistency
                value = str(value)
                
                # Check if we can extend the previous record (same value)
                if len(records_map[collection]) > 0 and records_map[collection][-1]["value"] == value:
                    if rect.tt_to != DATETIME_MAX:
                        records_map[collection][-1]["tt_to"] = rect.tt_to
                    if rect.vt_to != DATETIME_MAX:
                        latest_vt = rect.vt_to
                    continue

                # Create vt_fixed_record if there's a previous record
                if len(records_map[collection]) > 0:
                    vt_fixed_record = copy.deepcopy(records_map[collection][-1])
                    vt_fixed_record["tt_from"] = vt_fixed_record["tt_to"]
                    vt_fixed_record["tt_to"] = DATETIME_MAX
                    vt_fixed_record["vt_to"] = latest_vt

                    records_map[collection].append(vt_fixed_record)
                
                # Create new tt_fixed_record
                tt_fixed_record = {
                    "eref": eref,
                    "value": value,
                    "tt_from": rect.tt_from,
                    "tt_to": rect.tt_to,
                    "vt_from": rect.vt_from,
                    "vt_to": rect.vt_to,
                    "entity": entity,
                }
                
                records_map[collection].append(tt_fixed_record)
        
        # Insert records into each table
        for collection in self.collections:
            if records_map[collection]:
                try:
                    self.connection.execute("BEGIN TRANSACTION")
                    
                    insert_sql = f"""
                    INSERT INTO {collection} (eref, value, tt_from, tt_to, vt_from, vt_to, entity)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    records_to_insert = [
                        (
                            record["eref"],
                            record["value"],
                            record["tt_from"],
                            record["tt_to"],
                            record["vt_from"],
                            record["vt_to"],
                            record["entity"]
                        )
                        for record in records_map[collection]
                    ]
                    
                    self.connection.executemany(insert_sql, records_to_insert)
                    self.connection.execute("COMMIT")
                    print(f"{self.name}: Inserted {len(records_map[collection])} documents into {collection} table")
                    
                except Exception as e:
                    self.connection.execute("ROLLBACK")
                    print(f"Some payload inserts failed for {collection}: {e}")
                    raise

    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Query data with bitemporal filtering using SQL join - equivalent to Solution4's aggregation pipeline"""
        if not self.connection:
            await self.connect()
        
        # Convert inputs to strings for consistency
        name = str(name)
        age = str(age)
        
        # SQL equivalent of Solution4's MongoDB aggregation pipeline
        query = """
        SELECT 
            n.eref AS id,
            n.value AS name,
            a.value AS age,
            GREATEST(n.tt_from, a.tt_from) AS tt_from,
            LEAST(n.tt_to, a.tt_to) AS tt_to,
            GREATEST(n.vt_from, a.vt_from) AS vt_from,
            LEAST(n.vt_to, a.vt_to) AS vt_to
        FROM Name n
        JOIN Age a ON n.eref = a.eref AND n.entity = a.entity
        WHERE n.value = ? 
        AND a.value = ? 
        AND n.entity = ?
        AND n.tt_from <= ? AND n.tt_to > ?
        AND n.vt_from <= ? AND n.vt_to > ?
        AND a.tt_from <= ? AND a.tt_to > ?
        AND a.vt_from <= ? AND a.vt_to > ?
        AND n.tt_from < LEAST(n.tt_to, a.tt_to)
        AND n.vt_from < LEAST(n.vt_to, a.vt_to)
        AND a.tt_from < LEAST(n.tt_to, a.tt_to)
        AND a.vt_from < LEAST(n.vt_to, a.vt_to)
        """
        
        results = self.connection.execute(query, (
            name, age, entity, 
            tt, tt, vt, vt,  # for Name table conditions
            tt, tt, vt, vt   # for Age table conditions
        )).fetchall()
        
        # Convert results to list of dictionaries matching Solution4's format
        return [
            {
                "id": row[0],
                "name": row[1],
                "age": row[2],
                "tt_from": row[3],
                "tt_to": row[4],
                "vt_from": row[5],
                "vt_to": row[6]
            }
            for row in results
        ]

    async def get_all_entities(self, entity: str = "Student") -> List[str]:
        """Retrieve all unique entity IDs."""
        if not self.connection:
            await self.connect()

        # Use any table to get distinct entity references
        query = """
        SELECT DISTINCT eref
        FROM Name
        WHERE entity = ?
        """

        results = self.connection.execute(query, (entity,)).fetchall()
        return [row[0] for row in results]

    async def get_current_data(self, entity_id: str, tt: int, vt: int, entity: str = "Student") -> Dict[str, Any]:
        """Retrieve the current data for a given entity at a specific true and valid time."""
        if not self.connection:
            await self.connect()

        entity_id = str(entity_id)
        result_data = {}
        
        # Query each collection table to build the complete entity data
        for collection in self.collections:
            query = f"""
            SELECT value
            FROM {collection}
            WHERE eref = ?
            AND entity = ?
            AND tt_from <= ?
            AND tt_to > ?
            AND vt_from <= ?
            AND vt_to > ?
            ORDER BY tt_from DESC, vt_from DESC
            LIMIT 1
            """
            
            result = self.connection.execute(query, (
                entity_id, entity, tt, tt, vt, vt
            )).fetchone()
            
            if result:
                result_data[collection.lower()] = result[0]
        
        return result_data if result_data else None

    async def get_data_history(self, entity_id: str, entity: str = "Student") -> List[Dict[str, Any]]:
        """Retrieve all historical versions of data for a given entity."""
        if not self.connection:
            await self.connect()

        entity_id = str(entity_id)
        
        # Get all time periods from all collections for this entity
        time_periods = set()
        for collection in self.collections:
            query = f"""
            SELECT tt_from, tt_to, vt_from, vt_to
            FROM {collection}
            WHERE eref = ? AND entity = ?
            """
            results = self.connection.execute(query, (entity_id, entity)).fetchall()
            for row in results:
                time_periods.add((row[0], row[1], row[2], row[3]))
        
        # For each time period, get the data
        history = []
        for tt_from, tt_to, vt_from, vt_to in sorted(time_periods):
            data = {}
            for collection in self.collections:
                query = f"""
                SELECT value
                FROM {collection}
                WHERE eref = ?
                AND entity = ?
                AND tt_from <= ?
                AND tt_to > ?
                AND vt_from <= ?
                AND vt_to > ?
                LIMIT 1
                """
                result = self.connection.execute(query, (
                    entity_id, entity, tt_from, tt_from, vt_from, vt_from
                )).fetchone()
                
                if result:
                    data[collection.lower()] = result[0]
            
            if data:
                history.append({
                    "data": data,
                    "tt_from": tt_from,
                    "tt_to": tt_to,
                    "vt_from": vt_from,
                    "vt_to": vt_to
                })
        
        return history

    async def get_all_current_data(self, tt: int, vt: int, entity: str = "Student") -> List[Dict[str, Any]]:
        """Retrieve all current data at a specific true and valid time."""
        if not self.connection:
            await self.connect()

        # Get all entity IDs first
        entities = await self.get_all_entities(entity)
        
        results = []
        for entity_id in entities:
            data = await self.get_current_data(entity_id, tt, vt, entity)
            if data:
                data["id"] = entity_id
                results.append(data)
        
        return results

    async def delete_data(self, entity_id: str, tt: int, vt: int, entity: str = "Student") -> None:
        """Logically delete data by setting tt_to and vt_to to the current timestamp."""
        if not self.connection:
            await self.connect()

        entity_id = str(entity_id)
        
        # Update all collections for this entity
        for collection in self.collections:
            update_query = f"""
            UPDATE {collection}
            SET tt_to = ?, vt_to = ?
            WHERE eref = ?
            AND entity = ?
            AND tt_from <= ?
            AND tt_to > ?
            AND vt_from <= ?
            AND vt_to > ?
            """

            self.connection.execute(update_query, (
                tt, vt, entity_id, entity, tt, tt, vt, vt
            ))
        
        print(f"{self.name}: Logically deleted data for entity {entity_id} at tt={tt}, vt={vt}")

    async def query_bitemporal_data(self, query_params: Dict[str, Any], tt: int, vt: int, entity: str = "Student") -> List[Dict[str, Any]]:
        """Query data with bitemporal filtering and dynamic attribute matching."""
        if not self.connection:
            await self.connect()

        # Build a complex query that joins multiple tables based on query_params
        if not query_params:
            return await self.get_all_current_data(tt, vt, entity)
        
        # Start with the first query parameter
        first_key = list(query_params.keys())[0]
        first_value = str(query_params[first_key])
        
        # Find the corresponding collection name
        collection_name = None
        for collection in self.collections:
            if collection.lower() == first_key.lower():
                collection_name = collection
                break
        
        if not collection_name:
            return []
        
        # Build the base query
        query = f"""
        SELECT DISTINCT t1.eref
        FROM {collection_name} t1
        WHERE t1.value = ?
        AND t1.entity = ?
        AND t1.tt_from <= ?
        AND t1.tt_to > ?
        AND t1.vt_from <= ?
        AND t1.vt_to > ?
        """
        
        query_args = [first_value, entity, tt, tt, vt, vt]
        
        # Add joins for additional query parameters
        join_count = 2
        for key, value in list(query_params.items())[1:]:
            collection_name = None
            for collection in self.collections:
                if collection.lower() == key.lower():
                    collection_name = collection
                    break
            
            if collection_name:
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM {collection_name} t{join_count}
                    WHERE t{join_count}.eref = t1.eref
                    AND t{join_count}.entity = t1.entity
                    AND t{join_count}.value = ?
                    AND t{join_count}.tt_from <= ?
                    AND t{join_count}.tt_to > ?
                    AND t{join_count}.vt_from <= ?
                    AND t{join_count}.vt_to > ?
                )
                """
                query_args.extend([str(value), tt, tt, vt, vt])
                join_count += 1
        
        # Execute query to get matching entity IDs
        results = self.connection.execute(query, tuple(query_args)).fetchall()
        entity_ids = [row[0] for row in results]
        
        # Get full data for each matching entity
        full_results = []
        for entity_id in entity_ids:
            data = await self.get_current_data(entity_id, tt, vt, entity)
            if data:
                data["id"] = entity_id
                full_results.append(data)
        
        return full_results