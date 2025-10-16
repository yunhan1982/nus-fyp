import asyncio
import asyncpg
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from .bitemporal_space import Rectangle

class XTDBNativeSolution:
    """XTDB Solution using SQL with custom system timestamps"""
    
    def __init__(self, host: str = "localhost", port: int = 5433, 
                 database: str = "xtdb", user: str = "xtdb", password: str = "xtdb"):
        self.name = "XTDB Native Solution (SQL with Custom Timestamps)"
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]
        
    async def connect(self):
        """Connect to XTDB v2 via Postgres wire protocol"""
        if not self.connection:
            try:
                self.connection = await asyncpg.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                print(f"{self.name}: Connected to XTDB v2 via Postgres wire protocol at {self.host}:{self.port}")
            except Exception as e:
                print(f"{self.name}: Warning - Could not connect to XTDB: {e}")
    
    async def cleanup(self):
        """Close XTDB connection and cleanup resources"""
        if self.connection:
            await self.connection.close()
            self.connection = None
            print(f"{self.name}: Disconnected from XTDB")
    
    async def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    async def _log_query_stats(self, operation: str, result_count: int, execution_time: float, memory_used: float):
        """Log query statistics"""
        print(f"{self.name} - {operation}: {result_count} results, {execution_time:.3f}s, {memory_used:.1f}MB")
    
    async def initialize_collections(self):
        """Initialize XTDB collections (tables) if needed"""
        # XTDB v2 doesn't require explicit collection initialization
        pass
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles with custom system timestamps using SQL"""
        if not self.connection:
            await self.connect()
        
        try:
            # Group rectangles by their transaction time (system time) for batch insertion
            system_time_groups = {}
            for rect in rectangles:
                system_time = rect.tt_from  # Use transaction time as system time
                if system_time not in system_time_groups:
                    system_time_groups[system_time] = []
                system_time_groups[system_time].append(rect)
            
            # Insert each group with its specific system time
            for system_time, rect_group in system_time_groups.items():
                await self._insert_batch_with_system_time(rect_group, system_time, entity)
                
        except Exception as e:
            print(f"{self.name}: Error inserting rectangles: {e}")
            raise
    
    async def _insert_batch_with_system_time(self, rectangles: List[Rectangle], system_time: datetime, entity: str):
        """Insert a batch of rectangles using XTDB v2 RECORDS syntax"""
        try:
            # Insert each rectangle using XTDB v2's INSERT RECORDS syntax
            for rect in rectangles:
                # Extract data from rectangle's payload
                payload = rect.data.get("payload", {})
                entity_id = rect.data.get("id", rect.index)
                
                # Build the RECORDS query with literal values
                query = f"""
                    INSERT INTO {entity.lower()} RECORDS
                    {{
                        _id: '{entity_id}_{rect.index}',
                        entity_id: '{entity_id}',
                        name: '{payload.get("name", "")}',
                        age: {payload.get("age", 0)},
                        attr1: {payload.get("attr1", 0)},
                        attr2: {payload.get("attr2", 0)},
                        attr3: {payload.get("attr3", 0)},
                        attr4: {payload.get("attr4", 0)},
                        _valid_from: '{rect.vt_from.isoformat()}',
                        _valid_to: '{rect.vt_to.isoformat() if rect.vt_to else "9999-12-31T23:59:59Z"}'
                    }}
                """
                
                await self.connection.execute(query)
                    
        except Exception as e:
            print(f"{self.name}: Error in batch insert: {e}")
            raise
    
    async def get_all_entities(self, tt: datetime, vt: datetime, entity: str = "Student") -> List[str]:
        """Get all entity IDs at specific transaction and valid times"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            # Query with bitemporal constraints using ISO format strings
            rows = await self.connection.fetch("""
                SELECT DISTINCT entity_id 
                FROM student 
                FOR SYSTEM_TIME AS OF $1
                FOR VALID_TIME AS OF $2
            """, tt.isoformat(), vt.isoformat())
            
            entity_ids = [row['entity_id'] for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("GET_ALL_ENTITIES", len(entity_ids), execution_time, memory_after - memory_before)
            
            return entity_ids
            
        except Exception as e:
            print(f"{self.name}: Error getting entities: {e}")
            return []
    
    async def get_current_data(self, entity_id: str, tt: datetime, vt: datetime, entity: str = "Student") -> Dict[str, Any]:
        """Get current data for a specific entity at given times"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            row = await self.connection.fetchrow("""
                SELECT * FROM student 
                WHERE entity_id = $1
                FOR SYSTEM_TIME AS OF $2
                FOR VALID_TIME AS OF $3
                ORDER BY _system_from DESC
                LIMIT 1
            """, entity_id, tt.isoformat(), vt.isoformat())
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("GET_CURRENT_DATA", 1 if row else 0, execution_time, memory_after - memory_before)
            
            return dict(row) if row else {}
            
        except Exception as e:
            print(f"{self.name}: Error getting current data: {e}")
            return {}
    
    async def query_point(self, name: str, age: int, system_time: datetime, valid_time: datetime) -> List[Dict[str, Any]]:
        """Query for a specific point in bitemporal space"""
        if not self.connection:
            await self.connect()
        
        start_time = datetime.now()
        memory_before = await self._get_memory_usage()
        
        try:
            # Query with both system time and valid time constraints
            query = """
            SELECT * FROM xt.txs 
            FOR SYSTEM_TIME AS OF $1
            WHERE name = $2 AND age = $3 
            AND _valid_from <= $4 AND (_valid_to IS NULL OR _valid_to > $4)
            """
            
            rows = await self.connection.fetch(query, system_time, name, age, valid_time)
            results = [dict(row) for row in rows]
            
            execution_time = (datetime.now() - start_time).total_seconds()
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("Point Query", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error in point query: {e}")
            return []
    
    async def query_range_by_name_and_age(self, name_range: tuple, age_range: tuple, 
                                        system_time: datetime, valid_time: datetime) -> List[Dict[str, Any]]:
        """Query for a range in bitemporal space by name and age"""
        if not self.connection:
            await self.connect()
        
        start_time = datetime.now()
        memory_before = await self._get_memory_usage()
        
        try:
            query = """
            SELECT * FROM xt.txs 
            FOR SYSTEM_TIME AS OF $1
            WHERE name BETWEEN $2 AND $3 
            AND age BETWEEN $4 AND $5
            AND _valid_from <= $6 AND (_valid_to IS NULL OR _valid_to > $6)
            """
            
            rows = await self.connection.fetch(
                query, system_time, name_range[0], name_range[1], 
                age_range[0], age_range[1], valid_time
            )
            results = [dict(row) for row in rows]
            
            execution_time = (datetime.now() - start_time).total_seconds()
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("Range Query (Name+Age)", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error in range query: {e}")
            return []
    
    async def query_range_by_attribute(self, attr_name: str, attr_range: tuple, 
                                     system_time: datetime, valid_time: datetime) -> List[Dict[str, Any]]:
        """Query for a range in bitemporal space by attribute"""
        if not self.connection:
            await self.connect()
        
        start_time = datetime.now()
        memory_before = await self._get_memory_usage()
        
        try:
            # Use dynamic SQL for attribute queries
            query = f"""
            SELECT * FROM xt.txs 
            FOR SYSTEM_TIME AS OF $1
            WHERE {attr_name} BETWEEN $2 AND $3
            AND _valid_from <= $4 AND (_valid_to IS NULL OR _valid_to > $4)
            """
            
            rows = await self.connection.fetch(
                query, system_time, attr_range[0], attr_range[1], valid_time
            )
            results = [dict(row) for row in rows]
            
            execution_time = (datetime.now() - start_time).total_seconds()
            memory_after = await self._get_memory_usage()
            await self._log_query_stats(f"Range Query ({attr_name})", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error in attribute range query: {e}")
            return []