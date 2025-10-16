import asyncio
import asyncpg
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from .bitemporal_space import Rectangle

class XTDBSolution:
    def __init__(self, host: str = "localhost", port: int = 5433, 
                 database: str = "xtdb", user: str = "xtdb", password: str = "xtdb"):
        self.name = "XTDB v2 Solution (Postgres Wire Protocol)"
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        
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
        """Get current memory usage in MB (placeholder for XTDB)"""
        # XTDB doesn't expose memory usage directly like MongoDB
        return 0.0
    
    async def _log_query_stats(self, operation: str, result_count: int, execution_time: float, memory_used: float):
        """Log query statistics"""
        print(f"{self.name} - {operation}: {result_count} results, {execution_time:.3f}s, {memory_used:.2f}MB")
    
    async def initialize_collections(self):
        """Initialize XTDB connection via Postgres wire protocol"""
        await self.connect()
        
        # XTDB v2 doesn't require explicit table creation - it's schema-on-write
        # Just verify connection works
        try:
            # Test connection with a simple query
            await self.connection.fetch("SELECT 1")
            print(f"{self.name}: XTDB v2 connection initialized via Postgres wire protocol")
            print(f"{self.name}: Schema-on-write enabled - tables created automatically")
        except Exception as e:
            print(f"{self.name}: Warning - Connection test failed: {e}")
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles into XTDB with bitemporal support (legacy method name)"""
        if not rectangles:
            return
        
        # Use current time for both transaction and valid time
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        
        success = await self.insert_data(rectangles, now, now, entity)
        if not success:
            print(f"{self.name}: Failed to insert rectangles")
    
    async def insert_data(self, rectangles: List[Rectangle], tt: datetime, vt: datetime, entity: str = "Student") -> bool:
        """Insert rectangle data into XTDB with bitemporal support using RECORDS syntax"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            # Use XTDB v2's INSERT RECORDS syntax for bitemporal insertion
            for rect in rectangles:
                # Build the RECORDS query with literal values
                query = f"""
                    INSERT INTO student RECORDS
                    {{
                        _id: '{rect.index}',
                        entity_id: '{rect.index}',
                        name: 'Student_{rect.index}',
                        age: {rect.data.get('age', 25)},
                        attr1: {rect.data.get('attr1', 0)},
                        attr2: {rect.data.get('attr2', 0)},
                        attr3: {rect.data.get('attr3', 0)},
                        attr4: {rect.index},
                        _valid_from: '{vt.isoformat()}',
                        _valid_to: '9999-12-31T23:59:59Z'
                    }}
                """
                
                await self.connection.execute(query)
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("INSERT", len(rectangles), execution_time, memory_after - memory_before)
            
            return True
            
        except Exception as e:
            print(f"{self.name}: Error inserting data: {e}")
            return False
    
    async def update_data(self, entity_id: str, updates: Dict[str, Any], tt: datetime, vt: datetime, entity: str = "Student") -> bool:
        """Update data in XTDB with bitemporal support"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            # Build SET clause
            set_clauses = []
            values = []
            for key, value in updates.items():
                set_clauses.append(f"{key} = ${len(values) + 1}")
                values.append(value)
            
            # Add entity_id, tt, vt to values
            values.extend([entity_id, tt, vt])
            
            query = f"""
                UPDATE student 
                SET {', '.join(set_clauses)}
                WHERE entity_id = ${len(values) - 2}
                SETTING SYSTEM_TIME TO ${len(values) - 1},
                        VALID_TIME TO ${len(values)}
            """
            
            result = await self.connection.execute(query, *values)
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("UPDATE", 1, execution_time, memory_after - memory_before)
            
            return True
            
        except Exception as e:
            print(f"{self.name}: Error updating data: {e}")
            return False
    
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
            """, entity_id, tt, vt)
            
            if row:
                result = dict(row)
            else:
                result = {}
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("GET_CURRENT_DATA", 1 if row else 0, execution_time, memory_after - memory_before)
            
            return result
            
        except Exception as e:
            print(f"{self.name}: Error getting current data: {e}")
            return {}
    
    async def get_all_current_data(self, tt: datetime, vt: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Get all current data at specific times"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            rows = await self.connection.fetch("""
                SELECT * FROM student 
                FOR SYSTEM_TIME AS OF $1
                FOR VALID_TIME AS OF $2
            """, tt, vt)
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("GET_ALL_CURRENT_DATA", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error getting all current data: {e}")
            return []
    
    async def query_by_name_and_age_range(self, name: str, age: int, tt_from: datetime, tt_to: datetime, vt_from: datetime, vt_to: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Query by name and age with time ranges"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            rows = await self.connection.fetch("""
                SELECT * FROM student 
                WHERE name = $1 AND age >= $2
                FOR SYSTEM_TIME FROM $3 TO $4
                FOR VALID_TIME FROM $5 TO $6
            """, name, age, tt_from, tt_to, vt_from, vt_to)
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("QUERY_BY_NAME_AGE_RANGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error querying by name and age range: {e}")
            return []
    
    async def delta_since_vt_range(self, vt_from: datetime, vt_to: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Get changes in valid time range"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            rows = await self.connection.fetch("""
                SELECT * FROM student 
                FOR ALL VALID_TIME
                WHERE _valid_from >= $1 AND _valid_from <= $2
            """, vt_from, vt_to)
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("DELTA_SINCE_VT_RANGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error getting delta since VT range: {e}")
            return []
    
    async def delta_since_tt_range(self, tt_from: datetime, tt_to: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Get changes in transaction time range"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            rows = await self.connection.fetch("""
                SELECT * FROM student 
                FOR ALL SYSTEM_TIME
                WHERE _system_from >= $1 AND _system_from <= $2
            """, tt_from, tt_to)
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("DELTA_SINCE_TT_RANGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error getting delta since TT range: {e}")
            return []
    
    async def delete_data(self, entity_id: str, tt: datetime, vt: datetime, entity: str = "Student") -> bool:
        """Delete data with bitemporal support"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            await self.connection.execute("""
                DELETE FROM student 
                WHERE entity_id = $1
                SETTING SYSTEM_TIME TO $2,
                        VALID_TIME TO $3
            """, entity_id, tt, vt)
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("DELETE", 1, execution_time, memory_after - memory_before)
            
            return True
            
        except Exception as e:
            print(f"{self.name}: Error deleting data: {e}")
            return False
    
    async def get_data_history(self, entity_id: str, entity: str = "Student") -> List[Dict[str, Any]]:
        """Get full history of an entity"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            rows = await self.connection.fetch("""
                SELECT *, _system_from, _system_to, _valid_from, _valid_to
                FROM student 
                FOR ALL SYSTEM_TIME
                FOR ALL VALID_TIME
                WHERE entity_id = $1
                ORDER BY _system_from, _valid_from
            """, entity_id)
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("GET_DATA_HISTORY", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error getting data history: {e}")
            return []

async def main():
    """Test XTDB v2 solution"""
    solution = XTDBSolution()
    await solution.initialize_collections()
    
    # Test basic operations
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    
    # Create test rectangles
    rectangles = [
        Rectangle(1, 10, 20, 30, 40),
        Rectangle(2, 15, 25, 35, 45)
    ]
    
    # Insert data
    await solution.insert_data(rectangles, now, now)
    
    # Query data
    entities = await solution.get_all_entities(now, now)
    print(f"Entities: {entities}")
    
    await solution.cleanup()

if __name__ == "__main__":
    asyncio.run(main())