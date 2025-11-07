import asyncpg
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from .bitemporal_space import Rectangle

class XTDBHistoricalSolution:
    """XTDB v2 solution using valid time for historical data simulation"""
    
    def __init__(self, host: str = "localhost", port: int = 5433, 
                 database: str = "xtdb", user: str = "xtdb", password: str = "xtdb"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.name = "XTDB Historical (v2)"
        self.connection = None
    
    async def connect(self):
        """Connect to XTDB v2 via PostgreSQL wire protocol"""
        try:
            self.connection = await asyncpg.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print(f"{self.name}: Connected to XTDB v2")
            
        except Exception as e:
            print(f"{self.name}: Connection failed: {e}")
            raise
    
    async def cleanup(self):
        """Close connection to XTDB"""
        try:
            if self.connection:
                await self.connection.close()
                self.connection = None
                
            print(f"{self.name}: Disconnected from XTDB")
            
        except Exception as e:
            print(f"{self.name}: Cleanup error: {e}")
    
    async def initialize_collections(self):
        """Initialize collections (XTDB v2 creates tables automatically)"""
        # Ensure we have a live connection
        if not self.connection:
            await self.connect()
        print(f"{self.name}: XTDB v2 initialized - tables created automatically")
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles using XTDB v2 INSERT RECORDS with valid time control"""
        if not rectangles:
            return
        
        # Ensure connection before inserting
        if not self.connection:
            await self.connect()
        
        try:
            # Insert each rectangle using XTDB v2 SQL syntax
            for rect in rectangles:
                await self._insert_rectangle_v2(rect, entity)
            
            print(f"{self.name}: Inserted {len(rectangles)} rectangles using XTDB v2")
            
        except Exception as e:
            print(f"{self.name}: Error inserting rectangles: {e}")
            raise
    
    async def _insert_rectangle_v2(self, rect: Rectangle, entity: str):
        """Insert a single rectangle using XTDB v2 INSERT RECORDS syntax"""
        try:
            # Extract data from rectangle
            payload = rect.data.get("payload", {})
            entity_id = rect.data.get("id", rect.index)
            
            # Create document ID
            doc_id = f"student-{entity_id}-{rect.index}"
            name = payload.get("name", "")
            age = payload.get("age", 0)
            attr1 = payload.get("attr1", 0)
            attr2 = payload.get("attr2", 0)
            attr3 = payload.get("attr3", 0)
            attr4 = payload.get("attr4", 0)
            
            # Format valid time
            vt_from = rect.vt_from.isoformat()
            vt_to = rect.vt_to.isoformat() if rect.vt_to else None
            
            # Build INSERT INTO RECORDS query with valid time (XTDB v2 syntax)
            if vt_to:
                query = f"""
                INSERT INTO student RECORDS
                {{
                    _id: '{doc_id}',
                    entity_id: '{entity_id}',
                    name: '{name}',
                    age: {age},
                    attr1: {attr1},
                    attr2: {attr2},
                    attr3: {attr3},
                    attr4: {attr4},
                    _valid_from: '{vt_from}',
                    _valid_to: '{vt_to}'
                }}
                """
            else:
                query = f"""
                INSERT INTO student RECORDS
                {{
                    _id: '{doc_id}',
                    entity_id: '{entity_id}',
                    name: '{name}',
                    age: {age},
                    attr1: {attr1},
                    attr2: {attr2},
                    attr3: {attr3},
                    attr4: {attr4},
                    _valid_from: '{vt_from}',
                    _valid_to: '9999-12-31T23:59:59Z'
                }}
                """
            
            # Execute the query
            await self.connection.execute(query)
            
        except Exception as e:
            print(f"{self.name}: Error inserting rectangle: {e}")
            raise
    
    async def get_all_entities(self, tt: datetime, vt: datetime, entity: str = "Student") -> List[str]:
        """Get all entity IDs at specific valid time using XTDB v2 SQL"""
        try:
            # Ensure connection
            if not self.connection:
                await self.connect()
            
            # Format the valid time for the query
            vt_str = vt.isoformat()
            
            # Query using XTDB v2 SQL syntax with FOR VALID_TIME
            rows = await self.connection.fetch("""
                SELECT DISTINCT entity_id 
                FROM student 
                FOR VALID_TIME AS OF $1
                WHERE entity_id IS NOT NULL
            """, vt_str)
            
            entity_ids = [row['entity_id'] for row in rows]
            
            print(f"{self.name}: Found {len(entity_ids)} entities at valid time {vt}")
            return entity_ids
            
        except Exception as e:
            print(f"{self.name}: Error querying entities: {e}")
            return []
    
    async def query_point(self, name: str, age: int, system_time: datetime, valid_time: datetime) -> List[Dict[str, Any]]:
        """Query for specific name and age at given times using XTDB v2 SQL"""
        try:
            # Ensure connection
            if not self.connection:
                await self.connect()
            
            # Format the valid time for the query
            vt_str = valid_time.isoformat()
            
            # Query using XTDB v2 SQL syntax with FOR VALID_TIME
            rows = await self.connection.fetch("""
                 SELECT _id, entity_id, name, age, attr1, attr2, attr3, attr4, _valid_from, _valid_to
                 FROM student 
                 FOR VALID_TIME AS OF $1
                 WHERE name = $2 AND age = $3
             """, vt_str, name, str(age))
            
            # Convert rows to dictionaries
            results = []
            for row in rows:
                result = {
                    'id': row['_id'],
                    'entity_id': row['entity_id'],
                    'name': row['name'],
                    'age': row['age'],
                    'attr1': row['attr1'],
                    'attr2': row['attr2'],
                    'attr3': row['attr3'],
                    'attr4': row['attr4'],
                    'valid_from': row['_valid_from'],
                    'valid_to': row['_valid_to']
                }
                results.append(result)
            
            print(f"{self.name}: Found {len(results)} records for {name}, age {age} at valid time {valid_time}")
            return results
            
        except Exception as e:
            print(f"{self.name}: Error in point query: {e}")
            return []