import asyncio
import asyncpg
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional
from uuid import UUID, uuid4

from core.bitemporal_space import Rectangle

class XTDBSolution:
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "xtdb", user: str = "xtdb", password: str = ""):
        self.name = "XTDB v2 Solution (Postgres Wire Protocol)"
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]
        
    async def connect(self):
        """Establish connection to XTDB via Postgres wire protocol"""
        if not self.connection:
            try:
                self.connection = await asyncpg.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                print(f"{self.name}: Connected to XTDB v2 at {self.host}:{self.port}")
            except Exception as e:
                print(f"{self.name}: Warning - Could not connect to XTDB: {e}")
    
    async def cleanup(self):
        """Close database connection"""
        if self.connection:
            await self.connection.close()
            self.connection = None
            print(f"{self.name}: Disconnected from XTDB")
    
    async def initialize_collections(self):
        """Initialize XTDB connection - tables are created dynamically in XTDB v2"""
        await self.connect()
        
        # In XTDB v2, tables are created dynamically during INSERT operations
        # We just need to ensure we have a connection
        print(f"{self.name}: XTDB v2 connection initialized - tables will be created dynamically")
    
    def _generate_vref(self, data: Dict[str, Any]) -> str:
        """Generate version reference from data payload"""
        payload_str = json.dumps(data.get("payload", {}), sort_keys=True)
        return str(UUID(bytes=hashlib.md5(payload_str.encode()).digest()))
    
    def _generate_eref(self, entity_id: Any) -> str:
        """Generate entity reference from entity ID"""
        return str(UUID(bytes=hashlib.md5(str(entity_id).encode()).digest()))
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles into XTDB using SQL with dynamic table creation"""
        await self.connect()
        
        try:
            # In XTDB v2, we insert data directly and tables are created dynamically
            # Each table requires an _id primary key column
            for rect in rectangles:
                # Prepare the complete record with all temporal and payload data
                record_id = f"temporal-{rect.index}-{rect.data.get('id', 0)}"
                
                # Convert datetime objects to proper format
                tt_from = rect.tt_from if isinstance(rect.tt_from, datetime) else datetime.fromisoformat(str(rect.tt_from))
                tt_to = rect.tt_to if isinstance(rect.tt_to, datetime) else datetime.fromisoformat(str(rect.tt_to))
                vt_from = rect.vt_from if isinstance(rect.vt_from, datetime) else datetime.fromisoformat(str(rect.vt_from))
                vt_to = rect.vt_to if isinstance(rect.vt_to, datetime) else datetime.fromisoformat(str(rect.vt_to))
                
                # Insert into temporal_data table (will be created dynamically)
                # XTDB v2 uses standard SQL syntax without PostgreSQL-style parameters
                query = f"""
                INSERT INTO temporal_data 
                (_id, entity_id, eref, vref, tt_from, tt_to, vt_from, vt_to, payload)
                VALUES ('{record_id}', '{str(rect.data.get("id", 0))}', '{self._generate_eref(rect.data.get("id", 0))}', 
                        '{self._generate_vref(rect.data)}', '{tt_from.isoformat()}', '{tt_to.isoformat()}', 
                        '{vt_from.isoformat()}', '{vt_to.isoformat()}', '{json.dumps(rect.data.get("payload", {})).replace("'", "''")}');
                """
                await self.connection.execute(query)
            
            print(f"{self.name}: Inserted {len(rectangles)} rectangles into XTDB v2")
            
        except Exception as e:
            print(f"{self.name}: Exception during insert: {e}")
    
    async def query_by_name_and_age(self, name: str, age: int, tt: datetime, vt: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Query records by name and age at specific valid and transaction times using SQL"""
        await self.connect()
        
        try:
            # SQL query with time constraints on single table
            # Escape single quotes in name to prevent SQL injection
            escaped_name = name.replace("'", "''")
            query = f"""
            SELECT payload
            FROM temporal_data
            WHERE 
                payload LIKE '%"name": "{escaped_name}"%' AND
                payload LIKE '%"age": {age}%' AND
                tt_from <= '{tt.isoformat()}' AND tt_to > '{tt.isoformat()}' AND
                vt_from <= '{vt.isoformat()}' AND vt_to > '{vt.isoformat()}'
            """
            
            results = await self.connection.fetch(query)
            
            # Convert results to dictionaries
            return [json.loads(row["payload"]) for row in results]
            
        except Exception as e:
            print(f"{self.name}: Exception during query: {e}")
            return []
    
    async def get_all_entities(self, entity: str = "Student") -> List[str]:
        """Retrieve all unique entity IDs using SQL"""
        await self.connect()
        
        try:
            # SQL query to get distinct entity IDs
            query = "SELECT DISTINCT entity_id FROM temporal_data"
            
            results = await self.connection.fetch(query)
            return [str(row["entity_id"]) for row in results]
            
        except Exception as e:
            print(f"{self.name}: Exception during query: {e}")
            return []
    
    async def get_current_data(self, entity_id: str, tt: datetime, vt: datetime, entity: str = "Student") -> Optional[Dict[str, Any]]:
        """Retrieve current data for a given entity at specific times using SQL"""
        await self.connect()
        
        try:
            # SQL query to get current data for entity at specific times
            query = f"""
            SELECT payload
            FROM temporal_data
            WHERE 
                entity_id = '{entity_id}' AND
                tt_from <= '{tt.isoformat()}' AND tt_to > '{tt.isoformat()}' AND
                vt_from <= '{vt.isoformat()}' AND vt_to > '{vt.isoformat()}'
            LIMIT 1
            """
            
            result = await self.connection.fetchrow(query)
            
            if result:
                return json.loads(result["payload"])
            return None
            
        except Exception as e:
            print(f"{self.name}: Exception during query: {e}")
            return None
    
    async def get_all_current_data(self, tt: datetime, vt: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Retrieve all current data at specific times using SQL"""
        await self.connect()
        
        try:
            # SQL query to get all current data at specific times
            query = f"""
            SELECT payload
            FROM temporal_data
            WHERE 
                tt_from <= '{tt.isoformat()}' AND tt_to > '{tt.isoformat()}' AND
                vt_from <= '{vt.isoformat()}' AND vt_to > '{vt.isoformat()}'
            """
            
            results = await self.connection.fetch(query)
            
            # Convert results to dictionaries
            return [json.loads(row["payload"]) for row in results]
            
        except Exception as e:
            print(f"{self.name}: Exception during query: {e}")
            return []
    
    async def delete_data(self, entity_id: str, tt: datetime, vt: datetime, entity: str = "Student") -> bool:
        """Logically delete data by updating temporal bounds using SQL"""
        await self.connect()
        
        try:
            # Find current temporal records
            query = f"""
            SELECT _id
            FROM temporal_data
            WHERE 
                entity_id = '{entity_id}' AND
                tt_from <= '{tt.isoformat()}' AND tt_to > '{tt.isoformat()}' AND
                vt_from <= '{vt.isoformat()}' AND vt_to > '{vt.isoformat()}'
            """
            
            record = await self.connection.fetchrow(query)
            
            if not record:
                return False
            
            # Update temporal bounds to implement logical deletion
            update_query = f"""
            UPDATE temporal_data
            SET tt_to = '{tt.isoformat()}'
            WHERE _id = '{record["_id"]}'
            """
            
            await self.connection.execute(update_query)
            
            print(f"{self.name}: Logically deleted data for entity {entity_id}")
            return True
            
        except Exception as e:
            print(f"{self.name}: Exception during delete: {e}")
            return False
    
    async def get_data_history(self, entity_id: str, entity: str = "Student") -> List[Dict[str, Any]]:
        """Retrieve all historical versions of data for a given entity using SQL"""
        await self.connect()
        
        try:
            # SQL query to get all historical versions
            query = f"""
            SELECT 
                payload,
                tt_from,
                tt_to,
                vt_from,
                vt_to
            FROM temporal_data
            WHERE entity_id = '{entity_id}'
            ORDER BY tt_from, vt_from
            """
            
            results = await self.connection.fetch(query)
            
            history = []
            for row in results:
                # Safely handle datetime conversion
                tt_from = row["tt_from"].isoformat() if hasattr(row["tt_from"], 'isoformat') else str(row["tt_from"])
                tt_to = row["tt_to"].isoformat() if hasattr(row["tt_to"], 'isoformat') else str(row["tt_to"])
                vt_from = row["vt_from"].isoformat() if hasattr(row["vt_from"], 'isoformat') else str(row["vt_from"])
                vt_to = row["vt_to"].isoformat() if hasattr(row["vt_to"], 'isoformat') else str(row["vt_to"])
                
                history.append({
                    "data": json.loads(row["payload"]),
                    "tt_from": tt_from,
                    "tt_to": tt_to,
                    "vt_from": vt_from,
                    "vt_to": vt_to
                })
                
            return history
            
        except Exception as e:
            print(f"{self.name}: Exception during query: {e}")
            return []

# Usage example:
async def main():
    solution = XTDBSolution()
    try:
        await solution.initialize_collections()
        # Add your test operations here
    finally:
        await solution.cleanup()

if __name__ == "__main__":
    asyncio.run(main())