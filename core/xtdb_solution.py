import asyncio
import psycopg as pg
from datetime import datetime
from typing import List
from uuid import UUID
import hashlib

from .bitemporal_space import Rectangle

class XTDBSolution:
    def __init__(self, 
                 db_params: dict = {
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "bitemporal",
                    "user": "postgres",
                    "password": "postgres"
    }):
        self.name = "XTDBSolution"
        self.db_params = db_params or {
            "host": "localhost",
            "port": 5432,
            "dbname": "bitemporal",
            "user": "postgres",
            "password": "postgres"
        }
        self.conn = None

    async def connect(self):
        """Establish database connection"""
        if not self.conn:
            self.conn = await pg.AsyncConnection.connect(**self.db_params, autocommit=True)
            self.conn.adapters.register_dumper(str, pg.types.string.StrDumperVarchar)

    async def cleanup(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()

    async def initialize_collections(self):
        """Set up necessary tables and indexes"""
        await self.connect()
        
        async with self.conn.cursor() as cur:
            # Create Index_ts table
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS Index_ts (
                    vref VARCHAR,
                    eref VARCHAR,
                    tt_from TIMESTAMP,
                    tt_to TIMESTAMP,
                    vt_from TIMESTAMP,
                    vt_to TIMESTAMP
                )
            """)
            
            # Create Index_data table
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS Index_data (
                    vref VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    age INTEGER
                )
            """)
            
            # Create indexes
            await cur.execute("CREATE INDEX IF NOT EXISTS idx_ts_vref ON Index_ts (vref)")
            await cur.execute("CREATE INDEX IF NOT EXISTS idx_ts_tt ON Index_ts (tt_from, tt_to)")
            await cur.execute("CREATE INDEX IF NOT EXISTS idx_ts_vt ON Index_ts (vt_from, vt_to)")
            await cur.execute("CREATE INDEX IF NOT EXISTS idx_data_name ON Index_data (name)")
            await cur.execute("CREATE INDEX IF NOT EXISTS idx_data_age ON Index_data (age)")
            
        print(f"{self.name}: Initialized tables and indexes")

    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle]) -> None:
        """Insert rectangles into PostgreSQL using batch operations"""
        await self.connect()
        
        async with self.conn.cursor() as cur:
            for rect in rectangles:
                vref = str(UUID(bytes=hashlib.md5(str(rect.data).encode()).digest()))
                eref = str(UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest()))
                
                # Insert temporal data
                await cur.execute("""
                    INSERT INTO Index_ts (vref, eref, tt_from, tt_to, vt_from, vt_to)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (vref, eref, rect.tt_from, rect.tt_to, rect.vt_from, rect.vt_to))
                
                # Insert entity data
                try:
                    await cur.execute("""
                        INSERT INTO Index_data (vref, name, age)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (vref) DO NOTHING
                    """, (vref, rect.data.get("name"), rect.data.get("age")))
                except Exception as e:
                    print(f"Error inserting data document: {e}")
                    continue
        
        print(f"{self.name}: Inserted {len(rectangles)} rectangles")

    async def query_by_name_and_age(self, name: str, age: int, tt: datetime, vt: datetime):
        """Query records by name and age at specific valid and transaction times"""
        await self.connect()
        
        async with self.conn.cursor() as cur:
            await cur.execute("""
                SELECT d.* 
                FROM Index_data d
                JOIN Index_ts t ON d.vref = t.vref
                WHERE d.name = %s 
                AND d.age = %s
                AND t.tt_from <= %s
                AND t.tt_to > %s
                AND t.vt_from <= %s
                AND t.vt_to > %s
            """, (name, age, tt, tt, vt, vt))
            
            return await cur.fetchall()

# Usage example:
async def main():
    solution = XTDBSolution()
    try:
        await solution.initialize_collections()
    finally:
        await solution.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 