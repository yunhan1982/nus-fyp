import hashlib
from uuid import UUID
from typing import Dict, Any, List
from bson.binary import Binary, UUID_SUBTYPE
import motor.motor_asyncio
from .bitemporal_space import Rectangle


class Solution2:
    def __init__(self) -> None:
        self.name = "Solution2"
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
        self.db = self.client[self.name]
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]
    
    # async def cleanup(self):
    #     """Destructor to ensure client is closed"""
    #     if hasattr(self, 'client'):
    #         await self.client.close()

    async def initialize_collections(self):
        # Drop and create collections with indexes
        await self.db.Index.drop()
        await self.db.Payload.drop()
        
        # Create indexes dynamically for each field
        for field in self.indices:
            await self.db.Index.create_index([
                (field, 1),
                ("tt_from", 1),
                ("vt_from", 1)
            ])
            await self.db.Index.create_index([
                (field, 1),
                ("tt_to", 1),
                ("vt_to", 1)
            ])
            
        await self.db.Payload.create_index("vref", unique=True)
        
        print("Solution2 Index and Payload collection schemas created with appropriate indexes.")

    # Function to compute MD5 hash of data (for Payload)
    @staticmethod
    def compute_md5(data_dict: Dict[str, Any]) -> str:
        """Compute MD5 hash of a dictionary, ensuring JSON-like string format."""
        data_str = str(data_dict).replace("'", '"')
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    # Main function to insert rectangles
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles into collections asynchronously using batch operations."""        
        # Prepare batch document collections
        index_batch = []
        payload_batch = []
        payload_vrefs = set()
        
        # Process all rectangles
        for rect in rectangles:
            vref = UUID(bytes=hashlib.md5(str(rect.data["payload"]).encode()).digest())
            eref = UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest())
            
            # Create base index entry
            index_entry = {
                "vref": vref,
                "eref": eref,
                "tt_from": rect.tt_from,
                "tt_to": rect.tt_to,
                "vt_from": rect.vt_from,
                "vt_to": rect.vt_to,
                "entity": entity,
            }
            
            # Dynamically add all indexed fields
            for field in self.indices:
                index_entry[field] = rect.data["payload"].get(field)
                
            index_batch.append(index_entry)
            
            # Payload document (only add unique vrefs)
            if vref not in payload_vrefs:
                payload_entry = {
                    "vref": vref,
                    "data": rect.data["payload"],
                    "hash": Binary(bytes.fromhex(Solution2.compute_md5(rect.data["payload"])), UUID_SUBTYPE)
                }
                payload_batch.append(payload_entry)
                payload_vrefs.add(vref)
        
        # Perform batch inserts
        if index_batch:
            try:
                await self.db.Index.insert_many(index_batch, ordered=False)
                print(f"{self.name}: Inserted {len(index_batch)} documents into Index collection")
            except Exception as e:
                print(f"Some Index inserts failed: {e}")
        
        if payload_batch:
            try:
                await self.db.Payload.insert_many(payload_batch, ordered=False)
                print(f"{self.name}: Inserted {len(payload_batch)} documents into Payload collection")
            except Exception as e:
                print(f"Some Payload inserts failed: {e}")

    
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        # First query the Index collection to find matching records
        index_query = {
            "name": name,
            "age": age,
            "entity": entity,
            "tt_from": {"$lte": tt},
            "tt_to": {"$gt": tt},
            "vt_from": {"$lte": vt},
            "vt_to": {"$gt": vt}
        }
        
        # Get matching vrefs from Index
        cursor = self.db.Index.find(index_query)
        
        vrefs = [doc["vref"] for doc in await cursor.to_list(length=None)]
        
        if not vrefs:
            return []
            
        # Query Payload collection for the actual data
        payload_query = {"vref": {"$in": vrefs}}
        cursor = self.db.Payload.find(payload_query)
        
        # Return the data from matching payloads
        results = []
        async for doc in cursor:
            results.append(doc["data"])
            
        return results