import hashlib
from uuid import UUID
from typing import Dict, Any, List
from bson.binary import Binary, UUID_SUBTYPE
from bitemporal_space import Rectangle


class Solution2:
    def __init__(self, db) -> None:
        self.id = 2
        self.db = db

    async def initialize_collections(self):
        # Drop and create collections with indexes
        await self.db.Index.drop()
        await self.db.Payload.drop()
        
        # Create indexes
        await self.db.Index.create_index([
            ("name", 1),
            ("tt_to", 1),
            ("vt_to", 1)
        ])
        await self.db.Index.create_index([
            ("age", 1),
            ("tt_to", 1),
            ("vt_to", 1)
        ])
        await self.db.Payload.create_index("vref", unique=True)
        
        print("Index and Payload collection schemas created with appropriate indexes.")

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
            vref = UUID(bytes=hashlib.md5(str(rect.data).encode()).digest())
            eref = UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest())
            
            # Index document (no unique constraint on vref in Index collection)
            index_entry = {
                "vref": vref,
                "eref": eref,
                "tt_from": rect.tt_from,
                "tt_to": rect.tt_to,
                "vt_from": rect.vt_from,
                "vt_to": rect.vt_to,
                "entity": entity,
                "name": rect.data.get("name"),
                "age": rect.data.get("age")
            }
            index_batch.append(index_entry)
            
            # Payload document (only add unique vrefs)
            if vref not in payload_vrefs:
                payload_entry = {
                    "vref": vref,
                    "data": rect.data,
                    "hash": Binary(bytes.fromhex(Solution2.compute_md5(rect.data)), UUID_SUBTYPE)
                }
                payload_batch.append(payload_entry)
                payload_vrefs.add(vref)
        
        # Perform batch inserts
        if index_batch:
            try:
                await self.db.Index.insert_many(index_batch, ordered=False)
                print(f"Inserted {len(index_batch)} documents into Index collection")
            except Exception as e:
                print(f"Some Index inserts failed: {e}")
        
        if payload_batch:
            try:
                await self.db.Payload.insert_many(payload_batch, ordered=False)
                print(f"Inserted {len(payload_batch)} documents into Payload collection")
            except Exception as e:
                print(f"Some Payload inserts failed: {e}")