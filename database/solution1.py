import hashlib
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import List
import motor.motor_asyncio
from bitemporal_space import Rectangle



class Solution1: 
    def __init__(self) -> None:
        self.db = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')["Solution1"]

    
    async def initialize_collections(self):
        # Drop and create collections with indexes
        await self.db.Payloads.drop()
        await self.db.Timeslices.drop()
        await self.db.Index.drop()


        await self.db.Payloads.create_index("vref", unique=True)
        await self.db.Timeslices.create_index([("eref", 1), ("seq_no", 1)], unique=True)
        # await db.Index.create_index(
        #     ("eref", 1)
        # )
        await self.db.Index.create_index(
            ("hash", 1),
            ("vt_from", 1),
            ("tt_from", 1)
        )
        await self.db.Index.create_index([
            ("hash", 1),
            ("vt_to", 1),
            ("tt_to", 1)
        ])

    # Main function to insert rectangles
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student", indices: List[str] = ["name", "age"]) -> None:
        # Prepare batch document collections
        payloads_batch = []
        timeslices_batch = []
        index_batch = []
        
        # Track unique keys to avoid duplicates
        payload_vrefs = set()
        timeslice_keys = set()    
        # Process all rectangles
        for rect in rectangles:
            vref = UUID(bytes=hashlib.md5(str(rect.data).encode()).digest())
            eref = UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest())
            seq_no = str(rect.index)
            # Payload document
            if vref not in payload_vrefs:
                payload = {
                    "vref": vref,
                    "name": rect.data.get("name"),
                    "age": rect.data.get("age")
                }
                payloads_batch.append(payload)
                payload_vrefs.add(vref)
            
            # Timeslice document
            timeslice_key = (eref, seq_no)

            if timeslice_key not in timeslice_keys:
                timeslice = {
                    "vref": vref,
                    "eref": eref,
                    "seq_no": seq_no,  # For simplicity, using fixed sequence number
                    "tt_from": rect.tt_from,
                    "tt_to": rect.tt_to,
                    "vt_from": rect.vt_from,
                    "vt_to": rect.vt_to
                }
                timeslices_batch.append(timeslice)
                timeslice_keys.add(timeslice_key)
            

            key_hash = hashlib.md5(str(rect.data).encode('utf-8')).hexdigest()

            for key in indices:
                # Compute MD5 hash for the specific data[key]
                item = {key: rect.data[key]}
                key_hash = hashlib.md5(str(item).encode('utf-8')).hexdigest()
                    
                index_entry = {
                    "vref": vref,
                    "eref": eref,
                    "tt_from": rect.tt_from,
                    "tt_to": rect.tt_to,
                    "vt_from": rect.vt_from,
                    "vt_to": rect.vt_to,
                    "entity": entity,
                    "data": item,
                    "hash": Binary(bytes.fromhex(key_hash), UUID_SUBTYPE),  # Use hash of data[key]
                    "key": key  # Add the key to differentiate between name and age
                }

                index_batch.append(index_entry)

        # Perform batch inserts
        if payloads_batch:
            try:
                await self.db.Payloads.insert_many(payloads_batch, ordered=False)
                print(f"Solution1: Inserted {len(payloads_batch)} documents into Payload collection")
            except Exception as e:
                print(f"Some payload inserts failed: {e}")
        
        if timeslices_batch:
            try:
                await self.db.Timeslices.insert_many(timeslices_batch, ordered=False)
                print(f"Solution1: Inserted {len(timeslices_batch)} documents into Timeslices collection")
            except Exception as e:
                print(f"Some timeslice inserts failed: {e}")
        
        if index_batch:
            try:
                await self.db.Index.insert_many(index_batch, ordered=False)
                print(f"Solution1: Inserted {len(index_batch)} documents into Index collection")
            except Exception as e:
                print(f"Some index inserts failed: {e}")


    @staticmethod
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """
        Query records by both name and age at specific transaction and valid times.
        
        Args:
            db: Database connection
            name: Name to search for
            age: Age to search for
            tt: Transaction time to check
            vt: Valid time to check
            entity: Entity type (default: "Student")
        
        Returns:
            List of matching records
        """
        # Step 1: Create hash for name query and search by hash, vt, tt
        name_item = {"name": name}
        name_hash = Binary(bytes.fromhex(hashlib.md5(str(name_item).encode('utf-8')).hexdigest()), UUID_SUBTYPE)

        name_query = {
            "hash": name_hash,
            "entity": entity,
            "tt_from": {"$lte": tt},
            "tt_to": {"$gt": tt},
            "vt_from": {"$lte": vt},
            "vt_to": {"$gt": vt}
        }
        
        # Step 2: Get name index entries and filter for correct name
        name_index_entries = await self.db.Index.find(name_query).to_list(None)
        name_index_entries = [entry for entry in name_index_entries 
                            if entry.get("data", {}).get("name") == name]
        
        if not name_index_entries:
            return []
        
        # Step 3: Get all entity references (erefs) from name matches
        erefs = {entry["eref"] for entry in name_index_entries}
        
        # Step 4: Create hash for age query and search by hash, vt, tt, and eref
        age_item = {"age": age}
        age_hash = Binary(bytes.fromhex(hashlib.md5(str(age_item).encode('utf-8')).hexdigest()), UUID_SUBTYPE)
        
        age_query = {
            "hash": age_hash,
            "entity": entity,
            "eref": {"$in": list(erefs)},
            "tt_from": {"$lte": tt},
            "tt_to": {"$gt": tt},
            "vt_from": {"$lte": vt},
            "vt_to": {"$gt": vt}
        }
        
        # Step 5: Get age index entries and filter for correct age
        age_index_entries = await self.db.Index.find(age_query).to_list(None)
        age_index_entries = [entry for entry in age_index_entries 
                            if entry.get("data", {}).get("age") == age]
        
        if not age_index_entries:
            return []
        
        # Get intersection of entities that match both name and age
        matching_erefs = {entry["eref"] for entry in age_index_entries}
        
        # Step 6: Get all vrefs from matching entities
        matching_vrefs = set()
        for entry in name_index_entries + age_index_entries:
            if entry["eref"] in matching_erefs:
                matching_vrefs.add(entry["vref"])
        
        # Retrieve the actual payload documents
        if matching_vrefs:
            payloads = await self.db.Payloads.find({"vref": {"$in": list(matching_vrefs)}}).to_list(None)
            return payloads
        
        return []

