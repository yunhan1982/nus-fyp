import hashlib
from datetime import datetime
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import Dict, Any, List

# Rectangle class (assuming the same as in your original code)
class Rectangle:
    def __init__(self, data: Dict[str, Any], tt_from: datetime, tt_to: datetime, vt_from: datetime, vt_to: datetime, index: int):
        self.data = data
        self.tt_from = tt_from
        self.tt_to = tt_to
        self.vt_from = vt_from
        self.vt_to = vt_to
        self.index = index

# Main function to insert rectangles
async def insert_rectangle_to_collections(rectangles: List[Rectangle], db, entity: str = "Student", indices: List[str] = ["name", "age"]) -> None:
    """Insert rectangles into collections asynchronously using batch operations."""
    # Create indexes
    await db.Payloads.create_index("vref", unique=True)
    await db.Timeslices.create_index([("eref", 1), ("seq_no", 1)], unique=True)
    await db.Index.create_index([
        ("hash", 1),
        ("vt_to", 1),
        ("tt_to", 1)
    ])
    
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
            await db.Payloads.insert_many(payloads_batch, ordered=False)
        except Exception as e:
            print(f"Some payload inserts failed: {e}")
    
    if timeslices_batch:
        try:
            await db.Timeslices.insert_many(timeslices_batch, ordered=False)
        except Exception as e:
            print(f"Some timeslice inserts failed: {e}")
    
    if index_batch:
        try:
            await db.Index.insert_many(index_batch, ordered=False)
        except Exception as e:
            print(f"Some index inserts failed: {e}")