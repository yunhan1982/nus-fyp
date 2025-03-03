import hashlib
from uuid import UUID
from typing import List
from bitemporal_space import Rectangle


# Main function to insert rectangles
async def insert_rectangle_to_collections(rectangles: List[Rectangle], db) -> None:
    """Insert rectangles into collections asynchronously using batch operations."""
    # Create indexes
    await db.Index_data.create_index("vref", unique=True)
    await db.Index_data.create_index("name")
    await db.Index_data.create_index("age")
    
    # Prepare batch document collections
    index_ts_batch = []
    index_data_batch = []
    index_data_vrefs = set()
    
    # Process all rectangles
    for rect in rectangles:
        vref = UUID(bytes=hashlib.md5(str(rect.data).encode()).digest())
        eref = UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest())
        
        # Index_ts document (no unique constraint, so add all)
        index_ts = {
            "vref": vref,
            "eref": eref,
            "tt_from": rect.tt_from,
            "tt_to": rect.tt_to,
            "vt_from": rect.vt_from,
            "vt_to": rect.vt_to
        }
        index_ts_batch.append(index_ts)
        
        # Index_data document (only add unique vrefs)
        if vref not in index_data_vrefs:
            index_data = {
                "vref": vref,
                "name": rect.data.get("name"),
                "age": rect.data.get("age")
            }
            index_data_batch.append(index_data)
            index_data_vrefs.add(vref)
    
    # Perform batch inserts
    if index_ts_batch:
        try:
            await db.Index_ts.insert_many(index_ts_batch, ordered=False)
        except Exception as e:
            print(f"Some Index_ts inserts failed: {e}")
    
    if index_data_batch:
        try:
            await db.Index_data.insert_many(index_data_batch, ordered=False)
        except Exception as e:
            print(f"Some Index_data inserts failed: {e}")