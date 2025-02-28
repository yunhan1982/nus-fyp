import pymongo
import hashlib
from datetime import datetime, timezone
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import Dict, Any, Optional, List

from generate_rectangles import generate_rectangles
from generate_student_data import generate_student_data

# Constants
INFINITY = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# Rectangle class (from generate_rectangles output)
class Rectangle:
    def __init__(self, data: Dict[str, Any], tt_from: datetime, tt_to: datetime, vt_from: datetime, vt_to: datetime, index: int):
        self.data = data
        self.tt_from = tt_from
        self.tt_to = tt_to
        self.vt_from = vt_from
        self.vt_to = vt_to
        self.index = index

    def __repr__(self):
        return (f"Rectangle(data={self.data}, ttInterval=[{self.tt_from}, {self.tt_to}], "
                f"vtInterval=[{self.vt_from}, {self.vt_to}], index={self.index})")

# Connect to the local MongoDB client with standard UUID representation
client = pymongo.MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
db = client["Solution1"]

# Function to compute MD5 hash of data
def compute_md5(data_dict: Dict[str, Any]) -> str:
    """Compute MD5 hash of a dictionary, ensuring JSON-like string format."""
    data_str = str(data_dict).replace("'", '"')
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

# Define schema for Timeslices collection
def create_timeslices_schema():
    """Create and index the Timeslices collection."""
    db.Timeslices.drop()
    db.Timeslices.create_index([
        ("eref", pymongo.ASCENDING),
        ("seq_no", pymongo.ASCENDING)
    ], unique=True)
    print("Timeslices collection schema created with unique index on (eref, seq_no).")

# Define schema for Payload collection
def create_payload_schema():
    """Create and index the Payload collection."""
    db.Payload.drop()
    db.Payload.create_index("vref", unique=True)
    print("Payload collection schema created with unique index on vref.")

# Define schema for Indexes collection
def create_indexes_schema():
    """Create and index the Indexes collection."""
    db.Indexes.drop()
    db.Indexes.create_index([
        ("hash", pymongo.ASCENDING),
        ("vt_to", pymongo.ASCENDING),
        ("tt_to", pymongo.ASCENDING)
    ])
    print("Indexes collection schema created with B-tree index on (hash, vt_to, tt_to).")

# Insert a rectangle into Timeslices
def insert_timeslice_from_rectangle(rect: Rectangle, vref: UUID, eref: UUID) -> None:
    """Insert a timeslice from a Rectangle into Timeslices collection."""
    timeslice = {
        "eref": eref,
        "seq_no": rect.index,  # Use Rectangle.index as seq_no
        "tt_from": rect.tt_from,
        "tt_to": rect.tt_to,
        "vt_from": rect.vt_from,
        "vt_to": rect.vt_to,
        "vref": vref 
    }
    db.Timeslices.insert_one(timeslice)

# Insert a rectangle into Payload
def insert_payload_from_rectangle(rect: Rectangle, vref: UUID) -> None:
    """Insert a payload from a Rectangle into Payload collection."""
    # Check for vref collision
    if db.Payload.find_one({"vref": vref}):
        # print(f"Collision detected for vref: {vref}. Skipping insertion.")
        return  # Skip insertion if vref already exists

    payload = {
        "vref": vref,
        "data": rect.data,
        "hash": Binary(bytes.fromhex(compute_md5(rect.data)), UUID_SUBTYPE)
    }

    
    db.Payload.insert_one(payload)

# Insert a rectangle into Indexes
def insert_index_from_rectangle(rect: Rectangle, vref: UUID, eref: UUID, entity: str = "Student") -> None:
    # Insert into Indexes for each key in data
    for key, val in rect.data.items():
        # Compute MD5 hash for the specific data[key]
        item = {key: val}
        key_hash = hashlib.md5(str(item).encode('utf-8')).hexdigest()
            
        index = {
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
        db.Indexes.insert_one(index)  # Insert for each key in data


# Setup the schema
create_timeslices_schema()
create_payload_schema()
create_indexes_schema()

# Example data from your generate_rectangles output
rectangles = generate_rectangles(
    start_time=datetime(2024, 5, 16, 0, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 5, 20, 0, 0, 0, tzinfo=timezone.utc),
    num_ids=100,
    num_points_per_id=100,
    generate_data=generate_student_data
)

# Insert rectangles into collections

def insert_rectangle_to_collections(rectangles: List[Rectangle]) -> None:
    for rect in rectangles:
        vref = UUID(bytes=hashlib.md5(str(rect.data).encode()).digest())
        eref = UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest())


        insert_payload_from_rectangle(rect, vref)
        insert_timeslice_from_rectangle(rect, vref, eref)
        insert_index_from_rectangle(rect, vref, eref)


insert_rectangle_to_collections(rectangles)

# Verify the setup
# print("\nTimeslices collection:")
# for doc in db.Timeslices.find():
#     print(doc)

# print("\nPayload collection:")
# for doc in db.Payload.find():
#     print(doc)

# print("\nIndexes collection:")
# for doc in db.Indexes.find():
#     print(doc)

# Close the connection
client.close()