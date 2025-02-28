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
db = client["Solution2"]

# Function to compute MD5 hash of data (for Payload)
def compute_md5(data_dict: Dict[str, Any]) -> str:
    """Compute MD5 hash of a dictionary, ensuring JSON-like string format."""
    data_str = str(data_dict).replace("'", '"')
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

# Define schema for Index collection
def create_index_schema():
    """Create and index the Index collection."""
    db.Index.drop()
    # B-tree index on (name, tt_to, vt_to)
    db.Index.create_index([
        ("name", pymongo.ASCENDING),
        ("tt_to", pymongo.ASCENDING),
        ("vt_to", pymongo.ASCENDING)
    ])
    # B-tree index on (age, tt_to, vt_to)
    db.Index.create_index([
        ("age", pymongo.ASCENDING),
        ("tt_to", pymongo.ASCENDING),
        ("vt_to", pymongo.ASCENDING)
    ])
    print("Index collection schema created with B-tree indexes on (name, tt_to, vt_to) and (age, tt_to, vt_to).")

# Define schema for Payload collection
def create_payload_schema():
    """Create and index the Payload collection."""
    db.Payload.drop()
    db.Payload.create_index("vref", unique=True)
    print("Payload collection schema created with unique index on vref.")

# Insert a rectangle into Index collection
def insert_index_from_rectangle(rect: Rectangle, vref: UUID, eref: UUID, entity: str = "Student") -> None:
    """Insert a record from a Rectangle into Index collection."""
    index = {
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
    db.Index.insert_one(index)

# Insert a rectangle into Payload collection
def insert_payload_from_rectangle(rect: Rectangle, vref: UUID) -> None:
    """Insert a payload from a Rectangle into Payload collection."""
    if db.Payload.find_one({"vref": vref}):
        return  # Skip insertion if vref already exists

    payload = {
        "vref": vref,
        "data": rect.data,
        "hash": Binary(bytes.fromhex(compute_md5(rect.data)), UUID_SUBTYPE)
    }
    db.Payload.insert_one(payload)

# Insert a rectangle into both collections

# Setup the schema
create_index_schema()
create_payload_schema()

# Example data from generate_rectangles output
rectangles = generate_rectangles(
    start_time=datetime(2024, 5, 16, 0, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 5, 20, 0, 0, 0, tzinfo=timezone.utc),
    num_ids=100,
    num_points_per_id=100,
    generate_data=generate_student_data
)


def insert_rectangle_to_collections(rectangles: List[Rectangle]) -> None:
    for rect in rectangles:
        vref = UUID(bytes=hashlib.md5(str(rect.data).encode()).digest())
        eref = UUID(bytes=hashlib.md5(str(rect.data.get("id", 0)).encode()).digest())


        insert_payload_from_rectangle(rect, vref)
        insert_index_from_rectangle(rect, vref, eref)


insert_rectangle_to_collections(rectangles)


# Verify the setup
# print("\nIndex collection:")
# for doc in db.Index.find():
#     print(doc)

# print("\nPayload collection:")
# for doc in db.Payload.find():
#     print(doc)

# Example query: Find records for name "Student_10" at tt: 2024-06-10, vt: 2024-06-06
query = {
    "tt_from": {"$lte": datetime.fromisoformat("2024-06-10T00:00:00+00:00")},
    "tt_to": {"$gt": datetime.fromisoformat("2024-06-10T00:00:00+00:00")},
    "vt_from": {"$lte": datetime.fromisoformat("2024-06-06T00:00:00+00:00")},
    "vt_to": {"$gt": datetime.fromisoformat("2024-06-06T00:00:00+00:00")},
    "name": "Student_10"
}

print("\nQuery result for name 'Student_10' at 2024-06-10, 2024-06-06 from Index:")
for result in db.Index.find(query, {"_id": 0, "eref": 1, "tt_from": 1, "tt_to": 1, "vt_from": 1, "vt_to": 1, "name": 1, "age": 1}):
    print(result)

# Close the connection
client.close()