import pymongo
import hashlib
from datetime import datetime, timezone
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
db = client["Solution3"]

# Define schema for Index_ts collection
def create_index_ts_schema():
    """Create and index the Index_ts collection."""
    db.Index_ts.drop()
    # No specific index needed beyond natural _id, but you can add one if required
    print("Index_ts collection schema created.")

# Define schema for Index_data collection
def create_index_data_schema():
    """Create and index the Index_data collection."""
    db.Index_data.drop()
    db.Index_data.create_index("vref", unique=True)

    # B-tree index on name
    db.Index_data.create_index("name")
    # B-tree index on age
    db.Index_data.create_index("age")
    print("Index_data collection schema created with B-tree indexes on name and age.")

# Insert a rectangle into Index_ts collection
def insert_index_ts_from_rectangle(rect: Rectangle, eref: UUID, vref: UUID) -> None:
    """Insert a record from a Rectangle into Index_ts collection."""
    index_ts = {
        "vref": vref,
        "eref": eref,
        "tt_from": rect.tt_from,
        "tt_to": rect.tt_to,
        "vt_from": rect.vt_from,
        "vt_to": rect.vt_to
    }
    db.Index_ts.insert_one(index_ts)

# Insert a rectangle into Index_data collection
def insert_index_data_from_rectangle(rect: Rectangle, vref: UUID) -> None:
    """Insert a record from a Rectangle into Index_data collection."""

    if db.Index_data.find_one({"vref": vref}):
        return  # Skip insertion if vref already exists

    index_data = {
        "vref": vref,
        "name": rect.data.get("name"),
        "age": rect.data.get("age")
    }
    db.Index_data.insert_one(index_data)


# Setup the schema
create_index_ts_schema()
create_index_data_schema()

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

        insert_index_ts_from_rectangle(rect, eref, vref)
        insert_index_data_from_rectangle(rect, vref)


insert_rectangle_to_collections(rectangles)

# Verify the setup
# print("\nIndex_ts collection:")
# for doc in db.Index_ts.find():
#     print(doc)

# print("\nIndex_data collection:")
# for doc in db.Index_data.find():
#     print(doc)



# Example query: Find records for name "Student_10" at tt: 2024-06-10, vt: 2024-06-06
query_ts = {
    "tt_from": {"$lte": datetime.fromisoformat("2024-06-10T00:00:00+00:00")},
    "tt_to": {"$gt": datetime.fromisoformat("2024-06-10T00:00:00+00:00")},
    "vt_from": {"$lte": datetime.fromisoformat("2024-06-06T00:00:00+00:00")},
    "vt_to": {"$gt": datetime.fromisoformat("2024-06-06T00:00:00+00:00")}
}

print("\nQuery result for name 'Student_10' at 2024-06-10, 2024-06-06:")
index_ts_records = list(db.Index_ts.find(query_ts))
for ts in index_ts_records:
    data = db.Index_data.find_one({"vref": ts["vref"], "name": "Student_10"})
    if data:
        result = {
            "eref": ts["eref"],
            "tt_from": ts["tt_from"],
            "tt_to": ts["tt_to"],
            "vt_from": ts["vt_from"],
            "vt_to": ts["vt_to"],
            "name": data["name"],
            "age": data["age"]
        }
        print(result)

# Close the connection
client.close()