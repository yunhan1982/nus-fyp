import pymongo
import hashlib
from datetime import datetime
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import Dict, Any, Optional

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
    ])  # B-tree index on (hash, vt_to, tt_to)
    print("Indexes collection schema created with unique index on vref and B-tree index on (hash, vt_to, tt_to).")

# Insert a bitemporal record across all three collections
def insert_bitemporal_record(
    eref: UUID,
    seq_no: int,
    tt_from: datetime,
    tt_to: datetime,
    vt_from: datetime,
    vt_to: datetime,
    data: Dict[str, Any],
    entity: str = "Student",
    vref: Optional[UUID] = None
) -> None:
    """Insert a timeslice, payload, and index record into the database."""
    # Generate vref if not provided
    vref = vref or UUID(bytes=hashlib.md5(f"{eref}{seq_no}".encode()).digest())
    md5_hash = compute_md5(data)

    # Insert into Payload
    payload = {
        "vref": vref,
        "data": data,
        "hash": Binary(bytes.fromhex(md5_hash), UUID_SUBTYPE)
    }
    db.Payload.insert_one(payload)

    # Insert into Timeslices
    timeslice = {
        "eref": eref,
        "seq_no": seq_no,
        "tt_from": tt_from,
        "tt_to": tt_to,
        "vt_from": vt_from,
        "vt_to": vt_to,
        "vref": vref
    }
    db.Timeslices.insert_one(timeslice)

    # Insert into Indexes for each key in data
    for key, val in data.items():
        # Compute MD5 hash for the specific data[key]
        item = {key: val}
        key_hash = hashlib.md5(str(item).encode('utf-8')).hexdigest()
        
        index = {
            "vref": vref,
            "eref": eref,
            "tt_from": tt_from,
            "tt_to": tt_to,
            "vt_from": vt_from,
            "vt_to": vt_to,
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

# Example: Insert records
eref = UUID("123e4567-e89b-12d3-a456-426614174000")

# Insert initial records
insert_bitemporal_record(
    eref=eref,
    seq_no=1,
    tt_from=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
    tt_to=datetime.fromisoformat("9999-12-31T23:59:59+00:00"),
    vt_from=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
    vt_to=datetime.fromisoformat("2023-06-30T23:59:59+00:00"),
    data={"name": "Joe", "age": 11}
)

insert_bitemporal_record(
    eref=eref,
    seq_no=2,
    tt_from=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
    tt_to=datetime.fromisoformat("9999-12-31T23:59:59+00:00"),
    vt_from=datetime.fromisoformat("2023-01-01T00:00:00+00:00"),
    vt_to=datetime.fromisoformat("2023-06-30T23:59:59+00:00"),
    data={"name": "Joe", "age": 12}
)

insert_bitemporal_record(
    eref=eref,
    seq_no=3,
    tt_from=datetime.fromisoformat("2023-07-01T00:00:00+00:00"),
    tt_to=datetime.fromisoformat("9999-12-31T23:59:59+00:00"),
    vt_from=datetime.fromisoformat("2023-07-01T00:00:00+00:00"),
    vt_to=datetime.fromisoformat("9999-12-31T23:59:59+00:00"),
    data={"name": "Jozef", "age": 13}
)

insert_bitemporal_record(
    eref=eref,
    seq_no=4,
    tt_from=datetime.fromisoformat("2023-07-01T00:00:00+00:00"),
    tt_to=datetime.fromisoformat("9999-12-31T23:59:59+00:00"),
    vt_from=datetime.fromisoformat("2023-07-01T00:00:00+00:00"),
    vt_to=datetime.fromisoformat("9999-12-31T23:59:59+00:00"),
    data={"name": "Joe", "age": 14}
)

# Verify the setup
print("\nTimeslices collection:")
for doc in db.Timeslices.find():
    print(doc)

print("\nPayload collection:")
for doc in db.Payload.find():
    print(doc)

print("\nIndexes collection:")
for doc in db.Indexes.find():
    print(doc)

# Example query: Find records for name "Joe" at tt: 2023-03-01, vt: 2023-03-01
search_hash = compute_md5({"name": "Joe"})
query = {
    "tt_from": {"$lte": datetime.fromisoformat("2023-03-01T00:00:00+00:00")},
    "tt_to": {"$gt": datetime.fromisoformat("2023-03-01T00:00:00+00:00")},
    "vt_from": {"$lte": datetime.fromisoformat("2023-03-01T00:00:00+00:00")},
    "vt_to": {"$gt": datetime.fromisoformat("2023-03-01T00:00:00+00:00")},
    "hash": Binary(bytes.fromhex(search_hash), UUID_SUBTYPE),
    "data.name": "Joe"
}

print("\nQuery result for name 'Joe' at 2023-03-01 from Indexes:")
for result in db.Indexes.find(query, {
    "eref": 1, "tt_from": 1, "tt_to": 1, "vt_from": 1, "vt_to": 1, "data": 1, "_id": 0
}):
    print(result)

# Close the connection
client.close()