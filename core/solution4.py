import hashlib
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import List
from collections import defaultdict
import motor.motor_asyncio
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class Solution4: 
    def __init__(self) -> None:
        self.name = "Solution4"
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
        self.db = self.client[self.name]
        self.collections = ["Name", "Age", "Attr1", "Attr2", "Attr3", "Attr4"]

    # async def cleanup(self):
    #     """Destructor to ensure client is closed"""
    #     if hasattr(self, 'client'):
    #         await self.client.close()

    async def initialize_collections(self):
        # Drop and create collections with indexes
        for collection in self.collections:
            await self.db[collection].drop()
            await self.db[collection].create_index([
                ("value", 1),
                ("tt_from", 1),
                ("vt_from", 1)
            ])
            await self.db[collection].create_index([
                ("value", 1),
                ("tt_to", 1),
                ("vt_to", 1)
            ])
            await self.db[collection].create_index(
                [("eref", 1)]
            )


        print("Solution4 collection schemas created with appropriate indexes.")


    # Main function to insert rectangles
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        records_map = defaultdict(list)

        for rect in rectangles:
            
            eref = rect.data.get("id", 0)

            for collection in self.collections:
                value = rect.data["payload"].get(collection.lower())
                if len(records_map[collection]) > 0 and records_map[collection][-1]["value"] == value:
                    continue
                
                record = {
                    "eref": eref,
                    "value": value,
                    "tt_from": rect.tt_from,
                    "tt_to": rect.tt_to,
                    "vt_from": rect.vt_from,
                    "vt_to": rect.vt_to,
                    "entity": entity,
                }
                records_map[collection].append(record)
        
        for collection in self.collections:
            try:
                await self.db[collection].insert_many(records_map[collection], ordered=False)
                print(f"{self.name}: Inserted {len(records_map[collection])} documents into {collection} collection")
            except Exception as e:
                print(f"Some Payload inserts failed: {e}")


    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        pipeline = [
            # Match documents from Name collection
            {
                "$match": {
                    "value": name,
                    "entity": entity,
                    "tt_from": {"$lte": tt},
                    "tt_to": {"$gt": tt},
                    "vt_from": {"$lte": vt},
                    "vt_to": {"$gt": vt}
                }
            },
            # Lookup matching documents from Age collection
            {
                "$lookup": {
                    "from": "Age",
                    "let": {
                        "eref": "$eref",
                        "entity": "$entity"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$eref", "$$eref"]},
                                        {"$eq": ["$entity", "$$entity"]},
                                        {"$eq": ["$value", age]},
                                        {"$lte": ["$tt_from", tt]},
                                        {"$gt": ["$tt_to", tt]},
                                        {"$lte": ["$vt_from", vt]},
                                        {"$gt": ["$vt_to", vt]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "age_matches"
                }
            },
            # Filter out documents with no age matches
            {
                "$match": {
                    "age_matches": {"$ne": []}
                }
            },
            # Unwind the age matches array
            {
                "$unwind": "$age_matches"
            },
            # Project the final result format
            {
                "$project": {
                    "_id": 0,
                    "id": "$eref",
                    "name": "$value",
                    "age": "$age_matches.value",
                    "tt_from": {
                        "$max": ["$tt_from", "$age_matches.tt_from"]
                    },
                    "tt_to": {
                        "$min": ["$tt_to", "$age_matches.tt_to"]
                    },
                    "vt_from": {
                        "$max": ["$vt_from", "$age_matches.vt_from"]
                    },
                    "vt_to": {
                        "$min": ["$vt_to", "$age_matches.vt_to"]
                    }
                }
            },
            # Filter out invalid time periods
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$lt": ["$tt_from", "$tt_to"]},
                            {"$lt": ["$vt_from", "$vt_to"]}
                        ]
                    }
                }
            }
        ]
        
        results = await self.db["Name"].aggregate(pipeline).to_list(None)
        return results
        