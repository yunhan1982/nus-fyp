import hashlib
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import List
from collections import defaultdict
import motor.motor_asyncio
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionC: 
    def __init__(self) -> None:
        self.name = "SolutionC"
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
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
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
        
        # Get explain for aggregation pipeline
        explain_result = await self.db.command("explain", {"aggregate": "Name", "pipeline": pipeline, "cursor": {}})
        
        results = await self.db["Name"].aggregate(pipeline).to_list(None)
        
        # Log memory usage information
        final_memory = await self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Memory Usage: {memory_used:.2f} MB")
        
        # Log aggregation execution stats if available
        self._log_aggregation_stats("Aggregation Pipeline", explain_result)
        
        return results
    
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Range query records by name and age within VT and TT intervals."""
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
        pipeline = [
            # Match documents from Name collection within time intervals
            {
                "$match": {
                    "value": name,
                    "entity": entity,
                    "tt_from": {"$lt": tt_to},
                    "tt_to": {"$gt": tt_from},
                    "vt_from": {"$lt": vt_to},
                    "vt_to": {"$gt": vt_from}
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
                                        {"$lt": ["$tt_from", tt_to]},
                                        {"$gt": ["$tt_to", tt_from]},
                                        {"$lt": ["$vt_from", vt_to]},
                                        {"$gt": ["$vt_to", vt_from]}
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
        
        # Get explain for aggregation pipeline
        explain_result = await self.db.command("explain", {"aggregate": "Name", "pipeline": pipeline, "cursor": {}})
        
        results = await self.db["Name"].aggregate(pipeline).to_list(None)
        
        # Log memory usage information
        final_memory = await self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Log aggregation execution stats
        self._log_aggregation_stats("Range Aggregation Pipeline", explain_result)
        
        return results

    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Range query records by a single attribute within VT and TT intervals."""
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
        # Determine the collection name based on attribute (capitalize first letter)
        collection_name = attribute_name.capitalize()
        
        pipeline = [
            # Match documents from the attribute collection within time intervals
            {
                "$match": {
                    "value": attribute_value,
                    "entity": entity,
                    "tt_from": {"$lt": tt_to},
                    "tt_to": {"$gt": tt_from},
                    "vt_from": {"$lt": vt_to},
                    "vt_to": {"$gt": vt_from}
                }
            },
            # Project the final result format
            {
                "$project": {
                    "_id": 0,
                    "id": "$eref",
                    attribute_name: "$value",
                    "tt_from": "$tt_from",
                    "tt_to": "$tt_to",
                    "vt_from": "$vt_from",
                    "vt_to": "$vt_to"
                }
            }
        ]
        
        # Get explain for aggregation pipeline
        explain_result = await self.db.command("explain", {"aggregate": collection_name, "pipeline": pipeline, "cursor": {}})
        
        results = await self.db[collection_name].aggregate(pipeline).to_list(None)
        
        # Log memory usage information
        final_memory = await self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Log aggregation execution stats
        self._log_aggregation_stats(f"{attribute_name} Range Aggregation Pipeline", explain_result)
        
        return results

    async def delta_since_vt_range(self, name, age, vt_from, vt_to, tt, entity="Student"):
        """Find entities at two VT points (vt_from, tt) and (vt_to, tt).
        Returns a tuple (entity_at_start, entity_at_end) where each can be None if no entity exists."""
        from .timer import Timer
        timer = Timer()
        timer.start()
        
        # Query for entity at (vt_from, tt)
        entity_at_start = await self._query_at_point(name, age, vt_from, tt, entity)
        entity_at_end = await self._query_at_point(name, age, vt_to, tt, entity)
        
        timer.stop()
        memory_usage = await self._get_memory_usage()
        
        return {
            "result": (entity_at_start, entity_at_end),
            "execution_time": timer.get_elapsed_time(),
            "memory_usage": memory_usage
        }
    
    async def delta_since_tt_range(self, name, age, tt_from, tt_to, vt, entity="Student"):
        """Find entities at two TT points (vt, tt_from) and (vt, tt_to).
        Returns a tuple (entity_at_start, entity_at_end) where each can be None if no entity exists.
        If entities are identical, returns (None, None)."""
        from .timer import Timer
        timer = Timer()
        timer.start()
        
        # Query for entity at (vt, tt_from) and (vt, tt_to)
        entity_at_start = await self._query_at_point(name, age, vt, tt_from, entity)
        entity_at_end = await self._query_at_point(name, age, vt, tt_to, entity)
        
        # If both entities exist and are identical, return (None, None)
        if entity_at_start and entity_at_end and entity_at_start == entity_at_end:
            entity_at_start = None
            entity_at_end = None
        
        timer.stop()
        memory_usage = await self._get_memory_usage()
        
        return {
            "result": (entity_at_start, entity_at_end),
            "execution_time": timer.get_elapsed_time(),
            "memory_usage": memory_usage
        }
    
    async def _query_at_point(self, name, age, vt, tt, entity="Student"):
        """Helper method to query entity at a specific bitemporal point using aggregation."""
        try:
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
                },
                # Limit to first result
                {
                    "$limit": 1
                }
            ]
            
            results = await self.db["Name"].aggregate(pipeline).to_list(None)
            return results[0] if results else None
            
        except Exception:
            return None

    async def _get_memory_usage(self):
        """Get current memory usage from MongoDB server status."""
        try:
            server_status = await self.db.command("serverStatus")
            # Return memory usage in MB
            return server_status.get("mem", {}).get("resident", 0)
        except Exception:
            return 0
    
    def _log_aggregation_stats(self, query_name, explain_result):
        """Log aggregation execution statistics."""
        try:
            if explain_result and len(explain_result) > 0:
                stages = explain_result[0].get("stages", [])
                total_docs_examined = 0
                total_docs_returned = 0
                total_execution_time = 0
                
                for stage in stages:
                    if "executionStats" in stage:
                        stats = stage["executionStats"]
                        total_docs_examined += stats.get("totalDocsExamined", 0)
                        total_docs_returned += stats.get("totalDocsReturned", 0)
                        total_execution_time += stats.get("executionTimeMillis", 0)
                
                print(f"{self.name} {query_name} Stats: {total_docs_examined} docs examined, {total_docs_returned} returned, {total_execution_time}ms")
        except Exception:
            pass
        