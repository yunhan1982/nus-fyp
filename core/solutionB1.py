import hashlib
from bson.binary import Binary
from uuid import UUID
from typing import Dict, Any, List
import motor.motor_asyncio
from .bitemporal_space import Rectangle
from .utils.timing import Timer

class SolutionB1:
    def __init__(self) -> None:
        self.name = "SolutionB1"
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
        self.db = self.client[self.name]
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]
    
    # async def cleanup(self):
    #     """Destructor to ensure client is closed"""
    #     if hasattr(self, 'client'):
    #         await self.client.close()

    async def initialize_collections(self):
        # Drop and create collections with indexes
        await self.db.Index.drop()
        await self.db.Payload.drop()
        
        # Create indexes dynamically for each field
        for field in self.indices:
            await self.db.Index.create_index([
                (field, 1),
                ("tt_from", 1),
                ("vt_from", 1)
            ])
            await self.db.Index.create_index([
                (field, 1),
                ("tt_to", 1),
                ("vt_to", 1)
            ])
            
        await self.db.Payload.create_index("vref", unique=True)
        
        print(f"{self.name} Index and Payload collection schemas created with appropriate indexes.")

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
            # print(rect)
            vref = UUID(bytes=hashlib.md5(str(rect.data["payload"]).encode()).digest())
            eref = rect.data.get("id", 0)   
            
            # Create base index entry
            vref_binary = Binary.from_uuid(vref)
            index_entry = {
                "vref": vref_binary,
                "eref": eref,
                "tt_from": rect.tt_from,
                "tt_to": rect.tt_to,
                "vt_from": rect.vt_from,
                "vt_to": rect.vt_to,
                "entity": entity,
            }
            
            # Dynamically add all indexed fields
            for field in self.indices:
                index_entry[field] = rect.data["payload"].get(field)
                
            index_batch.append(index_entry)
            
            # Payload document (only add unique vrefs)
            if vref_binary not in payload_vrefs:
                payload_entry = {
                    "vref": vref_binary,
                    "data": rect.data["payload"],
                }
                payload_batch.append(payload_entry)
                payload_vrefs.add(vref_binary)
        
        # Perform batch inserts
        if index_batch:
            try:
                await self.db.Index.insert_many(index_batch, ordered=False)
                print(f"{self.name}: Inserted {len(index_batch)} documents into Index collection")
            except Exception as e:
                print(f"Some Index inserts failed: {e}")
        
        if payload_batch:
            try:
                await self.db.Payload.insert_many(payload_batch, ordered=False)
                print(f"{self.name}: Inserted {len(payload_batch)} documents into Payload collection")
            except Exception as e:
                print(f"Some Payload inserts failed: {e}")

    
    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
        # First query the Index collection to find matching records
        index_query = {
            "name": name,
            "age": age,
            "entity": entity,
            "tt_from": {"$lte": tt},
            "tt_to": {"$gt": tt},
            "vt_from": {"$lte": vt},
            "vt_to": {"$gt": vt}
        }
        
        # Get matching vrefs from Index with explain for memory stats
        index_explain = await self.db.command("explain", {"find": "Index", "filter": index_query})
        cursor = self.db.Index.find(index_query)
        
        vrefs = [doc["vref"] for doc in await cursor.to_list(length=None)]
        
        if not vrefs:
            return []
            
        # Query Payload collection for the actual data
        if len(vrefs) == 1:
            payload_query = {"vref": vrefs[0]}
        else:
            payload_query = {"vref": {"$in": vrefs}}
        
        # payload_explain = await self.db.command("explain", {"find": "Payload", "filter": payload_query})
        cursor = self.db.Payload.find(payload_query)
        
        # Return the data from matching payloads
        results = []
        async for doc in cursor:
            results.append(doc)
        
        # Log memory usage information
        final_memory = await self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Memory Usage: {memory_used:.2f} MB")
        
        # Log query execution stats if available
        self._log_query_stats("Index Query", index_explain)
        # self._log_query_stats("Payload Query", payload_explain)
            
        return results
    
    async def range_query_by_name_and_age(self, name, age, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Range query records by name and age within VT and TT intervals."""
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
        # Query the Index collection to find matching records within time intervals
        index_query = {
            "name": name,
            "age": age,
            "entity": entity,
            "tt_from": {"$lt": tt_to},
            "tt_to": {"$gt": tt_from},
            "vt_from": {"$lt": vt_to},
            "vt_to": {"$gt": vt_from}
        }
        
        # Get matching vrefs from Index with explain for memory stats
        index_explain = await self.db.command("explain", {"find": "Index", "filter": index_query})
        cursor = self.db.Index.find(index_query)
        
        vrefs = [doc["vref"] for doc in await cursor.to_list(length=None)]
        
        if not vrefs:
            return []
            
        # Query Payload collection for the actual data
        if len(vrefs) == 1:
            payload_query = {"vref": vrefs[0]}
        else:
            payload_query = {"vref": {"$in": vrefs}}
        
        cursor = self.db.Payload.find(payload_query)
        
        # Return the data from matching payloads
        results = []
        async for doc in cursor:
            results.append(doc)
        
        # Log memory usage information
        final_memory = await self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Log query execution stats
        self._log_query_stats("Index Range Query", index_explain)
            
        return results

    async def range_query_by_attribute(self, attribute_name, attribute_value, vt_from, vt_to, tt_from, tt_to, entity="Student"):
        """Range query records by a single attribute within VT and TT intervals."""
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
        # Query the Index collection to find matching records within time intervals
        index_query = {
            attribute_name: attribute_value,
            "entity": entity,
            "tt_from": {"$lt": tt_to},
            "tt_to": {"$gt": tt_from},
            "vt_from": {"$lt": vt_to},
            "vt_to": {"$gt": vt_from}
        }
        
        # Get matching vrefs from Index with explain for memory stats
        index_explain = await self.db.command("explain", {"find": "Index", "filter": index_query})
        cursor = self.db.Index.find(index_query)
        
        vrefs = [doc["vref"] for doc in await cursor.to_list(length=None)]
        
        if not vrefs:
            return []
            
        # Query Payload collection for the actual data
        if len(vrefs) == 1:
            payload_query = {"vref": vrefs[0]}
        else:
            payload_query = {"vref": {"$in": vrefs}}
        
        cursor = self.db.Payload.find(payload_query)
        
        # Return the data from matching payloads
        results = []
        async for doc in cursor:
            results.append(doc)
        
        # Log memory usage information
        final_memory = await self._get_memory_usage()
        memory_used = final_memory - initial_memory
        print(f"{self.name} Range Query Memory Usage: {memory_used:.2f} MB")
        
        # Log query execution stats
        self._log_query_stats(f"{attribute_name} Range Query", index_explain)
            
        return results

    async def delta_since_vt_range(self, attribute_name, attribute_value, vt_from, vt_to, tt, entity="Student"):
        """Find entities at two VT points (vt_from, tt) and (vt_to, tt).
        Returns a tuple (entity_at_start, entity_at_end) where each can be None if no entity exists."""
        timer = Timer("Delta Since VT Range Query Performance")
        timer.start()
        
        # Query for entity at (vt_from, tt)
        entity_at_start = await self._query_at_point(attribute_name, attribute_value, vt_from, tt, entity)
        entity_at_end = await self._query_at_point(attribute_name, attribute_value, vt_to, tt, entity)
        
        timer.stop()
        memory_usage = await self._get_memory_usage()
        print(f"{self.name} Delta VT Range Query - Memory: {memory_usage:.2f} MB")
        
        return (entity_at_start, entity_at_end)
    
    async def delta_since_tt_range(self, attribute_name, attribute_value, tt_from, tt_to, vt, entity="Student"):
        """Find entities at two TT points (vt, tt_from) and (vt, tt_to).
        Returns a tuple (entity_at_start, entity_at_end) where each can be None if no entity exists.
        If entities are identical, returns (None, None)."""
        timer = Timer("Delta Since TT Range Query Performance")
        timer.start()
        
        # Query for entity at (vt, tt_from) and (vt, tt_to)
        entity_at_start = await self._query_at_point(attribute_name, attribute_value, vt, tt_from, entity)
        entity_at_end = await self._query_at_point(attribute_name, attribute_value, vt, tt_to, entity)
        
        # If both entities exist and are identical, return (None, None)
        if entity_at_start and entity_at_end and entity_at_start == entity_at_end:
            entity_at_start = None
            entity_at_end = None
        
        timer.stop()
        memory_usage = await self._get_memory_usage()
        print(f"{self.name} Delta TT Range Query - Memory: {memory_usage:.2f} MB")
        
        return (entity_at_start, entity_at_end)
    
    async def _query_at_point(self, attribute_name, attribute_value, vt, tt, entity="Student"):
        """Helper method to query entity at a specific bitemporal point."""
        try:
            # Query the Index collection directly with attribute
            index_query = {
                attribute_name: attribute_value,
                "entity": entity,
                "tt_from": {"$lte": tt},
                "tt_to": {"$gt": tt},
                "vt_from": {"$lte": vt},
                "vt_to": {"$gt": vt}
            }
            
            cursor = self.db.Index.find(index_query)
            vrefs = [doc["vref"] for doc in await cursor.to_list(length=None)]
            
            if not vrefs:
                return None
            
            # Get the payload for the first matching vref
            payload = await self.db.Payload.find_one({"vref": vrefs[0]})
            return payload
            
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
    
    def _log_query_stats(self, query_name, explain_result):
        """Log query execution statistics."""
        try:
            execution_stats = explain_result.get("executionStats", {})
            if execution_stats:
                docs_examined = execution_stats.get("totalDocsExamined", 0)
                docs_returned = execution_stats.get("totalDocsReturned", 0)
                execution_time = execution_stats.get("executionTimeMillis", 0)
                print(f"{self.name} {query_name} Stats: {docs_examined} docs examined, {docs_returned} returned, {execution_time}ms")
        except Exception:
            pass