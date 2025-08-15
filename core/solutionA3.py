import hashlib
from bson.binary import Binary, UUID_SUBTYPE
from uuid import UUID
from typing import List
import motor.motor_asyncio
from .bitemporal_space import Rectangle
from .utils.timing import Timer


class SolutionA3: 
    def __init__(self) -> None:
        self.name = "SolutionA3"
        self.client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://localhost:27017/', uuidRepresentation='standard')
        self.db = self.client[self.name]
        self.indices = ["name", "age", "attr1", "attr2", "attr3", "attr4"]

    # async def cleanup(self):
    #     """Destructor to ensure client is closed"""
    #     if hasattr(self, 'client'):
    #         await self.client.close()

    async def initialize_collections(self):
        # Drop and create collections with indexes
        await self.db.Payloads.drop()
        await self.db.Timeslices.drop()
        await self.db.Index.drop()

        await self.db.Payloads.create_index("vref", unique=True)
        # await self.db.Timeslices.create_index([("eref", 1), ("seq_no", 1)], unique=True)

        await self.db.Index.create_index([
            ("hash", 1),
            ("vt_from", 1),
            ("tt_from", 1),
            ("vt_to", 1),
            ("tt_to", 1)
        ])

        print(f"{self.name} Index, Timeslices and Payload collection schemas created with appropriate indexes.")


    # Main function to insert rectangles
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        # Prepare batch document collections
        payloads_batch = []
        timeslices_batch = []
        index_batch = []
        
        # Track unique keys to avoid duplicates
        payload_vrefs = set()
        timeslice_keys = set()    
        # Process all rectangles
        for rect in rectangles:
            vref = UUID(bytes=hashlib.md5(str(rect.data["payload"]).encode()).digest())
            eref = rect.data.get("id", 0)
            seq_no = str(rect.index)
            # Payload document
            vref_binary = Binary.from_uuid(vref)
            if vref_binary not in payload_vrefs:
                payload = {
                    "vref": vref_binary,
                    "name": rect.data["payload"].get("name"),
                    "age": rect.data["payload"].get("age")
                }
                payloads_batch.append(payload)
                payload_vrefs.add(vref_binary)
            
            # Timeslice document
            timeslice_key = (eref, seq_no)

            if timeslice_key not in timeslice_keys:
                timeslice = {
                    "vref": vref_binary,
                    "eref": eref,
                    "seq_no": seq_no,  # For simplicity, using fixed sequence number
                    "tt_from": rect.tt_from,
                    "tt_to": rect.tt_to,
                    "vt_from": rect.vt_from,
                    "vt_to": rect.vt_to
                }
                timeslices_batch.append(timeslice)
                timeslice_keys.add(timeslice_key)
            

            for key in self.indices:
                # Compute MD5 hash for the specific data[key]
                item = {key: rect.data["payload"][key]}
                key_hash = hashlib.md5(str(item).encode('utf-8')).hexdigest()
                    
                index_entry = {
                    "vref": vref_binary,
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
                print(f"{self.name}: Inserted {len(payloads_batch)} documents into Payload collection")
            except Exception as e:
                print(f"Some payload inserts failed: {e}")
        
        if timeslices_batch:
            try:
                await self.db.Timeslices.insert_many(timeslices_batch, ordered=False)
                print(f"{self.name}: Inserted {len(timeslices_batch)} documents into Timeslices collection")
            except Exception as e:
                print(f"Some timeslice inserts failed: {e}")
        
        if index_batch:
            try:
                await self.db.Index.insert_many(index_batch, ordered=False)
                print(f"{self.name}: Inserted {len(index_batch)} documents into Index collection")
            except Exception as e:
                print(f"Some index inserts failed: {e}")

    async def query_by_name_and_age(self, name, age, tt, vt, entity="Student"):
        """Query records by both name and age with timing for each step."""
        timer = Timer("MongoDB Query Performance")
        timer.start()
        
        # Track memory usage
        initial_memory = await self._get_memory_usage()
        
        try:
            # Step 1: Create hash for name query
            name_item = {"name": name}
            name_hash = Binary(bytes.fromhex(hashlib.md5(str(name_item).encode('utf-8')).hexdigest()), UUID_SUBTYPE)
            timer.stage("Hash Creation - Name")

            name_query = {
                "hash": name_hash,
                "entity": entity,
                "tt_from": {"$lte": tt},
                "tt_to": {"$gt": tt},
                "vt_from": {"$lte": vt},
                "vt_to": {"$gt": vt},
                "key": "name"
            }
            
            # Step 2: Execute name query with explain for memory stats
            name_explain = await self.db.command("explain", {"find": "Index", "filter": name_query})
            name_index_entries = await self.db.Index.find(name_query).to_list(None)
            timer.stage("Name Query Execution")
            
            name_index_entries = [entry for entry in name_index_entries 
                                if entry.get("data", {}).get("name") == name]
            timer.stage("Name Filter")
            
            if not name_index_entries:
                return []
            
            # Step 3: Get erefs
            erefs = {entry["eref"] for entry in name_index_entries}
            timer.stage("ERef Extraction")

            # Step 4: Create hash for age query
            age_item = {"age": age}
            age_hash = Binary(bytes.fromhex(hashlib.md5(str(age_item).encode('utf-8')).hexdigest()), UUID_SUBTYPE)
            timer.stage("Hash Creation - Age")
            
            age_query = {
                "hash": age_hash,
                "entity": entity,
                "eref": {"$in": list(erefs)},
                "tt_from": {"$lte": tt},
                "tt_to": {"$gt": tt},
                "vt_from": {"$lte": vt},
                "vt_to": {"$gt": vt},
                "key": "age"
            }
            
            # Step 5: Execute age query with explain for memory stats
            age_explain = await self.db.command("explain", {"find": "Index", "filter": age_query})
            age_index_entries = await self.db.Index.find(age_query).to_list(None)
            timer.stage("Age Query Execution")
            
            age_index_entries = [entry for entry in age_index_entries 
                                if entry.get("data", {}).get("age") == age]
            timer.stage("Age Filter")
            
            if not age_index_entries:
                return []
            
            # Step 6: Get matching vrefs
            matching_vrefs = {entry["vref"] for entry in age_index_entries}
            timer.stage("vref Extraction")
            
            # Step 7: Final payload query with explain for memory stats
            if matching_vrefs:
                if len(matching_vrefs) == 1:
                    vref = next(iter(matching_vrefs))
                    payload_query = {"vref": vref}
                else:
                    payload_query = {"vref": {"$in": list(matching_vrefs)}}
                
                payload_explain = await self.db.command("explain", {"find": "Payloads", "filter": payload_query})
                payloads = await self.db.Payloads.find(payload_query).to_list(None)
                timer.stage("Payload Query")
                
                # Log memory usage information
                final_memory = await self._get_memory_usage()
                memory_used = final_memory - initial_memory
                print(f"{self.name} Memory Usage: {memory_used:.2f} MB")
                
                # Log query execution stats if available
                self._log_query_stats("Name Query", name_explain)
                self._log_query_stats("Age Query", age_explain)
                self._log_query_stats("Payload Query", payload_explain)
                
                return payloads
            
            return []
        finally:
            timer.stop()
    
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

