import asyncio
import asyncpg
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from kafka import KafkaProducer
from edn_format import dumps, Keyword
from .bitemporal_space import Rectangle
from .xtdb_solution import XTDBSolution

class XTDBReplaySolution(XTDBSolution):
    """XTDB solution that uses Kafka log replay for accurate bitemporal data storage"""
    
    def __init__(self, host: str = "localhost", port: int = 5433, 
                 database: str = "xtdb", user: str = "xtdb", password: str = "xtdb",
                 kafka_bootstrap: str = "localhost:9092", kafka_topic: str = "xtdb-tx-log"):
        super().__init__(host, port, database, user, password)
        self.name = "XTDB Replay Solution (Kafka Log Replay)"
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        self.producer = None
        self.tx_id_counter = 1000  # Start from 1000 to avoid conflicts
        
    async def connect(self):
        """Connect to both XTDB and Kafka"""
        await super().connect()
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_bootstrap],
                value_serializer=lambda x: x.encode('utf-8'),
                key_serializer=lambda x: str(x).encode('utf-8')
            )
            print(f"{self.name}: Connected to Kafka at {self.kafka_bootstrap}")
        except Exception as e:
            print(f"{self.name}: Warning - Could not connect to Kafka: {e}")
    
    async def cleanup(self):
        """Close both XTDB and Kafka connections"""
        await super().cleanup()
        if self.producer:
            self.producer.close()
            self.producer = None
            print(f"{self.name}: Disconnected from Kafka")
    
    def _datetime_to_iso(self, dt: datetime) -> str:
        """Convert datetime to ISO string format for XTDB"""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace('+00:00', 'Z')
    
    def _iso_to_epoch_ms(self, iso: str) -> int:
        """Convert ISO string to epoch milliseconds"""
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    
    def _make_edn_tx(self, tx_id: int, tx_time_iso: str, ops: List) -> str:
        """Create EDN transaction payload for XTDB"""
        ops_edn_parts = []
        for op in ops:
            op_name = ":" + op[0] if not op[0].startswith(":") else op[0]
            doc_edn = dumps(op[1])
            vt_str = op[2]  # already '#inst "..."'
            ops_edn_parts.append(f"[{op_name} {doc_edn} {vt_str}]")
        ops_block = "[" + " ".join(ops_edn_parts) + "]"
        
        edn_payload = (
            f"{{:xtdb/tx-id {tx_id} "
            f":xtdb/tx-time #inst \"{tx_time_iso}\" "
            f":xtdb/tx-ops {ops_block}}}"
        )
        return edn_payload
    
    def _send_tx_to_kafka(self, tx_id: int, tx_time: datetime, ops: List):
        """Send transaction to Kafka for log replay"""
        if not self.producer:
            print(f"{self.name}: Kafka producer not available")
            return
        
        tx_time_iso = self._datetime_to_iso(tx_time)
        edn = self._make_edn_tx(tx_id, tx_time_iso, ops)
        ts_ms = self._iso_to_epoch_ms(tx_time_iso)
        
        try:
            future = self.producer.send(
                self.kafka_topic, 
                value=edn, 
                key=tx_id, 
                timestamp_ms=ts_ms
            )
            result = future.get(timeout=10)
            print(f"{self.name}: Sent tx_id {tx_id} to partition {result.partition}, offset {result.offset}")
        except Exception as e:
            print(f"{self.name}: Error sending transaction to Kafka: {e}")
    
    async def insert_rectangle_to_collections(self, rectangles: List[Rectangle], entity: str = "Student") -> None:
        """Insert rectangles using Kafka log replay with proper bitemporal timestamps"""
        if not rectangles:
            return
        
        print(f"{self.name}: Inserting {len(rectangles)} rectangles to Kafka log for replay")
        
        for rect in rectangles:
            # Use the original transaction time from the rectangle for accurate replay
            tx_time = rect.tt_from
            vt_time = rect.vt_from
            
            # Create XTDB document from rectangle data
            doc_id = f"{entity.lower()}/{rect.data.get('id', rect.index)}"
            doc = {
                "xt/id": doc_id,
                **rect.data,
                "_vt_from": self._datetime_to_iso(rect.vt_from),
                "_vt_to": self._datetime_to_iso(rect.vt_to),
                "_tt_from": self._datetime_to_iso(rect.tt_from),
                "_tt_to": self._datetime_to_iso(rect.tt_to),
                "_index": rect.index
            }
            
            # Create transaction operation
            ops = [[
                "xtdb.api/put", 
                doc, 
                f"#inst \"{self._datetime_to_iso(vt_time)}\""
            ]]
            
            # Send to Kafka with the original transaction time
            self._send_tx_to_kafka(self.tx_id_counter, tx_time, ops)
            self.tx_id_counter += 1
        
        # Flush to ensure all messages are sent
        if self.producer:
            self.producer.flush()
        
        print(f"{self.name}: Successfully sent {len(rectangles)} transactions to Kafka")
    
    async def insert_data(self, rectangles: List[Rectangle], tt: datetime, vt: datetime, entity: str = "Student") -> bool:
        """Insert data using the original bitemporal timestamps from rectangles"""
        try:
            await self.insert_rectangle_to_collections(rectangles, entity)
            return True
        except Exception as e:
            print(f"{self.name}: Error inserting data: {e}")
            return False
    
    async def update_data(self, entity_id: str, updates: Dict[str, Any], tt: datetime, vt: datetime, entity: str = "Student") -> bool:
        """Update data using Kafka log replay"""
        try:
            doc_id = f"{entity.lower()}/{entity_id}"
            doc = {
                "xt/id": doc_id,
                **updates,
                "_updated_at": self._datetime_to_iso(vt)
            }
            
            ops = [[
                "xtdb.api/put", 
                doc, 
                f"#inst \"{self._datetime_to_iso(vt)}\""
            ]]
            
            self._send_tx_to_kafka(self.tx_id_counter, tt, ops)
            self.tx_id_counter += 1
            
            if self.producer:
                self.producer.flush()
            
            return True
        except Exception as e:
            print(f"{self.name}: Error updating data: {e}")
            return False
    
    async def query_by_name_and_age(self, name: str, age: int, tt: datetime, vt: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Query by name and age at specific bitemporal coordinates"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            # Query using XTDB's bitemporal capabilities
            tt_iso = self._datetime_to_iso(tt)
            vt_iso = self._datetime_to_iso(vt)
            
            rows = await self.connection.fetch("""
                SELECT * FROM student 
                FOR SYSTEM_TIME AS OF $1
                FOR VALID_TIME AS OF $2
                WHERE name = $3 AND CAST(age AS TEXT) = $4
            """, tt_iso, vt_iso, name, str(age))
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("QUERY_BY_NAME_AND_AGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error querying by name and age: {e}")
            return []
    
    async def range_query_by_name_and_age(self, name: str, age: int, vt_from: datetime, vt_to: datetime, 
                                        tt_from: datetime, tt_to: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Range query by name and age across bitemporal ranges"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            tt_from_iso = self._datetime_to_iso(tt_from)
            tt_to_iso = self._datetime_to_iso(tt_to)
            vt_from_iso = self._datetime_to_iso(vt_from)
            vt_to_iso = self._datetime_to_iso(vt_to)
            
            rows = await self.connection.fetch("""
                SELECT * FROM student 
                FOR SYSTEM_TIME FROM $1 TO $2
                FOR VALID_TIME FROM $3 TO $4
                WHERE name = $5 AND CAST(age AS TEXT) = $6
            """, tt_from_iso, tt_to_iso, vt_from_iso, vt_to_iso, name, str(age))
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("RANGE_QUERY_BY_NAME_AND_AGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error in range query by name and age: {e}")
            return []
    
    async def range_query_by_attribute(self, attribute_name: str, attribute_value: Any, vt_from: datetime, vt_to: datetime, 
                                     tt_from: datetime, tt_to: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Range query by any attribute across bitemporal ranges"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            tt_from_iso = self._datetime_to_iso(tt_from)
            tt_to_iso = self._datetime_to_iso(tt_to)
            vt_from_iso = self._datetime_to_iso(vt_from)
            vt_to_iso = self._datetime_to_iso(vt_to)
            
            # Dynamic query construction for any attribute
            query = f"""
                SELECT * FROM {entity.lower()} 
                FOR SYSTEM_TIME FROM $1 TO $2
                FOR VALID_TIME FROM $3 TO $4
                WHERE {attribute_name} = $5
            """
            
            rows = await self.connection.fetch(query, tt_from_iso, tt_to_iso, vt_from_iso, vt_to_iso, attribute_value)
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("RANGE_QUERY_BY_ATTRIBUTE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error in range query by attribute: {e}")
            return []
    
    async def delta_since_vt_range(self, attribute_name: str, attribute_value: Any, vt_from: datetime, vt_to: datetime, 
                                 tt: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Get changes in valid time range for specific attribute"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            tt_iso = self._datetime_to_iso(tt)
            vt_from_iso = self._datetime_to_iso(vt_from)
            vt_to_iso = self._datetime_to_iso(vt_to)
            
            query = """
                SELECT * FROM student 
                FOR SYSTEM_TIME AS OF $1
                FOR VALID_TIME FROM $2 TO $3
                WHERE CAST(age AS TEXT) = $4
            """
            
            rows = await self.connection.fetch(query, tt_iso, vt_from_iso, vt_to_iso, str(attribute_value))
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("DELTA_SINCE_VT_RANGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error getting delta since VT range: {e}")
            return []
    
    async def delta_since_tt_range(self, attribute_name: str, attribute_value: Any, tt_from: datetime, tt_to: datetime, 
                                 vt: datetime, entity: str = "Student") -> List[Dict[str, Any]]:
        """Get changes in transaction time range for specific attribute"""
        if not self.connection:
            await self.connect()
        
        try:
            start_time = asyncio.get_event_loop().time()
            memory_before = await self._get_memory_usage()
            
            tt_from_iso = self._datetime_to_iso(tt_from)
            tt_to_iso = self._datetime_to_iso(tt_to)
            vt_iso = self._datetime_to_iso(vt)
            
            query = """
                SELECT * FROM student 
                FOR SYSTEM_TIME FROM $1 TO $2
                FOR VALID_TIME AS OF $3
                WHERE CAST(age AS TEXT) = $4
            """
            
            rows = await self.connection.fetch(query, tt_from_iso, tt_to_iso, vt_iso, str(attribute_value))
            
            results = [dict(row) for row in rows]
            
            execution_time = asyncio.get_event_loop().time() - start_time
            memory_after = await self._get_memory_usage()
            await self._log_query_stats("DELTA_SINCE_TT_RANGE", len(results), execution_time, memory_after - memory_before)
            
            return results
            
        except Exception as e:
            print(f"{self.name}: Error getting delta since TT range: {e}")
            return []
    
    async def get_all_entities(self, tt: datetime, vt: datetime, entity: str = "Student") -> List[str]:
        """Get all entity IDs at specific bitemporal coordinates"""
        if not self.connection:
            await self.connect()
        
        try:
            tt_iso = self._datetime_to_iso(tt)
            vt_iso = self._datetime_to_iso(vt)
            
            rows = await self.connection.fetch(
                """
                SELECT DISTINCT entity_id FROM student 
                FOR SYSTEM_TIME AS OF $1
                FOR VALID_TIME AS OF $2
                """,
                tt_iso,
                vt_iso,
            )
            
            return [row["entity_id"] for row in rows]
            
        except Exception as e:
            print(f"{self.name}: Error getting all entities: {e}")
            return []