# produce_backdated_xtdb_txs.py
from kafka import KafkaProducer
import time
from edn_format import dumps, Keyword, loads
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = "localhost:9092"   # change if needed
TOPIC = "xtdb-tx-log"                # Match your XTDB Kafka topic name from docker-compose.yml
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP])

def iso_to_epoch_ms(iso: str) -> int:
    # iso like "2023-01-01T00:00:00Z"
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)

def make_edn_tx(tx_id: int, tx_time_iso: str, ops):
    # ops is a list like: [["put", {"xt/id": "user/1", "name": "Alice"}, "2023-01-01T00:00:00Z"]]
    # Build a Python dict that edn-format serializes to EDN form
    edn_map = {
        Keyword("xtdb/tx-id"): tx_id,
        Keyword("xtdb/tx-time"): "#inst \"" + tx_time_iso + "\"",  # we'll inject as raw EDN inst
        Keyword("xtdb/tx-ops"): []
    }

    edn_ops = []
    for op in ops:
        # op: ["put", {"xt/id": "user/1", "name": "Alice"}, "2023-01-01T00:00:00Z"]
        op_name = Keyword(op[0]) if not isinstance(op[0], Keyword) else op[0]
        doc = op[1]
        vt_iso = op[2] if len(op) > 2 else tx_time_iso
        # we will create a representation like: [:xtdb.api/put { ... } #inst "..."]
        # but edn-format cannot easily emit raw #inst via dumps, so we build strings manually.
        edn_ops.append([op[0], doc, "#inst \"" + vt_iso + "\""])

    # Unfortunately edn_format.dumps will escape the "#inst ..." string as a normal string.
    # So we'll build the EDN text manually for the entry for correctness.
    # Build EDN payload string:
    ops_edn_parts = []
    for op in edn_ops:
        op_name = ":" + op[0] if not op[0].startswith(":") else op[0]
        doc_edn = dumps(op[1])
        vt_str = op[2]  # already '#inst "..."'
        ops_edn_parts.append(f"[{op_name} {doc_edn} {vt_str}]")
    ops_block = "[" + " ".join(ops_edn_parts) + "]"

    edn_payload = "{:xtdb/tx-id " + str(tx_id) + " :xtdb/tx-time " + "#inst \"" + tx_time_iso + "\" :xtdb/tx-ops " + ops_block + "}"
    return edn_payload

def send_tx(tx_id: int, tx_time_iso: str, ops):
    edn = make_edn_tx(tx_id, tx_time_iso, ops)
    ts_ms = iso_to_epoch_ms(tx_time_iso)
    # key can be tx id
    key = str(tx_id).encode("utf-8")
    value = edn.encode("utf-8")
    # kafka-python supports timestamp_ms param to producer.send
    future = producer.send(TOPIC, value=value, key=key, timestamp_ms=ts_ms)
    result = future.get(timeout=10)
    print("Sent tx_id", tx_id, "to partition", result.partition, "offset", result.offset, "timestamp_ms", ts_ms)

if __name__ == "__main__":
    # Example: backdate two transactions
    send_tx(1001, "2023-01-01T00:00:00Z", [
        ["xtdb.api/put", {"xt/id": "user/joe", "name": "Joe"}, "2023-01-01T00:00:00Z"]
    ])
    send_tx(1002, "2023-02-01T00:00:00Z", [
        ["xtdb.api/put", {"xt/id": "user/jane", "name": "Jane"}, "2023-02-01T00:00:00Z"]
    ])
    producer.flush()
    
    # Additional example with student data for bitemporal testing
    send_tx(1003, "2023-03-01T00:00:00Z", [
        ["xtdb.api/put", {"xt/id": "student/1", "name": "Alice", "age": 20, "grade": "A"}, "2023-03-01T00:00:00Z"]
    ])
    send_tx(1004, "2023-04-01T00:00:00Z", [
        ["xtdb.api/put", {"xt/id": "student/2", "name": "Bob", "age": 21, "grade": "B"}, "2023-04-01T00:00:00Z"]
    ])
    producer.flush()
    print("All transactions sent successfully!")