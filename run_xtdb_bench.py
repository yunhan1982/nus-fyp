import asyncio
import time
from datetime import datetime, timezone, timedelta

from core.xtdb_replay_solution import XTDBReplaySolution


async def pick_existing_entity(solution: XTDBReplaySolution, tt: datetime, vt_candidates):
    for vt in vt_candidates:
        ids = await solution.get_all_entities(tt, vt)
        if ids:
            return ids[0], vt
    return None, None


async def main():
    solution = XTDBReplaySolution()
    await solution.connect()

    # Choose TT as now, and try VT dates known to be within generated data range
    tt = datetime.now(timezone.utc)
    vt_candidates = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2021, 6, 1, tzinfo=timezone.utc),
        datetime(2019, 1, 1, tzinfo=timezone.utc),
    ]

    entity_id, vt = await pick_existing_entity(solution, tt, vt_candidates)
    if not entity_id:
        print("No entities found for the tested VT dates. Please ensure XTDB is populated.")
        await solution.cleanup()
        return

    # Fetch current data to get attributes that are guaranteed to exist
    current = await solution.get_current_data(entity_id, tt, vt)
    if not current:
        print(f"Entity {entity_id} has no data at TT={tt.isoformat()} VT={vt.isoformat()}")
        await solution.cleanup()
        return

    name = current.get("name")
    age = current.get("age")
    attr1 = current.get("attr1")

    print(f"Using entity_id={entity_id}, name={name}, age={age}, attr1={attr1}, TT={tt.isoformat()}, VT={vt.isoformat()}")

    # Point query
    t0 = time.perf_counter()
    point_rows = await solution.query_by_name_and_age(name, age, tt, vt)
    point_time = time.perf_counter() - t0
    print(f"Point query results: {len(point_rows)} rows, runtime: {point_time:.6f}s")

    # Range query (use broad VT range within generated data window and recent TT range)
    vt_from = datetime(2018, 1, 1, tzinfo=timezone.utc)
    vt_to = datetime(2025, 3, 1, tzinfo=timezone.utc)
    tt_from = tt - timedelta(days=365)
    tt_to = tt

    t0 = time.perf_counter()
    range_rows = await solution.range_query_by_name_and_age(name, age, vt_from, vt_to, tt_from, tt_to)
    range_time = time.perf_counter() - t0
    print(f"Range query results: {len(range_rows)} rows, runtime: {range_time:.6f}s")

    # Delta since VT range on "age" using the same value from current row
    t0 = time.perf_counter()
    delta_vt_rows = await solution.delta_since_vt_range("age", age, vt_from, vt_to, tt)
    delta_vt_time = time.perf_counter() - t0
    print(f"Delta-since-VT query results: {len(delta_vt_rows)} rows, runtime: {delta_vt_time:.6f}s")

    # Delta since TT range on "age" using the same value from current row
    t0 = time.perf_counter()
    delta_tt_rows = await solution.delta_since_tt_range("age", age, tt_from, tt_to, vt)
    delta_tt_time = time.perf_counter() - t0
    print(f"Delta-since-TT query results: {len(delta_tt_rows)} rows, runtime: {delta_tt_time:.6f}s")

    await solution.cleanup()


if __name__ == "__main__":
    asyncio.run(main())