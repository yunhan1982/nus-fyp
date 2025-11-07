import asyncio
import json
import time
from datetime import datetime, timezone, timedelta

from core.xtdb_replay_solution import XTDBReplaySolution


async def collect_bench_results():
    solution = XTDBReplaySolution()
    await solution.connect()

    tt = datetime.now(timezone.utc)
    vt_candidates = [
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2021, 6, 1, tzinfo=timezone.utc),
        datetime(2019, 1, 1, tzinfo=timezone.utc),
    ]

    # Collect up to 10 distinct entity_ids across VT candidates
    entity_vt_map = {}
    for vt in vt_candidates:
        ids = await solution.get_all_entities(tt, vt)
        for eid in ids:
            if eid not in entity_vt_map:
                entity_vt_map[eid] = vt
                if len(entity_vt_map) >= 10:
                    break
        if len(entity_vt_map) >= 10:
            break

    if not entity_vt_map:
        await solution.cleanup()
        return []

    vt_from = datetime(2018, 1, 1, tzinfo=timezone.utc)
    vt_to = datetime(2025, 3, 1, tzinfo=timezone.utc)
    tt_from = tt - timedelta(days=365)
    tt_to = tt

    results = []

    for eid, vt in list(entity_vt_map.items())[:10]:
        current = await solution.get_current_data(eid, tt, vt)
        if not current:
            continue

        name = current.get("name")
        age = current.get("age")
        attr1 = current.get("attr1")

        # Point
        t0 = time.perf_counter()
        point_rows = await solution.query_by_name_and_age(name, age, tt, vt)
        point_time = time.perf_counter() - t0

        # Range
        t0 = time.perf_counter()
        range_rows = await solution.range_query_by_name_and_age(name, age, vt_from, vt_to, tt_from, tt_to)
        range_time = time.perf_counter() - t0

        # Delta VT
        t0 = time.perf_counter()
        delta_vt_rows = await solution.delta_since_vt_range("age", age, vt_from, vt_to, tt)
        delta_vt_time = time.perf_counter() - t0

        # Delta TT
        t0 = time.perf_counter()
        delta_tt_rows = await solution.delta_since_tt_range("age", age, tt_from, tt_to, vt)
        delta_tt_time = time.perf_counter() - t0

        results.append({
            "entity_id": eid,
            "name": name,
            "age": age,
            "attr1": attr1,
            "tt": tt.isoformat(),
            "vt": vt.isoformat(),
            "point": {"rows": len(point_rows), "runtime_sec": point_time},
            "range": {"rows": len(range_rows), "runtime_sec": range_time},
            "delta_vt": {"rows": len(delta_vt_rows), "runtime_sec": delta_vt_time},
            "delta_tt": {"rows": len(delta_tt_rows), "runtime_sec": delta_tt_time},
        })

    await solution.cleanup()

    return results


async def main():
    results = await collect_bench_results()
    output_path = "data/xtdb_bench_results.json"
    with open(output_path, "w") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(), "results": results}, f, indent=2)
    print(f"Wrote {len(results)} benchmark entries to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())