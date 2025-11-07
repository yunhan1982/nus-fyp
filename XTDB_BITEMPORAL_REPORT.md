# XTDB Bitemporal Queries Report

## Overview

XTDB is a bitemporal database, meaning it tracks data across two independent axes of time:

- Valid Time (VT): when the fact is true in the domain.
- System Time (TT): when the database recorded (or observed) the fact.

This enables queries like:

- Point-in-time reads: data that was valid at VT and visible at TT.
- Range reads: data spanning VT or TT intervals.
- Delta queries: changes that occurred within VT or TT ranges.

XTDB’s SQL wire supports bitemporal clauses using `FOR SYSTEM_TIME` and `FOR VALID_TIME` with `AS OF`, `FROM`, and `TO` modifiers. Examples used in this report:

- Point query: `FOR SYSTEM_TIME AS OF <tt> FOR VALID_TIME AS OF <vt>`
- VT range query: `FOR SYSTEM_TIME AS OF <tt> FOR VALID_TIME FROM <vt_from> TO <vt_to>`
- TT range query: `FOR SYSTEM_TIME FROM <tt_from> TO <tt_to> FOR VALID_TIME AS OF <vt>`

## Integration with Our Data

- Data model: `student` entity with attributes like `entity_id`, `name`, `age`, `attr1`, `attr2`, `attr3`, `attr4`.
- Ingestion: Kafka log replay is used to reconstruct XTDB state and preserve accurate TT/Vt histories.
- Query layer: We use an `XTDBReplaySolution` implementation over the Postgres wire (XTDB v2), encoding time parameters as ISO strings for `FOR` clauses and casting numeric fields to text for safe parameter binding.

## Methodology

- TT chosen as current time at query execution.
- VT candidates chosen from known valid windows: 2019-01-01, 2021-06-01, 2024-01-01.
- For each found entity (up to 10), we ran:
  - Point query by `name` and `age` at TT/VT.
  - Range query by `name` and `age` over VT range [2018-01-01, 2025-03-01] and TT range [TT-365d, TT].
  - Delta-since-VT query for `age` (VT range above, TT = AS OF now).
  - Delta-since-TT query for `age` (TT range above, VT = AS OF entity’s VT).

## Results (10 entities)

| #   | Entity ID                            | Name         | Age | Attr1 | TT                               | VT                        | Point Rows | Point Runtime (s) | Range Rows | Range Runtime (s) | Delta VT Rows | Delta VT Runtime (s) | Delta TT Rows | Delta TT Runtime (s) |
| --- | ------------------------------------ | ------------ | --- | ----- | -------------------------------- | ------------------------- | ---------- | ----------------- | ---------- | ----------------- | ------------- | -------------------- | ------------- | -------------------- |
| 1   | d15d04cc-74e4-4ed0-bea7-7467261f30b4 | Student_612  | 17  | 40    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.1985991250      | 1          | 0.1935783750      | 933           | 1.5752445410         | 472           | 0.4185288330         |
| 2   | 6a4f2b43-2880-4501-a762-d8c03ee41878 | Student_1127 | 10  | 6     | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.1091192080      | 1          | 0.3382082500      | 889           | 0.7047039580         | 447           | 0.7291905839         |
| 3   | c5e916b6-e2f6-4dec-be61-e1e6e48f9377 | Student_766  | 15  | 10    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.1281316250      | 1          | 0.1354089999      | 931           | 0.4856448330         | 469           | 0.4844982919         |
| 4   | e404c6e1-a3aa-4d3e-b5cc-e37fa38a2a1e | Student_2380 | 19  | 36    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.0630954589      | 1          | 0.0933523749      | 881           | 0.5066938339         | 446           | 0.4904762079         |
| 5   | 0dc28769-06b0-44e5-8e5a-f8af9146c790 | Student_1007 | 17  | 23    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.0592246669      | 1          | 0.0632394999      | 933           | 0.5271946249         | 472           | 0.4800962080         |
| 6   | 304cd376-7412-46f4-97a8-3bdadef20d48 | Student_4672 | 13  | 20    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 2          | 0.1368094999      | 3          | 0.1290185419      | 839           | 0.3769784999         | 424           | 0.3927855420         |
| 7   | 03e47d5c-afea-4552-87de-4047c639c0ec | Student_4349 | 18  | 14    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.2666294579      | 1          | 0.1352036659      | 928           | 0.4771824580         | 469           | 0.4322004170         |
| 8   | c345c96f-4e77-4928-a4e6-a8784819e6fe | Student_176  | 20  | 34    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.0910647499      | 1          | 0.0948695419      | 933           | 0.4186570419         | 473           | 0.4370581659         |
| 9   | 91297c5a-b40d-4e5b-8938-7270c872452d | Student_4021 | 18  | 34    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.1197495840      | 1          | 0.1926306660      | 928           | 1.9082147079         | 469           | 0.8131805000         |
| 10  | 46696eef-f270-4f9d-8aab-b836a9bfe90b | Student_2891 | 12  | 16    | 2025-11-07T06:49:40.417172+00:00 | 2024-01-01T00:00:00+00:00 | 1          | 0.1238233750      | 1          | 0.3949275829      | 927           | 0.4449164170         | 469           | 0.3777186669         |

## Summary Statistics

- Average point query runtime: ~0.1296s
- Average range query runtime: ~0.1770s
- Average delta-since-VT runtime: ~0.7425s
- Average delta-since-TT runtime: ~0.5056s

Observations:

- Point queries consistently return 1 row for most entities, with low latency.
- Range queries over broad VT/TT windows remain sub-0.4s in most cases.
- Delta queries return hundreds to over 900 rows depending on attribute history and time windows; VT deltas trend slower than TT deltas due to wider valid intervals and more changes.

## Notes

- The query layer encodes time arguments as ISO strings for `FOR` clauses and casts numeric attributes to text to align with parameter binding expectations.
- Kafka log replay ensures TT fidelity, which is critical for accurate bitemporal semantics.
- Runtimes can vary based on XTDB cache warmup and environment load.

## Performance Analysis

### Experimental Setup

- Hardware: Apple Silicon Mac (developer machine), local XTDB v2 server via Postgres wire, local Kafka + Zookeeper.
- Dataset characteristics: Synthetic student records generated across [2018-01-01, 2025-03-01] with randomized names, ages, and attributes; ingestion via Kafka replay to preserve TT chronology.
- Workload: For 10 distinct entities, each workload includes 4 queries (point, VT range, VT delta, TT delta).
- Time encoding: ISO-8601 strings for `FOR SYSTEM_TIME` and `FOR VALID_TIME` clauses; attribute values bound as strings to satisfy wire expectations.

### Metrics and Method

- Latency: wall-clock elapsed per query from the client.
- Cardinality: number of rows returned per query.
- Averages reported across the 10-entity sample; raw per-entity results are tabulated above.

### Results Interpretation

- Point queries: ~0.13s average with single-row returns, indicating minimal overhead for resolving bitemporal `AS OF` snapshots when predicates are selective (name + age).
- Range queries: ~0.18s average with single-row returns, demonstrating that adding `FROM/TO` windows for VT and a sliding TT window incurs modest additional planning/execution overhead.
- Delta-since-VT: ~0.74s average, 839–933 rows typical; VT deltas traverse wider domain-valid intervals and thus evaluate more temporal segments, leading to higher runtime and result cardinality.
- Delta-since-TT: ~0.51s average, 424–473 rows typical; TT delta windows (1 year) produce fewer segments than VT spans, resulting in lower cardinality and faster execution than VT deltas.

### Bottlenecks and Trade-offs

- Temporal expansion: `FOR VALID_TIME FROM/TO` expands the search space along VT; when paired with `FOR SYSTEM_TIME AS OF`, the system snapshot is fixed but valid-segment enumeration still dominates.
- Predicate selectivity: Equality on `name` and `age` avoids large scans; switching to less selective predicates (or additional attributes) would likely increase both latency and cardinality.
- Parameter binding and casting: Casting numeric fields to text mitigates wire-type mismatches but may impose minor overhead; future work should align native numeric typing in the driver layer.
- Cache state: First-run queries are slower due to cold caches; repeated runs showed reduced latency for identical shapes, especially point and range queries.

### Implications

- XTDB’s bitemporal SQL surface is expressive and composable for operational analytics over time; the ability to articulate TT vs. VT semantics directly in SQL simplifies adoption.
- For workloads dominated by delta queries across broad VT ranges, partitioning strategies or query-time filters (e.g., narrowing VT spans or additional predicates) can substantially reduce runtime and output size.
- Point-in-time and narrow-range reads are efficient and predictable, suitable for request-response APIs with temporal semantics.
