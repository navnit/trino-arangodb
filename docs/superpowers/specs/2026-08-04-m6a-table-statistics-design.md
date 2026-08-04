# M6-A — `getTableStatistics` row-count statistics: design

**Status:** design (approved in-session 2026-08-04).
**Milestone:** M6, slice A. Master spec §6.6. Exit criterion inherited from the M6 row: *row-count stats surfaced*.

Master spec §6.6 asks for a row-count estimate from the cheap collection count and, "when a pushed constraint exists, a best-effort selectivity estimate." The second half does not survive costing: an exact filtered count is a server-side `O(collection)` scan executed at *planning* time, per query planned, and there is no cheap approximation between "exact scan" and "fabricated coefficient." This design keeps the first half and records the second as a rejected decision (§2).

## 1. Scope

**In scope:**

- `ArangoMetadata.getTableStatistics(ConnectorSession, ConnectorTableHandle)` returning a `TableStatistics` with a **row-count estimate only**.
- `ArangoClient.countDocuments(database, collection)` — the driver's collection-count endpoint (`GET /_api/collection/{name}/count`), which RocksDB serves as cheap exact metadata.
- A TTL cache for counts, mirroring the schema cache.
- Config: `arangodb.statistics-enabled` (default `true`), `arangodb.statistics.cache-ttl` (default `5m`).

**Out of scope (recorded decisions, not deferred bugs):**

- **Column statistics** (NDV / min / max / null-fraction / data size). Master spec §6.6 marks them optional/post-v1. Deriving them means sampling or `COLLECT AGGREGATE` scans — real cost, speculative benefit until a concrete cross-catalog join needs them.
- **Filtered-count selectivity.** A handle carrying a pushed `TupleDomain` reports the *base* collection count (base-JDBC precedent: remote stats are table-level regardless of pushed predicate). Documented consequence: for a *fully*-pushed filter no residual `FilterNode` remains, so the base count is the scan's estimate — an upper bound, not an expectation. For a prefilter-only push (`BIGINT` range) the residual filter remains and Trino applies its own selectivity coefficient on top. The alternative — running the filtered `COLLECT WITH COUNT` at planning time — was rejected for the planning-cost reason above; returning `empty()` for filtered handles was rejected because it starves the CBO on exactly the scans joins are built from.
- **`ANALYZE` support.** There is nothing to collect ahead of time; the count is read live (TTL-cached) at planning.

## 2. Behavior matrix

`getTableStatistics` is a guard chain; the first matching row wins.

| Handle state | Result | Why |
|---|---|---|
| `arangodb.statistics-enabled=false` | `TableStatistics.empty()` | Kill switch, consistent with every other gated behavior in the connector. |
| `ArangoQueryHandle` (M6-B passthrough) | `empty()` | Opaque user AQL — no collection to count. `instanceof`-decline **first**, before any cast, exactly like the four pushdown hooks. |
| `ArangoTableHandle` with an aggregation descriptor | `empty()` | Output rows are groups, not collection rows; a global `COUNT(*)` outputs exactly 1 row, so the base count would be arbitrarily wrong. |
| Handle with pushed limit `n` | `min(count, n)` | The final-output upper bound. Under shard fan-out each split applies `LIMIT n` independently, so the scan operator can transiently emit up to `splits × n` rows, but Trino always re-enforces the final limit (`limitGuaranteed=false` on fan-out — M3), so `min` is the honest post-limit estimate. |
| Plain or filtered `ArangoTableHandle` | cached collection count | §1 recorded decision: filter presence does not change the number. |

## 3. Components

- **`ArangoClient.countDocuments(String database, String collection)`** → `long`. Wraps the driver's `collection.count()`. Deliberately **not** `countWithShardIds(db, col, List.of())`: that method exists for probe fidelity — it runs an AQL `COLLECT WITH COUNT INTO` through the same query path a real scan uses, and the AQL optimizer is not guaranteed to collapse it to a metadata read. Statistics want the opposite trade: the metadata endpoint, guaranteed cheap.
- **Count cache** — a second Guava `Cache<SchemaTableName, Long>` field in `ArangoMetadata` beside `columnCache`, `expireAfterWrite(config.getStatisticsCacheTtl())`. Same construction pattern, same rationale: planning may call `getTableStatistics` several times per query and across concurrent queries; a 5-minute-stale row count is fine for costing.
- **`ArangoConfig`** — `statisticsEnabled` (boolean, default `true`; property `arangodb.statistics-enabled`) and `statisticsCacheTtl` (Airlift `Duration`, default `5m`; property `arangodb.statistics.cache-ttl`).
- **No new package.** The `split` and `aggregation` packages earned extraction by holding real invariants (`ShardEligibility`, `ColumnGuard`). This is a guard chain plus one cached client call; it lives in `ArangoMetadata` the way the schema cache does.

## 4. Error handling — one deliberate deviation from the translation rule

A failed count (network partition, auth failure, collection dropped between plan steps) → `TableStatistics.empty()` plus a `WARN` log naming the table and cause. This deviates from `ArangoMetadata`'s standing rule ("everything except 1228 is rethrown as `GENERIC_INTERNAL_ERROR` so real failures aren't swallowed"), and the deviation is intentional: statistics are advisory by SPI contract — `empty()` is the documented "unknown" — and failing *planning* over an optional signal would kill queries whose scan path still works. The `WARN` keeps the failure observable; the subsequent scan surfaces the real error with the real translation rule if the problem is persistent. Database-not-found (1228) needs no special case here: it takes the same `empty()` path, and the scan produces the proper `SchemaNotFoundException`.

Failures are **not negative-cached**: only successful counts enter the cache, so a transient failure is retried on the next planning call rather than pinning `empty()` for a TTL.

## 5. Testing

**Unit — `ArangoMetadataTest` additions** (existing patterns: hand-written `ArangoClient` test-double subclasses, no mocking framework):

- Query handle → `empty()`; aggregated handle → `empty()`; `statistics-enabled=false` → `empty()`.
- Plain handle → row count equals the double's count; filtered handle → same count (filter ignored by design).
- Pushed limit → `min(count, n)`, both orderings (limit below and above count).
- Throwing double → `empty()`, planning does not fail.
- Counting double → two `getTableStatistics` calls within TTL hit the server once; a thrown first call followed by a successful second call returns the count (no negative caching).

**Integration — `ArangoConnectorQueryTest`:**

- `SHOW STATS FOR (SELECT * FROM t)` against the live container surfaces the seeded row count. This is the milestone exit criterion made executable.
- `SHOW STATS FOR (SELECT * FROM TABLE(arango.system.query(...)))` (or, if `SHOW STATS` rejects a table-function source, the `EXPLAIN` estimate) shows unknown row count — the passthrough decline observed end-to-end.

No new assumptions-pin test: `collection.count()` is a documented public driver/HTTP API, unlike the internal `shardIds` option or AQL `COLLECT` semantics that earned pins.

## 6. Config reference delta

| Property | Default | Purpose |
|---|---|---|
| `arangodb.statistics-enabled` | `true` | `false` returns unknown statistics everywhere — kill switch. |
| `arangodb.statistics.cache-ttl` | `5m` | How long a collection row count is cached for planning. |
