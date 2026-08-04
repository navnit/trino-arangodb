# M6-A — `getTableStatistics` row-count statistics: design

**Status:** design (approved in-session 2026-08-04).
**Milestone:** M6, slice A. Master spec §6.6. Exit criterion inherited from the M6 row: *row-count stats surfaced*.

Master spec §6.6 asks for a row-count estimate from the cheap collection count and, "when a pushed constraint exists, a best-effort selectivity estimate." The second half does not survive costing: an exact filtered count is a server-side `O(collection)` scan executed at *planning* time, per query planned, and there is no cheap approximation between "exact scan" and "fabricated coefficient." This design keeps the first half and records the second as a rejected decision (§2).

## 1. Scope

**In scope:**

- `ArangoMetadata.getTableStatistics(ConnectorSession, ConnectorTableHandle)` returning a `TableStatistics` with a **row-count estimate only**.
- `ArangoClient.countDocuments(database, collection)` — the driver's collection-count endpoint (`GET /_api/collection/{name}/count`); cost characteristics and caveats in §3.
- A TTL cache for counts, mirroring the schema cache.
- Config: `arangodb.statistics-enabled` (default `true`), `arangodb.statistics.cache-ttl` (default `5m`).

**Out of scope (recorded decisions, not deferred bugs):**

- **Column statistics** (NDV / min / max / null-fraction / data size). Master spec §6.6 marks them optional/post-v1. Deriving them means sampling or `COLLECT AGGREGATE` scans — real cost, speculative benefit until a concrete cross-catalog join needs them.
- **Filtered-count selectivity.** A handle carrying a pushed `TupleDomain` reports the *base* collection count (base-JDBC precedent: remote stats are table-level regardless of pushed predicate). Documented consequence: for a *fully*-pushed filter no residual `FilterNode` remains, so the base count is the scan's estimate — an upper bound, not an expectation. Any *residual*-predicate shape loses the row count above the scan entirely: with no column statistics `FilterStatsRule` cannot estimate (the unknown source stats carry `NaN` null-fractions, and `default_filter_factor_enabled=false` — the engine default — disables the fallback coefficient), so the `FilterNode`'s output is unknown and unknown propagates upward. That covers the prefilter-only `BIGINT` range, residual `VARCHAR` range, `IS NULL`/`IS NOT NULL`, and *every* predicate under `type-coercion=strict` (where `isPushable` declines all pushdown). M6-A's practical reach is therefore the fully-pushed and unfiltered shapes; that is accepted. The alternative — running the filtered `COLLECT WITH COUNT` at planning time — was rejected for the planning-cost reason above; returning `empty()` for filtered handles was rejected because it starves the CBO on exactly the scans joins are built from.
- **`ANALYZE` support.** There is nothing to collect ahead of time; the count is read live (TTL-cached) at planning.

## 2. Behavior matrix

`getTableStatistics` is a guard chain; the first matching row wins.

| Handle state | Result | Why |
|---|---|---|
| `arangodb.statistics-enabled=false` | `TableStatistics.empty()` | Kill switch. Its meaning is §6's wording — "returns unknown statistics everywhere" — **not** merely "never count server-side": it deliberately suppresses even the zero-I/O local answers below (the global-aggregate `1`, the grouped-plus-limit cap), so the guard order is pinned, not incidental. |
| `ArangoQueryHandle` (M6-B passthrough) | `empty()` | Opaque user AQL — no collection to count. `instanceof`-decline **first**, before any cast, exactly like the four pushdown hooks. |
| `ArangoTableHandle` with an aggregation descriptor, empty `groupingColumns` | `Estimate.of(1)`, no client call | A global aggregate outputs exactly one row — known exactly, not unknown. Matches the engine's own `AggregationStatsRule` (`outputRowCount = 1` for an empty groupBy). |
| `ArangoTableHandle` with an aggregation descriptor, non-empty `groupingColumns`, pushed limit `n` | `Estimate.of(n)`, no client call | `applyLimit` reports `limitGuaranteed=true` for aggregated handles (always a single split, `LIMIT` rendered after the `COLLECT`), so `n` is an exact **upper bound** — the same principle as the `min(count, n)` row, and equally free. Unlike that row there is no second bound, so the estimate is loose for a large limit over few groups (`GROUP BY k LIMIT 1000000` claims `n` where the truth may be 3), and because the guaranteed limit leaves no `LimitNode` above the scan, nothing downstream corrects it. Accepted: tight in the common small-limit case, and never *under*-estimates. |
| `ArangoTableHandle` with an aggregation descriptor, non-empty `groupingColumns`, no limit | `empty()` | Output rows are groups; group cardinality is unknowable without NDV stats, and the base count would be arbitrarily wrong. |
| Handle with pushed limit `n`, **only when** `arangodb.shard-parallelism-enabled=false` | `min(count, n)` | Mirrors `applyLimit`'s `limitGuaranteed`: single-split execution makes the pushed limit exact, so `min` is the scan's true output bound. |
| Handle with pushed limit `n`, shard parallelism on (default) | cached collection count (no `min`) | With fan-out each split applies `LIMIT n` independently, so the scan node really can emit up to `splits × n` rows; the engine keeps a `LimitNode` above the scan (`limitGuaranteed=false`) and its own `LimitStatsRule` applies the `min` there. Pre-applying `min` at the scan would misstate the scan node and gain nothing downstream. |
| Plain or filtered `ArangoTableHandle` | cached collection count | §1 recorded decision: filter presence does not change the number. |

## 3. Components

- **`ArangoClient.countDocuments(String database, String collection)`** → `long`. Wraps the driver's `collection.count()`. Deliberately **not** `countWithShardIds(db, col, List.of())`: that method exists for probe fidelity — it runs an AQL `COLLECT WITH COUNT INTO` through the same query path a real scan uses, and the AQL optimizer is not guaranteed to collapse it to a metadata read. Statistics want the opposite trade: the metadata endpoint, which is cheap exact metadata on a single-server RocksDB deployment; on a cluster a coordinator-served count is a per-shard fan-out (mitigated by the TTL cache), and its cost on Enterprise smart/edge collections is unverified with the community test image — the wording here is deliberately not "guaranteed cheap." The driver returns `CollectionPropertiesEntity.getCount()` as a nullable boxed `Long`; a `null` or negative must not reach `TableStatistics`, whose constructor throws on a negative row count. **Mechanism, named:** `countDocuments` returns `long` and **throws `IllegalStateException`** on a `null`/negative count; genuine server errors propagate as the driver's `ArangoDBException` (unchecked). On the metadata side the count runs inside `countCache.get(key, Callable)`, and the catch surface is `catch (ExecutionException | UncheckedExecutionException e)` — Guava wraps the Callable's checked throwables in the former and unchecked ones (both exception types above) in the latter, so one catch covers everything. This is deliberately **not** `resolve()`'s pattern: `resolve()` unwraps and *rethrows* via `throwIfUnchecked`; this path unwraps only to log the cause and returns `empty()` (§4).
- **Count cache** — a second Guava `Cache<SchemaTableName, Long>` field in `ArangoMetadata` beside `columnCache`, `expireAfterWrite(config.getStatisticsCacheTtl())`, built with `.ticker(ticker)` from the existing `@VisibleForTesting` constructor (`ArangoMetadata(client, resolver, config, Ticker)`) — that injected ticker plus `ArangoMetadataTest.ManualTicker` is the only deterministic way §5's TTL tests can exist. Same construction pattern as `columnCache`, same rationale: planning may call `getTableStatistics` several times per query and across concurrent queries; a 5-minute-stale row count is fine for costing.
- **`ArangoConfig`** — `statisticsEnabled` (boolean, default `true`; property `arangodb.statistics-enabled`) and `statisticsCacheTtl` (Airlift `Duration`, default `5m`; property `arangodb.statistics.cache-ttl`).
- **No new package.** The `split` and `aggregation` packages earned extraction by holding real invariants (`ShardEligibility`, `ColumnGuard`). This is a guard chain plus one cached client call; it lives in `ArangoMetadata` the way the schema cache does.
- **`precalculateStatistics` stays `false` — now a live decision, record it.** All four pushdown hooks pass `false` as the trailing `precalculateStatistics` argument of their `*ApplicationResult`. Before M6-A that flag was inert (stats were unknown either way); once `getTableStatistics` returns a row count it becomes the engine's lever over post-pushdown estimates (`statistics_precalculation_for_pushdown_enabled` defaults to `true`, so the engine would honor a `true`). Keeping `false` is correct: for filters, precalculation would run `FilterStatsRule` over a source with no column statistics, and with `default_filter_factor_enabled=false` (the engine default) that yields *unknown* — i.e. it would silently replace the base count with exactly the `empty()` §1 rejected. Do **not** flip these flags as part of M6-A.

## 4. Error handling — one deliberate deviation from the translation rule

A failed count (network partition, auth failure, collection dropped between plan steps; also a `null`/negative driver count per §3) → `TableStatistics.empty()` plus a `WARN` log naming the table and cause. This deviates from `ArangoMetadata`'s standing rule ("everything except 1228 is rethrown as `GENERIC_INTERNAL_ERROR` so real failures aren't swallowed"), and the deviation is intentional: statistics are advisory by SPI contract — `empty()` is the documented "unknown" — and failing *planning* over an optional signal would kill queries whose scan path still works. The `WARN` keeps the failure observable; if the problem is persistent, the subsequent scan fails on its own (note the read path has no translation rule — `ArangoPageSourceProvider`/`ArangoPageSource` propagate the raw driver exception; translation lives only in `ArangoMetadata` and `ArangoQueryFunction`), so the swallowed count failure never masks a real outage. Database-not-found (1228) needs no special case here: it takes the same `empty()` path, and the missing database is reported through the existing channels — `getTableHandle` returns `null` on 1228 (Trino's "table does not exist") and `listTables` skips the schema. (`SchemaNotFoundException` itself is thrown only on the M6-B passthrough path in `ArangoQueryFunction`, whose handles §2 declines before any count.)

Failures are **not negative-cached**: only successful counts enter the cache, so a transient failure is retried on the next planning call rather than pinning `empty()` for a TTL. Accepted consequence, stated explicitly: during a persistent outage every planning call against every affected table re-attempts and re-logs at `WARN`. Rate-limiting the log would need per-table failure state for a scenario in which queries are failing loudly anyway; the simple behavior is chosen deliberately.

## 5. Testing

**Unit — new `ArangoMetadataStatisticsTest`** (per-area convention alongside `ArangoMetadataLimitTest`/`ArangoMetadataAggregationTest`/`ArangoMetadataPassthroughTest`; existing patterns: hand-written `ArangoClient` test-double subclasses, no mocking framework):

- Query handle → `empty()`; grouped aggregated handle without limit → `empty()`; global aggregated handle (empty `groupingColumns`) → row count 1 with **zero client calls**; grouped aggregated handle with pushed limit `n` → row count `n` with **zero client calls**.
- `statistics-enabled=false` → `empty()` with **zero client calls** even against a throwing double — the flag short-circuits before the client, which is what makes it a kill switch rather than a result filter.
- Plain handle → row count equals the double's count; filtered handle → same count (filter ignored by design); empty collection → `Estimate.of(0)`.
- Pushed limit with `shard-parallelism-enabled=false` → `min(count, n)`, both orderings (limit below and above count); with shard parallelism on (default) → base count, no `min`.
- Handle carrying *both* an aggregation and a limit → the aggregation rows win (pins §2's guard ordering): global + limit → 1, never `min`; grouped + limit → `n` from the aggregation row, never the counted path.
- Throwing double → `empty()`, planning does not fail.
- Counting double + `ManualTicker`: two calls within TTL hit the server once; advancing past TTL re-counts; a thrown first call followed by a successful second call returns the count (no negative caching); two different tables get independent cache entries (key isolation).

**Config — `ArangoConfigTest`:** both new properties added to `assertRecordedDefaults` and `assertFullMapping` — Airlift's `ConfigAssertions` fails the existing test the moment a `@Config` property exists without them.

**Integration — `ArangoConnectorQueryTest`:**

- `SHOW STATS FOR (SELECT * FROM t)` against the live container surfaces the seeded row count. This is the milestone exit criterion made executable. Assertion shape: `SHOW STATS` emits one row per output column plus a summary row, and with no column statistics the per-column rows are all-NULL — assert on the summary row (`column_name IS NULL`, read `row_count`), not the full result set.
- `SHOW STATS FOR (SELECT * FROM TABLE(arango.system.query(...)))` shows unknown row count — the passthrough decline observed end-to-end (`ShowStatsRewrite` accepts any table subquery, so no `EXPLAIN` fallback is needed).

No new assumptions-pin test: `collection.count()` is a documented public driver/HTTP API, unlike the internal `shardIds` option or AQL `COLLECT` semantics that earned pins.

Deliberate coverage gap, recorded: `countDocuments`'s own null/negative `IllegalStateException` branch is untestable against a live server (the driver always returns a real count; a missing collection raises `ArangoDBException` instead) and the no-mocking policy rules out faking the driver. The metadata-side contract — an `IllegalStateException` from the count degrading to `empty()` — *is* covered, via the throwing test double.

## 6. Config reference delta

| Property | Default | Purpose |
|---|---|---|
| `arangodb.statistics-enabled` | `true` | `false` returns unknown statistics everywhere — kill switch. |
| `arangodb.statistics.cache-ttl` | `5m` | How long a collection row count is cached for planning. |

Naming note: the hyphen-leaf `-enabled` suffix and the dot-group `.cache-ttl` follow existing precedent (`arangodb.aggregation-pushdown-enabled`, `arangodb.schema.cache-ttl`) — the same word appearing both ways is deliberate, not an inconsistency. Both properties are also added to `README.md`'s user-facing config table, which documents every other knob.
