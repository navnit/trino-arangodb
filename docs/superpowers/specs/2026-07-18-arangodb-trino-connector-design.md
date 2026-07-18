# ArangoDB Trino Connector — Design Spec

- **Status:** Draft (rev 2 — incorporates Fable technical review)
- **Date:** 2026-07-18
- **Scope:** Read-only, built to scale (shard-parallel splits + filter/limit/projection/aggregation pushdown). No writes.
- **Target Trino:** latest stable line (~476/481 SPI); exact version pinned at implementation start. JDK requirement tracks that release — verify at pin time (recent lines require Java 23, may be Java 24). Build with Maven + Airlift bootstrap, matching Trino's build.
- **ArangoDB support:** minimum version pinned at implementation start (≥ 3.11 assumed); `arangodb-java-driver` **7.x over HTTP/2 + VelocyPack module** (VelocyStream was removed in the driver 7.x line and server-side in 3.12 — do not depend on it).
- **Language:** Java (mandatory — Trino connectors are JVM plugins loaded via the Connector SPI).

### Revision note (rev 2)
This revision fixes three correctness issues found in review: (1) pushed aggregation now runs on a **single split** because Trino treats connector aggregation output as final (§6.4); (2) pushed range filters now carry **type/null guards** because AQL has a total cross-type ordering (§6.1); (3) shard-parallel splits treat the AQL `shardIds` option as an **internal API** with a runtime duplication guard and version pin (§5.1). Plus statistics, connection resilience, a per-cell coercion policy, and numerous smaller fixes.

---

## 1. Overview & Goals

### 1.1 Purpose

Expose ArangoDB collections as queryable tables in Trino so operators can run distributed SQL analytics across ArangoDB data and join it with any other Trino catalog.

### 1.2 Goals

- Present each ArangoDB **database as a Trino schema** and each **collection (document or edge) as a table**.
- Resolve schemas for schemaless collections via a **layered strategy** (§4) that improves on the MongoDB connector's known weaknesses.
- Push work down to ArangoDB via **AQL generation**: `FILTER` (with type-safe guards), `LIMIT`, projection (including nested fields), and aggregation.
- Scale reads by generating **one split per shard subset** so Trino workers scan a sharded collection in parallel (non-aggregated queries).
- Provide a raw-AQL **`query()` table function** as the escape hatch for graph traversals and anything that does not map to relational SQL.
- Expose **table statistics** so cross-catalog join planning is cost-aware.

### 1.3 Non-Goals (explicit)

- **No writes.** No `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, or schema mutation by the connector.
- **No graph traversal engine.** AQL `FOR v IN OUTBOUND ...` is reachable only through the `query()` passthrough (§7).
- **No cross-collection join pushdown.** Joins run in the Trino engine.
- **No schema authoring UI.** The optional override collection is authored out-of-band.
- **No cross-split snapshot consistency.** Splits execute as independent AQL queries at different wall-clock times; a concurrently-mutated collection can yield a read that is not a single point-in-time snapshot (the MongoDB connector shares this limitation). Documented, not solved.

### 1.4 Success Criteria

- `SELECT` over document and edge collections returns correct, correctly-typed rows.
- A predicate + `LIMIT` query is fully pushed to AQL (`isFullyPushedDown()` in tests).
- On a sharded cluster, a full non-aggregated scan produces **N splits for N shards** and runs concurrently, and a CI check proves per-shard row counts **sum exactly** to the collection count (no gaps/dupes).
- Aggregations return correct results (validated against a single-node reference).
- The connector passes Trino's `BaseConnectorTest` read-side conformance suite against a Testcontainers ArangoDB.

---

## 2. Architecture

### 2.1 SPI Component Map

| Layer | Class | Responsibility |
|---|---|---|
| Plugin entry | `ArangoPlugin implements io.trino.spi.Plugin` | Returns the `ConnectorFactory`. |
| Factory | `ArangoConnectorFactory implements ConnectorFactory` | Reads catalog config, builds a Guice/Airlift injector, produces `ArangoConnector`. |
| Connector | `ArangoConnector implements Connector` | Wires transaction handle, `Metadata`, `SplitManager`, `PageSourceProvider`, session properties, and table functions. |
| Metadata | `ArangoMetadata implements ConnectorMetadata` | Lists schemas/tables/columns; implements pushdown hooks, `applyTableFunction`, and `getTableStatistics`. |
| Split mgmt | `ArangoSplitManager implements ConnectorSplitManager` | Turns a (pushed-down) table handle into splits, sharding-aware; single split for aggregated/passthrough handles; consumes `DynamicFilter`. |
| Read | `ArangoPageSourceProvider` / `ArangoPageSource` | Executes AQL for one split, builds columnar `Page`s. |
| Client | `ArangoClient` (wraps `arangodb-java-driver` 7.x) | Connection/host pool, coordinator selection, metadata calls, AQL execution with bind vars + `shardIds`, streaming cursors, retries. |
| AQL gen | `AqlBuilder` | Deterministically renders a table handle's pushed-down state into a parameterized AQL string. |
| Schema | `SchemaResolver` | Implements the layered schema strategy (§4), lazily and fault-tolerantly. |
| Table fn | `ArangoQueryFunction` (`query()`) | Raw-AQL passthrough (§7). |

**Coordinator selection is an `ArangoClient` concern, not a split concern.** `ConnectorSplit` network addresses are Trino *worker* scheduling hints and must not be used to route to an ArangoDB coordinator. The client owns coordinator selection/failover from its host pool.

### 2.2 Handles (SPI value objects)

- `ArangoTableHandle` — `schema`, `table`, and accumulated pushdown state: `TupleDomain<ColumnHandle> constraint`, `OptionalLong limit`, projected columns/expressions, an optional aggregation descriptor, and an `aggregated` flag (true ⇒ split manager emits exactly one split, §6.4).
- `ArangoColumnHandle` — `name`, Trino `Type`, ArangoDB **document path** (dotted, e.g. `address.city`), `hidden` flag.
- `ArangoSplit` — the base AQL (or enough to rebuild it), the assigned `shardIds` subset (empty ⇒ whole collection), and any split-local `_key`-range bound. No external routing address.
- `ArangoTransactionHandle` — trivial singleton (read-only).

### 2.3 Data Flow & the pushdown contract

```
Trino planner
  └─ ArangoMetadata.getTableHandle / getColumnHandles        (schema via SchemaResolver, lazy)
  └─ applyFilter / applyLimit / applyProjection / applyAggregation
        └─ each folds state into a NEW ArangoTableHandle
  └─ ArangoMetadata.getTableStatistics(handle)               (row-count estimate)
  └─ ArangoSplitManager.getSplits(handle, dynamicFilter)
        └─ aggregated/passthrough → 1 split; else shards → ArangoSplit[]
  └─ per split: ArangoPageSource
        └─ ArangoClient runs AQL (bind vars + shardIds), streams cursor batches
        └─ VelocyPack docs → Trino Blocks → Page
```

**Pushdown contract (corrected).** Every `apply*` hook returns `Optional` — empty means "I decline; Trino keeps doing it itself." A hook must **only** claim work it can reproduce with identical SQL semantics; when it can't (unsupported expression, unsafe type, collation-sensitive comparison, or an aggregation that would violate the single-split rule), it declines. This is the safety principle: *the connector may leave work to Trino, but whatever it claims it must compute correctly.* (The earlier "correctness never depends on pushdown" phrasing was wrong — a mis-wired aggregation or unguarded filter can produce wrong answers, which is exactly why §6 constrains what may be claimed.)

---

## 3. Data Model Mapping

### 3.1 Namespace mapping

| Trino | ArangoDB |
|---|---|
| Catalog | One ArangoDB deployment (one connector config) |
| Schema | Database — enumerated via `GET /_api/database` (and `GET /_api/database/user` when running as a restricted read-only user, so listing doesn't fail) |
| Table | Collection — document (`type=2`) or edge (`type=3`) |
| Row | Document |
| Column | Top-level field (nested fields via `ROW`) |

System collections (names beginning `_`) are hidden by default. System attributes `_key`/`_id`/`_rev` are exposed as **hidden `VARCHAR` columns**; edge collections additionally expose `_from`/`_to` as **visible `VARCHAR`** so relationships reconstruct via ordinary SQL joins.

### 3.2 Type mapping (VelocyPack/JSON → Trino)

| ArangoDB value | Trino type | Notes |
|---|---|---|
| bool | `BOOLEAN` | |
| integer number | `BIGINT` | |
| integer exceeding `BIGINT` / uint64 | `DECIMAL(38,0)` | See numeric policy below. |
| floating number | `DOUBLE` | |
| high-precision decimal (from schema source) | `DECIMAL(p,s)` | Only when a schema source declares precision/scale. |
| string | `VARCHAR` | |
| ISO-8601 string **with** `Z`/offset (schema-declared) | `TIMESTAMP WITH TIME ZONE` | Never inferred from sampling. |
| ISO-8601 local string (schema-declared) | `TIMESTAMP(3)` | Schema-declared only. |
| epoch-millis number (schema-declared as date) | `TIMESTAMP(3)` | ArangoDB `DATE_NOW()` encoding; opt-in via schema source. |
| array | `ARRAY(element)` | Element from schema source or merged sampling; mixed elements → `ARRAY(VARCHAR)` or `JSON`. |
| object | `ROW(field ...)` | Nested; paths become dotted column paths for projection pushdown. |
| null / absent | column present, value `NULL` | Never drops the column (§4.1). |
| `_key`/`_id`/`_rev`/`_from`/`_to` | `VARCHAR` | System/edge attributes. |

**Numeric policy.** A field seen as integer in some documents and floating in others **widens to `DOUBLE`** (it is *not* degraded to `VARCHAR`). Integers beyond `BIGINT` range or unsigned 64-bit values map to `DECIMAL(38,0)`. Only genuinely incompatible categories (e.g. number vs object) fall back to `VARCHAR`/`JSON` per `mixed-type-strategy`.

---

## 4. Schema Resolution (`SchemaResolver`)

Schemas resolve by **precedence — first match wins**:

1. **Explicit override collection** (writable, user-curated). Same document shape as Trino's Mongo connector, plus a `path` field for nested flattening:
   ```json
   { "table": "<collection>",
     "fields": [ { "name": "city", "path": "address.city", "type": "varchar", "hidden": false } ] }
   ```
   Collection name is `arangodb.schema-collection`, **default `trino_schema`** (a normal, non-underscore collection — ArangoDB reserves the leading-underscore namespace for system collections and requires `isSystem:true` to create them, unlike MongoDB's `_schema`). The connector **reads** this collection; it never writes it.

2. **Native collection `schema` validation rules** (ArangoDB ≥ 3.7 JSON Schema) — a **strong hint**, not gospel. Validation has levels (`none`/`moderate`/`strict`), `additionalProperties` defaults to true, and pre-existing documents are never revalidated, so `properties` may be neither complete nor accurate. Therefore: take declared fields/types as high-confidence, **merge with sampling** to catch undeclared fields, and trust the schema as *closed* (no extra sampling) only when level is `strict` and `additionalProperties:false`. Conversion still degrades gracefully (§4.4) if a stored value contradicts the rule.

3. **Sampling fallback** — read `arangodb.schema.sample-size` documents (default 1000) and infer via merge rules (§4.1).

### 4.1 Sampling that fixes the Mongo weaknesses
The Mongo connector often infers from a single document and **drops fields it sees as null/empty** ([presto#2934](https://github.com/prestosql/presto/issues/2934)). This connector instead:
- **Merges the field union across all sampled documents** — a field in *any* sampled doc becomes a column.
- Keeps a column when it is null/absent in some docs; takes its type from docs where present.
- On numeric int/float conflict, **widens to `DOUBLE`**; on incompatible-category conflict, degrades to `VARCHAR`/`JSON` — **never drops**.
- Recurses into nested objects with the same rules.

**Sampling bias (honest caveat).** `FOR d IN @@col LIMIT @n RETURN d` returns documents in primary-index (`_key`) order — oldest/lowest keys first — so schema drift introduced in newer documents can be missed, and on a cluster the limit is often satisfied from a subset of shards. `arangodb.schema.sample-random` (`SORT RAND()`) removes the bias at full-scan cost and is off by default.

### 4.2 Lazy, fault-tolerant resolution
Schema is resolved **lazily per table**, not eagerly for the whole catalog, because sampling every collection during `information_schema` / `SHOW TABLES` enumeration is prohibitively expensive. During enumeration, a table whose schema cannot be resolved (empty collection, sampling error) is **listed but its column resolution is deferred/skipped with a warning** — it must not fail the whole listing. A hard `ARANGODB_SCHEMA_ERROR` (with guidance) is raised only when such a table is actually queried.

### 4.3 Determinism
Resolved schemas are cached per (schema, table) for a configurable TTL so a query sees one stable schema; a stale schema surfaces as a normal `NULL` on a missing field, not an error.

### 4.4 Per-cell coercion policy (single definition; referenced by §6.1 and §9.1)
When a stored value's runtime type does not match the resolved column type:
- **`lenient` (default):** the cell reads as `NULL`; a per-query counter is incremented and logged at debug.
- **`strict` (`arangodb.type-coercion=strict`):** raise `ARANGODB_TYPE_CONVERSION_ERROR`.

This policy is the semantic anchor for pushed filters: because a type-mismatched value reads as `NULL`, a pushed predicate that **guards on type** (§6.1) and thereby excludes that value is *consistent* with SQL evaluating `NULL <op> literal` to `UNKNOWN` (excluded). Guard + coercion agree by construction.

### 4.5 Config
- `arangodb.schema-collection` (default `trino_schema`)
- `arangodb.schema.sample-size` (default 1000)
- `arangodb.schema.sample-random` (default false)
- `arangodb.schema.mixed-type-strategy` = `varchar` | `json` (default `varchar`)
- `arangodb.schema.cache-ttl` (default e.g. 5m)
- `arangodb.type-coercion` = `lenient` | `strict` (default `lenient`)
- `arangodb.case-insensitive-name-matching` (default false) — ArangoDB names are case-sensitive; maps lowercased Trino identifiers back to real names, with a documented collision error.

---

## 5. Split Generation (`ArangoSplitManager`)

Aggregated handles (§6.4) and `query()` passthrough handles (§7) always emit **exactly one split**. Everything below is for ordinary non-aggregated scans.

### 5.1 Cluster (sharded) deployments — shard-parallel
- Discover `numberOfShards` and shard IDs (collection properties / shard-distribution admin API).
- Group shards into `ceil(shards / arangodb.shards-per-split)` groups (default 1 shard/split); emit one `ArangoSplit` per group carrying its `shardIds`.
- Each `ArangoPageSource` runs the same base AQL with `options: { shardIds: [...] }`.

**`shardIds` is an internal ArangoDB API — guarded accordingly.** The option is real and battle-tested (ArangoDB's own official Spark datasource reads per shard exactly this way), but ArangoDB documents it as internal/"use at your own risk." If a server ever ignores it, **every split runs the full query → silent N× duplication.** Mitigations, all required:
1. **Version pin** — assert a minimum ArangoDB version at startup; refuse shard-splitting below it.
2. **Startup capability check** — verify shard-targeting narrows results (probe a known collection; a shard-scoped count must be < full count when >1 shard).
3. **CI correctness gate** — a test asserting per-shard counts **sum to** the collection count.
4. **Safe fallback** — if any check is inconclusive, fall back to a **single split** (correct, just not parallel) rather than risk duplication.
5. **SmartGraph edges** — SmartGraph/Enterprise edge collections store remote edges twice across internal `_from_`/`_to_` sub-shards (AQL normally dedupes); naive per-shard enumeration can double-count. Exclude such collections from shard-splitting (single split) pending explicit verification.

### 5.2 Single-server deployments
- No shards → **one split**.
- Optional `_key`-range splitting (config-gated, off by default) for very large collections. **Caveat:** auto-generated `_key`s are increasing numeric *strings*, whose lexicographic order ≠ numeric order, so range boundaries must be derived carefully (e.g. by sampling actual key boundaries), not by assuming numeric ranges. Given the complexity, this stays off unless a deployment needs it.

### 5.3 Dynamic filtering
`getSplits` receives Trino's `DynamicFilter`. **v1 support is best-effort and non-blocking:** completed dynamic-filter domains available at split-creation time are folded into each split's AQL `FILTER ... IN (...)`; the connector does not block waiting for them. Fuller (blocking, per-split-refined) dynamic filtering is deferred post-v1. This is stated so join-heavy federation queries still benefit without over-scoping v1.

### 5.4 Config
- `arangodb.shards-per-split` (default 1)
- `arangodb.max-splits` (safety cap)
- `arangodb.query.batch-size` (cursor `batchSize`)
- `arangodb.key-range-splitting` (default false)

---

## 6. Pushdown (`ArangoMetadata.apply*` → `AqlBuilder`)

All pushdown emits **parameterized AQL** (bind vars `@value0…`) to prevent injection and enable server-side planning.

### 6.1 `applyFilter` — predicate → `FILTER`, with type/null guards
AQL defines a **total cross-type ordering** `null < bool < number < string < array < object`, so a naïve `d.age < @v` would match documents where `age` is null/absent, and `d.age > @v` would match documents where `age` is a *string*. Pushed predicates therefore carry **type guards derived from the column's Trino type**:

- **Equality / `IN`** — safe without a guard: AQL `==` is type-strict and does not coerce (`d.age == 21` never matches `"21"`), and `d.f == null` never matches a non-null. Pushed for all types.
- **Numeric range** (`<`,`>`,`<=`,`>=` on `BIGINT`/`DOUBLE`/`DECIMAL`) — emit `IS_NUMBER(d.f) AND d.f > @v`.
- **`IS NULL` / `IS NOT NULL`** — `d.f == null` / `d.f != null` (a missing field is `null` in AQL, matching SQL `NULL` for an absent value).
- **String range** — **not pushed by default.** ArangoDB compares strings with server ICU collation while Trino compares by codepoint, so `name > 'M'` can disagree. Only string **equality/`IN`** is pushed. String range pushdown is available behind `arangodb.string-range-pushdown` for deployments that have verified a matching collation.

Predicates the builder can express (with guards) are reported **handled** and removed from Trino's residual; anything else stays in Trino. Consistency with §4.4: guarding out a type-mismatched value matches reading it as `NULL`.

### 6.2 `applyLimit` — `LIMIT n`
- Emits `LIMIT n` in the AQL. **`limitGuaranteed` is reported true only for a single-split handle**; with multiple splits each split caps at `n`, so the total can be up to `n × splits` and Trino must apply the final `LIMIT` (reported `false`). Pushing the per-split cap is still a useful reduction.

### 6.3 `applyProjection` — projection & nested fields
- Push required columns and **nested field references** (`d.address.city`) into `RETURN { city: d.address.city, ... }`, so only needed fields cross the wire.

### 6.4 `applyAggregation` — single-split aggregation
**Corrected model.** Trino's aggregation pushdown *replaces* the aggregation node and treats the connector's output as **final** — there is no connector-controllable partial/final split. If an aggregated handle then fanned out into N shard-splits, Trino would emit **N duplicate "final" rows**. Therefore:

- When `applyAggregation` succeeds, the returned handle sets `aggregated = true`, and `ArangoSplitManager` emits **exactly one split** (no shard fan-out). ArangoDB still parallelizes the single AQL across its shards internally, so DB-side parallelism is retained; only Trino-worker-level parallelism is given up — the correct trade.
- Because the aggregate is computed completely in one AQL query, **all of `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` are safe to push** (single-split means no re-aggregation, so `AVG` needs no partial-state decomposition):
  - `COUNT(*)` → `COLLECT WITH COUNT INTO c RETURN c`
  - grouped → `COLLECT g = d.<key> AGGREGATE s = SUM(d.<x>), a = AVERAGE(d.<y>), mn = MIN(d.<z>) RETURN {g, s, a, mn}`
- Only aggregations whose inputs are pushable column paths are claimed; otherwise decline and let Trino aggregate.
- `DISTINCT` aggregates and `HAVING`/complex grouping-set forms are out of scope for v1 (declined).

### 6.5 AQL generation example
`SELECT city, count(*) FROM users WHERE age > 21 GROUP BY city` →
```aql
FOR d IN @@col
  FILTER IS_NUMBER(d.age) AND d.age > @value0
  COLLECT city = d.city WITH COUNT INTO cnt
  RETURN { city, cnt }
```
bind `{ "@col": "users", "value0": 21 }`, run as a **single split** (aggregated). Note the `IS_NUMBER` guard (§6.1) and single-split execution (§6.4).

### 6.6 Statistics — `getTableStatistics`
Return at least a **row-count estimate** from the cheap collection `count` (RocksDB exposes exact counts inexpensively) and, when a pushed constraint exists, a best-effort selectivity estimate. This lets the Trino cost-based optimizer order joins across catalogs instead of guessing. Column-level NDV/min/max are optional/post-v1.

---

## 7. AQL Passthrough — `query()` Table Function

Mirrors the Mongo/JDBC `query()` polymorphic table function pattern (`io.trino.plugin.mongodb.ptf.Query`):

```sql
SELECT * FROM
  TABLE(arango.system.query(
    database => 'mydb',
    query    => 'FOR v IN OUTBOUND "users/42" GRAPH "social" RETURN v'));
```

- Implemented as `ArangoQueryFunction extends AbstractConnectorTableFunction`; `ArangoMetadata.applyTableFunction` returns the passthrough table handle that the split manager executes as a **single split** (opaque AQL, no shard rewriting).
- **Result schema derivation:** sample **k result rows** (`FOR r IN (<query>) LIMIT @k RETURN r`) and apply the §4.1 merge rules — not a single-row `LIMIT 1`, whose lone row may not represent later rows. This costs a **double execution** of the user's AQL (once for schema at analysis, once for data); that cost is documented, and an optional explicit column-descriptor argument lets callers skip the analysis run for expensive traversals.
- Only path to graph traversals, geo, and ArangoSearch full-text.
- **Read-only enforcement (robust):** validate the AQL via ArangoDB's **parse-only endpoint** (`POST /_api/query`) and inspect the returned AST for data-modification nodes (`INSERT`/`UPDATE`/`REPLACE`/`REMOVE`/`UPSERT`) — a keyword scan is too fragile (keywords appear in strings/attribute names/subqueries). Primary control remains a **read-only ArangoDB user**. Because `query()` bypasses Trino table/column-level security, it can be disabled entirely via `arangodb.query-function-enabled=false`.

---

## 8. Configuration Reference

| Property | Default | Purpose |
|---|---|---|
| `arangodb.hosts` | — | Comma-separated `host:port` coordinators. |
| `arangodb.user` / `arangodb.password` | — | Basic auth (use Trino secrets). |
| `arangodb.auth.jwt` | — | Optional JWT instead of basic auth. |
| `arangodb.ssl.enabled` | false | Enable TLS. |
| `arangodb.ssl.truststore-path` / `-password` | — | TLS trust config. |
| `arangodb.schema-collection` | `trino_schema` | Override-collection name. |
| `arangodb.schema.sample-size` | 1000 | Docs sampled when inferring. |
| `arangodb.schema.sample-random` | false | `SORT RAND()` sampling. |
| `arangodb.schema.mixed-type-strategy` | `varchar` | Incompatible-conflict target (`varchar`\|`json`). |
| `arangodb.schema.cache-ttl` | 5m | Resolved-schema cache TTL. |
| `arangodb.type-coercion` | `lenient` | Per-cell mismatch policy (`lenient`\|`strict`). |
| `arangodb.case-insensitive-name-matching` | false | Map lowercased Trino names to real names. |
| `arangodb.shards-per-split` | 1 | Shards grouped per split. |
| `arangodb.max-splits` | (cap) | Safety limit on split count. |
| `arangodb.key-range-splitting` | false | Single-server key-range splits. |
| `arangodb.string-range-pushdown` | false | Push string range predicates (collation-verified only). |
| `arangodb.query-function-enabled` | true | Enable/disable the `query()` passthrough. |
| `arangodb.query.batch-size` | (driver default) | AQL cursor `batchSize`. |
| `arangodb.query.stream` | true | Use streaming cursors (bounded memory). |
| `arangodb.query.cursor-ttl` | — | Cursor TTL; must exceed long-running Trino query spans. |
| `arangodb.query.timeout` | — | AQL execution timeout. |
| `arangodb.retry.max-attempts` / `-backoff` | — | Retry policy for transient failures. |

Credentials should be supplied via Trino's secrets support, not inline.

---

## 9. Error Handling, Resilience, Security, Testing

### 9.1 Error handling
- `ArangoErrorCode` enum: `ARANGODB_CONNECTION_ERROR`, `ARANGODB_SCHEMA_ERROR`, `ARANGODB_QUERY_ERROR`, `ARANGODB_TYPE_CONVERSION_ERROR`.
- Missing collection → `TableNotFoundException`.
- Empty/unresolvable collection is tolerated during listing (§4.2) and errors only when queried.
- Per-cell type mismatch handled per §4.4 (lenient NULL vs strict error).
- AQL failures surface the ArangoDB error number and message.

### 9.2 Resilience (connection & cursor)
- **Coordinator failover:** `ArangoClient` maintains the host pool (`acquireHostList`) and retries transient failures with backoff.
- **Streaming cursors** (`stream:true`) to bound worker memory on large scans.
- **Cursor TTL** configured to exceed expected Trino query duration (long federated queries can otherwise outlive the default cursor lifetime mid-scan).
- Idempotent read retries only; no retry of a partially-consumed cursor (re-issue the split's AQL instead).

### 9.3 Security
- TLS to coordinators; credentials via secrets; never log credentials; redact connection URLs in logs/`EXPLAIN`.
- **Read-only trust boundary:** connector performs only reads; passthrough AQL validated read-only via AST inspection (§7); deployment guide recommends a read-only DB user; `query()` disableable.

### 9.4 Testing
- **Integration:** `TestingArangoServer` via Testcontainers (`arangodb/arangodb`); single-server **and** a multi-shard cluster fixture.
- **Conformance:** extend `BaseConnectorTest` (read subset); assert writes are cleanly rejected.
- **Type mapping:** `SqlDataTypeTest` round-trips for every §3.2 mapping, incl. nested `ROW`/`ARRAY`, numeric widening, and mixed-type degradation.
- **Pushdown:** `QueryAssert.isFullyPushedDown()` / `isNotFullyPushedDown()` (not the JDBC-only `assertConditionallyPushedDown`) for filter/limit/projection/aggregation; **negative tests** that unguarded/unsafe predicates (string range without the flag, mixed-type range) stay in Trino and return correct rows.
- **Split correctness (guards B3):** multi-shard scan returns exactly the same rows/counts as a single-split scan (no gaps/dupes); startup capability check tested; SmartGraph edge collection falls back to single split.
- **Aggregation correctness:** grouped/aggregate results (incl. `AVG`) match a single-node reference; verify aggregated handle produces exactly one split.
- **Filter-guard semantics:** collection with mixed/absent-typed fields returns SQL-correct rows under pushed range filters.
- **Schema regression:** reproduce the Mongo null-drop case (§4.1) proving the column is retained.

---

## 10. Milestones (delivery order)

| # | Milestone | Contents | Exit criteria |
|---|---|---|---|
| **M1** | Read skeleton | Plugin/Factory/Connector/Metadata; DB/collection listing (lazy, fault-tolerant); merge-sampling schema; single-split full scan; base + widening type mapping; coercion policy | `SELECT *` returns correct rows/types; `SHOW TABLES` tolerates unresolvable collections |
| **M2** | Filter + limit + projection pushdown | `applyFilter` **with type/null guards**, `applyLimit` (single-split guarantee rule), `applyProjection` (nested) | `isFullyPushedDown()` for guarded predicates; mixed-type filter returns SQL-correct rows |
| **M3** | Shard-parallel splits | Shard discovery; `shardIds` execution; version pin + capability check + CI count-sum gate + single-split fallback; SmartGraph exclusion | N shards ⇒ N splits, counts sum exactly; fallback proven |
| **M4** | Aggregation pushdown | `applyAggregation` → **single-split** COUNT/SUM/MIN/MAX/AVG + GROUP BY | Aggregates correct vs reference; aggregated handle = 1 split |
| **M5** | Schema sources + `query()` + stats | override reader (`trino_schema`); validation-rule hint+merge; `ArangoQueryFunction` (AST read-only check, k-row schema, disable flag); `getTableStatistics` | Precedence honored; graph reachable via `query()`; row-count stats surfaced |
| **M6** | Hardening | TLS/auth, secrets, case-insensitive matching, cursor/failover resilience, best-effort dynamic filtering, `BaseConnectorTest` conformance, docs | Conformance green; deployment guide published |

---

## Appendix A — Key references
- Trino connector development: https://trino.io/docs/current/develop/connectors.html
- Trino MongoDB connector (precedent): https://trino.io/docs/current/connector/mongodb.html
- Mongo inference drops columns on null/empty: https://github.com/prestosql/presto/issues/2934
- Open request for user-defined schemas in Mongo (ArangoDB provides natively): https://github.com/trinodb/trino/issues/24106
- ArangoDB Java driver 7.x (HTTP/2 + VelocyPack): `com.arangodb:arangodb-java-driver`
- ArangoDB schema validation (collection JSON Schema, levels none/moderate/strict): ArangoDB ≥ 3.7
- ArangoDB AQL `shardIds` query option (internal API; used by ArangoDB's official Spark datasource for per-shard reads)

## Appendix B — Review resolution (rev 2)
Fable review: **3 blockers** — B1 aggregation partial/final model (fixed §6.4, single-split), B2 filter cross-type ordering (fixed §6.1, type guards), B3 `shardIds` internal-API duplication risk (fixed §5.1, guards + version pin + fallback + SmartGraph). **13 should-fix** — validation-as-hint §4/2, `trino_schema` default §4/S3, `limitGuaranteed` §6.2/S1, coercion policy §4.4/S9, collation string-range §6.1/S10, numeric widening §3.2/S11, statistics §6.6/S6, dynamic filtering §5.3/S7, resilience §9.2/S8, robust read-only check §7/S4, passthrough schema §7/S5, lazy/fault-tolerant listing §4.2/S12, pushdown-contract rewording §2.3/S13. **9 minor** — `isFullyPushedDown` §9.4/N1, JDK verify preamble/N2, coordinator-in-client §2.1/N3, sampling bias §4.1/N4, timestamp WITH TIME ZONE + epoch §3.2/N5, `/_api/database(/user)` §3.1/N6, driver 7.x/HTTP2 preamble/N7, `applyTableFunction` wiring §7/N8, snapshot-consistency non-goal §1.3/N9.
