# ArangoDB Trino Connector — Design Spec

- **Status:** Draft for review
- **Date:** 2026-07-18
- **Scope:** Read-only, built to scale (shard-parallel splits + filter/limit/projection/aggregation pushdown). No writes.
- **Target Trino:** latest stable line (~476/481 SPI); exact version pinned at implementation start. Requires Java 23, Maven, Airlift bootstrap (matching Trino's build).
- **Language:** Java (mandatory — Trino connectors are JVM plugins loaded via the Connector SPI).

---

## 1. Overview & Goals

### 1.1 Purpose

Expose ArangoDB collections as queryable tables in Trino so operators can run distributed SQL analytics across ArangoDB data and join it with any other Trino catalog.

### 1.2 Goals

- Present each ArangoDB **database as a Trino schema** and each **collection (document or edge) as a table**.
- Resolve schemas for schemaless collections via a **layered strategy** (§4) that improves on the MongoDB connector's known weaknesses.
- Push work down to ArangoDB via **AQL generation**: `FILTER`, `LIMIT`, projection (including nested fields), and aggregation.
- Scale reads by generating **one split per shard subset** so Trino workers scan a sharded collection in parallel.
- Provide a raw-AQL **`query()` table function** as the escape hatch for graph traversals and anything that does not map to relational SQL.

### 1.3 Non-Goals (explicit)

- **No writes.** No `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, or `_schema` mutation by the connector. The connector reads only.
- **No graph traversal engine.** AQL `FOR v IN OUTBOUND ...` is not modeled in relational SQL; it is reachable only through the `query()` passthrough (§7).
- **No cross-collection join pushdown.** Joins are executed by the Trino engine.
- **No schema authoring UI.** The optional `_schema` override collection is authored by users out-of-band.

### 1.4 Success Criteria

- `SELECT` over document and edge collections returns correct, correctly-typed rows.
- A predicate + `LIMIT` query shows **no residual filter** in `EXPLAIN` (i.e., it was pushed to AQL).
- On a sharded cluster, a full scan produces **N splits for N shards** and runs them concurrently.
- The connector passes Trino's `BaseConnectorTest` read-side conformance suite against a Testcontainers ArangoDB.

---

## 2. Architecture

### 2.1 SPI Component Map

The connector implements the standard Trino Connector SPI surface. Concrete classes (working names):

| Layer | Class | Responsibility |
|---|---|---|
| Plugin entry | `ArangoPlugin implements io.trino.spi.Plugin` | Returns the `ConnectorFactory`. |
| Factory | `ArangoConnectorFactory implements ConnectorFactory` | Reads catalog config, builds a Guice/Airlift injector, produces `ArangoConnector`. |
| Connector | `ArangoConnector implements Connector` | Wires transaction handle, `Metadata`, `SplitManager`, `PageSourceProvider`, session properties, and table functions. |
| Metadata | `ArangoMetadata implements ConnectorMetadata` | Lists schemas/tables/columns; implements all pushdown hooks. |
| Split mgmt | `ArangoSplitManager implements ConnectorSplitManager` | Turns a (pushed-down) table handle into splits, sharding-aware. |
| Read | `ArangoPageSourceProvider` / `ArangoPageSource` | Executes AQL for one split, builds columnar `Page`s. |
| Client | `ArangoClient` (wraps `arangodb-java-driver`) | Connection pool, metadata calls, AQL execution with bind vars + `shardIds`. |
| AQL gen | `AqlBuilder` | Deterministically renders a table handle's pushed-down state into a parameterized AQL string. |
| Schema | `SchemaResolver` | Implements the layered schema strategy (§4). |
| Table fn | `ArangoQueryFunction` (`query()`) | Raw-AQL passthrough (§7). |

### 2.2 Handles (SPI value objects)

- `ArangoTableHandle` — carries `schema`, `table`, and the **accumulated pushdown state**: `TupleDomain<ColumnHandle> constraint`, `OptionalLong limit`, projected columns/expressions, and an optional `AggregationApplication`. Pushdown hooks return a *new* handle with more state folded in; splits and the page source read from it.
- `ArangoColumnHandle` — `name`, Trino `Type`, the ArangoDB **document path** (dotted, e.g. `address.city`), and `hidden` flag.
- `ArangoSplit` — the base AQL (or enough to rebuild it) plus the **assigned `shardIds` subset** and target host/coordinator.
- `ArangoTransactionHandle` — trivial singleton (read-only, no multi-statement transactions needed).

### 2.3 Data Flow

```
Trino planner
  └─ ArangoMetadata.getTableHandle / getColumnHandles      (schema via SchemaResolver)
  └─ applyFilter / applyLimit / applyProjection / applyAggregation
        └─ each folds state into a new ArangoTableHandle
  └─ ArangoSplitManager.getSplits(handle)
        └─ AqlBuilder renders base AQL; shards → ArangoSplit[]
  └─ per split: ArangoPageSource
        └─ ArangoClient runs AQL (bind vars + shardIds), streams cursor batches
        └─ VelocyPack/JSON docs → Trino Blocks → Page
```

`★ Design note:` Pushdown is *additive and negotiated*. Each `apply*` hook returns `Optional<...>` — empty means "I can't take this," and Trino keeps applying it itself. Correctness never depends on pushdown succeeding; pushdown only removes work. This lets us ship M1 with zero pushdown and add hooks incrementally without risking wrong results.

---

## 3. Data Model Mapping

### 3.1 Namespace mapping

| Trino | ArangoDB |
|---|---|
| Catalog | One ArangoDB deployment (one connector config) |
| Schema | Database (`db._databases()`) |
| Table | Collection — document (`type=2`) **or** edge (`type=3`) |
| Row | Document |
| Column | Top-level document field (nested fields addressable via `ROW`) |

System collections (names beginning `_`) are hidden by default; system attributes `_key`, `_id`, `_rev` are exposed as **hidden `VARCHAR` columns** (selectable by name, excluded from `SELECT *`), mirroring how the Mongo connector treats `_id`. Edge collections additionally expose `_from` and `_to` as visible `VARCHAR` columns, so relationships can be reconstructed with ordinary SQL joins.

### 3.2 Type mapping (VelocyPack/JSON → Trino)

| ArangoDB value | Trino type | Notes |
|---|---|---|
| bool | `BOOLEAN` | |
| integer number | `BIGINT` | VelocyPack int types collapse to `BIGINT`. |
| floating number | `DOUBLE` | |
| string | `VARCHAR` | Default for text. |
| ISO-8601 date string | `TIMESTAMP(3)` **only if** a schema source (validation rule / `_schema`) declares it; otherwise `VARCHAR` | ArangoDB has no native date type — dates are strings/numbers. Never inferred as timestamp from sampling. |
| array | `ARRAY(element)` | Element type from schema source or merged sampling. Mixed-element arrays → `ARRAY(VARCHAR)` or `JSON`. |
| object | `ROW(field ...)` | Nested; field paths become dotted column paths for projection pushdown. |
| null / absent | column still present; value `NULL` | See §4.3 — never drops the column. |
| heterogeneous field (conflicting types across docs) | `VARCHAR` (or `JSON` if configured) | Degrade, don't drop. |
| `_key`/`_id`/`_rev`/`_from`/`_to` | `VARCHAR` | System/edge attributes. |

`JSON` fallback is opt-in via a config flag so users who prefer structured `ROW`/`ARRAY` get it by default.

---

## 4. Schema Resolution (`SchemaResolver`)

ArangoDB collections are schemaless, so table schemas are resolved by **precedence — first match wins**:

1. **Explicit `_schema` override collection** (writable, user-curated). Same document shape as Trino's Mongo connector for operator familiarity:
   ```json
   { "table": "<collection>",
     "fields": [ { "name": "city", "path": "address.city", "type": "varchar", "hidden": false } ] }
   ```
   `path` (dotted document path) is an ArangoDB addition enabling flattened columns over nested docs. Collection name configurable via `arangodb.schema-collection` (default `_schema`). The connector **reads** this collection; it never writes it (read-only).

2. **Native collection `schema` validation rules** (ArangoDB ≥ 3.7 JSON Schema). When a collection declares validation, its JSON Schema `properties` are the **authoritative** column set and types. *This is the key advantage over MongoDB, which has no server-side schema and where user-defined schemas remain an open, unbuilt request ([trinodb/trino#24106](https://github.com/trinodb/trino/issues/24106)).*

3. **Sampling fallback** — read `arangodb.schema.sample-size` documents (default 1000) and infer.

### 4.1 Sampling that fixes the Mongo weaknesses

The Mongo connector frequently infers from a single document and **drops any field it sees as `null` or empty**, even deep in a nested object ([presto#2934](https://github.com/prestosql/presto/issues/2934)). This connector instead:

- **Merges the field union across all sampled documents** (a field present in *any* sampled doc becomes a column).
- On a field that is `null`/empty/absent in some docs, **keeps the column** and takes its type from the docs where it is present.
- On **type conflict** across docs, **degrades to `VARCHAR`** (or `JSON`), never drops.
- Recurses into nested objects to build `ROW` types with the same merge rules.

Sampling uses AQL, e.g. `FOR d IN @@col LIMIT @n RETURN d` (optionally `SORT RAND()` for a spread, config-gated for cost).

### 4.2 Config for schema
- `arangodb.schema-collection` (default `_schema`)
- `arangodb.schema.sample-size` (default 1000)
- `arangodb.schema.sample-random` (default false)
- `arangodb.schema.mixed-type-strategy` = `varchar` | `json` (default `varchar`)
- `arangodb.case-insensitive-name-matching` (default false) — ArangoDB names are case-sensitive; Trino lowercases identifiers, so this maps lowercased Trino names back to real names (with a documented collision error).

### 4.3 Determinism
Sampling results are cached per (schema, table) for the session/planning window so a query sees one stable schema. Cache TTL is configurable; a stale schema surfaces as a normal read of a missing/extra field (`NULL`), not an error.

---

## 5. Split Generation (`ArangoSplitManager`)

The goal is worker-parallel scans. The split boundary is the **shard**.

### 5.1 Cluster (sharded) deployments
- Query collection properties for `numberOfShards` and the shard IDs (`getProperties()` / shard-distribution admin API).
- Group shards into `ceil(shards / arangodb.shards-per-split)` groups (default 1 shard per split).
- Emit one `ArangoSplit` per group carrying its `shardIds` subset.
- Each `ArangoPageSource` runs the **same base AQL** with the AQL query option `options: { shardIds: [...] }`, so ArangoDB restricts execution to those shards. Disjoint shard subsets ⇒ no double-counting, full coverage.

`★ Insight:` ArangoDB's AQL API accepts a `shardIds` execution option — that single feature is what makes shard-per-split parallelism clean. Without it we'd fall back to `_key`-range slicing, which requires knowing key distribution. Shard-targeting sidesteps that entirely.

### 5.2 Single-server deployments
- No shards → default to **one split** (whole-collection AQL).
- Optional `_key`-range splitting (config-gated) for large single-server collections: derive range boundaries and emit range-filtered AQL splits. Off by default (adds a boundary-scan cost).

### 5.3 Config
- `arangodb.shards-per-split` (default 1)
- `arangodb.max-splits` (safety cap)
- `arangodb.query.batch-size` (cursor `batchSize`, controls fetch granularity)

---

## 6. Pushdown (`ArangoMetadata.apply*` → `AqlBuilder`)

All pushdown is expressed as **parameterized AQL** (bind vars `@value0…`) to prevent injection and let ArangoDB plan/cache.

### 6.1 `applyFilter` — predicate → `FILTER`
- Input: `Constraint` with a `TupleDomain<ColumnHandle>`.
- Translate per-column `Domain`s:
  - equality → `d.<path> == @v`
  - ranges → `d.<path> > @v`, `>=`, `<`, `<=` (and compound ranges)
  - discrete set (IN) → `d.<path> IN [@v0, @v1, ...]`
  - `IS NULL` / `IS NOT NULL` → `d.<path> == null` / `!= null`
- Columns/predicates AQL can express are reported as **handled** (removed from Trino's residual); anything unsupported (e.g. complex `ConnectorExpression` functions in the first cut) stays in Trino. Returns a new `ArangoTableHandle` with the folded constraint.

### 6.2 `applyLimit` — `LIMIT n`
- Push `LIMIT` when there is no residual Trino-side filter that would change row counts. Report as `limitGuaranteed` only when fully enforced by AQL.

### 6.3 `applyProjection` — projection & nested fields
- Push required columns (and **nested field references**, `d.address.city`) into `RETURN { city: d.address.city, ... }`, so only needed fields cross the wire. Reduces payload dramatically for wide documents.

### 6.4 `applyAggregation` — `COUNT/SUM/MIN/MAX/AVG` (+ GROUP BY)
- `COUNT(*)` → `COLLECT WITH COUNT INTO c RETURN c`.
- Grouped/aggregate → `COLLECT g = d.<key> AGGREGATE s = SUM(d.<x>), mn = MIN(d.<y>) RETURN {g, s, mn}`.
- Only push aggregations whose inputs are already-pushable column paths; otherwise return empty and let Trino aggregate. Interacts with splits: pushed aggregates run **per shard**, and Trino performs the **final cross-split re-aggregation** (partial-aggregation model) — the spec requires partial/final splitting so per-shard `SUM`/`COUNT` combine correctly.

### 6.5 AQL generation example
`SELECT city, count(*) FROM users WHERE age > 21 GROUP BY city` →
```aql
FOR d IN @@col
  FILTER d.age > @value0
  COLLECT city = d.city WITH COUNT INTO cnt
  RETURN { city, cnt }
```
bind: `{ "@col": "users", "value0": 21 }`, run per shard with `{ shardIds: [...] }`, Trino re-aggregates across splits.

---

## 7. AQL Passthrough — `query()` Table Function

Mirrors the Mongo/JDBC `query()` polymorphic table function pattern (`io.trino.plugin.mongodb.ptf.Query`). Signature:

```sql
SELECT * FROM
  TABLE(arango.system.query(database => 'mydb', query => 'FOR v IN OUTBOUND "users/42" GRAPH "social" RETURN v'));
```

- Implemented as `ArangoQueryFunction extends AbstractConnectorTableFunction`; analysis runs the AQL once (or inspects a `LIMIT 1`) to derive the result column schema via the same type-mapping logic, then returns a passthrough handle the split manager executes as a **single split** (no shard rewriting — the user's AQL is opaque).
- This is the **only** path to graph traversals, `GEO`, full-text (ArangoSearch), and other non-relational AQL.
- **Read-only enforcement:** the connector rejects passthrough AQL containing data-modification operations (`INSERT`/`UPDATE`/`REPLACE`/`REMOVE`/`UPSERT`) via AQL parse inspection, and the deployment guide recommends a **read-only ArangoDB user** as defense in depth.

---

## 8. Configuration Reference

| Property | Default | Purpose |
|---|---|---|
| `arangodb.hosts` | — | Comma-separated `host:port` (coordinators). |
| `arangodb.user` / `arangodb.password` | — | Basic auth. |
| `arangodb.auth.jwt` | — | Optional JWT instead of basic auth. |
| `arangodb.ssl.enabled` | false | Enable TLS. |
| `arangodb.ssl.truststore-path` / `-password` | — | TLS trust config. |
| `arangodb.schema-collection` | `_schema` | Override-collection name. |
| `arangodb.schema.sample-size` | 1000 | Docs sampled when inferring. |
| `arangodb.schema.sample-random` | false | `SORT RAND()` sampling. |
| `arangodb.schema.mixed-type-strategy` | `varchar` | Conflict degrade target (`varchar`\|`json`). |
| `arangodb.case-insensitive-name-matching` | false | Map lowercased Trino names to real names. |
| `arangodb.shards-per-split` | 1 | Shards grouped per split. |
| `arangodb.max-splits` | (cap) | Safety limit on split count. |
| `arangodb.query.batch-size` | (driver default) | AQL cursor `batchSize`. |
| `arangodb.query.timeout` | — | AQL execution timeout. |

Credentials should be supplied via Trino's secrets support, not inline.

---

## 9. Error Handling, Security, Testing

### 9.1 Error handling
- Dedicated `ArangoErrorCode` enum (`ARANGODB_CONNECTION_ERROR`, `ARANGODB_SCHEMA_ERROR`, `ARANGODB_QUERY_ERROR`, `ARANGODB_TYPE_CONVERSION_ERROR`).
- Missing collection → `TableNotFoundException`.
- Empty collection with no schema source → actionable `ARANGODB_SCHEMA_ERROR` ("define a `_schema` entry, a validation rule, or ensure documents exist").
- AQL failures surfaced with the ArangoDB error number and message.

### 9.2 Security
- TLS to coordinators; credentials via secrets.
- **Read-only trust boundary:** connector performs only reads; passthrough AQL is validated read-only (§7); deployment guide recommends a read-only DB user.
- No credential logging; redact connection URLs in logs/`EXPLAIN`.

### 9.3 Testing
- **Integration:** `TestingArangoServer` via Testcontainers (`arangodb/arangodb` image); spin up single-server and a multi-shard cluster fixture.
- **Conformance:** extend Trino's `BaseConnectorTest` (read subset), assert unsupported writes are cleanly rejected.
- **Type mapping:** `SqlDataTypeTest` round-trips for every mapping in §3.2, including nested `ROW`/`ARRAY` and mixed-type degradation.
- **Pushdown:** `EXPLAIN`-based assertions that filter/limit/projection/aggregation leave **no residual** in Trino (`assertConditionallyPushedDown`), plus a negative test that unsupported predicates stay in Trino.
- **Split correctness:** multi-shard scan returns exactly the same rows/counts as a single-split scan (no gaps/dupes); aggregation re-combines correctly across splits.
- **Schema:** regression test reproducing the Mongo null-drop case (§4.1) proving the column is retained.

---

## 10. Milestones (delivery order)

| # | Milestone | Contents | Exit criteria |
|---|---|---|---|
| **M1** | Read skeleton | Plugin/Factory/Connector/Metadata; list schemas/tables/columns; sampling schema (merge-based, §4.1); single-split full scan; base type mapping (§3.2) | `SELECT *` over a collection returns correct rows/types |
| **M2** | Filter + limit + projection pushdown | `applyFilter`, `applyLimit`, `applyProjection` → `AqlBuilder`; bind vars | `EXPLAIN` shows no residual filter/limit; nested projection works |
| **M3** | Shard-parallel splits | Shard discovery; `shardIds` execution option; partial/final split wiring | N shards ⇒ N splits, concurrent, correct counts |
| **M4** | Aggregation pushdown | `applyAggregation` (COUNT/SUM/MIN/MAX/AVG, GROUP BY) with per-shard partials + Trino final | Aggregates pushed; cross-split re-aggregation correct |
| **M5** | Schema sources + `query()` passthrough | `_schema` override reader; native validation-rule reader; precedence; `ArangoQueryFunction` with read-only enforcement | Precedence honored; graph traversal reachable via `query()` |
| **M6** | Hardening | TLS/auth, case-insensitive matching, secrets, `BaseConnectorTest` conformance, docs | Conformance suite green; deployment guide published |

---

## Appendix A — Key references
- Trino connector development: https://trino.io/docs/current/develop/connectors.html
- Trino MongoDB connector (precedent): https://trino.io/docs/current/connector/mongodb.html
- Mongo inference drops columns on null/empty: https://github.com/prestosql/presto/issues/2934
- Open request for user-defined schemas in Mongo (ArangoDB gives this natively): https://github.com/trinodb/trino/issues/24106
- ArangoDB Java driver: `com.arangodb:arangodb-java-driver`
- ArangoDB schema validation (JSON Schema on collections): ArangoDB ≥ 3.7
- ArangoDB AQL `shardIds` query option: per-shard execution targeting
