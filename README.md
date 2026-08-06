# trino-arangodb

[![CI](https://github.com/navnit/trino-arangodb/actions/workflows/ci.yml/badge.svg)](https://github.com/navnit/trino-arangodb/actions/workflows/ci.yml)

A [Trino](https://trino.io) connector that lets you run SQL against [ArangoDB](https://arangodb.com).
ArangoDB **databases map to Trino schemas** and **collections map to tables**; schemas are
inferred by sampling documents by default, or pinned explicitly per table via a
[schema-override collection](#schema-overrides). The connector is currently **read-only**, with
equality/IN filter pushdown for all scalar types, guarded numeric range pushdown, and `LIMIT`
pushdown.

> **Status.** Milestones **M1**–**M5** and **M6-A**–**M6-C** are complete (**M5**: aggregation
> pushdown — `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` and `GROUP BY` executed as an AQL `COLLECT` on a
> single split; **M6-B**: an `arango.system.query` table function for raw AQL passthrough — see
> [AQL passthrough](#aql-passthrough); **M6-C**: user-curated schema-override documents and
> declared `decimal`/`timestamp` types — see [Schema overrides](#schema-overrides)). Writes
> (`INSERT`/`DELETE`) are out of scope for now — see [Limitations](#limitations).

## Requirements

| | |
|---|---|
| **Build JDK** | Java 24 (`maven.compiler.release=24`) |
| **Trino** | 476 (`trino-spi` `provided`-scope) |
| **ArangoDB** | 3.11+ (CI/tests run against 3.12.x) |
| **Tests** | Docker running locally (tests use [Testcontainers](https://testcontainers.com) against a real ArangoDB) |

Maven itself is not required to be on `PATH` in every environment; if `mvn` is missing, the
project is set up for [SDKMAN!](https://sdkman.io) (`source ~/.sdkman/bin/sdkman-init.sh`).

## Build

```bash
mvn package        # produces the trino-plugin artifact under target/
```

`packaging` is `trino-plugin`, so the build emits a self-contained plugin directory
(`target/trino-arangodb-<version>/`). Copy that directory into your Trino installation's
`plugin/` directory (e.g. `plugin/arangodb/`) and restart the coordinator/workers.

## Configure a catalog

Create `etc/catalog/arango.properties` in your Trino installation (the file name becomes the
catalog name, so this catalog is reachable as `arango`):

```properties
connector.name=arangodb
arangodb.hosts=localhost:8529
arangodb.user=root
arangodb.password=
```

### All configuration properties

| Property | Default | Description |
|---|---|---|
| `arangodb.hosts` | `localhost:8529` | Comma-separated `host:port` coordinator list. |
| `arangodb.user` | `root` | ArangoDB user. |
| `arangodb.password` | *(empty)* | ArangoDB password (marked security-sensitive; masked in logs). |
| `arangodb.schema.sample-size` | `1000` | Number of documents sampled per collection to infer its schema. |
| `arangodb.schema.sample-random` | `false` | Sample randomly (vs. the first N documents). |
| `arangodb.schema.mixed-type-strategy` | `VARCHAR` | Fallback type when a field holds genuinely incompatible types across the sample. (`JSON` is accepted but not yet wired to an output type.) |
| `arangodb.schema.cache-ttl` | `5m` | How long a resolved collection schema is cached before re-sampling. |
| `arangodb.type-coercion` | `lenient` | Per-cell type-mismatch policy — see [Type coercion](#type-coercion). |
| `arangodb.shards-per-split` | `1` | Target number of shards grouped into each split on cluster fan-out. See [Sharding / parallelism](#sharding--parallelism). |
| `arangodb.max-splits` | `32` | Hard cap on the number of splits per collection scan. |
| `arangodb.shard-parallelism-enabled` | `true` | Set to `false` to force single-split scans unconditionally and never invoke the internal `shardIds` option. |
| `arangodb.aggregation-pushdown-enabled` | `true` | Set `false` to compute every aggregate in Trino instead of pushing it into AQL. See [Aggregation pushdown](#aggregation-pushdown). |
| `arangodb.query-function-enabled` | `true` | Set `false` to remove the `arango.system.query` table function entirely. See [AQL passthrough](#aql-passthrough). |
| `arangodb.statistics-enabled` | `true` | Expose row-count table statistics to the optimizer; `false` returns unknown statistics everywhere. |
| `arangodb.statistics.cache-ttl` | `5m` | How long a collection row count is cached for planning. |
| `arangodb.schema-collection` | `trino_schema` | Per-database collection holding user-curated schema-override documents. See [Schema overrides](#schema-overrides). |

## Data model

| ArangoDB | Trino |
|---|---|
| Database | Schema |
| Collection (non-system) | Table |
| Document | Row |
| Attribute | Column |

Every collection exposes ArangoDB's system attributes `_key`, `_id`, `_rev` as **hidden**
`VARCHAR` columns. For **edge collections**, `_from` and `_to` are additionally exposed as
**visible** `VARCHAR` columns.

### Schema inference & type mapping

There is no fixed schema in ArangoDB, so the connector samples up to `sample-size` documents
and takes the **union of fields**, merging each field's observed types:

| Sampled value | Trino type |
|---|---|
| Boolean | `BOOLEAN` |
| Integer within signed 64-bit | `BIGINT` |
| Integer beyond signed 64-bit / `uint64` | `DECIMAL(38,0)` |
| Other number (fractional / floating point) | `DOUBLE` |
| String | `VARCHAR` |
| Array | `ARRAY(...)` (values materialize recursively) |
| Object | `ROW(...)` (values materialize recursively) |
| Field seen only as `null` | `VARCHAR` |
| Field with incompatible mixed types | `VARCHAR` (per `mixed-type-strategy`) |

Merging an integer-typed and a floating-point occurrence of the same field **widens to
`DOUBLE`**. `ARRAY`/`ROW`/`DECIMAL` columns are inferred and shown by `DESCRIBE`/`SHOW COLUMNS`,
and (since M4) selecting their **values** materializes them recursively: under
`arangodb.type-coercion=lenient` a type-mismatched leaf reads as `NULL` (only that element/field,
not the whole row), while `strict` raises `ARANGODB_TYPE_CONVERSION_ERROR` with a path to the
offending leaf (e.g. `col[2].b`). As of M6-C, this also applies to a sampled `DECIMAL(38,0)`
column, not just a declared `decimal(p,s)` override — decimal materialization is one
type-dispatched code path regardless of how the column's type was determined, so a numeric
string now converts too (exact fit at the column's scale only; under `strict` this loosens what
was previously an error into a successful parse). See "Decimals should be stored as strings"
under [Schema overrides](#schema-overrides) for the exact-fit rule.

## Schema overrides

Sampling is the default schema source, but you can pin a table's schema explicitly instead by
writing a document to a per-database **schema-override collection** (`arangodb.schema-collection`,
default `trino_schema`). When a matching override doc exists for a table, it is the **complete**
user-column set — sampling does not run for that table at all.

### Doc shape

One document per table, in the override collection of the *same database* as the described table:

```json
{ "table": "orders",
  "fields": [
    { "name": "total",     "type": "decimal(12,2)" },
    { "name": "placed_at", "type": "timestamp(3) with time zone", "hidden": false }
  ] }
```

- `table` — required, non-empty string: the described collection's name.
- `fields` — required, non-empty array of `{name, type, hidden}` objects:
  - `name` — required, non-empty, must not start with `_` (system attributes are appended
    automatically), and must not duplicate another field's name case-insensitively (Trino resolves
    column identifiers case-insensitively, so `Total` and `total` would be indistinguishable at
    query time).
  - `type` — required string, parsed by Trino's own type parser then checked against the
    supported vocabulary below.
  - `hidden` — optional boolean, default `false`.
- `_key`/`_id`/`_rev`/`_from`/`_to` are appended automatically exactly as on the sampling path
  (hidden `VARCHAR` system attributes always; `_from`/`_to` visible `VARCHAR` for edge
  collections) — do not declare them yourself.

Any unrecognized key anywhere in the document (a typo like `"hiden"`, or the not-yet-supported
`path`) is rejected loudly, as is a second document claiming the same `table` — ambiguity is never
resolved silently. Validation is strict on purpose: the collection is user-curated, so a typo must
never silently change a schema.

### Type vocabulary

| Declared type | Notes |
|---|---|
| `boolean` / `bigint` / `double` / `varchar` | Same behavior as sampled columns. `varchar(n)` (bounded) is rejected — use unbounded `varchar`. |
| `decimal(p,s)` | Arbitrary precision/scale. **Store decimals as strings** — see below. |
| `timestamp(3)` | ISO-8601 **local** date-time string (no zone/offset), e.g. `2026-08-05T12:34:56.789`. Fractional seconds finer than millis (e.g. `.7891`) are a mismatch, not rounded. |
| `timestamp(3) with time zone` | ISO-8601 date-time string **with** a `Z`/offset, e.g. `2026-08-05T12:34:56.789+05:30`. Same finer-than-millis rule; the offset must be a whole minute within ±14:00. |
| `array(t)` | Recursive: `array(timestamp(3))`, `array(decimal(10,2))`, etc. |
| `row("f" t, ...)` | Recursive, e.g. `row("amount" decimal(10,2), "note" varchar)`. See the quoting rule below. |

Alias spellings that resolve to the same underlying type are accepted: bare `timestamp` (→
`timestamp(3)`), bare `decimal` (→ `decimal(38,0)`), and `timestamp(3) without time zone` (→
`timestamp(3)`). Not supported at all: `date`, `time`, `real`/`integer`/`smallint`/`tinyint`,
`json`, `uuid`, `varbinary`, timestamp precisions other than 3, and `char`.

**Decimals should be stored as strings.** ArangoDB has no native decimal type, so a `decimal(p,s)`
column also accepts a plain JSON number — but a stored `double` only matches when its *exact
binary value* fits the declared scale: `0.25` at `decimal(p,2)` matches, but `12.34` does **not**
(`12.34` isn't exactly representable in binary floating point, so it reads as a type mismatch —
`NULL` under `lenient`, an error under `strict`). Storing the value as a decimal **string**
(`"12.34"`) avoids this entirely and is the encoding the type exists for.

**Quoting rule for nested `row(...)` fields.** SQL identifiers inside a type string are lowercased
unless double-quoted. `row(placedAt timestamp(3))` parses as field name `placedat`, which then
never matches a stored attribute named `placedAt` — the column reads all `NULL`, silently, with no
error. Always double-quote a nested field name that isn't already all-lowercase:
`row("placedAt" timestamp(3))`.

### Accepted limitations

- **A misspelled (or wrong-case, unquoted) `name` reads as an all-`NULL` column, not an error.**
  The override exists specifically to avoid sampling, so the connector has no way to check a
  declared name against what's actually stored. This applies in both `lenient` and `strict`
  coercion mode — a stored ArangoDB "attribute absent" is AQL `null`, which becomes SQL `NULL`
  before any type check ever runs.
- **No renaming.** Field-path remapping (`path`) is not supported yet, so a declared `name` is
  simultaneously the Trino column name *and* the exact, case-sensitive ArangoDB attribute name — an
  override cannot expose attribute `placedAt` as column `placed_at`, for example. A doc containing
  `path` is rejected explicitly ("not yet supported") rather than silently ignored.

### Operational notes

- **Index the override collection on `table`.** Every lookup is `FOR d IN <collection> FILTER
  d.table == @t LIMIT 2 RETURN d`; without a persistent index on `table`, each lookup scans the
  whole collection (bounded in practice — one doc per table — but a persistent index makes it
  O(1)).
- The override collection itself is an ordinary collection: it shows up in `SHOW TABLES` and is
  queryable like any other table (hiding it would be surprising, and querying it is useful for
  debugging).
- A malformed override doc only breaks queries against *that* table — it's validated lazily, at
  column-resolution time, not when the catalog is enumerated. `SHOW TABLES` and `SHOW COLUMNS`
  on sibling tables keep working.
- With no override collection present at all, nothing changes: every table resolves via sampling,
  same as before this feature existed. The absence is detected with one cheap existence check per
  database (cached for `arangodb.schema.cache-ttl`), not a failed query per table.

## Sharding / parallelism

On a cluster deployment, a collection scan can fan out into multiple Trino splits instead of
always reading the whole collection as a single unit. On every `getSplits` call, the connector
works through:

1. **Discover** — fetch the collection's shard count, sharding strategy, and SmartJoin attribute.
2. **Allowlist gate** — a collection is only eligible for fan-out if it has more than one shard,
   its sharding strategy is a non-smart hash strategy (`hash`, `community-compat`, or
   `enterprise-compat`), and it has no `smartJoinAttribute`. SmartGraph/SmartJoin collections are
   always excluded — their edges can live in multiple internal sub-shards, which would
   double-count rows under naive per-shard enumeration.
3. **Enumerate** — list the collection's shard IDs.
4. **Group** — partition the shard IDs into balanced groups: the number of splits is
   `min(ceil(shardCount / arangodb.shards-per-split), arangodb.max-splits)`. Every shard lands in
   exactly one group; `arangodb.max-splits` is a hard cap that can force more than
   `arangodb.shards-per-split` shards into a single group once the ceiling exceeds it.
5. **Probe** — before trusting ArangoDB's internal `shardIds` query option, the connector
   requires both a version pin (the server must report **≥ 3.11**) and an active capability
   probe: for the groups about to be emitted, the sum of the per-group `shardIds`-scoped counts
   must equal the full collection count. The verdict is computed once per connector process and
   cached; an inconclusive probe (e.g. an empty collection) is retried on a later call rather
   than cached.
6. **Emit** — one Trino split per shard group, each split executing its own AQL query scoped to
   that group's `shardIds`.

Non-smart hash collections with more than one shard, on a cluster, are the only case that gets
more than one split. **Every other case falls back to a single split** that scans the whole
collection: SmartGraph/SmartJoin collections, satellite collections, single-server deployments
(no sharding at all), and any multi-shard collection that fails the allowlist gate, fails the
capability probe, or whose shard discovery throws. A multi-shard collection that falls back this
way logs a `WARN` so the fallback is observable.

Set `arangodb.shard-parallelism-enabled=false` to force single-split scans unconditionally — this
also skips the version/capability probe and never invokes the internal `shardIds` option.

**Interaction with `LIMIT` pushdown:** with shard-parallelism enabled (the default), a pushed
`LIMIT n` runs independently within each split's own AQL cursor — a per-split reduction, not a
global one — so Trino still applies the final `LIMIT` itself over the merged results. Only with
`arangodb.shard-parallelism-enabled=false` (always single-split) is a pushed limit exact.

## Predicate & LIMIT pushdown

The read path is **type-exact** (see below), which lets the connector push filters into AQL
knowing the server-side predicate admits exactly the values the reader would keep.

| Predicate | `BOOLEAN` | `VARCHAR` | `BIGINT` | `DOUBLE` |
|---|:---:|:---:|:---:|:---:|
| `=` / `IN` (equality) | ✅ full | ✅ full | ✅ full | ✅ full |
| `<` `>` `<=` `>=` (range) | — | residual | ⚠️ prefilter + residual | ✅ full |
| `IS NULL` / `IS NOT NULL` | residual | residual | residual | residual |

- **Fully pushed down** (`= / IN` for all scalar types, and `DOUBLE` range) — enforced entirely
  by AQL; nothing is left for Trino to re-check.
- **`BIGINT` range** is pushed as a **wire-reducing prefilter** *and* kept as a Trino residual:
  its `IS_NUMBER` guard admits a safe superset (fractional and out-of-`int64` values that the
  read path reads as `NULL`), so Trino re-checks after read.
- **`DOUBLE` comparisons** render as `IS_NUMBER(d.f) AND (d.f + 0.0) <op> @v` — the `+ 0.0`
  promotes a stored `int64` into double space so AQL compares exactly what the reader rounds to.
- `LIMIT` is pushed into the scan. It is exact only for a single-split scan; with
  shard-parallelism enabled (the default), the pushed limit is a per-split reduction, not a
  global guarantee — see [Sharding / parallelism](#sharding--parallelism).
- **Strict mode disables pushdown entirely** (`type-coercion=strict`), so a type-mismatched row
  is never silently dropped server-side before the strict error can be raised.

## Aggregation pushdown

`COUNT`/`SUM`/`MIN`/`MAX`/`AVG` and single-grouping-set `GROUP BY` (including `SELECT DISTINCT`)
are pushed into an AQL `COLLECT ... AGGREGATE`. An aggregated scan always runs as **exactly one
split** — Trino treats a connector's aggregate output as final, so fanning out across shards would
emit one duplicate row per split. ArangoDB still parallelizes that single query across its own
shards internally.

Every aggregate input and grouping key is wrapped in a type guard that reproduces the read path
exactly, so the pushed query computes over precisely the values a non-pushed scan would have
materialized. Values the reader would treat as `NULL` become AQL `null`, which AQL's aggregates
ignore — matching SQL.

| Aggregate | Pushed for | Computed in Trino instead |
|---|---|---|
| `count(*)` | always | — |
| `count(col)` | `BOOLEAN`, `VARCHAR`, `BIGINT`, `DOUBLE` | structured / `DECIMAL` columns |
| `min` / `max` | `BIGINT`, `DOUBLE` | `VARCHAR` — ArangoDB orders strings by the server's collation, Trino by codepoint |
| `sum` / `avg` | `DOUBLE` | `BIGINT` — AQL accumulates sums in double, losing precision past 2⁵³ and turning Trino's `sum(bigint)` overflow error into a silent wrong answer |
| `GROUP BY` key | `BOOLEAN`, `VARCHAR`, `BIGINT`, `DOUBLE` | structured / `DECIMAL` columns |

Also declined: `DISTINCT` aggregates, `HAVING`, `GROUPING SETS`/`CUBE`/`ROLLUP`, ordered aggregates,
aggregates with a `FILTER (WHERE ...)` clause, and any aggregate whose argument is not a plain
column reference. Declining costs performance, never correctness — Trino computes those itself.

Two interactions worth knowing:

- **Strict coercion disables aggregation pushdown entirely**, for the same reason it disables filter
  pushdown: a pushed aggregate would silently absorb the type mismatch strict mode exists to report.
- **A `BIGINT` range predicate suppresses aggregation pushdown** on that query. Such a predicate is
  a *prefilter* — enforced partly in AQL and partly by Trino's re-check — so an aggregate computed
  server-side alone would count rows the re-check drops. `... WHERE double_col > 100 GROUP BY city`
  pushes; `... WHERE bigint_col > 100 GROUP BY city` does not. Missed optimization, correct answer.

## AQL passthrough

`arango.system.query(database, query)` is a table function that runs a raw AQL query verbatim and
exposes its result as a Trino table, for graph traversals and other AQL that has no relational
equivalent:

```sql
SELECT *
FROM TABLE(arango.system.query(
    database => 'shop',
    query => 'WITH users FOR v IN 1..2 OUTBOUND "users/ada" follows RETURN {name: v.name}'));
```

Read-only is enforced by inspecting the query's `EXPLAIN` plan and rejecting anything that is not
purely `"read"` access, but this check is defense in depth, not the primary control — deploy the
connector with a read-only ArangoDB user as the actual guarantee. The query also runs once at
planning time (to sample rows and derive a schema) and once again at execution, so it must be
side-effect-free and must return at least one row, or planning fails. The planning-time sample is
`arangodb.schema.sample-size` rows (default 1000) — the same knob schema inference uses for
ordinary table scans. `LIMIT` cannot be pushed into opaque AQL, and the execution cursor is
non-streaming, so the server materializes the full passthrough result regardless of any Trino-side
`LIMIT` (matching the existing scan path's cursor behavior). Set
`arangodb.query-function-enabled=false` to remove the function entirely.

## Type coercion

`arangodb.type-coercion` controls what happens when a stored value's runtime type does not match
its inferred Trino column type (ArangoDB is schemaless, so this is expected):

- **`lenient`** (default) — the mismatched cell reads as `NULL`.
- **`strict`** — reading a mismatched cell raises `ARANGODB_TYPE_CONVERSION_ERROR`.

Coercion is intentionally exact rather than lossy: a number stored under a `VARCHAR` column, or a
fractional value (`42.5`) stored under a `BIGINT` column, is a *mismatch*. A fractional-free
number under `BIGINT` (e.g. `42.0`) still reads as `42`. This exactness is what makes filter
pushdown safe — the pushed AQL and the reader agree on exactly which values qualify.

## Limitations

- **Read-only** — no `INSERT`/`UPDATE`/`DELETE`.
- **Shard-parallel fan-out is narrow by design** — only non-smart, multi-shard hash collections on
  a cluster get more than one split; SmartGraph/SmartJoin collections, satellite collections, and
  single-server deployments always scan as a single split. See
  [Sharding / parallelism](#sharding--parallelism).
- **No cross-split snapshot consistency** — each split executes as an independent AQL query, so a
  concurrently-mutated collection can yield a read that is not a single point-in-time snapshot
  across splits (the same limitation applies, in miniature, to any single AQL cursor). Documented,
  not solved.
- **Non-finite stored doubles** (`Infinity`/`NaN`) can be dropped by a fully-pushed `DOUBLE`
  predicate. This is unreachable via normal JSON ingestion (ArangoDB cannot represent them in
  JSON) and only affects documents written by a native-VelocyPack driver — an accepted limitation.

- **A `DOUBLE` `sum`/`avg` that overflows reads back as `0`, not `Infinity`.** JSON and VelocyPack
  cannot carry non-finite doubles, so an overflowing pushed sum is reported as `0`. This needs
  summands within a few orders of magnitude of `DBL_MAX`; it shares a root cause with the
  non-finite-doubles note above.
- **`sum`/`avg` over `DOUBLE` may differ in the last bits** between the pushed and non-pushed plan,
  because summation order differs. Trino's own multi-split `sum(double)` is already
  order-dependent in the same way.
- **`min`/`max` over a `DOUBLE` column holding both `-0.0` and `0.0`** may differ in the sign of the
  returned zero. The two values are equal under SQL `=`.
- **`SELECT DISTINCT col ... LIMIT n` does not push** — Trino plans it as a `DistinctLimitNode`,
  which connector aggregation pushdown does not match.

## Development

```bash
mvn test                                                   # full suite (needs Docker)
mvn test -Dtest=AqlBuilderTest                             # one test class
mvn test -Dtest=TypeMapperTest#mergeIntAndFloatWidensToDouble   # one test method
```

The suite uses **no mocking framework**: integration-style tests spin up a real ArangoDB
container via Testcontainers, and end-to-end SQL runs against a live container through Trino's
`DistributedQueryRunner`. Where a test must avoid a live server (e.g. metadata error paths), it
uses a hand-written `ArangoClient` subclass as a test double.

### Static analysis

A Docker-free static-analysis stack runs as a separate CI job and can be run locally:

```bash
mvn spotless:check          # google-java-format (AOSP/4-space), ratcheted to origin/master
mvn spotless:apply          # auto-fix formatting on changed files
mvn checkstyle:check        # import hygiene, naming, @Override, equals/hashCode, empty blocks
mvn compile spotbugs:check  # SpotBugs + FindSecBugs bug/security patterns (needs compiled classes)
```

Formatting is enforced on a **ratchet** (changed files only) so the existing hand-tuned source is
left untouched; pre-existing Checkstyle/SpotBugs findings are grandfathered with documented
suppressions under `config/`, and the gates apply to new/changed code going forward. Optional
local git hooks are available via `.pre-commit-config.yaml` (`pre-commit install`). See
[`CLAUDE.md`](CLAUDE.md) and the [design spec](docs/superpowers/specs/2026-07-22-static-analysis-tooling-design.md)
for the full rationale.

See [`CLAUDE.md`](CLAUDE.md) for a detailed architecture walkthrough (SPI wiring, the
Metadata → SplitManager → PageSourceProvider → PageSource read path, error translation, and the
`pom.xml` dependency-pin rationale).

## License

Apache License 2.0 — see [`LICENSE`](LICENSE). One file
(`src/main/java/io/arango/trino/type/UnknownType.java`) is a relocation of Trino's own
`io.trino.spi.type.UnknownType`, also Apache-2.0; this is recorded in [`NOTICE`](NOTICE).
