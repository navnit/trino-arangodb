# trino-arangodb

[![CI](https://github.com/navnit/trino-arangodb/actions/workflows/ci.yml/badge.svg)](https://github.com/navnit/trino-arangodb/actions/workflows/ci.yml)

A [Trino](https://trino.io) connector that lets you run SQL against [ArangoDB](https://arangodb.com).
ArangoDB **databases map to Trino schemas** and **collections map to tables**; schemas are
inferred by sampling documents. The connector is currently **read-only**, with equality/IN
filter pushdown for all scalar types, guarded numeric range pushdown, and `LIMIT` pushdown.

> **Status.** Milestones **M1** ("read skeleton") and **M2** ("filter-pushdown completion")
> are complete. Writes (`INSERT`/`DELETE`), value materialization for `ARRAY`/`ROW`/`DECIMAL`,
> and shard-parallel splits are out of scope so far — see [Limitations](#limitations).

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
| `arangodb.type-coercion` | `lenient` | Per-cell type-mismatch policy — see [Type coercion](#type-coercion). |

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
| Array | `ARRAY(...)` *(schema only — not yet materializable)* |
| Object | `ROW(...)` *(schema only — not yet materializable)* |
| Field seen only as `null` | `VARCHAR` |
| Field with incompatible mixed types | `VARCHAR` (per `mixed-type-strategy`) |

Merging an integer-typed and a floating-point occurrence of the same field **widens to
`DOUBLE`**. `ARRAY`/`ROW`/`DECIMAL` columns are inferred and shown by `DESCRIBE`/`SHOW COLUMNS`,
but selecting their **values** raises `NOT_SUPPORTED` until a later milestone (see
[Limitations](#limitations)).

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
- `LIMIT` is pushed into the scan (single-split, so the pushed limit is exact).
- **Strict mode disables pushdown entirely** (`type-coercion=strict`), so a type-mismatched row
  is never silently dropped server-side before the strict error can be raised.

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
- **`ARRAY` / `ROW` / `DECIMAL` values are not materializable yet** — such columns appear in the
  schema, but projecting them raises `NOT_SUPPORTED`. Filtering/selecting other columns of the
  same table is unaffected.
- **One split per table** — no shard/range fan-out; a collection is scanned as a single unit.
- **Schema cache has no TTL** — resolved column metadata is memoized per table for the connector's
  lifetime.
- **Non-finite stored doubles** (`Infinity`/`NaN`) can be dropped by a fully-pushed `DOUBLE`
  predicate. This is unreachable via normal JSON ingestion (ArangoDB cannot represent them in
  JSON) and only affects documents written by a native-VelocyPack driver — an accepted limitation.

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

See [`CLAUDE.md`](CLAUDE.md) for a detailed architecture walkthrough (SPI wiring, the
Metadata → SplitManager → PageSourceProvider → PageSource read path, error translation, and the
`pom.xml` dependency-pin rationale).

## License

Apache License 2.0 — see [`LICENSE`](LICENSE). One file
(`src/main/java/io/arango/trino/type/UnknownType.java`) is a relocation of Trino's own
`io.trino.spi.type.UnknownType`, also Apache-2.0; this is recorded in [`NOTICE`](NOTICE).
