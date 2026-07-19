# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Trino connector plugin (`packaging: trino-plugin`) that lets Trino run SQL queries against ArangoDB. ArangoDB databases map to Trino schemas, collections map to tables. Currently at milestone M1 ("read skeleton"): read-only, full-collection scans, no pushdown.

## Commands

Maven is not on `PATH` by default in this environment; if `mvn` reports "command not found", run:

```bash
source ~/.sdkman/bin/sdkman-init.sh
```

Build requires Java 24 (`maven.compiler.release=24` in `pom.xml`).

```bash
mvn package                          # build the trino-plugin artifact
mvn test                             # run the full test suite
mvn test -Dtest=TypeMapperTest       # run a single test class
mvn test -Dtest=TypeMapperTest#mergeIntAndFloatWidensToDouble   # single test method
```

Most test classes (`ArangoClientTest`, `ArangoConnectorQueryTest`, `ArangoPageSourceProviderTest`, and others using `TestingArangoServer`) spin up a real ArangoDB container via Testcontainers — **Docker must be running locally** for `mvn test` to pass. There are no mocks of ArangoDB anywhere in the test suite; everything runs against a live container, including full end-to-end SQL queries via Trino's `DistributedQueryRunner` (see `ArangoConnectorQueryTest`).

There is no linter/formatter plugin configured in `pom.xml`.

### pom.xml is a standalone module with no parent POM

This means several dependency versions that a real Trino connector would normally inherit from `io.trino:trino-root`'s `dependencyManagement` are pinned explicitly here instead, each with a long comment explaining *why* (`trino-spi`-transitive `provided`-scope pins for `jackson-annotations`/`opentelemetry-api`/`opentelemetry-context`; `io.airlift` `jaxrs`/`http-client`/`jmx` pinned to `336` to fix a `discovery-server:1.37`-induced skew; `junit-jupiter-api`/`junit-platform-launcher` pins to fix JUnit5 mediation). **Do not remove or "simplify" these pins** without reading the comment above each one — they exist to work around real dependency-mediation bugs, not stylistic preference. The Surefire `argLine` and `api.version=1.51` system property are similarly required workarounds (the latter for Testcontainers 1.20.4's shaded docker-java client rejecting modern Docker Engine's default API version) — also documented inline.

## Architecture

### SPI wiring (Plugin → Connector)

Trino discovers the connector via `ArangoPlugin.getConnectorFactories()` → `ArangoConnectorFactory` (registered under the name `"arangodb"`). `ArangoConnectorFactory.create()` boots an Airlift `Bootstrap`/Guice `Injector` from `ArangoModule`, then pulls a singleton `ArangoConnector` out of it. `ArangoModule` binds every component (`ArangoConfig`, `ArangoClient`, `TypeMapper`, `SchemaResolver`, `AqlBuilder`, `ArangoMetadata`, `ArangoSplitManager`, `ArangoPageSourceProvider`, `ArangoConnector`) as `Scopes.SINGLETON`. `ArangoConnector` just hands out these singletons for `getMetadata()`/`getSplitManager()`/`getPageSourceProvider()`, and its `shutdown()` calls `LifeCycleManager.stop()`, which triggers `ArangoClient`'s `@PreDestroy` (`close()` on the underlying `ArangoDB` driver instance).

`ArangoConfig` (bound via Airlift's `@Config`) holds every user-facing setting: `arangodb.hosts` (comma-separated `host:port` list), `arangodb.user`, `arangodb.password` (`@ConfigSecuritySensitive`), `arangodb.schema.sample-size` (default 1000), `arangodb.schema.sample-random`, `arangodb.schema.mixed-type-strategy` (`VARCHAR` or `JSON`, though `JSON` is not actually wired up yet — see `TypeMapper.merge`).

### Read path (Metadata → SplitManager → PageSourceProvider → PageSource)

This is the part that spans the most files and is worth understanding as one flow:

1. **`ArangoMetadata`** (`ConnectorMetadata`) — `listSchemaNames` = ArangoDB databases, `listTables`/`getTableHandle` = non-system collections within a database. Column info comes from `resolve()`, which calls `SchemaResolver.resolveColumns()` and memoizes the result per `SchemaTableName` in an unbounded `ConcurrentHashMap` (a real TTL cache is deferred past M1).
2. **`SchemaResolver`** samples up to `sampleSize` documents from the collection (`ArangoClient.sampleDocuments`) and infers a schema by taking the *union* of fields across all sampled docs, merging types per field via `TypeMapper.merge`. System attributes (`_key`, `_id`, `_rev`) are always added as hidden `VARCHAR` columns; `_from`/`_to` are added as visible `VARCHAR` columns only when the collection is an edge collection. A field seen only as `null` across the whole sample resolves through the `UnknownType.UNKNOWN` bottom sentinel down to `VARCHAR` (recursively, even nested inside `ROW`/`ARRAY`) — see `SchemaResolver.resolveUnknown`.
3. **`TypeMapper.inferType`/`merge`** implement the type-inference rules: `Boolean→BOOLEAN`, `Integer/Long→BIGINT`, `BigInteger` beyond 63 bits or `uint64→DECIMAL(38,0)`, other `Number→DOUBLE`, `String→VARCHAR`, `List→ARRAY(...)` (recursively merging element types), `Map→ROW(...)`. Merging an int-typed and double-typed occurrence of the same field widens to `DOUBLE`; merging any other genuinely incompatible pair falls back to `VARCHAR` regardless of `mixed-type-strategy` (the `JSON` strategy value exists in config but isn't implemented as an actual output type yet).
4. **`ArangoSplitManager`** always returns exactly one `ArangoSplit` per table (M1 has no shard/range fan-out — a collection is scanned as a single unit).
5. **`ArangoPageSourceProvider.createPageSource`** builds the AQL query via `AqlBuilder.buildScan` (M1: always `FOR d IN @@col RETURN d`, i.e. an unconditional full scan — no predicate or projection pushdown yet) and constructs an `ArangoPageSource` over the resulting cursor. Before doing so, it calls `checkMaterializable` on every *requested* column: if any has an `ArrayType`, `RowType`, or `DecimalType`, it throws `TrinoException(NOT_SUPPORTED, ...)` immediately, before the query runs. **Value materialization for ARRAY/ROW/DECIMAL is not implemented in M1** — the schema still infers and reports these types correctly (they show up in `SHOW COLUMNS`/`DESCRIBE`), but actually reading their values is out of scope until a later milestone. This check only fires for columns Trino actually projects, so a query on a table with an unsupported column that doesn't select that column is unaffected.
6. **`ArangoPageSource`** streams `ArangoCursor<BaseDocument>` into Trino `Page`s in batches of 1024 rows. Value coercion (`appendValue`) is *lenient*: any type mismatch or unexpected value silently becomes `NULL` rather than throwing — M1 implements only this lenient mode, with no config toggle for stricter behavior. The structured-type branch in `appendValue` is dead in practice because `checkMaterializable` already rejects those columns upstream; it remains only as a defensive fallback.

### Error translation

`ArangoMetadata` distinguishes "the ArangoDB database doesn't exist" (driver error number 1228, checked via `isDatabaseNotFound`) from every other `ArangoDBException`. The former is treated as Trino's normal "schema/table not found" (return `null` / skip), while everything else (auth failure, network partition, etc.) is rethrown as `TrinoException(GENERIC_INTERNAL_ERROR, ...)` so real failures aren't silently swallowed as "not found."

### `UnknownType` relocation

`io.arango.trino.type.UnknownType` is a verbatim Apache-2.0 relocation of Trino's own `io.trino.type.UnknownType`. It had to be copied rather than depended on: Trino's real `UnknownType` lives in the `trino-main` query-engine module, not in the public `trino-spi` contract a connector plugin can depend on, and Trino's plugin classloader only exposes `trino-spi` classes to plugins — a direct reference would fail with `NoClassDefFoundError` at runtime even if it compiled. This is documented in the class's own Javadoc and in `NOTICE`.

### Package layout

- `io.arango.trino` — SPI implementation classes (`ArangoPlugin`, `ArangoConnectorFactory`, `ArangoModule`, `ArangoConnector`, `ArangoConfig`, `ArangoMetadata`, `ArangoSplitManager`, `ArangoPageSourceProvider`, `ArangoPageSource`, `ArangoTransactionHandle`)
- `io.arango.trino.client` — `ArangoClient`, the sole wrapper around the ArangoDB Java driver (`ArangoDB`/`ArangoCursor`); also holds test-only seeding helpers (`createDatabaseForTest`, `insertForTest`, etc.) used by both unit and integration tests
- `io.arango.trino.handle` — SPI handle records (`ArangoTableHandle`, `ArangoColumnHandle`, `ArangoSplit`), all Jackson-serializable
- `io.arango.trino.schema` — `SchemaResolver`, the sampling/inference pipeline
- `io.arango.trino.type` — `TypeMapper` and the relocated `UnknownType`
- `io.arango.trino.aql` — `AqlBuilder`, translates a table scan into an AQL query string + bind vars

`TestingArangoServer` (`src/test`) wraps the Testcontainers-managed ArangoDB instance shared by integration-style tests.
