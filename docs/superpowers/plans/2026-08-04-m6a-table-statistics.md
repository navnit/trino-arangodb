# M6-A Table Statistics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `ArangoMetadata.getTableStatistics` returning a TTL-cached row-count estimate from ArangoDB's collection-count endpoint, per the approved spec `docs/superpowers/specs/2026-08-04-m6a-table-statistics-design.md`.

**Architecture:** A guard chain in `ArangoMetadata` (kill switch → passthrough decline → aggregation rows → limit interaction → cached count) over one new client method, `ArangoClient.countDocuments`. A second Guava cache beside `columnCache`, built on the same injected `Ticker`. Count failures degrade to `TableStatistics.empty()` + WARN — never fail planning — with no negative caching.

**Tech Stack:** Java 25, Trino SPI 483 (`TableStatistics`/`Estimate`), ArangoDB Java driver 7.13 (`ArangoCollection.count()`), Guava cache, Airlift config, JUnit 5 + AssertJ (no mocking framework — hand-written `ArangoClient` subclasses as doubles), Testcontainers for ITs.

## Global Constraints

- Maven needs `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` is not found; build requires Java 25.
- Docker must be running for `mvn test` (Testcontainers); pure-unit classes can run alone via `-Dtest=<Class>`.
- No mocking framework anywhere — test doubles are hand-written `ArangoClient` subclasses calling `super(new ArangoConfig())` (the driver connects lazily, so this never dials a server).
- Spotless is ratcheted to `origin/master` with google-java-format AOSP (4-space). Run `mvn spotless:apply` before each commit.
- Do NOT flip the trailing `precalculateStatistics` `false` argument of any `*ApplicationResult` in `ArangoMetadata` — spec §3 records why (`FilterStatsRule` + `default_filter_factor_enabled=false` would replace the base count with unknown).
- Config naming: `arangodb.statistics-enabled` (hyphen-leaf kill switch), `arangodb.statistics.cache-ttl` (dot-group TTL) — matches `arangodb.aggregation-pushdown-enabled` / `arangodb.schema.cache-ttl` precedent.
- Commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Config properties `statistics-enabled` and `statistics.cache-ttl`

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Produces: `ArangoConfig.isStatisticsEnabled()` → `boolean` (default `true`); `ArangoConfig.getStatisticsCacheTtl()` → `io.airlift.units.Duration` (default 5 minutes). Fluent setters `setStatisticsEnabled(boolean)` / `setStatisticsCacheTtl(Duration)` returning `ArangoConfig`. Tasks 3–4 consume the getters.

- [ ] **Step 1: Write the failing test**

In `ArangoConfigTest.testDefaults()`, add to the `recordDefaults` chain (order within the chain does not matter; put them after `.setQueryFunctionEnabled(true)`):

```java
                        .setStatisticsEnabled(true)
                        .setStatisticsCacheTtl(new Duration(5, MINUTES))
```

In `testExplicitPropertyMappings()`, add to the `props` builder:

```java
                        .put("arangodb.statistics-enabled", "false")
                        .put("arangodb.statistics.cache-ttl", "10m")
```

and to the `expected` chain:

```java
                        .setStatisticsEnabled(false)
                        .setStatisticsCacheTtl(new Duration(10, MINUTES))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -Dtest=ArangoConfigTest`
Expected: COMPILE ERROR — `cannot find symbol: method setStatisticsEnabled(boolean)` (a compile failure is this step's "red"; Airlift's `ConfigAssertions` can't run until the methods exist).

- [ ] **Step 3: Write minimal implementation**

In `ArangoConfig.java`, add fields after `private boolean queryFunctionEnabled = true;`:

```java
    private boolean statisticsEnabled = true;
    private Duration statisticsCacheTtl = new Duration(5, MINUTES);
```

Add accessors at the end of the class, following the existing style exactly:

```java
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    @Config("arangodb.statistics-enabled")
    @ConfigDescription(
            "Expose row-count table statistics to the optimizer; false returns unknown statistics everywhere")
    public ArangoConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getStatisticsCacheTtl() {
        return statisticsCacheTtl;
    }

    @Config("arangodb.statistics.cache-ttl")
    @ConfigDescription("How long a collection row count is cached for planning")
    public ArangoConfig setStatisticsCacheTtl(Duration statisticsCacheTtl) {
        this.statisticsCacheTtl = statisticsCacheTtl;
        return this;
    }
```

(`Duration`, `MINUTES`, `@MinDuration`, `@NotNull` are already imported in this file.)

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -Dtest=ArangoConfigTest`
Expected: PASS (both methods).

- [ ] **Step 5: Commit**

```bash
mvn spotless:apply
git add src/main/java/io/arango/trino/ArangoConfig.java src/test/java/io/arango/trino/ArangoConfigTest.java
git commit -m "feat: statistics-enabled + statistics.cache-ttl config (M6-A)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `ArangoClient.countDocuments`

**Files:**
- Modify: `src/main/java/io/arango/trino/client/ArangoClient.java` (add one method near `countWithShardIds`, ~line 110)
- Test: `src/test/java/io/arango/trino/client/ArangoClientTest.java`

**Interfaces:**
- Produces: `public long countDocuments(String database, String collection)` — returns the metadata-level document count; throws `IllegalStateException` on a `null` or negative driver count; propagates `ArangoDBException` on server errors. Task 4 consumes it.

**Requires Docker running** (this test class boots a Testcontainers ArangoDB).

- [ ] **Step 1: Write the failing tests**

In `ArangoClientTest`, add two tests. The existing `@BeforeAll` already seeds `shop.users` with 2 documents; also seed an empty collection for the zero case — add this line at the end of `setup()`:

```java
        client.createDocumentCollectionForTest("shop", "empty_col");
```

New tests:

```java
    @Test
    void countDocumentsReturnsSeededCount() {
        assertThat(client.countDocuments("shop", "users")).isEqualTo(2L);
    }

    @Test
    void countDocumentsOnEmptyCollectionIsZero() {
        assertThat(client.countDocuments("shop", "empty_col")).isEqualTo(0L);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest=ArangoClientTest`
Expected: COMPILE ERROR — `cannot find symbol: method countDocuments`.

- [ ] **Step 3: Write minimal implementation**

In `ArangoClient.java`, directly below `countWithShardIds` (so the two counts and their contrasting comments sit together):

```java
    /**
     * Metadata-level count (GET /_api/collection/{name}/count) for table statistics (spec M6-A
     * §3). Deliberately not countWithShardIds: that runs an AQL COLLECT WITH COUNT for probe
     * fidelity, and the optimizer is not guaranteed to collapse it to a metadata read. The driver
     * returns a nullable boxed Long; null or negative must not reach TableStatistics, whose
     * constructor throws on a negative row count — surface it as a failure the caller degrades
     * to unknown stats instead.
     */
    public long countDocuments(String database, String collection) {
        Long count = arango.db(database).collection(collection).count().getCount();
        if (count == null || count < 0) {
            throw new IllegalStateException(
                    "ArangoDB returned no usable count for %s.%s: %s"
                            .formatted(database, collection, count));
        }
        return count;
    }
```

(`CollectionPropertiesEntity` — the return type of `count()` — is already imported in this file.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=ArangoClientTest`
Expected: PASS (all tests in the class, including pre-existing ones).

- [ ] **Step 5: Commit**

```bash
mvn spotless:apply
git add src/main/java/io/arango/trino/client/ArangoClient.java src/test/java/io/arango/trino/client/ArangoClientTest.java
git commit -m "feat: ArangoClient.countDocuments metadata-level count (M6-A)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `getTableStatistics` decline paths (kill switch, passthrough, aggregation)

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java`
- Create: `src/test/java/io/arango/trino/ArangoMetadataStatisticsTest.java`

**Interfaces:**
- Consumes: `config.isStatisticsEnabled()` (Task 1).
- Produces: `@Override public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)` on `ArangoMetadata`. This task implements the guard chain with the count path left as a plain uncached `client.countDocuments` call; Task 4 adds the cache, limit interaction, and error handling around it.

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/io/arango/trino/ArangoMetadataStatisticsTest.java`. Decline paths depend only on config and handle shape, so `new ArangoMetadata(null, null, config)` (the established null-deps pattern from `ArangoMetadataLimitTest`) proves they never touch the client:

```java
package io.arango.trino;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * Targets M6-A spec §2's guard chain: rows that decline (or answer without a count) must do so
 * before any client call — constructed with a null client so an accidental count throws NPE.
 */
class ArangoMetadataStatisticsTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VarcharType.VARCHAR, false, List.of("city"));

    private static ArangoTableHandle plainHandle() {
        return new ArangoTableHandle(
                "shop", "users", false, TupleDomain.all(), OptionalLong.empty(), Optional.empty());
    }

    private static ArangoAggregation globalAggregation() {
        // count(*): empty groupingColumns — the connector emits exactly one row (spec §2).
        return new ArangoAggregation(
                List.of(),
                List.of(
                        new AggregateSpec(
                                AggregateSpec.Kind.COUNT_STAR,
                                Optional.empty(),
                                "count",
                                BigintType.BIGINT)));
    }

    private static ArangoAggregation groupedAggregation() {
        // bare GROUP BY city (a pushed SELECT DISTINCT) — group cardinality unknowable.
        return new ArangoAggregation(List.of(CITY), List.of());
    }

    @Test
    void disabledFlagReturnsEmptyWithoutClientCall() {
        ArangoMetadata metadata =
                new ArangoMetadata(null, null, new ArangoConfig().setStatisticsEnabled(false));
        TableStatistics stats = metadata.getTableStatistics(null, plainHandle());
        assertThat(stats.getRowCount().isUnknown())
                .as("kill switch must short-circuit before the client")
                .isTrue();
    }

    @Test
    void queryHandleReturnsEmpty() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoQueryHandle handle =
                new ArangoQueryHandle("shop", "FOR d IN users RETURN d", List.of());
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
    }

    @Test
    void globalAggregationReportsExactlyOneRowWithoutClientCall() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = plainHandle().withAggregation(globalAggregation());
        TableStatistics stats = metadata.getTableStatistics(null, handle);
        assertThat(stats.getRowCount().getValue()).isEqualTo(1.0);
    }

    @Test
    void groupedAggregationReturnsEmpty() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = plainHandle().withAggregation(groupedAggregation());
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
    }

    @Test
    void aggregationRowsWinOverLimit() {
        // Pins spec §2's guard ordering: a handle carrying both an aggregation and a limit takes
        // the aggregation rows (here: global -> 1), never the limit/count path.
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle =
                plainHandle().withAggregation(globalAggregation()).withLimit(5);
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().getValue())
                .isEqualTo(1.0);
    }

    @Test
    void groupedAggregationWithPushedLimitReportsLimitWithoutClientCall() {
        // applyLimit reports limitGuaranteed=true for aggregated handles (single split, LIMIT
        // rendered after the COLLECT), so the pushed limit is an exact output cap (spec §2).
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle =
                plainHandle().withAggregation(groupedAggregation()).withLimit(10);
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().getValue())
                .isEqualTo(10.0);
    }

    @Test
    void disabledFlagWinsOverAggregation() {
        // Kill switch is the first row of the matrix: even the client-free Estimate.of(1) answer
        // is suppressed when statistics are disabled.
        ArangoMetadata metadata =
                new ArangoMetadata(null, null, new ArangoConfig().setStatisticsEnabled(false));
        ArangoTableHandle handle = plainHandle().withAggregation(globalAggregation());
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest=ArangoMetadataStatisticsTest`
Expected: FAIL. Without an override, `ConnectorMetadata`'s default `getTableStatistics` returns `TableStatistics.empty()`, so the `empty()`-expecting tests pass vacuously, but `globalAggregationReportsExactlyOneRowWithoutClientCall`, `aggregationRowsWinOverLimit`, and `groupedAggregationWithPushedLimitReportsLimitWithoutClientCall` fail: `Estimate.getValue()` on an unknown estimate returns `NaN` (it does not throw), so AssertJ reports `NaN` ≠ `1.0`/`10.0`. That is the red signal; note which tests failed.

- [ ] **Step 3: Write minimal implementation**

In `ArangoMetadata.java`:

Add imports (the file already imports `Cache`, `CacheBuilder`, `Ticker`, `ArangoQueryHandle` — verify; add what's missing):

```java
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
```

Add the override (place it after `applyAggregation` so the pushdown hooks and the stats hook read as one planning-surface group):

```java
    /**
     * M6-A (spec 2026-08-04): row-count-only statistics. A guard chain — first matching rule
     * wins: kill switch, passthrough decline, aggregation rows (a global aggregate is exactly
     * one row, a grouped one unknowable), then the counted path (Task 4 adds cache/limit/error
     * handling).
     */
    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session, ConnectorTableHandle table) {
        if (!config.isStatisticsEnabled()) {
            return TableStatistics.empty();
        }
        // Same instanceof-decline-first pattern as the pushdown hooks: opaque user AQL has no
        // collection to count.
        if (table instanceof ArangoQueryHandle) {
            return TableStatistics.empty();
        }
        ArangoTableHandle handle = (ArangoTableHandle) table;
        if (handle.aggregation().isPresent()) {
            // A global aggregate (empty groupingColumns) emits exactly one row — known exactly,
            // matching the engine's own AggregationStatsRule.
            if (handle.aggregation().get().groupingColumns().isEmpty()) {
                return TableStatistics.builder().setRowCount(Estimate.of(1)).build();
            }
            // Grouped: cardinality is unknowable without NDV stats — unless a pushed limit caps
            // it. applyLimit reports limitGuaranteed=true for aggregated handles (single split,
            // LIMIT rendered after the COLLECT), so the cap is exact (spec §2).
            if (handle.limit().isPresent()) {
                return TableStatistics.builder()
                        .setRowCount(Estimate.of(handle.limit().getAsLong()))
                        .build();
            }
            return TableStatistics.empty();
        }
        long count = client.countDocuments(handle.schema(), handle.table());
        return TableStatistics.builder().setRowCount(Estimate.of(count)).build();
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=ArangoMetadataStatisticsTest`
Expected: PASS — all 7 tests.

- [ ] **Step 5: Commit**

```bash
mvn spotless:apply
git add src/main/java/io/arango/trino/ArangoMetadata.java src/test/java/io/arango/trino/ArangoMetadataStatisticsTest.java
git commit -m "feat: getTableStatistics guard chain — kill switch, passthrough, aggregation (M6-A)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Counted path — TTL cache, limit interaction, error degradation

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java`
- Modify: `src/test/java/io/arango/trino/ArangoMetadataStatisticsTest.java`

**Interfaces:**
- Consumes: `client.countDocuments(schema, table)` (Task 2), `config.getStatisticsCacheTtl()` (Task 1), the existing `@VisibleForTesting ArangoMetadata(client, resolver, config, Ticker)` constructor.
- Produces: the finished `getTableStatistics` — Task 5's ITs and nothing else depend on it.

- [ ] **Step 1: Write the failing tests**

Add to `ArangoMetadataStatisticsTest`'s import block (Task 3's header does not have them; without the first, `extends ArangoClient` fails to compile — `ArangoClient` lives in `io.arango.trino.client`, a different package from this test):

```java
import static io.airlift.slice.Slices.utf8Slice;

import io.arango.trino.client.ArangoClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
```

Then add a counting double, a ManualTicker copy (the one in `ArangoMetadataTest` is private to that class), and the counted-path tests:

```java
    /** Per-table counts + call counter; flaky mode throws once then succeeds. */
    private static class CountingArangoClient extends ArangoClient {
        final java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();
        final java.util.Map<String, Long> countsByCollection;
        boolean failNextCall;

        CountingArangoClient(java.util.Map<String, Long> countsByCollection) {
            super(new ArangoConfig());
            this.countsByCollection = countsByCollection;
        }

        @Override
        public long countDocuments(String database, String collection) {
            calls.incrementAndGet();
            if (failNextCall) {
                failNextCall = false;
                throw new IllegalStateException("simulated count failure");
            }
            return countsByCollection.get(collection);
        }
    }

    private static final class ManualTicker extends com.google.common.base.Ticker {
        long nanos;

        @Override
        public long read() {
            return nanos;
        }

        void advance(long amount, java.util.concurrent.TimeUnit unit) {
            nanos += unit.toNanos(amount);
        }
    }

    private static ArangoTableHandle handleFor(String collection) {
        return new ArangoTableHandle(
                "shop",
                collection,
                false,
                TupleDomain.all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    @Test
    void plainHandleSurfacesCount() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        assertThat(metadata.getTableStatistics(null, handleFor("users")).getRowCount().getValue())
                .isEqualTo(42.0);
    }

    @Test
    void filteredHandleSurfacesSameBaseCount() {
        // Spec §1 recorded decision: filter presence does not change the number.
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        ArangoTableHandle filtered =
                handleFor("users")
                        .withConstraint(
                                TupleDomain.withColumnDomains(
                                        java.util.Map.<ColumnHandle, Domain>of(
                                                CITY,
                                                Domain.singleValue(
                                                        VarcharType.VARCHAR,
                                                        utf8Slice("london")))));
        assertThat(metadata.getTableStatistics(null, filtered).getRowCount().getValue())
                .isEqualTo(42.0);
    }

    @Test
    void emptyCollectionIsZeroNotUnknown() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("empty_col", 0L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        TableStatistics stats = metadata.getTableStatistics(null, handleFor("empty_col"));
        assertThat(stats.getRowCount().isUnknown()).isFalse();
        assertThat(stats.getRowCount().getValue()).isEqualTo(0.0);
    }

    @Test
    void limitMinAppliedOnlyWhenSingleSplit() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L));
        // Default config: shard parallelism on -> limit NOT applied (the engine's retained
        // LimitNode does the min; pre-applying would misstate the scan node, spec §2).
        ArangoMetadata fanOut = new ArangoMetadata(client, null, new ArangoConfig());
        assertThat(
                        fanOut.getTableStatistics(null, handleFor("users").withLimit(5))
                                .getRowCount()
                                .getValue())
                .isEqualTo(42.0);
        // Parallelism off -> pushed limit is exact (mirrors applyLimit's limitGuaranteed) -> min.
        ArangoMetadata singleSplit =
                new ArangoMetadata(
                        client, null, new ArangoConfig().setShardParallelismEnabled(false));
        assertThat(
                        singleSplit
                                .getTableStatistics(null, handleFor("users").withLimit(5))
                                .getRowCount()
                                .getValue())
                .isEqualTo(5.0);
        // ...and a limit above the count changes nothing.
        assertThat(
                        singleSplit
                                .getTableStatistics(null, handleFor("users").withLimit(100))
                                .getRowCount()
                                .getValue())
                .isEqualTo(42.0);
    }

    @Test
    void countIsCachedWithinTtlAndRefreshedAfter() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L));
        ManualTicker ticker = new ManualTicker();
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig(), ticker);
        ArangoTableHandle handle = handleFor("users");

        metadata.getTableStatistics(null, handle);
        metadata.getTableStatistics(null, handle);
        assertThat(client.calls.get()).as("second call within TTL served from cache").isEqualTo(1);

        ticker.advance(6, java.util.concurrent.TimeUnit.MINUTES); // default TTL is 5m
        metadata.getTableStatistics(null, handle);
        assertThat(client.calls.get()).as("expired entry re-counts").isEqualTo(2);
    }

    @Test
    void cacheKeysAreIsolatedPerTable() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L, "orders", 7L));
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        assertThat(metadata.getTableStatistics(null, handleFor("users")).getRowCount().getValue())
                .isEqualTo(42.0);
        assertThat(metadata.getTableStatistics(null, handleFor("orders")).getRowCount().getValue())
                .isEqualTo(7.0);
        assertThat(client.calls.get()).isEqualTo(2);
    }

    @Test
    void countFailureDegradesToEmptyAndIsNotNegativeCached() {
        CountingArangoClient client =
                new CountingArangoClient(java.util.Map.of("users", 42L));
        client.failNextCall = true;
        ArangoMetadata metadata = new ArangoMetadata(client, null, new ArangoConfig());
        ArangoTableHandle handle = handleFor("users");

        // Failure -> empty(), planning does not throw (spec §4 deviation).
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().isUnknown()).isTrue();
        // No negative caching: the very next call retries and succeeds.
        assertThat(metadata.getTableStatistics(null, handle).getRowCount().getValue())
                .isEqualTo(42.0);
        assertThat(client.calls.get()).isEqualTo(2);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -Dtest=ArangoMetadataStatisticsTest`
Expected: FAIL — `limitMinAppliedOnlyWhenSingleSplit` (min not applied), `countIsCachedWithinTtlAndRefreshedAfter` (2 calls, no cache), `countFailureDegradesToEmptyAndIsNotNegativeCached` (exception propagates). `plainHandleSurfacesCount`, `filteredHandleSurfacesSameBaseCount`, `emptyCollectionIsZeroNotUnknown`, `cacheKeysAreIsolatedPerTable` pass already (Task 3's direct count path) — that is expected; the red tests are the ones driving this task.

- [ ] **Step 3: Write the implementation**

In `ArangoMetadata.java`:

Add import (only this one is new — `UncheckedExecutionException` is **already imported** at the top of the file for `resolve()`'s catch; re-adding it would trip Checkstyle's `RedundantImport` rule. As in Task 3: verify, add only what's missing):

```java
import io.airlift.log.Logger;
```

Add fields (logger at the top of the class beside `ERROR_DATABASE_NOT_FOUND`; cache beside `columnCache`):

```java
    private static final Logger log = Logger.get(ArangoMetadata.class);
```

```java
    // Row-count cache for getTableStatistics (M6-A spec §3): planning may ask several times per
    // query and across concurrent queries; a TTL-stale count is fine for costing. Only successful
    // counts enter (no negative caching — a transient failure retries on the next planning call).
    private final Cache<SchemaTableName, Long> countCache;
```

In the `@VisibleForTesting` constructor, after the `columnCache` assignment:

```java
        this.countCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(config.getStatisticsCacheTtl().toMillis(), MILLISECONDS)
                        .ticker(ticker)
                        .build();
```

Replace the last two lines of `getTableStatistics` (from `long count = ...` to the end) with:

```java
        long count;
        try {
            count =
                    countCache.get(
                            handle.schemaTableName(),
                            () -> client.countDocuments(handle.schema(), handle.table()));
        } catch (ExecutionException | UncheckedExecutionException e) {
            // Deliberate deviation from this class's translation rule (spec §4): statistics are
            // advisory — empty() is the SPI's "unknown" — and failing planning over an optional
            // signal would kill queries whose scan path still works. WARN keeps it observable;
            // per-call WARN during an outage is accepted (queries are failing loudly anyway).
            log.warn(
                    e.getCause(),
                    "Statistics unavailable for %s; returning unknown",
                    handle.schemaTableName());
            return TableStatistics.empty();
        }
        long rowCount = count;
        // min(count, limit) only when the pushed limit is exact (single-split — mirrors
        // applyLimit's limitGuaranteed). Under fan-out the scan node really can emit up to
        // splits*n rows and the engine's retained LimitNode applies the min itself (spec §2).
        if (handle.limit().isPresent() && !config.isShardParallelismEnabled()) {
            rowCount = Math.min(rowCount, handle.limit().getAsLong());
        }
        return TableStatistics.builder().setRowCount(Estimate.of(rowCount)).build();
```

(`ExecutionException` is already imported for `columnCache`; `Cache`/`CacheBuilder`/`MILLISECONDS` likewise.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn test -Dtest=ArangoMetadataStatisticsTest`
Expected: PASS — all 14 tests.

- [ ] **Step 5: Run the neighboring unit suites to catch regressions**

Run: `mvn test -Dtest='ArangoMetadataTest,ArangoMetadataLimitTest,ArangoMetadataAggregationTest,ArangoMetadataPassthroughTest,ArangoConfigTest'`
Expected: PASS — `getTableStatistics` is additive; any failure here means the constructor or import changes broke something.

- [ ] **Step 6: Commit**

```bash
mvn spotless:apply
git add src/main/java/io/arango/trino/ArangoMetadata.java src/test/java/io/arango/trino/ArangoMetadataStatisticsTest.java
git commit -m "feat: TTL-cached count path with limit min and error degradation (M6-A)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Integration tests — `SHOW STATS` end-to-end

**Files:**
- Modify: `src/test/java/io/arango/trino/ArangoConnectorQueryTest.java`

**Interfaces:**
- Consumes: the finished `getTableStatistics` (Task 4) and the existing seeded data — `shop.users` holds exactly 2 documents in this class's `@BeforeAll`.

**Requires Docker running.**

- [ ] **Step 1: Write the tests**

Add to `ArangoConnectorQueryTest`:

A `SHOW` statement is never a relation in Trino's grammar, so `SELECT ... FROM (SHOW STATS ...)` cannot parse — execute `SHOW STATS FOR (...)` directly and filter the summary row in Java. `SHOW STATS` emits one row per output column plus a summary row; with no column statistics the per-column rows are all-NULL, so assert on the summary row (`column_name`, field 0, is NULL) and read `row_count` at field index 4 (the 5th column of `SHOW STATS` output):

```java
    @Test
    void showStatsSurfacesRowCount() {
        // M6 exit criterion made executable: "row-count stats surfaced" (spec §5).
        MaterializedResult r = queryRunner.execute("SHOW STATS FOR (SELECT * FROM users)");
        var summary =
                r.getMaterializedRows().stream()
                        .filter(row -> row.getField(0) == null) // summary row: column_name IS NULL
                        .findFirst()
                        .orElseThrow();
        assertThat(((Number) summary.getField(4)).doubleValue()).isEqualTo(2.0);
    }

    @Test
    void showStatsForPassthroughIsUnknown() {
        // The ArangoQueryHandle decline observed end-to-end: unknown row_count is SQL NULL.
        MaterializedResult r =
                queryRunner.execute(
                        "SHOW STATS FOR (SELECT * FROM TABLE(arango.system.query("
                                + "database => 'shop',"
                                + " query => 'FOR d IN users RETURN d')))");
        var summary =
                r.getMaterializedRows().stream()
                        .filter(row -> row.getField(0) == null)
                        .findFirst()
                        .orElseThrow();
        assertThat(summary.getField(4)).isNull();
    }
```

- [ ] **Step 2: Run the tests**

Run: `mvn test -Dtest=ArangoConnectorQueryTest`
Expected: PASS — both new tests and all pre-existing ones (the class shares one seeded container; the new tests add no seed data and mutate nothing).

- [ ] **Step 3: Commit**

```bash
mvn spotless:apply
git add src/test/java/io/arango/trino/ArangoConnectorQueryTest.java
git commit -m "test: SHOW STATS surfaces row count end-to-end; passthrough unknown (M6-A)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Documentation — README config table, CLAUDE.md architecture note

**Files:**
- Modify: `README.md` (user-facing config table, ~lines 60-66)
- Modify: `CLAUDE.md` (the "What this is" milestone line and the Read-path/`ArangoConfig` descriptions)

- [ ] **Step 1: Update README config table**

Add two rows to the configuration table in `README.md`, matching its existing column format (find the table containing `arangodb.schema.cache-ttl` and append in its style):

```markdown
| `arangodb.statistics-enabled` | `true` | Expose row-count table statistics to the optimizer; `false` returns unknown statistics everywhere. |
| `arangodb.statistics.cache-ttl` | `5m` | How long a collection row count is cached for planning. |
```

- [ ] **Step 2: Update CLAUDE.md**

Three edits:

1. In the opening "What this is" paragraph, extend the milestone list: after the M6-B clause, add `; and row-count table statistics from the cheap collection count, TTL-cached (M6-A)`.
2. In the `ArangoConfig` paragraph, add to the settings enumeration (following its established phrasing style): `` `arangodb.statistics-enabled` (default `true` — expose row-count table statistics; `false` returns unknown statistics everywhere), `arangodb.statistics.cache-ttl` (default `5m` — how long a collection row count is cached for planning) ``.
3. Add a numbered entry to the "Read path" architecture section, after item 8 (`ArangoMetadata.applyAggregation`):

```markdown
9. **`ArangoMetadata.getTableStatistics`** (M6-A) returns a row-count-only `TableStatistics` from `ArangoClient.countDocuments` (the metadata-level `GET /_api/collection/{name}/count`, deliberately not the AQL-based `countWithShardIds`), cached per `SchemaTableName` in a second Guava cache with `expireAfterWrite(arangodb.statistics.cache-ttl)`. It is a guard chain: `arangodb.statistics-enabled=false` → empty; `ArangoQueryHandle` → empty (opaque AQL); aggregated handle → `Estimate.of(1)` for a global aggregate (empty `groupingColumns` = exactly one output row), `Estimate.of(limit)` for a grouped one carrying a pushed limit (an aggregated handle is always a single split with `LIMIT` after the `COLLECT`, so the cap is exact), empty for a grouped one without; otherwise the cached count, with `min(count, limit)` applied only when `arangodb.shard-parallelism-enabled=false` (mirroring `applyLimit`'s `limitGuaranteed` — under fan-out the engine's retained `LimitNode` applies the min itself). A pushed filter does **not** change the number (recorded decision — base-JDBC precedent; a fully-pushed filter's scan estimate is therefore an upper bound). Count failures degrade to `TableStatistics.empty()` + `WARN` instead of the class's usual `GENERIC_INTERNAL_ERROR` rethrow — statistics are advisory, and failing planning over an optional signal would kill queries whose scan path still works; failures are never negative-cached. The four pushdown hooks keep passing `precalculateStatistics=false` — flipping it would have the engine re-derive post-pushdown stats via `FilterStatsRule`, which with no column statistics yields *unknown* and would silently discard the row count.
```

- [ ] **Step 3: Verify the full build**

Run: `mvn test`
Expected: PASS — full suite (Docker required). This is the milestone-complete verification.

- [ ] **Step 4: Commit**

```bash
mvn spotless:apply
git add README.md CLAUDE.md
git commit -m "docs: README config rows + CLAUDE.md architecture note for M6-A stats

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
