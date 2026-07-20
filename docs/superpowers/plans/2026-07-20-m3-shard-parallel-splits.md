# M3 — Shard-parallel splits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a sharded ArangoDB collection scan in parallel across Trino workers — one split per shard-group — while falling back to today's single whole-collection split whenever per-shard correctness cannot be positively confirmed.

**Architecture:** `ArangoSplitManager` discovers a collection's sharding properties, applies a positive allowlist gate, enumerates shard IDs, and groups them into balanced splits; each `ArangoSplit` carries a `List<String> shardIds` that `ArangoPageSourceProvider` threads into the driver's `AqlQueryOptions.shardIds(...)` option. The `shardIds` option is an internal ArangoDB API, so fan-out is guarded by a version pin, an active runtime capability probe (cached process-wide), a CI count-sum gate, SmartGraph exclusion, and safe single-split fallback. `AqlBuilder` is untouched — shard scoping is a query option, orthogonal to M2 pushdown.

**Tech Stack:** Java 24, Trino SPI (~476/481), `arangodb-java-driver` 7.13.0 (`core` module), Airlift config/Guice, JUnit 5, Testcontainers 1.20.4 (single-node `GenericContainer` + multi-container `ComposeContainer`), Maven Surefire (unit) + Failsafe (IT).

## Global Constraints

- **Java 24** (`maven.compiler.release=24`); `mvn` needs `source ~/.sdkman/bin/sdkman-init.sh` first.
- **Docker must be running** — most tests use Testcontainers; cluster ITs use a compose file.
- **Minimum ArangoDB version pinned at `3.11`** (master spec §0) — the version-pin constant. **CI cluster + existing single-node tests run image `arangodb/arangodb:3.12`** — reuse this tag; do not introduce another version.
- **Do not add or "simplify" `pom.xml` dependency pins** without reading the inline comment above each (dependency-mediation workarounds, not style).
- **`shardIds` is an internal ArangoDB API** — every fan-out path must be guarded; when any check is inconclusive, fall back to a single split rather than risk N× duplication.
- **New code lives in package `io.arango.trino.split`** (pure split logic) except the handle/config/manager edits.
- **TDD**: write the failing test first, watch it fail, implement minimally, watch it pass, commit. Frequent commits.
- **Branch:** work on `m3-shard-parallel-splits` (already cut from master); squash-merge at the end.
- **Spec:** `docs/superpowers/specs/2026-07-20-m3-shard-parallel-splits-design.md` (elaborates master-spec §5.1/§5.4).

---

### Task 1: `ArangoSplit` carries `shardIds`

**Files:**
- Modify: `src/main/java/io/arango/trino/handle/ArangoSplit.java`
- Modify: `src/main/java/io/arango/trino/ArangoSplitManager.java` (keep it compiling: emit one empty-`shardIds` split)
- Test: `src/test/java/io/arango/trino/handle/ArangoSplitTest.java`

**Interfaces:**
- Produces: `record ArangoSplit(List<String> shardIds)` — empty list ⇒ whole-collection scan; Jackson round-trippable; `getRetainedSizeInBytes()` accounts for the list.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.handle;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoSplitTest {
    private static final JsonCodec<ArangoSplit> CODEC = jsonCodec(ArangoSplit.class);

    @Test
    void roundTripsWithShardIds() {
        ArangoSplit split = new ArangoSplit(List.of("s100001", "s100002"));
        assertEquals(split, CODEC.fromJson(CODEC.toJson(split)));
    }

    @Test
    void roundTripsEmpty() {
        ArangoSplit split = new ArangoSplit(List.of());
        assertEquals(split, CODEC.fromJson(CODEC.toJson(split)));
        assertTrue(split.shardIds().isEmpty());
    }

    @Test
    void retainedSizeGrowsWithShards() {
        long empty = new ArangoSplit(List.of()).getRetainedSizeInBytes();
        long two = new ArangoSplit(List.of("s100001", "s100002")).getRetainedSizeInBytes();
        assertTrue(two > empty, "retained size must include the shard-id list");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoSplitTest`
Expected: COMPILE FAILURE — `ArangoSplit()` has no `shardIds` component / constructor arity mismatch.

- [ ] **Step 3: Implement the record**

```java
package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record ArangoSplit(List<String> shardIds) implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(ArangoSplit.class);

    @JsonCreator
    public ArangoSplit(@JsonProperty("shardIds") List<String> shardIds) {
        this.shardIds = List.copyOf(requireNonNull(shardIds, "shardIds is null"));
    }

    @JsonProperty
    @Override
    public List<String> shardIds() {
        return shardIds;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE + estimatedSizeOf(shardIds, SizeOf::estimatedSizeOf);
    }
}
```

Then keep `ArangoSplitManager` compiling — replace `new ArangoSplit()` with the empty-`shardIds` fallback:

```java
// in ArangoSplitManager.getSplits (rewired fully in Task 7)
return new FixedSplitSource(java.util.List.of(new ArangoSplit(java.util.List.of())));
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoSplitTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/handle/ArangoSplit.java \
        src/main/java/io/arango/trino/ArangoSplitManager.java \
        src/test/java/io/arango/trino/handle/ArangoSplitTest.java
git commit -m "feat: ArangoSplit carries shardIds (empty = whole-collection)"
```

---

### Task 2: Config knobs (`shards-per-split`, `max-splits`, `shard-parallelism-enabled`)

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java` (create if absent; otherwise extend)

**Interfaces:**
- Produces: `int getShardsPerSplit()` (default 1), `int getMaxSplits()` (default 32), `boolean isShardParallelismEnabled()` (default true), plus fluent setters `setShardsPerSplit(int)`, `setMaxSplits(int)`, `setShardParallelismEnabled(boolean)`.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoConfigShardingTest {
    @Test
    void defaults() {
        ArangoConfig c = new ArangoConfig();
        assertEquals(1, c.getShardsPerSplit());
        assertEquals(32, c.getMaxSplits());
        assertTrue(c.isShardParallelismEnabled());
    }

    @Test
    void setters() {
        ArangoConfig c = new ArangoConfig()
                .setShardsPerSplit(4)
                .setMaxSplits(8)
                .setShardParallelismEnabled(false);
        assertEquals(4, c.getShardsPerSplit());
        assertEquals(8, c.getMaxSplits());
        assertFalse(c.isShardParallelismEnabled());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoConfigShardingTest`
Expected: COMPILE FAILURE — getters/setters undefined.

- [ ] **Step 3: Add the config fields**

Add to `ArangoConfig` (imports already present: `io.airlift.configuration.Config`, `io.airlift.configuration.ConfigDescription`, `jakarta.validation.constraints.Min`):

```java
    private int shardsPerSplit = 1;
    private int maxSplits = 32;
    private boolean shardParallelismEnabled = true;

    @Min(1)
    public int getShardsPerSplit() {
        return shardsPerSplit;
    }

    @Config("arangodb.shards-per-split")
    @ConfigDescription("Target number of shards grouped into each split on cluster fan-out")
    public ArangoConfig setShardsPerSplit(int shardsPerSplit) {
        this.shardsPerSplit = shardsPerSplit;
        return this;
    }

    @Min(1)
    public int getMaxSplits() {
        return maxSplits;
    }

    @Config("arangodb.max-splits")
    @ConfigDescription("Hard cap on the number of splits per collection scan")
    public ArangoConfig setMaxSplits(int maxSplits) {
        this.maxSplits = maxSplits;
        return this;
    }

    public boolean isShardParallelismEnabled() {
        return shardParallelismEnabled;
    }

    @Config("arangodb.shard-parallelism-enabled")
    @ConfigDescription("Enable per-shard parallel splits on clusters; false forces a single split and never uses the internal shardIds API")
    public ArangoConfig setShardParallelismEnabled(boolean shardParallelismEnabled) {
        this.shardParallelismEnabled = shardParallelismEnabled;
        return this;
    }
```

**If `ArangoConfigTest` already exists and uses `ConfigAssertions.assertFullMapping`/`assertRecordedDefaults`**, add these three keys to its maps: `.put("arangodb.shards-per-split", "4")`, `.put("arangodb.max-splits", "8")`, `.put("arangodb.shard-parallelism-enabled", "false")`, and the matching `.setShardsPerSplit(4).setMaxSplits(8).setShardParallelismEnabled(false)` on the expected object, plus defaults `1/32/true` in the recorded-defaults object.

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoConfigShardingTest` (and the existing `ArangoConfigTest` if extended)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoConfig.java src/test/java/io/arango/trino/ArangoConfigShardingTest.java
git commit -m "feat: add shard-parallelism config knobs"
```

---

### Task 3: `ShardGrouping` — pure balanced partition

**Files:**
- Create: `src/main/java/io/arango/trino/split/ShardGrouping.java`
- Test: `src/test/java/io/arango/trino/split/ShardGroupingTest.java`

**Interfaces:**
- Produces: `static List<List<String>> ShardGrouping.partition(List<String> shardIds, int shardsPerSplit, int maxSplits)` — returns `min(ceil(N/shardsPerSplit), maxSplits)` balanced groups; every shard in exactly one group (the invariant); empty input ⇒ empty list.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.split;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardGroupingTest {
    private static List<String> shards(int n) {
        List<String> s = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            s.add("s" + i);
        }
        return s;
    }

    @Test
    void oneShardPerSplitByDefault() {
        assertEquals(List.of(List.of("s1"), List.of("s2"), List.of("s3")),
                ShardGrouping.partition(shards(3), 1, 32));
    }

    @Test
    void groupsBySizeWhenShardsPerSplitAboveOne() {
        // ceil(3/2) = 2 groups, balanced -> [2,1]
        assertEquals(List.of(List.of("s1", "s2"), List.of("s3")),
                ShardGrouping.partition(shards(3), 2, 32));
    }

    @Test
    void maxSplitsIsAHardCapThatForcesLargerGroups() {
        // N=10, S=1 would be 10 groups, but cap=3 wins -> 3 groups, sizes [4,3,3]
        List<List<String>> groups = ShardGrouping.partition(shards(10), 1, 3);
        assertEquals(3, groups.size());
        assertEquals(List.of(4, 3, 3), groups.stream().map(List::size).toList());
    }

    @Test
    void emptyInputYieldsNoGroups() {
        assertTrue(ShardGrouping.partition(List.of(), 1, 32).isEmpty());
    }

    @Test
    void rejectsNonPositiveArgs() {
        assertThrows(IllegalArgumentException.class, () -> ShardGrouping.partition(shards(3), 0, 32));
        assertThrows(IllegalArgumentException.class, () -> ShardGrouping.partition(shards(3), 1, 0));
    }

    @Test
    void invariantHoldsAcrossManyCombinations() {
        for (int n = 1; n <= 40; n++) {
            for (int s = 1; s <= 6; s++) {
                for (int m = 1; m <= 12; m++) {
                    List<String> input = shards(n);
                    List<List<String>> groups = ShardGrouping.partition(input, s, m);
                    int expected = Math.min((n + s - 1) / s, m);
                    assertEquals(expected, groups.size(), "split count for n=" + n + " s=" + s + " m=" + m);
                    // union == input, in order, no dupes:
                    List<String> flat = groups.stream().flatMap(List::stream).toList();
                    assertEquals(input, flat, "partition must cover every shard exactly once");
                    // balanced: sizes differ by at most 1
                    int min = groups.stream().mapToInt(List::size).min().orElse(0);
                    int max = groups.stream().mapToInt(List::size).max().orElse(0);
                    assertTrue(max - min <= 1, "groups must be balanced");
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ShardGroupingTest`
Expected: COMPILE FAILURE — `ShardGrouping` does not exist.

- [ ] **Step 3: Implement the pure function**

```java
package io.arango.trino.split;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class ShardGrouping {
    private ShardGrouping() {}

    /**
     * Partition shard IDs into balanced groups so the union of per-group scans equals the full scan.
     * <p>splits = min(ceil(N / shardsPerSplit), maxSplits). {@code maxSplits} is a HARD cap that can
     * force more than {@code shardsPerSplit} shards into a group when ceil(N/shardsPerSplit) exceeds it.
     * Every shard appears in exactly one group (the M3 correctness invariant).
     */
    public static List<List<String>> partition(List<String> shardIds, int shardsPerSplit, int maxSplits) {
        checkArgument(shardsPerSplit >= 1, "shardsPerSplit must be >= 1");
        checkArgument(maxSplits >= 1, "maxSplits must be >= 1");
        int n = shardIds.size();
        if (n == 0) {
            return List.of();
        }
        int groupsBySize = (n + shardsPerSplit - 1) / shardsPerSplit; // ceil(n / shardsPerSplit)
        int groups = Math.min(groupsBySize, maxSplits);
        int base = n / groups;
        int remainder = n % groups;
        List<List<String>> result = new ArrayList<>(groups);
        int idx = 0;
        for (int g = 0; g < groups; g++) {
            int size = base + (g < remainder ? 1 : 0); // first `remainder` groups get one extra
            result.add(List.copyOf(shardIds.subList(idx, idx + size)));
            idx += size;
        }
        return List.copyOf(result);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ShardGroupingTest`
Expected: PASS (6 tests, including the exhaustive invariant loop).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/split/ShardGrouping.java src/test/java/io/arango/trino/split/ShardGroupingTest.java
git commit -m "feat: balanced shard-to-split partition (invariant-preserving)"
```

---

### Task 4: `ShardingInfo` + `ShardEligibility` — pure allowlist gate

**Files:**
- Create: `src/main/java/io/arango/trino/split/ShardingInfo.java`
- Create: `src/main/java/io/arango/trino/split/ShardEligibility.java`
- Test: `src/test/java/io/arango/trino/split/ShardEligibilityTest.java`

**Interfaces:**
- Produces: `record ShardingInfo(Integer numberOfShards, String shardingStrategy, String smartJoinAttribute)`.
- Produces: `static Optional<String> ShardEligibility.ineligibilityReason(ShardingInfo info)` — empty ⇒ eligible for fan-out; present ⇒ human-readable reason (for the WARN log). Allowlist built from verified `ShardingStrategy` enum constants `{HASH, COMMUNITY_COMPAT, ENTERPRISE_COMPAT}`.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.split;

import com.arangodb.entity.ShardingStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardEligibilityTest {
    private static String name(ShardingStrategy s) {
        return s.getInternalName();
    }

    @Test
    void eligibleForNonSmartHashMultiShard() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.HASH), null)).isEmpty());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(2, name(ShardingStrategy.COMMUNITY_COMPAT), null)).isEmpty());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(4, name(ShardingStrategy.ENTERPRISE_COMPAT), null)).isEmpty());
    }

    @Test
    void ineligibleWhenNotMultiShard() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(1, name(ShardingStrategy.HASH), null)).isPresent());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(null, name(ShardingStrategy.HASH), null)).isPresent());
    }

    @Test
    void ineligibleForSmartStrategies() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.ENTERPRISE_HASH_SMART_EDGE), null)).isPresent());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.ENTERPRISE_HEX_SMART_VERTEX), null)).isPresent());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.ENTERPRISE_SMART_EDGE_COMPAT), null)).isPresent());
    }

    @Test
    void ineligibleForSmartJoin() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.HASH), "region")).isPresent());
    }

    @Test
    void unknownStrategyFailsSafe() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, "some-future-strategy", null)).isPresent());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ShardEligibilityTest`
Expected: COMPILE FAILURE — `ShardingInfo`/`ShardEligibility` do not exist.

- [ ] **Step 3: Implement the record and the gate**

`ShardingInfo.java`:

```java
package io.arango.trino.split;

public record ShardingInfo(Integer numberOfShards, String shardingStrategy, String smartJoinAttribute) {}
```

`ShardEligibility.java`:

```java
package io.arango.trino.split;

import com.arangodb.entity.ShardingStrategy;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

public final class ShardEligibility {
    private ShardEligibility() {}

    // Non-smart hash strategies: one shard per document, safe for per-shard enumeration.
    // Built from verified driver enum constants; the three *SMART* strategies are excluded
    // (SmartGraph edges live in multiple internal sub-shards -> double-count risk, master spec §5.1.5).
    private static final Set<String> ALLOWED_STRATEGIES = Stream.of(
                    ShardingStrategy.HASH,
                    ShardingStrategy.COMMUNITY_COMPAT,
                    ShardingStrategy.ENTERPRISE_COMPAT)
            .map(ShardingStrategy::getInternalName)
            .collect(toUnmodifiableSet());

    /** Empty ⇒ eligible for shard-parallel fan-out. Present ⇒ reason it is not (for the WARN log). */
    public static Optional<String> ineligibilityReason(ShardingInfo info) {
        Integer shards = info.numberOfShards();
        if (shards == null || shards <= 1) {
            return Optional.of("numberOfShards=" + shards + " (not a multi-shard collection)");
        }
        if (info.smartJoinAttribute() != null) {
            return Optional.of("SmartJoin collection (smartJoinAttribute=" + info.smartJoinAttribute() + ")");
        }
        String strategy = info.shardingStrategy();
        if (strategy == null || !ALLOWED_STRATEGIES.contains(strategy)) {
            return Optional.of("sharding strategy '" + strategy + "' is not a supported non-smart hash strategy " + ALLOWED_STRATEGIES);
        }
        return Optional.empty();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ShardEligibilityTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/split/ShardingInfo.java \
        src/main/java/io/arango/trino/split/ShardEligibility.java \
        src/test/java/io/arango/trino/split/ShardEligibilityTest.java
git commit -m "feat: allowlist gate over verified ShardingStrategy constants"
```

---

### Task 5: `ArangoClient` — discovery, enumeration, shard-scoped count, shard-scoped query

**Files:**
- Modify: `src/main/java/io/arango/trino/client/ArangoClient.java`
- Test: `src/test/java/io/arango/trino/client/ArangoClientShardingTest.java` (single-node Testcontainers via `TestingArangoServer`)

**Interfaces:**
- Consumes: `ShardingInfo` (Task 4).
- Produces:
  - `ShardingInfo getShardingInfo(String database, String collection)`
  - `List<String> listShardIds(String database, String collection)` — raw `GET /_api/collection/{name}/shards`; throws `ArangoDBException` on a non-cluster server (a fallback trigger).
  - `String serverVersion()`
  - `long countWithShardIds(String database, String collection, List<String> shardIds)` — counts through the **same `AqlQueryOptions.shardIds` path a real scan uses** (empty list ⇒ full count).
  - `ArangoCursor<Map> query(String database, String aql, Map<String,Object> bindVars, List<String> shardIds)` — scan overload; empty list delegates to the existing no-option `query`.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.client;

import com.arangodb.ArangoDBException;
import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.split.ShardEligibility;
import io.arango.trino.split.ShardingInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoClientShardingTest {
    private static TestingArangoServer server;
    private static ArangoClient client;
    private static final String DB = "shard_client_test";
    private static final String COLL = "docs";

    @BeforeAll
    static void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, COLL);
        for (int i = 0; i < 5; i++) {
            client.insertForTest(DB, COLL, Map.of("_key", "k" + i, "v", i));
        }
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void serverVersionIsReported() {
        assertTrue(client.serverVersion().startsWith("3."), "expected a 3.x version, got " + client.serverVersion());
    }

    @Test
    void singleNodeCollectionIsIneligible() {
        ShardingInfo info = client.getShardingInfo(DB, COLL);
        // single-server: numberOfShards is 1 (or null) -> gate rejects, so no fan-out is attempted
        assertFalse(ShardEligibility.ineligibilityReason(info).isEmpty());
    }

    @Test
    void listShardIdsThrowsOnSingleServer() {
        // /_api/collection/{name}/shards is cluster-only; on single-server it errors -> fallback trigger
        assertThrows(ArangoDBException.class, () -> client.listShardIds(DB, COLL));
    }

    @Test
    void countWithEmptyShardIdsIsFullCount() {
        assertEquals(5L, client.countWithShardIds(DB, COLL, List.of()));
    }

    @Test
    void queryWithEmptyShardIdsMatchesLegacyPath() {
        String aql = "FOR d IN @@col RETURN {\"v\": d.v}";
        Map<String, Object> bind = Map.of("@col", COLL);
        long viaNew = count(client.query(DB, aql, bind, List.of()));
        long viaOld = count(client.query(DB, aql, bind));
        assertEquals(viaOld, viaNew);
        assertEquals(5L, viaNew);
    }

    private static long count(com.arangodb.ArangoCursor<?> cursor) {
        long n = 0;
        while (cursor.hasNext()) { cursor.next(); n++; }
        return n;
    }
}
```

*(`TestingArangoServer` exposes `host()`/`port()`/`hostPort()`/`rootPassword()` — there is **no** `config()` accessor — so build the `ArangoConfig` manually, exactly as `src/test/java/io/arango/trino/client/ArangoClientTest.java` already does.)*

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoClientShardingTest`
Expected: COMPILE FAILURE — the new `ArangoClient` methods don't exist.

- [ ] **Step 3: Implement the client methods**

Add imports to `ArangoClient`: `com.arangodb.Request`, `com.arangodb.Response`, `com.arangodb.model.AqlQueryOptions`, `com.arangodb.entity.CollectionPropertiesEntity`, `io.arango.trino.split.ShardingInfo`, `java.util.List`. Add:

```java
    public ShardingInfo getShardingInfo(String database, String collection) {
        CollectionPropertiesEntity p = arango.db(database).collection(collection).getProperties();
        return new ShardingInfo(p.getNumberOfShards(), p.getShardingStrategy(), p.getSmartJoinAttribute());
    }

    @SuppressWarnings("unchecked")
    public List<String> listShardIds(String database, String collection) {
        Request<Void> req = new Request.Builder<Void>()
                .db(database)
                .method(Request.Method.GET)
                .path("/_api/collection/" + collection + "/shards")
                .build();
        Response<Map> resp = arango.execute(req, Map.class);
        Object shards = resp.getBody().get("shards");
        if (!(shards instanceof List<?> list)) {
            return List.of();
        }
        return list.stream().map(String::valueOf).toList();
    }

    public String serverVersion() {
        return arango.getVersion().getVersion();
    }

    /** Count through the same shardIds option path a real scan uses (empty list = full count). */
    public long countWithShardIds(String database, String collection, List<String> shardIds) {
        AqlQueryOptions options = new AqlQueryOptions();
        if (!shardIds.isEmpty()) {
            options.shardIds(shardIds.toArray(String[]::new));
        }
        ArangoCursor<Long> cursor = arango.db(database).query(
                "FOR d IN @@col COLLECT WITH COUNT INTO n RETURN n",
                Long.class,
                Map.of("@col", collection),
                options);
        return cursor.hasNext() ? cursor.next() : 0L;
    }

    public ArangoCursor<Map> query(String database, String aql, Map<String, Object> bindVars, List<String> shardIds) {
        if (shardIds.isEmpty()) {
            return arango.db(database).query(aql, Map.class, bindVars);
        }
        AqlQueryOptions options = new AqlQueryOptions().shardIds(shardIds.toArray(String[]::new));
        return arango.db(database).query(aql, Map.class, bindVars, options);
    }
```

*(The exact `Request.Builder` chain is validated by Task 8 against a real cluster — the only place `/_api/.../shards` returns success. If the 7.13.0 builder differs, adjust here; the Task 8 boot test is the acceptance gate.)*

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoClientShardingTest`
Expected: PASS (5 tests) — `listShardIds` throwing on single-server is asserted, not a failure.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/client/ArangoClient.java src/test/java/io/arango/trino/client/ArangoClientShardingTest.java
git commit -m "feat: ArangoClient shard discovery, enumeration, shard-scoped count/query"
```

---

### Task 6: `ShardFanoutCapability` — version pin + cached capability probe

**Files:**
- Create: `src/main/java/io/arango/trino/split/ShardFanoutCapability.java`
- Test: `src/test/java/io/arango/trino/split/ShardFanoutCapabilityTest.java` (pure + hand-written `ArangoClient` test double — same pattern as `ArangoMetadataTest`)

**Interfaces:**
- Consumes: `ArangoClient` (Task 5: `serverVersion()`, `countWithShardIds(...)`).
- Produces:
  - `boolean canFanOut(String database, String collection, List<List<String>> groups)` — true only if the version pin passes AND an active probe **over the actual emit groups** confirms `shardIds` narrows results. A *conclusive* verdict (ENABLED / server-ignores-shardIds) is cached process-wide; an *inconclusive* one (empty collection / exception / all-data-in-one-group) is NOT cached and is retried on the next fan-out.
  - `static boolean sumMatchesFull(ArangoClient client, String db, String coll, List<List<String>> groups)` — the shared Σ(per-group counts)==full-count check, reused by the probe **and** the Task 9 IT (so probe and CI verify the same computation).
  - `static boolean isVersionAtLeastMinimum(String actual)`; `static final String MIN_ARANGO_VERSION = "3.11"`.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.split;

import com.arangodb.ArangoCursor;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardFanoutCapabilityTest {

    // Hand-written test double (no network; ArangoDB driver connects lazily so super(config) is safe).
    private static final class FakeClient extends ArangoClient {
        private final String version;
        long full;                    // mutable so a test can simulate an initially-empty collection
        Map<String, Long> perShard;
        final AtomicInteger versionCalls = new AtomicInteger();

        FakeClient(String version, long full, Map<String, Long> perShard) {
            super(new ArangoConfig().setHosts("localhost:8529"));
            this.version = version;
            this.full = full;
            this.perShard = perShard;
        }

        @Override
        public String serverVersion() {
            versionCalls.incrementAndGet();
            return version;
        }

        @Override
        public long countWithShardIds(String db, String coll, List<String> shardIds) {
            if (shardIds.isEmpty()) {
                return full;
            }
            return shardIds.stream().mapToLong(s -> perShard.getOrDefault(s, 0L)).sum();
        }

        @Override
        public ArangoCursor<Map> query(String db, String aql, Map<String, Object> bindVars, List<String> shardIds) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void versionComparison() {
        assertTrue(ShardFanoutCapability.isVersionAtLeastMinimum("3.12.1"));
        assertTrue(ShardFanoutCapability.isVersionAtLeastMinimum("3.11.0"));
        assertTrue(ShardFanoutCapability.isVersionAtLeastMinimum("3.12.0-devel"));
        assertFalse(ShardFanoutCapability.isVersionAtLeastMinimum("3.10.9"));
    }

    @Test
    void enabledWhenVersionAndProbePass() {
        FakeClient client = new FakeClient("3.12.0", 100, Map.of("s1", 60L, "s2", 40L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertTrue(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }

    @Test
    void disabledBelowVersionPin() {
        FakeClient client = new FakeClient("3.10.5", 100, Map.of("s1", 60L, "s2", 40L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertFalse(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }

    @Test
    void disabledWhenServerIgnoresShardIds() {
        // server ignores shardIds -> every shard-scoped count == full -> sum (200) != full (100)
        FakeClient client = new FakeClient("3.12.0", 100, Map.of("s1", 100L, "s2", 100L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertFalse(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }

    @Test
    void verdictIsCachedAfterFirstCall() {
        FakeClient client = new FakeClient("3.12.0", 100, Map.of("s1", 60L, "s2", 40L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2")));
        int afterFirst = client.versionCalls.get();
        cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2")));
        assertEquals(afterFirst, client.versionCalls.get(), "probe must not re-run after the verdict is cached");
    }

    @Test
    void inconclusiveFirstCallDoesNotLatch() {
        // Empty collection on first probe -> inconclusive; must NOT cache DISABLED permanently.
        FakeClient client = new FakeClient("3.12.0", 0, Map.of());
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertFalse(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
        // Collection later has data -> the retry must now succeed (proves no permanent latch).
        client.full = 100;
        client.perShard = Map.of("s1", 60L, "s2", 40L);
        assertTrue(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ShardFanoutCapabilityTest`
Expected: COMPILE FAILURE — `ShardFanoutCapability` does not exist.

- [ ] **Step 3: Implement the capability**

```java
package io.arango.trino.split;

import com.google.inject.Inject;
import io.arango.trino.client.ArangoClient;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ShardFanoutCapability {
    // Minimum ArangoDB version trusted for the internal shardIds option (master spec §0).
    static final String MIN_ARANGO_VERSION = "3.11";

    private enum Verdict { UNKNOWN, ENABLED, DISABLED }

    private final ArangoClient client;
    private final AtomicReference<Verdict> verdict = new AtomicReference<>(Verdict.UNKNOWN);

    @Inject
    public ShardFanoutCapability(ArangoClient client) {
        this.client = client;
    }

    /**
     * True only if the server is trusted (version pin) AND an active probe confirms the internal
     * shardIds option actually narrows results. Computed once on the first eligible collection and
     * cached for the connector's process lifetime (spec §8.2 — lazy-first-fan-out interpretation).
     */
    public boolean canFanOut(String database, String collection, List<List<String>> groups) {
        if (verdict.get() == Verdict.UNKNOWN) {
            Verdict computed = compute(database, collection, groups);
            if (computed != Verdict.UNKNOWN) {          // only a CONCLUSIVE verdict latches
                verdict.compareAndSet(Verdict.UNKNOWN, computed);
            }
            return computed == Verdict.ENABLED;         // inconclusive -> false this call, retried next
        }
        return verdict.get() == Verdict.ENABLED;
    }

    // Probes the ACTUAL groups about to be emitted (so multi-element shardIds arrays are exercised).
    // ENABLED / DISABLED are conclusive (cached); UNKNOWN is inconclusive (not cached, retried).
    private Verdict compute(String database, String collection, List<List<String>> groups) {
        try {
            if (!isVersionAtLeastMinimum(client.serverVersion())) {
                return Verdict.DISABLED;                 // conclusive: too old, will not improve
            }
            long full = client.countWithShardIds(database, collection, List.of());
            if (full == 0) {
                return Verdict.UNKNOWN;                   // inconclusive: empty collection, retry later
            }
            long sum = 0;
            boolean anyNarrower = false;
            for (List<String> group : groups) {
                long c = client.countWithShardIds(database, collection, group);
                sum += c;
                if (c < full) {
                    anyNarrower = true;
                }
            }
            if (sum != full) {
                return Verdict.DISABLED;                  // conclusive: server ignored shardIds -> would N×-duplicate
            }
            return anyNarrower ? Verdict.ENABLED : Verdict.UNKNOWN; // all-in-one-group -> can't confirm narrowing, retry
        }
        catch (RuntimeException e) {
            return Verdict.UNKNOWN;                       // inconclusive: transient, fall back this call, retry later
        }
    }

    /** Σ(per-group shard-scoped counts) == full count. Shared by the probe and the CI count-sum gate. */
    public static boolean sumMatchesFull(ArangoClient client, String database, String collection, List<List<String>> groups) {
        long full = client.countWithShardIds(database, collection, List.of());
        long sum = groups.stream()
                .mapToLong(g -> client.countWithShardIds(database, collection, g))
                .sum();
        return sum == full;
    }

    static boolean isVersionAtLeastMinimum(String actual) {
        return compareVersions(actual, MIN_ARANGO_VERSION) >= 0;
    }

    private static int compareVersions(String a, String b) {
        String[] pa = a.split("[.-]");
        String[] pb = b.split("[.-]");
        int len = Math.max(pa.length, pb.length);
        for (int i = 0; i < len; i++) {
            int va = i < pa.length ? leadingInt(pa[i]) : 0;
            int vb = i < pb.length ? leadingInt(pb[i]) : 0;
            if (va != vb) {
                return Integer.compare(va, vb);
            }
        }
        return 0;
    }

    private static int leadingInt(String s) {
        int end = 0;
        while (end < s.length() && Character.isDigit(s.charAt(end))) {
            end++;
        }
        return end == 0 ? 0 : Integer.parseInt(s.substring(0, end));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ShardFanoutCapabilityTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/split/ShardFanoutCapability.java src/test/java/io/arango/trino/split/ShardFanoutCapabilityTest.java
git commit -m "feat: version pin + cached shardIds capability probe"
```

---

### Task 7: Wire `ArangoSplitManager` + `ArangoPageSourceProvider` + Guice binding

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoSplitManager.java`
- Modify: `src/main/java/io/arango/trino/ArangoPageSourceProvider.java`
- Modify: `src/main/java/io/arango/trino/ArangoModule.java` (bind `ShardFanoutCapability` singleton)
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java` — `applyLimit` must report `limitGuaranteed=false` once a table can fan out (master spec §6.2 blocker)
- Test: `src/test/java/io/arango/trino/ArangoSplitManagerTest.java` (single-node: disabled-flag and single-shard both ⇒ one split)
- Test: `src/test/java/io/arango/trino/ArangoMetadataLimitTest.java`

**Interfaces:**
- Consumes: `ArangoConfig` (Task 2), `ArangoClient` (Task 5), `ShardEligibility`/`ShardGrouping`/`ShardFanoutCapability`/`ShardingInfo` (Tasks 3/4/6), `ArangoSplit` (Task 1), `ArangoTableHandle.schema()/.table()`.
- Produces: `ArangoSplitManager.getSplits` emitting N splits (fan-out) or one empty-`shardIds` split (fallback); `ArangoPageSourceProvider` threading `split.shardIds()` into the scan.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.split.ShardFanoutCapability;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoSplitManagerTest {
    private static TestingArangoServer server;
    private static ArangoClient client;
    private static final String DB = "split_mgr_test";
    private static final String COLL = "docs";

    @BeforeAll
    static void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, COLL);
        client.insertForTest(DB, COLL, Map.of("_key", "k0", "v", 0));
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private static ArangoTableHandle handle() {
        // ArangoTableHandle is a 5-arg record: (String schema, String table, boolean edge,
        // TupleDomain<ColumnHandle> constraint, OptionalLong limit). Only schema()/table() are read here.
        return new ArangoTableHandle(DB, COLL, false, io.trino.spi.predicate.TupleDomain.all(), java.util.OptionalLong.empty());
    }

    private static List<ArangoSplit> collect(ArangoSplitManager mgr) {
        ConnectorSplitSource source = mgr.getSplits(null, null, handle(), DynamicFilter.EMPTY, Constraint.alwaysTrue());
        return source.getNextBatch(1000).getNow(null).getSplits().stream().map(ArangoSplit.class::cast).toList();
    }

    @Test
    void disabledFlagForcesSingleEmptySplit() {
        ArangoConfig config = new ArangoConfig().setShardParallelismEnabled(false);
        ArangoSplitManager mgr = new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        List<ArangoSplit> splits = collect(mgr);
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }

    @Test
    void singleNodeCollectionFallsBackToSingleSplit() {
        ArangoConfig config = new ArangoConfig(); // parallelism enabled by default
        ArangoSplitManager mgr = new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        List<ArangoSplit> splits = collect(mgr);
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }
}
```

*(The `ArangoTableHandle` constructor args above mirror the M2 record — confirm its exact components and adjust the literal; only `schema()`/`table()` are exercised.)*

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoSplitManagerTest`
Expected: COMPILE FAILURE — `ArangoSplitManager` constructor still takes no args.

- [ ] **Step 3: Rewrite `ArangoSplitManager`**

```java
package io.arango.trino;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.split.ShardEligibility;
import io.arango.trino.split.ShardFanoutCapability;
import io.arango.trino.split.ShardGrouping;
import io.arango.trino.split.ShardingInfo;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;

public class ArangoSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(ArangoSplitManager.class);

    private final ArangoClient client;
    private final ArangoConfig config;
    private final ShardFanoutCapability capability;

    @Inject
    public ArangoSplitManager(ArangoClient client, ArangoConfig config, ShardFanoutCapability capability) {
        this.client = client;
        this.config = config;
        this.capability = capability;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        return new FixedSplitSource(splitsFor((ArangoTableHandle) table));
    }

    private static final ArangoSplit SINGLE = new ArangoSplit(List.of());

    private List<ArangoSplit> splitsFor(ArangoTableHandle handle) {
        if (!config.isShardParallelismEnabled()) {
            return List.of(SINGLE);
        }
        String db = handle.schema();
        String coll = handle.table();
        try {
            ShardingInfo info = client.getShardingInfo(db, coll);
            Optional<String> reason = ShardEligibility.ineligibilityReason(info);
            if (reason.isPresent()) {
                boolean multiShard = info.numberOfShards() != null && info.numberOfShards() > 1;
                if (multiShard) {
                    log.warn("Collection %s.%s has %d shards but is scanned serially: %s",
                            db, coll, info.numberOfShards(), reason.get());
                }
                else {
                    log.debug("Collection %s.%s scanned serially: %s", db, coll, reason.get());
                }
                return List.of(SINGLE);
            }
            List<String> shardIds = client.listShardIds(db, coll);
            if (shardIds.size() <= 1) {
                return List.of(SINGLE);
            }
            List<List<String>> groups = ShardGrouping.partition(shardIds, config.getShardsPerSplit(), config.getMaxSplits());
            if (!capability.canFanOut(db, coll, groups)) { // probe the ACTUAL groups we would emit
                log.warn("Collection %s.%s scanned serially: server did not confirm the shardIds option "
                        + "(version pin or capability probe failed)", db, coll);
                return List.of(SINGLE);
            }
            return groups.stream().map(ArangoSplit::new).toList();
        }
        catch (RuntimeException e) {
            log.warn(e, "Collection %s.%s scanned serially: shard discovery failed", db, coll);
            return List.of(SINGLE);
        }
    }
}
```

- [ ] **Step 4: Thread `shardIds` through `ArangoPageSourceProvider`**

In `createPageSource`, cast the split and pass its shard IDs to the new `query` overload:

```java
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ArangoSplit arangoSplit = (ArangoSplit) split;
        List<ArangoColumnHandle> cols = columns.stream()
                .map(ArangoColumnHandle.class::cast).toList();
        cols.forEach(ArangoPageSourceProvider::checkMaterializable);
        AqlQuery q = aqlBuilder.buildScan(handle, cols);
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars(), arangoSplit.shardIds()),
                cols, config.getTypeCoercion());
```

Add `import io.arango.trino.handle.ArangoSplit;`.

- [ ] **Step 5: Bind the capability singleton in `ArangoModule`**

After the `ArangoSplitManager` binding, add:

```java
        binder.bind(io.arango.trino.split.ShardFanoutCapability.class).in(Scopes.SINGLETON);
```

- [ ] **Step 6: Write the failing `limitGuaranteed` test (master-spec §6.2 blocker)**

Once a table can fan out, each split applies `LIMIT n` independently, so a pushed limit is a per-split reduction, not an exact cap. `applyLimit` must report `limitGuaranteed=false` whenever shard-parallelism is enabled (a table *might* fan out), and `true` only when it is disabled (always single split). Construct `ArangoMetadata` exactly as the existing `ArangoMetadataTest` does (test-double `ArangoClient`), passing the config under test.

```java
package io.arango.trino;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoMetadataLimitTest {
    // newMetadata(config), tableHandle(), and SESSION follow ArangoMetadataTest's existing
    // test-double pattern (no live server needed — applyLimit's limitGuaranteed depends only on config).

    @Test
    void limitNotGuaranteedWhenParallelismEnabled() {
        ArangoMetadata metadata = newMetadata(new ArangoConfig()); // parallelism enabled by default
        Optional<LimitApplicationResult<ConnectorTableHandle>> r = metadata.applyLimit(SESSION, tableHandle(), 10);
        assertTrue(r.isPresent());
        assertFalse(r.get().limitGuaranteed(), "a fan-out-capable table must not report an exact limit");
    }

    @Test
    void limitGuaranteedWhenParallelismDisabled() {
        ArangoMetadata metadata = newMetadata(new ArangoConfig().setShardParallelismEnabled(false));
        Optional<LimitApplicationResult<ConnectorTableHandle>> r = metadata.applyLimit(SESSION, tableHandle(), 10);
        assertTrue(r.isPresent());
        assertTrue(r.get().limitGuaranteed(), "a single-split table reports an exact limit");
    }
}
```

*(Copy `newMetadata`/`tableHandle`/`SESSION` from `ArangoMetadataTest` verbatim — the exact `ArangoMetadata` constructor arity and `applyLimit(session, handle, limit)` signature live there.)*

- [ ] **Step 7: Fix `ArangoMetadata.applyLimit`**

`ArangoMetadata` already injects `ArangoConfig config`. Replace the hardcoded `limitGuaranteed=true` (whose comment reads "M2 is single-split always") with:

```java
        // Exact only for a single-split scan. With shard-parallelism enabled the table may fan out
        // (ArangoSplitManager), and each split applies LIMIT n independently (total ≤ n × splits), so
        // Trino must apply the final LIMIT -> report false. Disabled ⇒ always one split ⇒ exact ⇒ true.
        boolean limitGuaranteed = !config.isShardParallelismEnabled();
        return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), limitGuaranteed, false));
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoSplitManagerTest,ArangoMetadataLimitTest && mvn test`
Expected: both new classes PASS; full unit suite green (empty `shardIds` preserves the old read path). **If the existing `ArangoMetadataTest` asserts `limitGuaranteed=true`, update it** — with the default config (parallelism enabled) the value is now `false`; add `.setShardParallelismEnabled(false)` to that test's config to keep asserting `true`, or flip its expectation.

- [ ] **Step 9: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoSplitManager.java \
        src/main/java/io/arango/trino/ArangoPageSourceProvider.java \
        src/main/java/io/arango/trino/ArangoModule.java \
        src/main/java/io/arango/trino/ArangoMetadata.java \
        src/test/java/io/arango/trino/ArangoSplitManagerTest.java \
        src/test/java/io/arango/trino/ArangoMetadataLimitTest.java
git commit -m "feat: wire shard-parallel splits; limitGuaranteed=false under multi-split"
```

---

### Task 8: Cluster bring-up + shard-enumeration proof (Failsafe IT)

**Purpose:** de-risk the highest-uncertainty item in isolation — a multi-container ArangoDB cluster booting reliably under Testcontainers — and prove the one API only testable there: `listShardIds` against a real cluster.

**Files:**
- Create: `src/test/resources/arangodb-cluster-compose.yml`
- Create: `src/test/java/io/arango/trino/TestingArangoCluster.java`
- Create: `src/test/java/io/arango/trino/ShardParallelClusterIT.java`

**Interfaces:**
- Produces: `TestingArangoCluster` (AutoCloseable) exposing `ArangoConfig config()` pointed at the coordinator; a booted 3-shard collection for downstream tasks.

- [ ] **Step 1: Write the compose file**

`src/test/resources/arangodb-cluster-compose.yml` — explicit agency×1 + coordinator×1 + dbserver×2 (matches the chosen topology; more deterministic than the starter). **Validate ports/args against ArangoDB's official 3.12 cluster reference; the Step 4 boot test is the acceptance gate.**

```yaml
services:
  agency:
    image: arangodb/arangodb:3.12
    command: >
      arangod --server.authentication=false
      --server.endpoint=tcp://0.0.0.0:8531
      --agency.my-address=tcp://agency:8531 --agency.endpoint=tcp://agency:8531
      --agency.activate=true --agency.size=1 --agency.supervision=true
  dbserver1:
    image: arangodb/arangodb:3.12
    command: >
      arangod --server.authentication=false
      --server.endpoint=tcp://0.0.0.0:8530
      --cluster.my-address=tcp://dbserver1:8530 --cluster.my-role=PRIMARY
      --cluster.agency-endpoint=tcp://agency:8531
    depends_on: [agency]
  dbserver2:
    image: arangodb/arangodb:3.12
    command: >
      arangod --server.authentication=false
      --server.endpoint=tcp://0.0.0.0:8530
      --cluster.my-address=tcp://dbserver2:8530 --cluster.my-role=PRIMARY
      --cluster.agency-endpoint=tcp://agency:8531
    depends_on: [agency]
  coordinator:
    image: arangodb/arangodb:3.12
    command: >
      arangod --server.authentication=false
      --server.endpoint=tcp://0.0.0.0:8529
      --cluster.my-address=tcp://coordinator:8529 --cluster.my-role=COORDINATOR
      --cluster.agency-endpoint=tcp://agency:8531
    ports: ["8529"]
    depends_on: [agency, dbserver1, dbserver2]
```

- [ ] **Step 2: Write the cluster harness**

`TestingArangoCluster.java`:

```java
package io.arango.trino;

import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.time.Duration;

public final class TestingArangoCluster implements AutoCloseable {
    private final ComposeContainer compose;

    public TestingArangoCluster() {
        compose = new ComposeContainer(new File("src/test/resources/arangodb-cluster-compose.yml"))
                .withExposedService("coordinator", 8529,
                        Wait.forHttp("/_api/version").forStatusCode(200)
                                .withStartupTimeout(Duration.ofMinutes(3)));
        compose.start();
    }

    public ArangoConfig config() {
        String host = compose.getServiceHost("coordinator", 8529);
        int port = compose.getServicePort("coordinator", 8529);
        return new ArangoConfig().setHosts(host + ":" + port);
    }

    @Override
    public void close() {
        compose.stop();
    }
}
```

- [ ] **Step 3: Write the boot + enumeration IT**

`ShardParallelClusterIT.java` (note the `IT` suffix → Failsafe, not Surefire):

```java
package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.split.ShardEligibility;
import io.arango.trino.split.ShardingInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardParallelClusterIT {
    private static TestingArangoCluster cluster;
    private static ArangoClient client;
    static final String DB = "shard_it";
    static final String COLL = "docs";

    @BeforeAll
    static void setup() {
        cluster = new TestingArangoCluster();
        client = new ArangoClient(cluster.config());
        client.createDatabaseForTest(DB);
        client.createShardedCollectionForTest(DB, COLL, 3); // added below
        for (int i = 0; i < 1000; i++) {
            client.insertForTest(DB, COLL, Map.of("_key", "k" + i, "v", i));
        }
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void clusterReportsThreeShards() {
        ShardingInfo info = client.getShardingInfo(DB, COLL);
        assertEquals(3, info.numberOfShards());
        assertTrue(ShardEligibility.ineligibilityReason(info).isEmpty(),
                "a 3-shard hash collection must be eligible for fan-out");
    }

    @Test
    void enumerationReturnsAllShardIds() {
        List<String> shards = client.listShardIds(DB, COLL);
        assertEquals(3, shards.size());
        assertEquals(3, shards.stream().distinct().count(), "shard IDs must be distinct");
    }
}
```

Add the sharded-collection test helper to `ArangoClient` (next to the existing `createDocumentCollectionForTest`):

```java
    public void createShardedCollectionForTest(String db, String name, int numberOfShards) {
        if (!arango.db(db).collection(name).exists()) {
            arango.db(db).createCollection(name,
                    new com.arangodb.model.CollectionCreateOptions().numberOfShards(numberOfShards));
        }
    }
```

- [ ] **Step 4: Run the IT to verify cluster boot + enumeration**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn verify -Dit.test=ShardParallelClusterIT -DskipUTs=false`
(If the module's Failsafe binding uses a different property, run `mvn verify -Dit.test=ShardParallelClusterIT`.)
Expected: PASS (2 tests). **If the cluster does not boot, fix the compose file here — this task's sole job is a reliably-booting cluster.** Common fixes: bump `withStartupTimeout`; correct `arangod` role args against the 3.12 reference; add `healthcheck:` blocks (agency + dbservers) so `depends_on` waits for *readiness* rather than mere container start — `depends_on` alone only orders start, a likely flakiness source given the coordinator races the agency/dbservers.

- [ ] **Step 5: Commit**

```bash
git add src/test/resources/arangodb-cluster-compose.yml \
        src/test/java/io/arango/trino/TestingArangoCluster.java \
        src/test/java/io/arango/trino/ShardParallelClusterIT.java \
        src/main/java/io/arango/trino/client/ArangoClient.java
git commit -m "test: boot ArangoDB cluster and prove shard enumeration"
```

---

### Task 9: Correctness assertions — count-sum gate, N⇒splits, cap, fallback (Failsafe IT)

**Files:**
- Create: `src/test/java/io/arango/trino/ShardParallelCorrectnessIT.java` (reuses `TestingArangoCluster`)

**Interfaces:**
- Consumes: `TestingArangoCluster`, `ArangoClient`, `ShardFanoutCapability.sumMatchesFull(...)`, `ShardGrouping.partition(...)`, `ArangoSplitManager`.

- [ ] **Step 1: Write the correctness IT**

```java
package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.split.ShardFanoutCapability;
import io.arango.trino.split.ShardGrouping;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardParallelCorrectnessIT {
    private static TestingArangoCluster cluster;
    private static ArangoClient client;
    private static final String DB = "shard_correct_it";
    private static final String COLL = "docs";
    private static final int DOCS = 1000;

    @BeforeAll
    static void setup() {
        cluster = new TestingArangoCluster();
        client = new ArangoClient(cluster.config());
        client.createDatabaseForTest(DB);
        client.createShardedCollectionForTest(DB, COLL, 3);
        for (int i = 0; i < DOCS; i++) {
            client.insertForTest(DB, COLL, Map.of("_key", "k" + i, "v", i));
        }
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    private static ArangoTableHandle handle() {
        // 5-arg record: (schema, table, edge, constraint, limit).
        return new ArangoTableHandle(DB, COLL, false, TupleDomain.all(), OptionalLong.empty());
    }

    private static List<ArangoSplit> splits(ArangoConfig config) {
        ArangoSplitManager mgr = new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        ConnectorSplitSource src = mgr.getSplits(null, null, handle(), DynamicFilter.EMPTY, Constraint.alwaysTrue());
        return src.getNextBatch(1000).getNow(null).getSplits().stream().map(ArangoSplit.class::cast).toList();
    }

    @Test
    void perShardCountsSumToTotalWithNoGapsOrDupes() {
        List<String> shards = client.listShardIds(DB, COLL);
        // count-sum: Σ(per-shard counts) == full (shared function, same path as the runtime probe)
        assertTrue(ShardFanoutCapability.sumMatchesFull(client, DB, COLL, shards.stream().map(List::of).toList()));
        // no-dupes: each _key appears in exactly one shard
        Set<String> all = new HashSet<>();
        int total = 0;
        for (String shard : shards) {
            List<String> keys = keysInShard(shard);
            total += keys.size();
            all.addAll(keys);
        }
        assertEquals(DOCS, total, "sum of per-shard key counts");
        assertEquals(DOCS, all.size(), "no key may appear in two shards");
    }

    @Test
    void threeShardsYieldThreeSplitsByDefault() {
        List<ArangoSplit> splits = splits(new ArangoConfig()); // S=1, M=32
        assertEquals(3, splits.size());
        assertEquals(3, splits.stream().flatMap(s -> s.shardIds().stream()).distinct().count());
    }

    @Test
    void maxSplitsCapGroupsShardsAndStillCovers() {
        List<ArangoSplit> splits = splits(new ArangoConfig().setMaxSplits(2)); // cap below shard count
        assertEquals(2, splits.size());
        List<List<String>> groups = splits.stream().map(ArangoSplit::shardIds).toList();
        assertTrue(ShardFanoutCapability.sumMatchesFull(client, DB, COLL, groups), "capped grouping must still cover all docs");
    }

    @Test
    void disabledFlagForcesSingleSplitOnCluster() {
        List<ArangoSplit> splits = splits(new ArangoConfig().setShardParallelismEnabled(false));
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }

    private static List<String> keysInShard(String shard) {
        var cursor = client.query(DB, "FOR d IN @@col RETURN d._key", Map.of("@col", COLL), List.of(shard));
        List<String> keys = new ArrayList<>();
        while (cursor.hasNext()) {
            keys.add(String.valueOf(cursor.next().get("_key")));
        }
        return keys;
    }
}
```

- [ ] **Step 2: Run to verify it fails first (before Task 7 wiring is present it wouldn't compile; here it should PASS)**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn verify -Dit.test=ShardParallelCorrectnessIT`
Expected: PASS (4 tests) — this is the milestone exit proof (N shards ⇒ N splits, counts sum exactly, cap covers, fallback proven).

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/arango/trino/ShardParallelCorrectnessIT.java
git commit -m "test: count-sum gate, N-splits, cap grouping, fallback on real cluster"
```

---

### Task 10: CI wiring + documentation

**Files:**
- Modify: `.github/workflows/ci.yml` (or the existing workflow that runs `mvn verify`)
- Modify: `CLAUDE.md`
- Modify: `README.md`

**Interfaces:** none (docs + CI).

- [ ] **Step 1: Confirm the ITs run under Failsafe in CI**

The e2e smoke-test task already switched CI to `mvn verify`. Verify the cluster ITs are included by the Failsafe `*IT.java` pattern and that the CI runner has Docker (it already does for Testcontainers). If the workflow pins `-Dit.test=PackagingSmokeIT`, broaden it to run all ITs (remove the filter) so `ShardParallelClusterIT` and `ShardParallelCorrectnessIT` execute. Cluster ITs are heavier — no code change needed beyond ensuring they are not excluded.

- [ ] **Step 2: Update `CLAUDE.md`**

- In "What this is", change the milestone line from "M1 (read skeleton)" to note M3: shard-parallel splits on cluster deployments.
- Under `### Read path`, update point 4 (`ArangoSplitManager`) to describe fan-out: discovery → allowlist gate → enumeration → grouping → N splits, with single-split fallback; note the new `io.arango.trino.split` package.
- Add the three new config keys (`arangodb.shards-per-split`, `arangodb.max-splits`, `arangodb.shard-parallelism-enabled`) to the `ArangoConfig` paragraph.

- [ ] **Step 3: Update `README.md`**

- Add the three config keys to the configuration table with defaults (1 / 32 / true).
- Add a "Sharding / parallelism" subsection: on a cluster, non-smart hash collections with >1 shard are scanned one split per shard-group; SmartGraph/SmartJoin/satellite/single-server fall back to a single split; the `shardIds` internal API is guarded by a version pin (≥3.11) and an active capability probe.
- Under limitations, note no cross-split snapshot consistency (master spec §1.3).

- [ ] **Step 4: Run the full build**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn verify`
Expected: full unit suite + all ITs (packaging smoke + both shard ITs) green.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml CLAUDE.md README.md
git commit -m "docs: document shard-parallel splits (M3) + run cluster ITs in CI"
```

---

## Self-Review

**1. Spec coverage** (against `2026-07-20-m3-shard-parallel-splits-design.md`):

| Spec item | Task |
|---|---|
| §3 correctness invariant | Task 3 (partition), Task 9 (count-sum + no-dupe proof) |
| §4 `ArangoSplit` carries shardIds | Task 1 |
| §4 config knobs | Task 2 |
| §5 shard discovery (properties + enumeration) | Task 5 (unit), Task 8 (cluster proof) |
| §6 allowlist gate | Task 4 (pure), Task 7 (wired) |
| §7 grouping formula (both knobs, precedence) | Task 3 |
| §8.1 version pin | Task 6 |
| §8.2 capability probe (lazy-cached) | Task 6 |
| §8.3 CI count-sum gate | Task 9 + Task 10 |
| §8.4 safe fallback | Task 7 (all branches → single split) |
| §8.5 SmartGraph exclusion | Task 4 (strategy allowlist + smartJoin) |
| §10 per-shard execution (AqlBuilder untouched) | Task 5 (query overload), Task 7 (threading) |
| §11 kill-switch deviation | Task 2 + Task 7 |
| §12 WARN observability | Task 7 (explicit multi-shard WARN branch) |
| **master-spec §6.2** — `limitGuaranteed=false` under multi-split | Task 7 (Steps 6–7) — added after Fable review |

No gaps. (The Fable review caught that the original plan checked coverage only against the M3 design spec's own item list and missed the master-spec §6.2 `limitGuaranteed` requirement — a real correctness bug under multi-split. Now covered by Task 7 Steps 6–7 and listed in the M3 design spec §4 components table.)

**2. Placeholder scan:** No "TBD"/"handle edge cases"/"similar to". The test config construction (`new ArangoConfig().setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword())`) and the 5-arg `ArangoTableHandle(schema, table, edge, constraint, limit)` are concrete (both corrected after the Fable review — `TestingArangoServer` has no `config()` accessor and the handle record is 5-arg, verified against source). The "copy from `ArangoMetadataTest`" pointers reference an existing test-double harness rather than inventing one. The compose file is concrete with a named acceptance gate (Task 8 Step 4).

**3. Type consistency:** `ArangoSplit(List<String> shardIds)`, `ShardGrouping.partition(List,int,int)→List<List<String>>`, `ShardingInfo(Integer,String,String)`, `ShardEligibility.ineligibilityReason(ShardingInfo)→Optional<String>`, `ShardFanoutCapability.canFanOut(String,String,List<String>)→boolean` / `sumMatchesFull(ArangoClient,String,String,List<List<String>>)→boolean`, `ArangoClient.{getShardingInfo,listShardIds,serverVersion,countWithShardIds,query(...,List<String>)}` — names/signatures match across all consuming tasks.
