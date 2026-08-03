# M6-B — AQL passthrough `query()` table function Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SELECT * FROM TABLE(arango.system.query(database => 'shop', query => '<raw AQL>'))` executes read-only AQL (including cluster `WITH` graph traversals) with an explain-plan allowlist gate, first-batch schema derivation, single-split execution, and a config kill switch.

**Architecture:** A polymorphic table function (`ArangoQueryFunction`, Mongo-`ptf.Query`-shaped) whose `analyze()` runs strictly explain → `AqlReadOnlyGate.check` → `firstBatch` (stream=true, disposed cursor) → schema derivation via `TypeMapper.merge`. The result is a **separate** handle type `ArangoQueryHandle` — filters/limits/projections/aggregations decline on it by `instanceof` dispatch, the split manager short-circuits to one split, and the page-source provider runs the stored query verbatim through a `PassthroughCursor` adapter that turns a non-object row into a user error. `ArangoPageSource`, `ValueMaterializer`, `TypeMapper`, and `AqlBuilder` are untouched.

**Tech Stack:** Java 25, Maven, Trino SPI 483 (`io.trino.spi.function.table.*`), ArangoDB Java driver 7.13 (`core-7.13.0`), Airlift 439 (`AbstractConfigurationAwareModule`/`conditionalModule`), JUnit + AssertJ, Testcontainers (ArangoDB 3.12). No mocking framework — test doubles are hand-written `ArangoClient` subclasses.

**Design spec:** `docs/superpowers/specs/2026-07-26-m6b-aql-passthrough-query-function-design.md`. Section references (§3, §4.1, …) point into it.

## Global Constraints

- **Build:** Java 25 (`maven.compiler.release=25`). Run `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` reports "command not found".
- **Docker must be running** for any test using `TestingArangoServer` / `TestingArangoCluster`.
- **No mocking framework.** Test doubles are hand-written subclasses (`new ArangoClient(new ArangoConfig()) { @Override ... }` — the constructor does not connect).
- **Gate ordering is a correctness invariant (§3.1):** `analyze()` must run explain → gate → `firstBatch`, in that order. `firstBatch` executes the query for real; transposing the steps executes a query the gate is about to reject.
- **Fail closed (§3):** the gate admits only entries whose `type` is exactly the string `"read"`. Absent, null, non-string, `"exclusive"`, unknown values, or an unrecognizable explain shape all reject.
- **The four pushdown hooks and the split manager must never see through an `ArangoQueryHandle`** — every `instanceof` dispatch goes *before* the existing `(ArangoTableHandle)` cast.
- **Naming, verbatim:** function `arango.system.query` (`SCHEMA_NAME = "system"`, `NAME = "query"`); config `arangodb.query-function-enabled` (default `true`); error code `ARANGODB_QUERY_NOT_READ_ONLY(1, USER_ERROR)`; synthesized table name `new SchemaTableName(database, "query")`.
- **Sample size `k` reuses `arangodb.schema.sample-size`** (default 1000). No new sample-size property (§7).
- **Explain goes over raw HTTP** (`POST /_api/explain` via `Request.Builder`, the `listShardIds` pattern) — never `explainQuery` (deprecated) nor `explainAqlQuery` (no typed accessors) (§8.1).
- **Spotless is ratcheted and file-granular** — Task 0 pre-formats the five pre-AOSP files this milestone edits so logic diffs stay readable (§8.3).
- **Static-analysis gates before the final commit:** `mvn spotless:check`, `mvn checkstyle:check`, `mvn compile spotbugs:check` — new files are not grandfathered.

## File Structure

New files:

| File | Responsibility |
|---|---|
| `src/main/java/io/arango/trino/ptf/ArangoQueryFunction.java` | `Provider<ConnectorTableFunction>`; nested `QueryFunction` (`analyze()` = gate ordering + schema derivation); nested `QueryFunctionHandle` record |
| `src/main/java/io/arango/trino/ptf/AqlReadOnlyGate.java` | Pure allowlist verdict over a raw explain response |
| `src/main/java/io/arango/trino/ptf/PassthroughCursor.java` | `ArangoCursor<Map>` adapter over `ArangoCursor<Object>`; turns an execution-time non-object row into `INVALID_FUNCTION_ARGUMENT` |
| `src/main/java/io/arango/trino/handle/ArangoQueryHandle.java` | `(database, query, columns)` `ConnectorTableHandle` record |
| Tests | `AqlReadOnlyGateTest`, `ArangoClientPassthroughTest`, `AqlPassthroughAssumptionsTest`, `ArangoQueryHandleTest`, `ArangoQueryFunctionTest`, `ArangoMetadataPassthroughTest`, `ArangoConnectorQueryFunctionTest`, `PassthroughClusterIT` |

Modified files: `ArangoConfig` (+flag), `ArangoErrorCode` (+code), `ArangoClient` (+`explainPlan`/`firstBatch`/`queryPassthrough` + test helpers), `SchemaResolver` (`resolveUnknown` → public static), `ArangoMetadata` (applyTableFunction + dispatch), `ArangoSplitManager` (short-circuit), `ArangoPageSourceProvider` (dispatch), `ArangoModule` (config-aware refactor), `ArangoConnector` (`getTableFunctions`), `ArangoConfigTest`, `ArangoSplitManagerTest`, `CLAUDE.md`, `README.md`.

---

## Task 0: Commit pending spec edit + formatting-only prep commit

**Why:** the spec has a 2-line uncommitted §4.1 edit from the previous session. And Spotless's ratchet (`ratchetFrom=origin/master`) is file-granular: the five pre-existing files below were hand-formatted in M1–M3 and will be fully reflowed by google-java-format the moment M6-B touches them. Reformatting them in one no-logic commit keeps every later diff readable. (The other files M6-B modifies — `ArangoConfig`, `ArangoMetadata`, `ArangoSplitManager`, `ArangoPageSourceProvider`, `ArangoConfigTest`, `ArangoSplitManagerTest` — were already AOSP-formatted by M5's Task 0.)

**Files:**
- Commit: `docs/superpowers/specs/2026-07-26-m6b-aql-passthrough-query-function-design.md`
- Modify (temporarily, revert before commit): `pom.xml` (the `spotless-maven-plugin` `<configuration>` block)
- Reformat: `src/main/java/io/arango/trino/client/ArangoClient.java`, `src/main/java/io/arango/trino/ArangoConnector.java`, `src/main/java/io/arango/trino/ArangoModule.java`, `src/main/java/io/arango/trino/ArangoErrorCode.java`, `src/main/java/io/arango/trino/schema/SchemaResolver.java`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing. Behavior must be byte-identical — whitespace/wrapping/import order only.

- [ ] **Step 1: Commit the pending spec edit**

```bash
git add docs/superpowers/specs/2026-07-26-m6b-aql-passthrough-query-function-design.md
git commit -m "docs: M6-B spec — §4.1 column-path rule for derived columns"
```

- [ ] **Step 2: Record the pre-formatting test baseline**

```bash
source ~/.sdkman/bin/sdkman-init.sh 2>/dev/null
mvn -q test 2>&1 | tail -20
```

Expected: BUILD SUCCESS. Note the test count — it must be identical at the end of this task.

- [ ] **Step 3: Temporarily neutralize the ratchet and scope Spotless to the five files**

In `pom.xml`, inside the `spotless-maven-plugin` `<configuration>` block: comment out `<ratchetFrom>origin/master</ratchetFrom>` and add this `<includes>` list inside the `<java>` element (M5 Task 0 did the same maneuver):

```xml
<includes>
    <include>src/main/java/io/arango/trino/client/ArangoClient.java</include>
    <include>src/main/java/io/arango/trino/ArangoConnector.java</include>
    <include>src/main/java/io/arango/trino/ArangoModule.java</include>
    <include>src/main/java/io/arango/trino/ArangoErrorCode.java</include>
    <include>src/main/java/io/arango/trino/schema/SchemaResolver.java</include>
</includes>
```

- [ ] **Step 4: Apply, then restore pom.xml exactly**

```bash
mvn spotless:apply && git diff --stat
git checkout -- pom.xml
```

Expected: some or all of the five files listed with changes; after the checkout, `git diff --stat -- pom.xml` is empty.

- [ ] **Step 5: Verify no behavior change**

```bash
mvn -q test 2>&1 | tail -20
```

Expected: BUILD SUCCESS, same test count as Step 2.

- [ ] **Step 6: Commit**

```bash
git add src
git commit -m "style: reformat M6-B-touched files with google-java-format (AOSP)

No behavior change. The Spotless ratchet is file-granular, so M6-B's edits
to these hand-formatted M1-M3 files would reflow them anyway; one
formatting-only commit keeps the M6-B logic diffs reviewable."
```

---

## Task 1: Config flag + error code

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Modify: `src/main/java/io/arango/trino/ArangoErrorCode.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `ArangoConfig.isQueryFunctionEnabled()` / `setQueryFunctionEnabled(boolean)` (default `true`, property `arangodb.query-function-enabled`) — read by Task 9's conditional module binding. `ArangoErrorCode.ARANGODB_QUERY_NOT_READ_ONLY` — thrown by Task 6.

- [ ] **Step 1: Write the failing test**

In `ArangoConfigTest.testDefaults`, add to the `recordDefaults` chain:

```java
                        .setQueryFunctionEnabled(true)
```

In `testExplicitPropertyMappings`, add to the props map and the expected config:

```java
                        .put("arangodb.query-function-enabled", "false")
```
```java
                        .setQueryFunctionEnabled(false)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ArangoConfigTest`
Expected: COMPILATION ERROR — `cannot find symbol: method setQueryFunctionEnabled`.

- [ ] **Step 3: Implement**

In `ArangoConfig`, next to the other boolean flags:

```java
    private boolean queryFunctionEnabled = true;
```

```java
    public boolean isQueryFunctionEnabled() {
        return queryFunctionEnabled;
    }

    @Config("arangodb.query-function-enabled")
    @ConfigDescription(
            "Register the arango.system.query passthrough table function; false removes it entirely")
    public ArangoConfig setQueryFunctionEnabled(boolean queryFunctionEnabled) {
        this.queryFunctionEnabled = queryFunctionEnabled;
        return this;
    }
```

In `ArangoErrorCode`, extend the enum:

```java
    ARANGODB_TYPE_CONVERSION_ERROR(0, USER_ERROR),
    ARANGODB_QUERY_NOT_READ_ONLY(1, USER_ERROR);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ArangoConfigTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoConfig.java src/main/java/io/arango/trino/ArangoErrorCode.java src/test/java/io/arango/trino/ArangoConfigTest.java
git commit -m "feat: arangodb.query-function-enabled flag + ARANGODB_QUERY_NOT_READ_ONLY error code"
```

---

## Task 2: `AqlReadOnlyGate` — the pure allowlist verdict

**Files:**
- Create: `src/main/java/io/arango/trino/ptf/AqlReadOnlyGate.java`
- Test: `src/test/java/io/arango/trino/ptf/AqlReadOnlyGateTest.java`

**Interfaces:**
- Consumes: a raw explain response `Map<String, Object>` (produced by Task 3's `ArangoClient.explainPlan`; shape: `{"plan": {"collections": [{"name": ..., "type": ...}, ...], ...}, ...}`).
- Produces: `AqlReadOnlyGate.check(Map<String, Object>) → Optional<AqlReadOnlyGate.Rejection>`; `record Rejection(Kind kind, String reason)`; `enum Kind { NOT_READ_ONLY, SYSTEM_COLLECTION }`. Empty optional = admitted. Task 6 maps `NOT_READ_ONLY → ARANGODB_QUERY_NOT_READ_ONLY` and `SYSTEM_COLLECTION → INVALID_FUNCTION_ARGUMENT` (§9).

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.ptf;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.ptf.AqlReadOnlyGate.Kind;
import io.arango.trino.ptf.AqlReadOnlyGate.Rejection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AqlReadOnlyGateTest {
    /** Builds {"plan": {"collections": [{name,type}...]}} from (name, type) pairs; a null type
     * puts an entry with no "type" key at all (the absent case). */
    private static Map<String, Object> explain(String... nameTypePairs) {
        List<Map<String, Object>> collections = new ArrayList<>();
        for (int i = 0; i < nameTypePairs.length; i += 2) {
            Map<String, Object> entry = new HashMap<>();
            entry.put("name", nameTypePairs[i]);
            if (nameTypePairs[i + 1] != null) {
                entry.put("type", nameTypePairs[i + 1]);
            }
            collections.add(entry);
        }
        return Map.of("plan", Map.of("collections", collections));
    }

    @Test
    void allReadAdmits() {
        assertThat(AqlReadOnlyGate.check(explain("users", "read", "follows", "read"))).isEmpty();
    }

    @Test
    void emptyCollectionsAdmits() {
        // RETURN 1..10 plans with no collections at all (§3 row 2)
        assertThat(AqlReadOnlyGate.check(explain())).isEmpty();
    }

    @Test
    void anyWriteRejectsNamingTheCollection() {
        Optional<Rejection> verdict =
                AqlReadOnlyGate.check(explain("users", "read", "follows", "write"));
        assertThat(verdict).isPresent();
        assertThat(verdict.get().kind()).isEqualTo(Kind.NOT_READ_ONLY);
        assertThat(verdict.get().reason()).contains("follows");
    }

    // Fail closed under novelty (§3): everything that is not exactly "read" rejects.
    @Test
    void absentTypeRejects() {
        assertThat(AqlReadOnlyGate.check(explain("users", null))).isPresent();
    }

    @Test
    void exclusiveTypeRejects() {
        assertThat(AqlReadOnlyGate.check(explain("users", "exclusive"))).isPresent();
    }

    @Test
    void unknownTypeRejects() {
        assertThat(AqlReadOnlyGate.check(explain("users", "readwrite"))).isPresent();
    }

    @Test
    void missingPlanRejects() {
        assertThat(AqlReadOnlyGate.check(Map.of("error", false))).isPresent();
    }

    @Test
    void missingCollectionsListRejects() {
        assertThat(AqlReadOnlyGate.check(Map.of("plan", Map.of("nodes", List.of())))).isPresent();
    }

    @Test
    void nonMapCollectionsEntryRejects() {
        assertThat(AqlReadOnlyGate.check(Map.of("plan", Map.of("collections", List.of("users")))))
                .isPresent();
    }

    @Test
    void systemCollectionRejectsAsItsOwnKind() {
        // read-typed but _-prefixed: the connector's own hiding convention (§3.3)
        Optional<Rejection> verdict = AqlReadOnlyGate.check(explain("_graphs", "read"));
        assertThat(verdict).isPresent();
        assertThat(verdict.get().kind()).isEqualTo(Kind.SYSTEM_COLLECTION);
        assertThat(verdict.get().reason()).contains("_graphs");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=AqlReadOnlyGateTest`
Expected: COMPILATION ERROR — `package io.arango.trino.ptf does not exist` / `cannot find symbol: AqlReadOnlyGate`.

- [ ] **Step 3: Implement**

```java
package io.arango.trino.ptf;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The one place M6-B's safety invariant lives (spec §3): a passthrough query is admitted only if
 * every collection in its execution plan is accessed exactly {@code "read"}. This is an allowlist —
 * ArangoDB must declare a query's write collections up front to take locks, so a query cannot
 * mutate a collection this list does not carry. Anything unrecognized (absent type, new access
 * modes like "exclusive", a reshaped explain response) fails closed.
 */
public final class AqlReadOnlyGate {
    public enum Kind {
        NOT_READ_ONLY,
        SYSTEM_COLLECTION
    }

    public record Rejection(Kind kind, String reason) {}

    private AqlReadOnlyGate() {}

    public static Optional<Rejection> check(Map<String, Object> explainResponse) {
        if (!(explainResponse.get("plan") instanceof Map<?, ?> plan)) {
            return Optional.of(
                    new Rejection(Kind.NOT_READ_ONLY, "explain response carried no plan object"));
        }
        if (!(plan.get("collections") instanceof List<?> collections)) {
            return Optional.of(
                    new Rejection(Kind.NOT_READ_ONLY, "explain plan carried no collections list"));
        }
        for (Object entry : collections) {
            if (!(entry instanceof Map<?, ?> collection)) {
                return Optional.of(
                        new Rejection(
                                Kind.NOT_READ_ONLY, "unrecognized entry in plan collections"));
            }
            // A non-string name is only cosmetic here: the "read" type check below is still
            // exact for such an entry, so this fallback cannot open the gate — it only means
            // the _-prefix hardening (which needs a real name) does not apply to it.
            String name =
                    collection.get("name") instanceof String s ? s : "<unnamed collection>";
            Object type = collection.get("type");
            if (!"read".equals(type)) {
                return Optional.of(
                        new Rejection(
                                Kind.NOT_READ_ONLY,
                                "collection '%s' is accessed '%s', not 'read'"
                                        .formatted(name, type)));
            }
            if (name.startsWith("_")) {
                // This connector hides system collections from listTables; the passthrough keeps
                // that convention. Hardening, not a guarantee — DOCUMENT("_users/x") resolves at
                // runtime and never appears in the plan (§3.3).
                return Optional.of(
                        new Rejection(
                                Kind.SYSTEM_COLLECTION,
                                "system collection '%s'".formatted(name)));
            }
        }
        return Optional.empty();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=AqlReadOnlyGateTest`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ptf/AqlReadOnlyGate.java src/test/java/io/arango/trino/ptf/AqlReadOnlyGateTest.java
git commit -m "feat: AqlReadOnlyGate — explain-plan allowlist, fail closed"
```

---

## Task 3: `ArangoClient` passthrough methods + test seeding helpers

**Files:**
- Modify: `src/main/java/io/arango/trino/client/ArangoClient.java`
- Test: `src/test/java/io/arango/trino/client/ArangoClientPassthroughTest.java`

**Interfaces:**
- Consumes: the private `arango` driver field; the `Request.Builder` raw-HTTP pattern already used by `listShardIds` (`ArangoClient.java:79-90`).
- Produces (for Tasks 4/6/8):
  - `Map<String, Object> explainPlan(String database, String aql)` — raw `POST /_api/explain`, full response body; throws `ArangoDBException` on syntax error / unbound bind parameter / unknown collection / unknown database.
  - `List<Object> firstBatch(String database, String aql, int k)` — executes with `batchSize(k)` **and `stream(true)`** (§4: without stream, the server materializes the whole result before the first batch), reads at most `k` rows typed `Object` (so non-object rows arrive as `String`/`Number`/`null` instead of a driver deserialization failure), and **always disposes the cursor in `finally`**. The returned list may contain nulls — do not use `List.copyOf`.
  - `ArangoCursor<Object> queryPassthrough(String database, String aql)` — execution-time cursor, `Object`-typed for the same reason.
  - Test-only helpers: `registerAqlFunctionForTest(db, name, code)` (raw `POST /_api/aqlfunction`), `createGraphForTest(db, graph, edgeCollection, vertexCollection)`, `createArangoSearchViewForTest(db, view, collection)`.

- [ ] **Step 1: Verify driver classes for the new helpers exist (they are not yet imported anywhere)**

```bash
CORE=$(find ~/.m2 -path '*com/arangodb*' -name 'core-7*.jar' | head -1)
javap -cp "$CORE" com.arangodb.entity.EdgeDefinition com.arangodb.model.arangosearch.ArangoSearchCreateOptions com.arangodb.entity.arangosearch.CollectionLink 2>&1 | grep -E "^(public|Compiled|Error)"
```

Expected: all three classes found (`Error: class not found` for none). If `ArangoSearchCreateOptions`/`CollectionLink` live elsewhere, locate with `unzip -l "$CORE" | grep -i arangosearch` and adjust imports.

- [ ] **Step 2: Write the failing test**

```java
package io.arango.trino.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.arangodb.ArangoDBException;
import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoClientPassthroughTest {
    private static final String DB = "client_ptf";
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "docs");
        for (int i = 0; i < 10; i++) {
            client.insertForTest(DB, "docs", Map.of("_key", "k" + i, "v", (long) i));
        }
    }

    @AfterAll
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void explainPlanCarriesPerCollectionAccessType() {
        Map<String, Object> explain = client.explainPlan(DB, "FOR d IN docs RETURN d");
        Object plan = explain.get("plan");
        assertThat(plan).isInstanceOf(Map.class);
        Object collections = ((Map<?, ?>) plan).get("collections");
        assertThat(collections).isInstanceOf(List.class);
        Map<?, ?> entry = (Map<?, ?>) ((List<?>) collections).get(0);
        assertThat(entry.get("name")).isEqualTo("docs");
        assertThat(entry.get("type")).isEqualTo("read");
    }

    // Task 6's §9 error routing branches on getErrorNum() from exceptions raised by the raw
    // Request path — a different driver code path from the typed API ArangoMetadata already
    // relies on. Prove the error numbers survive it HERE, where the dependency is created,
    // so a mismatch fails one early test instead of four downstream ones.
    @Test
    void explainPlanSyntaxErrorCarriesAqlErrorNum() {
        assertThatThrownBy(() -> client.explainPlan(DB, "THIS IS NOT AQL"))
                .isInstanceOfSatisfying(
                        ArangoDBException.class,
                        e -> assertThat(e.getErrorNum()).isBetween(1500, 1599));
    }

    @Test
    void explainPlanUnknownDatabaseCarries1228() {
        assertThatThrownBy(() -> client.explainPlan("no_such_db", "FOR d IN docs RETURN d"))
                .isInstanceOfSatisfying(
                        ArangoDBException.class, e -> assertThat(e.getErrorNum()).isEqualTo(1228));
    }

    @Test
    void explainPlanUnknownCollectionCarries1203() {
        assertThatThrownBy(() -> client.explainPlan(DB, "FOR d IN nope RETURN d"))
                .isInstanceOfSatisfying(
                        ArangoDBException.class, e -> assertThat(e.getErrorNum()).isEqualTo(1203));
    }

    @Test
    void explainPlanDoesNotExecute() {
        // explain of an INSERT must not write (it only plans)
        long before = client.countWithShardIds(DB, "docs", List.of());
        client.explainPlan(DB, "INSERT {x: 1} INTO docs");
        assertThat(client.countWithShardIds(DB, "docs", List.of())).isEqualTo(before);
    }

    @Test
    void firstBatchIsBoundedByK() {
        List<Object> rows = client.firstBatch(DB, "FOR d IN docs RETURN d", 3);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)).isInstanceOf(Map.class);
    }

    @Test
    void firstBatchReturnsAllRowsWhenFewerThanK() {
        assertThat(client.firstBatch(DB, "FOR d IN docs RETURN d", 100)).hasSize(10);
    }

    @Test
    void firstBatchYieldsRawScalarsAndNulls() {
        // Object-typed on purpose: non-object rows must arrive inspectable, not as a driver
        // deserialization failure (§4.1 detection happens in connector code)
        List<Object> rows = client.firstBatch(DB, "FOR x IN [1, \"two\", null] RETURN x", 10);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(1)).isEqualTo("two");
        assertThat(rows.get(2)).isNull();
    }

    @Test
    void firstBatchOfEmptyResultIsEmpty() {
        assertThat(client.firstBatch(DB, "FOR d IN docs FILTER false RETURN d", 5)).isEmpty();
    }

    @Test
    void queryPassthroughStreamsObjectRows() {
        var cursor = client.queryPassthrough(DB, "FOR d IN docs LIMIT 2 RETURN d");
        try {
            assertThat(cursor.hasNext()).isTrue();
            assertThat(cursor.next()).isInstanceOf(Map.class);
        } finally {
            try {
                cursor.close();
            } catch (Exception ignored) {
            }
        }
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q test -Dtest=ArangoClientPassthroughTest`
Expected: COMPILATION ERROR — `cannot find symbol: method explainPlan`.

- [ ] **Step 4: Implement the three methods**

In `ArangoClient`, after `countWithShardIds` (new imports: `java.util.ArrayList`, `java.util.Collections`, `io.airlift.log.Logger`; new field `private static final Logger log = Logger.get(ArangoClient.class);` — the class has none today):

```java
    /**
     * Raw POST /_api/explain (spec §8.1): the gate needs plan.collections[].type, which the
     * driver's non-deprecated typed API does not expose. Full response body, uninterpreted —
     * AqlReadOnlyGate owns all shape validation so it can fail closed.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> explainPlan(String database, String aql) {
        Request<Map<String, Object>> req =
                new Request.Builder<Map<String, Object>>()
                        .db(database)
                        .method(Request.Method.POST)
                        .path("/_api/explain")
                        .body(Map.of("query", aql))
                        .build();
        return (Map<String, Object>) arango.execute(req, Map.class).getBody();
    }

    /**
     * First batch of a streaming execution, at most {@code k} rows, cursor always disposed.
     * stream(true) is load-bearing (spec §4): a non-stream cursor materializes the COMPLETE
     * result server-side before serving the first batch, which is the cost this method exists
     * to avoid. Object-typed so a non-object row arrives inspectable rather than as a driver
     * deserialization failure. The result may contain nulls.
     */
    public List<Object> firstBatch(String database, String aql, int k) {
        ArangoCursor<Object> cursor =
                arango.db(database)
                        .query(aql, Object.class, new AqlQueryOptions().batchSize(k).stream(true));
        try {
            List<Object> out = new ArrayList<>();
            while (out.size() < k && cursor.hasNext()) {
                out.add(cursor.next());
            }
            return Collections.unmodifiableList(out);
        } finally {
            // A stream cursor holds a server-side query snapshot open until disposed or TTL.
            try {
                cursor.close();
            } catch (Exception e) {
                // logged, not rethrown: disposal failure must not mask the rows already read
                // (referencing e also keeps SpotBugs DE_MIGHT_IGNORE satisfied — this file is
                // not grandfathered for it)
                log.debug(e, "Failed to dispose first-batch cursor");
            }
        }
    }

    /**
     * Execution-time passthrough cursor; Object-typed for the same reason as firstBatch.
     * Deliberately NOT stream(true): the execution path consumes the whole result anyway, and
     * this matches the existing scan path's cursor behavior (§4's streaming argument is about
     * planning-time cost only).
     */
    public ArangoCursor<Object> queryPassthrough(String database, String aql) {
        return arango.db(database).query(aql, Object.class);
    }
```

- [ ] **Step 5: Add the test-only helpers**

In the existing `// ---- test-only seeding helpers ----` section (new imports: `com.arangodb.entity.EdgeDefinition`, `com.arangodb.model.arangosearch.ArangoSearchCreateOptions`, `com.arangodb.entity.arangosearch.CollectionLink` — adjust per Step 1's javap):

```java
    public void registerAqlFunctionForTest(String db, String name, String code) {
        Request<Map<String, Object>> req =
                new Request.Builder<Map<String, Object>>()
                        .db(db)
                        .method(Request.Method.POST)
                        .path("/_api/aqlfunction")
                        .body(Map.of("name", name, "code", code, "isDeterministic", false))
                        .build();
        arango.execute(req, Map.class);
    }

    public void createGraphForTest(
            String db, String graph, String edgeCollection, String vertexCollection) {
        if (!arango.db(db).graph(graph).exists()) {
            arango.db(db)
                    .createGraph(
                            graph,
                            List.of(
                                    new EdgeDefinition()
                                            .collection(edgeCollection)
                                            .from(vertexCollection)
                                            .to(vertexCollection)));
        }
    }

    public void createArangoSearchViewForTest(String db, String view, String collection) {
        if (!arango.db(db).view(view).exists()) {
            arango.db(db)
                    .createArangoSearch(
                            view,
                            new ArangoSearchCreateOptions()
                                    .link(CollectionLink.on(collection).includeAllFields(true)));
        }
    }
```

- [ ] **Step 6: Run test to verify it passes**

Run: `mvn -q test -Dtest=ArangoClientPassthroughTest`
Expected: PASS (10 tests).

- [ ] **Step 7: Static-analysis gates on the modified main file (not grandfathered for new findings)**

```bash
mvn checkstyle:check && mvn compile spotbugs:check
```

Expected: pass. Fix any finding now, in this task — the Spotless ratchet makes late edits to `ArangoClient.java` expensive (full-file reflow in an unrelated commit).

- [ ] **Step 8: Commit**

```bash
git add src/main/java/io/arango/trino/client/ArangoClient.java src/test/java/io/arango/trino/client/ArangoClientPassthroughTest.java
git commit -m "feat: ArangoClient explainPlan (raw /_api/explain), streaming firstBatch, queryPassthrough"
```

---

## Task 4: `AqlPassthroughAssumptionsTest` — pin the server-side invariants

**Files:**
- Test: `src/test/java/io/arango/trino/aql/AqlPassthroughAssumptionsTest.java`

**Interfaces:**
- Consumes: `ArangoClient.explainPlan` / `query` / `countWithShardIds` / the Task 3 test helpers; `AqlReadOnlyGate.check`.
- Produces: nothing for later tasks — this is the analogue of `AqlSemanticsAssumptionsTest`: the test that fails if an ArangoDB upgrade changes the invariant the gate's soundness rests on (§3.2, §11).

This test pins **measured server behavior**, so every assertion below was already probed against 3.12.4 (spec Appendix B); a failure here means the server changed, not the test.

- [ ] **Step 1: Write the test (goes green immediately — it pins existing behavior against existing code)**

```java
package io.arango.trino.aql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.ptf.AqlReadOnlyGate;
import io.arango.trino.ptf.AqlReadOnlyGate.Kind;
import io.arango.trino.ptf.AqlReadOnlyGate.Rejection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Pins the explain-plan access-mode semantics AqlReadOnlyGate's soundness rests on (spec §3,
 * Appendix B). If an ArangoDB upgrade changes any row here, the gate's argument must be revisited
 * rather than silently lost — the analogue of AqlSemanticsAssumptionsTest for M5.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AqlPassthroughAssumptionsTest {
    private static final String DB = "gate_probe";
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "users");
        client.insertForTest(DB, "users", Map.of("_key", "a", "name", "ann"));
        client.insertForTest(DB, "users", Map.of("_key", "b", "name", "bob"));
        client.createEdgeCollectionForTest(DB, "follows");
        client.insertForTest(DB, "follows", Map.of("_from", "users/a", "_to", "users/b"));
        client.createGraphForTest(DB, "social", "follows", "users");
    }

    @AfterAll
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private Optional<Rejection> verdict(String aql) {
        return AqlReadOnlyGate.check(client.explainPlan(DB, aql));
    }

    private long userCount() {
        return client.countWithShardIds(DB, "users", List.of());
    }

    // ---- §3 table: reads admit ----

    @Test
    void plainReadAdmits() {
        assertThat(verdict("FOR d IN users RETURN d")).isEmpty();
    }

    @Test
    void noCollectionQueryAdmits() {
        assertThat(verdict("RETURN 1..10")).isEmpty();
    }

    @Test
    void anonymousTraversalAdmits() {
        assertThat(verdict("FOR v IN 1..1 OUTBOUND \"users/a\" follows RETURN v")).isEmpty();
    }

    @Test
    void namedGraphTraversalAdmits() {
        assertThat(verdict("FOR v IN 1..1 OUTBOUND \"users/a\" GRAPH \"social\" RETURN v"))
                .isEmpty();
    }

    @Test
    void insertKeywordInStringLiteralAdmits() {
        // exactly the false positive a keyword scan would produce (§3 row 5)
        assertThat(verdict("FOR d IN users FILTER d.name == \"INSERT INTO\" RETURN d")).isEmpty();
    }

    // ---- §3 table: every data-modification form rejects ----

    @Test
    void insertRejects() {
        assertRejectsAsWrite("INSERT {x: 1} INTO users", "users");
    }

    @Test
    void updateRejects() {
        assertRejectsAsWrite("FOR d IN users UPDATE d WITH {x: 1} IN users", "users");
    }

    @Test
    void removeRejects() {
        assertRejectsAsWrite("FOR d IN users REMOVE d IN users", "users");
    }

    @Test
    void replaceRejects() {
        assertRejectsAsWrite("FOR d IN users REPLACE d WITH {y: 2} IN users", "users");
    }

    @Test
    void upsertRejects() {
        assertRejectsAsWrite(
                "UPSERT {_key: \"a\"} INSERT {n: 1} UPDATE {n: 2} IN users", "users");
    }

    @Test
    void subqueryInsertRejects() {
        assertRejectsAsWrite(
                "FOR d IN users LET x = (INSERT {q: 1} INTO users RETURN NEW) RETURN x", "users");
    }

    @Test
    void crossCollectionInsertRejectsTheWrittenCollection() {
        assertRejectsAsWrite("FOR d IN users INSERT {c: d.name} INTO follows", "follows");
    }

    private void assertRejectsAsWrite(String aql, String collection) {
        Optional<Rejection> v = verdict(aql);
        assertThat(v).isPresent();
        assertThat(v.get().kind()).isEqualTo(Kind.NOT_READ_ONLY);
        assertThat(v.get().reason()).contains(collection);
    }

    // ---- §3.2: a UDF escapes the gate but not the server's transaction registration ----

    @Test
    void udfWriteIsAdmittedByGateButBlockedByServer() {
        // The UDF body catches the server's refusal and returns it as a string — this matches
        // the spec's Appendix B probe, whose recorded RESULT was
        // ["BLOCKED: unregistered collection used in transaction: users [write]"], i.e. a
        // returned value, not a raised exception. Asserting on the returned string via the
        // Object-typed firstBatch avoids the Map.class deserialization confound entirely.
        client.registerAqlFunctionForTest(
                DB,
                "EVIL::WRITE",
                "function (x) { try { require(\"@arangodb\").db.users.save({x: x});"
                        + " return \"WROTE\"; } catch (e) { return \"BLOCKED: \" + String(e)"
                        + " + (e.errorMessage || \"\"); } }");
        // the gate admits: a UDF call declares no collections at all
        assertThat(verdict("RETURN EVIL::WRITE(\"pwned\")")).isEmpty();

        long before = userCount();
        // the server's lock-declaration refusal IS the closure argument the gate's soundness
        // rests on (§3.2); a "WROTE" result here means an ArangoDB upgrade relaxed it
        assertThat(String.valueOf(
                        client.firstBatch(DB, "RETURN EVIL::WRITE(\"pwned\")", 1).get(0)))
                .contains("unregistered collection");
        // dynamic invocation forms too — the ones an AST denylist would have to enumerate
        assertThat(String.valueOf(
                        client.firstBatch(DB, "RETURN CALL(\"EVIL::WRITE\", \"dyn\")", 1).get(0)))
                .contains("unregistered collection");
        assertThat(String.valueOf(
                        client.firstBatch(DB, "RETURN APPLY(\"EVIL::WRITE\", [\"dyn2\"])", 1)
                                .get(0)))
                .contains("unregistered collection");
        // the real invariant: nothing was written
        assertThat(userCount()).isEqualTo(before);
    }

    // ---- §3.4: explain refuses a declared-but-unbound bind parameter ----

    @Test
    void unboundBindParameterRejectsAtExplain() {
        assertThatThrownBy(
                        () -> client.explainPlan(DB, "FOR d IN users FILTER d.age > @minAge RETURN d"))
                .hasMessageContaining("bind parameter");
    }

    // ---- rows the §3 table lacked (§11): view read and SHORTEST_PATH ----

    @Test
    void arangoSearchViewReadAdmits() {
        client.createArangoSearchViewForTest(DB, "users_view", "users");
        assertThat(verdict("FOR d IN users_view SEARCH d.name == \"ann\" RETURN d")).isEmpty();
    }

    @Test
    void shortestPathAdmits() {
        assertThat(
                        verdict(
                                "FOR v IN OUTBOUND SHORTEST_PATH \"users/a\" TO \"users/b\" follows RETURN v"))
                .isEmpty();
    }
}
```

- [ ] **Step 2: Run it**

Run: `mvn -q test -Dtest=AqlPassthroughAssumptionsTest`
Expected: PASS (16 tests). If `udfWriteIsAdmittedByGateButBlockedByServer` observes a raised exception instead of the returned "BLOCKED: ..." string, re-measure, switch the assertion to `assertThatThrownBy` over the same `firstBatch` call, and record the observed behavior in the test comment — the test must document what the server actually does. If `arangoSearchViewReadAdmits` fails because the view name is not in `plan.collections[]` in an unexpected shape, inspect with a temporary `System.out.println(client.explainPlan(...))`, then pin whatever the measured shape is — the point of this test is to record reality.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/arango/trino/aql/AqlPassthroughAssumptionsTest.java
git commit -m "test: pin explain access-mode semantics, UDF transaction-registration block, view/SHORTEST_PATH plans"
```

---

## Task 5: `ArangoQueryHandle` + Jackson round-trip

**Files:**
- Create: `src/main/java/io/arango/trino/handle/ArangoQueryHandle.java`
- Test: `src/test/java/io/arango/trino/handle/ArangoQueryHandleTest.java`

**Interfaces:**
- Consumes: `ArangoColumnHandle` (existing record).
- Produces: `record ArangoQueryHandle(String database, String query, List<ArangoColumnHandle> columns) implements ConnectorTableHandle` with `SchemaTableName schemaTableName()` returning `new SchemaTableName(database, "query")`. Used by Tasks 6–10.

- [ ] **Step 1: Write the failing test**

The columns carry Trino `Type`s, which plain Jackson cannot round-trip. Serialize types the way the engine wire does — by `TypeId` string — and deserialize with trino-main's `TypeDeserializer` over `TESTING_TYPE_MANAGER` (both on the test classpath via `trino-testing`):

```java
package io.arango.trino.handle;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.TypeDeserializer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArangoQueryHandleTest {
    // Serializes a Type the way the engine does on the wire: by TypeId. The matching
    // deserializer is trino-main's TypeDeserializer over TESTING_TYPE_MANAGER.
    private static final class TestTypeSerializer extends JsonSerializer<Type> {
        @Override
        public void serialize(Type value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value.getTypeId().getId());
        }
    }

    static JsonCodecFactory codecFactory() {
        // JsonMapperProvider, not ObjectMapperProvider: airlift-439's JsonCodecFactory
        // constructors take JsonMapper / Provider<JsonMapper> only (verified via javap)
        JsonMapperProvider provider = new JsonMapperProvider();
        provider.setJsonSerializers(Map.of(Type.class, new TestTypeSerializer()));
        provider.setJsonDeserializers(
                Map.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        return new JsonCodecFactory(provider);
    }

    private static final JsonCodec<ArangoQueryHandle> CODEC =
            codecFactory().jsonCodec(ArangoQueryHandle.class);

    static ArangoQueryHandle sample() {
        return new ArangoQueryHandle(
                "shop",
                "WITH users FOR v IN 1..1 OUTBOUND \"users/a\" follows RETURN {name: v.name}",
                List.of(
                        new ArangoColumnHandle("name", VarcharType.VARCHAR, false, List.of("name")),
                        new ArangoColumnHandle("age", BigintType.BIGINT, false, List.of("age")),
                        new ArangoColumnHandle(
                                "address",
                                RowType.rowType(
                                        RowType.field(
                                                "tags", new ArrayType(VarcharType.VARCHAR))),
                                false,
                                List.of("address"))));
    }

    @Test
    void roundTripsThroughJson() {
        ArangoQueryHandle handle = sample();
        assertThat(CODEC.fromJson(CODEC.toJson(handle))).isEqualTo(handle);
    }

    @Test
    void schemaTableNameSynthesizesQueryAsTableName() {
        assertThat(sample().schemaTableName()).isEqualTo(new SchemaTableName("shop", "query"));
    }

    @Test
    void columnsAreDefensivelyCopied() {
        assertThat(sample().columns()).isUnmodifiable();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ArangoQueryHandleTest`
Expected: COMPILATION ERROR — `cannot find symbol: ArangoQueryHandle`.

- [ ] **Step 3: Implement**

```java
package io.arango.trino.handle;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;

/**
 * A passthrough query (spec §5): a SEPARATE handle type, not a field on ArangoTableHandle, so
 * "filter/limit/projection/aggregation pushed at opaque user AQL" is unrepresentable rather than
 * a state every hook must remember to decline. The query travels here, not on the split (§5.1).
 */
public record ArangoQueryHandle(
        @JsonProperty("database") String database,
        @JsonProperty("query") String query,
        @JsonProperty("columns") List<ArangoColumnHandle> columns)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoQueryHandle {
        requireNonNull(database, "database is null");
        requireNonNull(query, "query is null");
        columns = List.copyOf(requireNonNull(columns, "columns is null"));
    }

    /** A passthrough has no table identity; this synthesized name renders in EXPLAIN (§5.2). */
    public SchemaTableName schemaTableName() {
        return new SchemaTableName(database, "query");
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ArangoQueryHandleTest`
Expected: PASS (3 tests). If `TypeDeserializer` fails to resolve the row type, print the serialized JSON and check the id string parses via `TESTING_TYPE_MANAGER.getType(...)`; the id emitted by `getTypeId()` is the canonical signature, which `InternalTypeManager` parses.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/handle/ArangoQueryHandle.java src/test/java/io/arango/trino/handle/ArangoQueryHandleTest.java
git commit -m "feat: ArangoQueryHandle — separate ConnectorTableHandle for passthrough queries"
```

---

## Task 6: `ArangoQueryFunction` — the PTF itself

**Files:**
- Create: `src/main/java/io/arango/trino/ptf/ArangoQueryFunction.java`
- Modify: `src/main/java/io/arango/trino/schema/SchemaResolver.java` (make `resolveUnknown` `public static` — one-word visibility change; its javadoc already describes the recursion)
- Test: `src/test/java/io/arango/trino/ptf/ArangoQueryFunctionTest.java`
- Modify test: `src/test/java/io/arango/trino/handle/ArangoQueryHandleTest.java` (add `QueryFunctionHandle` round-trip — spec §11 requires both handles round-tripped)

**Interfaces:**
- Consumes: `ArangoClient.explainPlan`/`firstBatch` (Task 3), `AqlReadOnlyGate.check` (Task 2), `ArangoQueryHandle` (Task 5), `TypeMapper.inferType`/`merge`, `SchemaResolver.resolveUnknown`, `ArangoConfig.getSampleSize()`/`getMixedTypeStrategy()`, `ArangoErrorCode.ARANGODB_QUERY_NOT_READ_ONLY` (Task 1).
- Produces:
  - `public class ArangoQueryFunction implements Provider<ConnectorTableFunction>` with `@Inject ArangoQueryFunction(ArangoClient, TypeMapper, ArangoConfig)`, constants `SCHEMA_NAME = "system"`, `NAME = "query"`, and `public static final String NON_OBJECT_ROW_MESSAGE` (shared with Task 8's `PassthroughCursor`).
  - `public record QueryFunctionHandle(@JsonProperty("tableHandle") ArangoQueryHandle tableHandle) implements ConnectorTableFunctionHandle` — nested in `ArangoQueryFunction`; consumed by Task 7's `applyTableFunction`.
  - Arguments named `DATABASE` and `QUERY`, both `VARCHAR`, return spec `GENERIC_TABLE`.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino.ptf;

import static io.arango.trino.ArangoErrorCode.ARANGODB_QUERY_NOT_READ_ONLY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.airlift.slice.Slices;
import io.arango.trino.ArangoConfig;
import io.arango.trino.ArangoTransactionHandle;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.ptf.ArangoQueryFunction.QueryFunctionHandle;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoQueryFunctionTest {
    private static final String DB = "ptf_test";
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "users");
        client.insertForTest(DB, "users", Map.of("_key", "a", "name", "ann", "age", 36L));
        client.insertForTest(DB, "users", Map.of("_key", "b", "name", "bob", "age", 41L));
    }

    @AfterAll
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private TableFunctionAnalysis analyze(String database, String aql) {
        return analyze(database, aql, new ArangoConfig());
    }

    private TableFunctionAnalysis analyze(String database, String aql, ArangoConfig config) {
        AbstractConnectorTableFunction fn =
                (AbstractConnectorTableFunction)
                        new ArangoQueryFunction(client, new TypeMapper(), config).get();
        // session and access control are unused by the implementation; the engine supplies
        // real ones in the e2e test
        return fn.analyze(
                null,
                ArangoTransactionHandle.INSTANCE,
                Map.of(
                        "DATABASE", new ScalarArgument(VARCHAR, Slices.utf8Slice(database)),
                        "QUERY", new ScalarArgument(VARCHAR, Slices.utf8Slice(aql))),
                null);
    }

    private static List<String> fieldNames(TableFunctionAnalysis analysis) {
        return analysis.getReturnedType().orElseThrow().getFields().stream()
                .map(f -> f.getName().orElseThrow())
                .toList();
    }

    // ---- §4.1: object rows derive a schema exactly like SchemaResolver would ----

    @Test
    void objectRowsDeriveNamedTypedColumns() {
        TableFunctionAnalysis analysis =
                analyze(DB, "FOR d IN users RETURN {name: d.name, age: d.age}");
        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        assertThat(fieldNames(analysis)).containsExactly("name", "age");
        assertThat(descriptor.getFields().get(0).getType()).contains(VarcharType.VARCHAR);
        assertThat(descriptor.getFields().get(1).getType()).contains(BigintType.BIGINT);

        QueryFunctionHandle handle = (QueryFunctionHandle) analysis.getHandle();
        ArangoQueryHandle table = handle.tableHandle();
        assertThat(table.database()).isEqualTo(DB);
        assertThat(table.query()).isEqualTo("FOR d IN users RETURN {name: d.name, age: d.age}");
        assertThat(table.columns())
                .extracting(ArangoColumnHandle::name)
                .containsExactly("name", "age");
        // §4.1: path is List.of(name); the read path extracts by name
        assertThat(table.columns().get(0).path()).containsExactly("name");
        // derived columns are never hidden — a passthrough result has no collection identity
        assertThat(table.columns()).allMatch(c -> !c.hidden());
    }

    @Test
    void systemAttributesAreOrdinaryVisibleColumns() {
        TableFunctionAnalysis analysis = analyze(DB, "FOR d IN users RETURN d");
        assertThat(fieldNames(analysis)).contains("_key", "_id", "_rev", "name", "age");
    }

    @Test
    void nullOnlyFieldResolvesToVarchar() {
        TableFunctionAnalysis analysis =
                analyze(DB, "FOR x IN [{a: null}, {a: null}] RETURN x");
        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        assertThat(descriptor.getFields().get(0).getType()).contains(VarcharType.VARCHAR);
    }

    @Test
    void caseCollidingKeysBothBecomeColumns() {
        TableFunctionAnalysis analysis = analyze(DB, "RETURN {Name: 1, name: 2}");
        assertThat(fieldNames(analysis)).containsExactlyInAnyOrder("Name", "name");
    }

    @Test
    void schemaIsInferredFromThePrefixOnly() {
        // k = 2: the third row's extra field is invisible (§10.3, recorded limitation)
        TableFunctionAnalysis analysis =
                analyze(
                        DB,
                        "FOR x IN [{a: 1}, {a: 2}, {a: 3, b: 4}] RETURN x",
                        new ArangoConfig().setSampleSize(2));
        assertThat(fieldNames(analysis)).containsExactly("a");
    }

    // ---- §4.1: rejections ----

    @Test
    void nonObjectRowsReject() {
        assertThatThrownBy(() -> analyze(DB, "FOR d IN users RETURN d.name"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("RETURN {");
    }

    @Test
    void mixedBatchRejects() {
        assertThatThrownBy(() -> analyze(DB, "FOR x IN [{a: 1}, 42, \"str\", null] RETURN x"))
                .hasMessageContaining("RETURN {");
    }

    @Test
    void emptyResultRejects() {
        assertThatThrownBy(() -> analyze(DB, "FOR d IN users FILTER false RETURN d"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("no rows");
    }

    @Test
    void emptyObjectsOnlyReject() {
        // zero derivable columns is the same zero-column-table problem as zero rows
        assertThatThrownBy(() -> analyze(DB, "RETURN {}")).hasMessageContaining("no columns");
    }

    @Test
    void emptyStringKeyRejects() {
        assertThatThrownBy(() -> analyze(DB, "RETURN {\"\": 1}"))
                .hasMessageContaining("empty-string attribute key");
    }

    @Test
    void nullArgumentRejects() {
        AbstractConnectorTableFunction fn =
                (AbstractConnectorTableFunction)
                        new ArangoQueryFunction(client, new TypeMapper(), new ArangoConfig()).get();
        assertThatThrownBy(
                        () ->
                                fn.analyze(
                                        null,
                                        ArangoTransactionHandle.INSTANCE,
                                        Map.of(
                                                "DATABASE",
                                                        new ScalarArgument(
                                                                VARCHAR, Slices.utf8Slice(DB)),
                                                "QUERY", new ScalarArgument(VARCHAR, null)),
                                        null))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("QUERY");
    }

    // ---- §3/§3.1/§3.3: the gate, and its ordering before firstBatch ----

    @Test
    void insertIsRejectedWithoutExecuting() {
        long before = client.countWithShardIds(DB, "users", List.of());
        assertThatThrownBy(() -> analyze(DB, "INSERT {x: 1} INTO users"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(ARANGODB_QUERY_NOT_READ_ONLY.toErrorCode()))
                .hasMessageContaining("users");
        // §3.1: gate runs to completion BEFORE firstBatch — the count-unchanged assertion is the
        // one that fails if the ordering ever inverts
        assertThat(client.countWithShardIds(DB, "users", List.of())).isEqualTo(before);
    }

    @Test
    void systemCollectionReadRejects() {
        // _graphs exists in every database; reading it plans as an ordinary read (§3.3)
        assertThatThrownBy(() -> analyze(DB, "FOR g IN _graphs RETURN g"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("_graphs");
    }

    // ---- §9: error translation ----

    @Test
    void unknownDatabaseThrowsSchemaNotFound() {
        assertThatThrownBy(() -> analyze("no_such_db", "FOR d IN users RETURN d"))
                .isInstanceOf(SchemaNotFoundException.class);
    }

    @Test
    void unknownCollectionIsAUserError() {
        assertThatThrownBy(() -> analyze(DB, "FOR d IN nope RETURN d"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()));
    }

    @Test
    void syntaxErrorIsAUserErrorCarryingTheServerMessage() {
        assertThatThrownBy(() -> analyze(DB, "THIS IS NOT AQL"))
                .isInstanceOfSatisfying(
                        TrinoException.class,
                        e ->
                                assertThat(e.getErrorCode())
                                        .isEqualTo(INVALID_FUNCTION_ARGUMENT.toErrorCode()))
                .hasMessageContaining("syntax");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ArangoQueryFunctionTest`
Expected: COMPILATION ERROR — `cannot find symbol: ArangoQueryFunction`.

- [ ] **Step 3: Make `SchemaResolver.resolveUnknown` public static**

In `SchemaResolver`, change `private static Type resolveUnknown(Type type)` to `public static Type resolveUnknown(Type type)`. (The passthrough derivation reuses it so the same data infers the same types whether scanned or passed through, §4.1.)

- [ ] **Step 4: Implement `ArangoQueryFunction`**

```java
package io.arango.trino.ptf;

import static io.arango.trino.ArangoErrorCode.ARANGODB_QUERY_NOT_READ_ONLY;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

import com.arangodb.ArangoDBException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * arango.system.query(database, query): raw AQL passthrough (spec, whole document). analyze()
 * order is a correctness invariant (§3.1): explain -> AqlReadOnlyGate -> firstBatch. firstBatch
 * executes the query for real, so it must come strictly after a clean gate verdict.
 */
public class ArangoQueryFunction implements Provider<ConnectorTableFunction> {
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";
    public static final String NON_OBJECT_ROW_MESSAGE =
            "Query returned a non-object row; a table needs named columns. "
                    + "Return an object instead, e.g. RETURN {name: d.name}";

    // ArangoDB error numbers. 1228 = database not found; 1203 = collection or view not found;
    // 1500-1599 = AQL query errors (parse error 1501, missing bind parameter 1551, ...). The
    // 15xx range and 1203 are user errors in a user-supplied query string (§9).
    private static final int ERROR_DATABASE_NOT_FOUND = 1228;
    private static final int ERROR_DATA_SOURCE_NOT_FOUND = 1203;
    private static final int ERROR_QUERY_RANGE_START = 1500;
    private static final int ERROR_QUERY_RANGE_END = 1600;

    private final ArangoClient client;
    private final TypeMapper typeMapper;
    private final ArangoConfig config;

    @Inject
    public ArangoQueryFunction(ArangoClient client, TypeMapper typeMapper, ArangoConfig config) {
        this.client = requireNonNull(client, "client is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new QueryFunction(client, typeMapper, config);
    }

    public static class QueryFunction extends AbstractConnectorTableFunction {
        private final ArangoClient client;
        private final TypeMapper typeMapper;
        private final ArangoConfig config;

        QueryFunction(ArangoClient client, TypeMapper typeMapper, ArangoConfig config) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("DATABASE")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("QUERY")
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
            this.client = client;
            this.typeMapper = typeMapper;
            this.config = config;
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {
            String database = stringArgument(arguments, "DATABASE");
            String aql = stringArgument(arguments, "QUERY");

            // §3.1 ordering: explain -> gate -> firstBatch. Transposing gate and firstBatch
            // would execute a query the gate is about to reject.
            Map<String, Object> explain =
                    translate(database, () -> client.explainPlan(database, aql));
            AqlReadOnlyGate.check(explain)
                    .ifPresent(
                            rejection -> {
                                throw reject(rejection);
                            });
            List<Object> rows =
                    translate(
                            database,
                            () -> client.firstBatch(database, aql, config.getSampleSize()));
            List<ArangoColumnHandle> columns = deriveColumns(rows);

            Descriptor returnedType =
                    new Descriptor(
                            columns.stream()
                                    .map(
                                            c ->
                                                    new Descriptor.Field(
                                                            c.name(), Optional.of(c.type())))
                                    .collect(ImmutableList.toImmutableList()));
            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(
                            new QueryFunctionHandle(
                                    new ArangoQueryHandle(database, aql, columns)))
                    .build();
        }

        // §4.1: field union across the batch, per-field types via TypeMapper.merge — identical
        // inference to SchemaResolver, so the same data types the same whether scanned or
        // passed through. No hidden columns: a passthrough result has no collection identity.
        private List<ArangoColumnHandle> deriveColumns(List<Object> rows) {
            if (rows.isEmpty()) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Query returned no rows at planning time, so no schema can be derived; "
                                + "a passthrough query must return at least one row");
            }
            LinkedHashMap<String, Type> fields = new LinkedHashMap<>();
            for (Object row : rows) {
                if (!(row instanceof Map<?, ?> document)) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, NON_OBJECT_ROW_MESSAGE);
                }
                for (Map.Entry<?, ?> entry : document.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    if (key.isEmpty()) {
                        // Descriptor.Field would throw an engine-internal IllegalArgumentException
                        // on an empty name; make it a user error instead (§4.1)
                        throw new TrinoException(
                                INVALID_FUNCTION_ARGUMENT,
                                "Query returned an object with an empty-string attribute key,"
                                        + " which cannot become a column name");
                    }
                    Type inferred = typeMapper.inferType(entry.getValue());
                    fields.merge(
                            key,
                            inferred,
                            (a, b) -> typeMapper.merge(a, b, config.getMixedTypeStrategy()));
                }
            }
            if (fields.isEmpty()) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Query returned only empty objects, so no columns can be derived");
            }
            return fields.entrySet().stream()
                    .map(
                            e ->
                                    new ArangoColumnHandle(
                                            e.getKey(),
                                            SchemaResolver.resolveUnknown(e.getValue()),
                                            false,
                                            List.of(e.getKey())))
                    .collect(ImmutableList.toImmutableList());
        }

        private static String stringArgument(Map<String, Argument> arguments, String name) {
            if (!(arguments.get(name) instanceof ScalarArgument scalar)
                    || scalar.getValue() == null) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT, name + " argument is required");
            }
            return ((Slice) scalar.getValue()).toStringUtf8();
        }

        private static TrinoException reject(AqlReadOnlyGate.Rejection rejection) {
            return switch (rejection.kind()) {
                case NOT_READ_ONLY ->
                        new TrinoException(
                                ARANGODB_QUERY_NOT_READ_ONLY,
                                "Only read-only AQL can be passed through: "
                                        + rejection.reason());
                case SYSTEM_COLLECTION ->
                        new TrinoException(
                                INVALID_FUNCTION_ARGUMENT,
                                "Query reads "
                                        + rejection.reason()
                                        + ", which this connector does not expose");
            };
        }

        // §9 classification. 1228 -> SchemaNotFoundException (the missing thing is a database;
        // TableNotFoundException would render as "Table 'db.query' does not exist", which is
        // misleading). 1203 and the 15xx AQL range -> user error carrying the server message.
        // Anything else -> GENERIC_INTERNAL_ERROR, matching ArangoMetadata's translation rule.
        private static <T> T translate(String database, Supplier<T> call) {
            try {
                return call.get();
            } catch (ArangoDBException e) {
                Integer errorNum = e.getErrorNum();
                if (errorNum != null && errorNum == ERROR_DATABASE_NOT_FOUND) {
                    throw new SchemaNotFoundException(database);
                }
                if (errorNum != null
                        && (errorNum == ERROR_DATA_SOURCE_NOT_FOUND
                                || (errorNum >= ERROR_QUERY_RANGE_START
                                        && errorNum < ERROR_QUERY_RANGE_END))) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
                }
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR, "ArangoDB request failed: " + e.getMessage(), e);
            }
        }
    }

    public record QueryFunctionHandle(@JsonProperty("tableHandle") ArangoQueryHandle tableHandle)
            implements ConnectorTableFunctionHandle {
        @JsonCreator
        public QueryFunctionHandle {
            requireNonNull(tableHandle, "tableHandle is null");
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=ArangoQueryFunctionTest`
Expected: PASS (16 tests). Two places measured behavior could deviate — fix the *test* only if the observed value is defensible: (a) `syntaxErrorIsAUserErrorCarryingTheServerMessage` assumes the server message contains "syntax"; (b) `unknownCollectionIsAUserError` assumes explain raises 1203 for an unknown collection.

- [ ] **Step 6: Add the `QueryFunctionHandle` round-trip (spec §11 — both handles cross the coordinator/worker boundary)**

In `ArangoQueryHandleTest`, add:

```java
    @Test
    void queryFunctionHandleRoundTripsThroughJson() {
        JsonCodec<ArangoQueryFunction.QueryFunctionHandle> codec =
                codecFactory().jsonCodec(ArangoQueryFunction.QueryFunctionHandle.class);
        ArangoQueryFunction.QueryFunctionHandle handle =
                new ArangoQueryFunction.QueryFunctionHandle(sample());
        assertThat(codec.fromJson(codec.toJson(handle))).isEqualTo(handle);
    }
```

with import `io.arango.trino.ptf.ArangoQueryFunction`.

- [ ] **Step 7: Run both test classes**

Run: `mvn -q test -Dtest='ArangoQueryFunctionTest,ArangoQueryHandleTest'`
Expected: PASS.

- [ ] **Step 8: Static-analysis gates on the new/modified main files**

```bash
mvn checkstyle:check && mvn compile spotbugs:check
```

Expected: pass (`ArangoQueryFunction.java` and the `SchemaResolver` edit are not grandfathered). Fix findings now, not in Task 12.

- [ ] **Step 9: Commit**

```bash
git add src/main/java/io/arango/trino/ptf/ArangoQueryFunction.java src/main/java/io/arango/trino/schema/SchemaResolver.java src/test/java/io/arango/trino/ptf/ArangoQueryFunctionTest.java src/test/java/io/arango/trino/handle/ArangoQueryHandleTest.java
git commit -m "feat: arango.system.query PTF — gate-ordered analyze, first-batch schema derivation"
```

---

## Task 7: `ArangoMetadata` — applyTableFunction + handle dispatch + four declines

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java`
- Test: `src/test/java/io/arango/trino/ArangoMetadataPassthroughTest.java`

**Interfaces:**
- Consumes: `QueryFunctionHandle` (Task 6), `ArangoQueryHandle` (Task 5).
- Produces: `applyTableFunction` returning `TableFunctionApplicationResult`; `getTableMetadata`/`getColumnHandles` handling `ArangoQueryHandle`; `applyFilter`/`applyLimit`/`applyProjection`/`applyAggregation` each declining an `ArangoQueryHandle` **before their existing `(ArangoTableHandle)` cast**.

- [ ] **Step 1: Write the failing test**

```java
package io.arango.trino;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.ptf.ArangoQueryFunction.QueryFunctionHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** All four pushdown hooks decline a passthrough (spec §6) — each row here is the test for the
 * failure family that produced M5's TupleDomain.none() and N-splits findings. No container: the
 * dispatch must return before any client call. */
class ArangoMetadataPassthroughTest {
    private static final ArangoColumnHandle NAME =
            new ArangoColumnHandle("name", VarcharType.VARCHAR, false, List.of("name"));
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BigintType.BIGINT, false, List.of("age"));

    private static ArangoQueryHandle queryHandle() {
        return new ArangoQueryHandle(
                "shop", "FOR d IN users RETURN {name: d.name, age: d.age}", List.of(NAME, AGE));
    }

    private static ArangoMetadata metadata() {
        // the builder does not connect; any call that reaches ArangoDB would throw
        ArangoClient client = new ArangoClient(new ArangoConfig());
        return new ArangoMetadata(
                client,
                new SchemaResolver(client, new TypeMapper(), new ArangoConfig()),
                new ArangoConfig());
    }

    @Test
    void applyFilterDeclines() {
        Constraint constraint =
                new Constraint(
                        TupleDomain.withColumnDomains(
                                Map.of(AGE, Domain.singleValue(BigintType.BIGINT, 36L))));
        assertThat(metadata().applyFilter(null, queryHandle(), constraint)).isEmpty();
    }

    @Test
    void applyLimitDeclines() {
        assertThat(metadata().applyLimit(null, queryHandle(), 5)).isEmpty();
    }

    @Test
    void applyProjectionDeclines() {
        assertThat(metadata().applyProjection(null, queryHandle(), List.of(), Map.of())).isEmpty();
    }

    @Test
    void applyAggregationDeclines() {
        assertThat(
                        metadata()
                                .applyAggregation(
                                        null, queryHandle(), List.of(), Map.of(),
                                        List.of(List.of())))
                .isEmpty();
    }

    @Test
    void applyTableFunctionUnwrapsTheHandleAndItsColumns() {
        Optional<TableFunctionApplicationResult<io.trino.spi.connector.ConnectorTableHandle>>
                result = metadata().applyTableFunction(null, new QueryFunctionHandle(queryHandle()));
        assertThat(result).isPresent();
        assertThat(result.get().getTableHandle()).isEqualTo(queryHandle());
        assertThat(result.get().getColumnHandles()).containsExactly(NAME, AGE);
    }

    @Test
    void applyTableFunctionIgnoresForeignHandles() {
        assertThat(metadata().applyTableFunction(null, new ConnectorTableFunctionHandle() {}))
                .isEmpty();
    }

    @Test
    void getTableMetadataSynthesizesTheQueryTableName() {
        ConnectorTableMetadata tableMetadata = metadata().getTableMetadata(null, queryHandle());
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("shop", "query"));
        assertThat(tableMetadata.getColumns())
                .extracting(c -> c.getName())
                .containsExactly("name", "age");
    }

    @Test
    void getColumnHandlesServesTheDerivedColumns() {
        Map<String, ColumnHandle> handles = metadata().getColumnHandles(null, queryHandle());
        assertThat(handles).containsExactly(Map.entry("name", NAME), Map.entry("age", AGE));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ArangoMetadataPassthroughTest`
Expected: FAIL. The test class compiles (`applyTableFunction` exists as a `ConnectorMetadata` default returning `Optional.empty()`), so the observed failures are: the four hook tests die with `ClassCastException: ArangoQueryHandle cannot be cast to ArangoTableHandle`, `applyTableFunctionUnwrapsTheHandleAndItsColumns` fails on the empty default, and the `getTableMetadata`/`getColumnHandles` tests die with the same `ClassCastException`.

- [ ] **Step 3: Implement**

In `ArangoMetadata` (new imports: `io.arango.trino.handle.ArangoQueryHandle`, `io.arango.trino.ptf.ArangoQueryFunction.QueryFunctionHandle`, `io.trino.spi.function.table.ConnectorTableFunctionHandle` — note the `function.table` package, NOT `connector`; the existing `io.trino.spi.connector.*` wildcard already covers `TableFunctionApplicationResult`, which genuinely does live in `connector`):

At the top of `getTableMetadata`:

```java
        if (table instanceof ArangoQueryHandle queryHandle) {
            // a passthrough has no collection identity; the synthesized name renders in EXPLAIN
            // (spec §5.2 — PlanPrinter reaches here through the getTableName default chain)
            return new ConnectorTableMetadata(
                    queryHandle.schemaTableName(),
                    queryHandle.columns().stream()
                            .map(ArangoColumnHandle::toColumnMetadata)
                            .collect(ImmutableList.toImmutableList()));
        }
```

At the top of `getColumnHandles`:

```java
        if (table instanceof ArangoQueryHandle queryHandle) {
            ImmutableMap.Builder<String, ColumnHandle> derived = ImmutableMap.builder();
            for (ArangoColumnHandle column : queryHandle.columns()) {
                derived.put(column.name(), column);
            }
            return derived.buildOrThrow();
        }
```

At the top of `applyFilter`, `applyLimit`, `applyProjection`, and `applyAggregation` (all four, before anything touches `table`):

```java
        // Opaque user AQL: nothing can be pushed into it (spec §6). Must precede the
        // ArangoTableHandle cast.
        if (table instanceof ArangoQueryHandle) {
            return Optional.empty();
        }
```

New method (anywhere after `applyAggregation`):

```java
    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
            ConnectorSession session, ConnectorTableFunctionHandle handle) {
        if (!(handle instanceof QueryFunctionHandle queryFunctionHandle)) {
            return Optional.empty();
        }
        ArangoQueryHandle table = queryFunctionHandle.tableHandle();
        return Optional.of(
                new TableFunctionApplicationResult<>(
                        table, ImmutableList.copyOf(table.columns())));
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ArangoMetadataPassthroughTest`
Expected: PASS (8 tests).

- [ ] **Step 5: Run the neighboring metadata suites to prove no regression on the collection path**

Run: `mvn -q test -Dtest='ArangoMetadataTest,ArangoMetadataLimitTest,ArangoMetadataAggregationTest'`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoMetadata.java src/test/java/io/arango/trino/ArangoMetadataPassthroughTest.java
git commit -m "feat: metadata dispatch for passthrough handles — applyTableFunction, synthesized name, four declines"
```

---

## Task 8: Split manager short-circuit + `PassthroughCursor` + page-source dispatch

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoSplitManager.java`
- Create: `src/main/java/io/arango/trino/ptf/PassthroughCursor.java`
- Modify: `src/main/java/io/arango/trino/ArangoPageSourceProvider.java`
- Test: `src/test/java/io/arango/trino/ArangoSplitManagerTest.java` (add one test)
- Test: `src/test/java/io/arango/trino/ptf/ArangoQueryFunctionTest.java` (add two execution-path tests)

**Interfaces:**
- Consumes: `ArangoQueryHandle`, `ArangoClient.queryPassthrough` (Task 3), `ArangoQueryFunction.NON_OBJECT_ROW_MESSAGE` (Task 6).
- Produces: `getSplits` on a passthrough → exactly one empty-shard split, **before any shard discovery**; `createPageSource` on a passthrough → `ArangoPageSource` over `new PassthroughCursor(client.queryPassthrough(...))`. `ArangoPageSource` itself stays untouched.

- [ ] **Step 1: Write the failing split-manager test**

Add to `ArangoSplitManagerTest`:

```java
    @Test
    void passthroughHandleShortCircuitsBeforeShardDiscovery() {
        // a client whose discovery methods all blow up: the short-circuit must come first (§6)
        ArangoClient noDiscovery =
                new ArangoClient(new ArangoConfig()) {
                    @Override
                    public ShardingInfo getShardingInfo(String database, String collection) {
                        throw new AssertionError("shard discovery must not run for a passthrough");
                    }

                    @Override
                    public List<String> listShardIds(String database, String collection) {
                        throw new AssertionError("shard discovery must not run for a passthrough");
                    }
                };
        ArangoSplitManager mgr =
                new ArangoSplitManager(
                        noDiscovery, new ArangoConfig(), new ShardFanoutCapability(noDiscovery));
        io.arango.trino.handle.ArangoQueryHandle handle =
                new io.arango.trino.handle.ArangoQueryHandle(
                        DB,
                        "FOR d IN docs RETURN {v: d.v}",
                        List.of(
                                new io.arango.trino.handle.ArangoColumnHandle(
                                        "v", BigintType.BIGINT, false, List.of("v"))));
        ConnectorSplitSource source =
                mgr.getSplits(null, null, handle, Set.of(), Constraint.alwaysTrue());
        List<ArangoSplit> splits =
                source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).getNow(null).stream()
                        .map(ArangoSplit.class::cast)
                        .toList();
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -q test -Dtest=ArangoSplitManagerTest`
Expected: FAIL — `ClassCastException` (or the AssertionError) out of `getSplits`.

- [ ] **Step 3: Implement the short-circuit**

In `ArangoSplitManager.getSplits`, before the cast (new import `io.arango.trino.handle.ArangoQueryHandle`):

```java
        // A passthrough is opaque AQL: there is no collection to enumerate shards for, no way
        // to rewrite it per shard, and Trino treats its output as final — N splits would emit
        // N duplicate result sets (spec §6). Checked before any discovery round trip.
        if (table instanceof ArangoQueryHandle) {
            return new FixedSplitSource(List.of(SINGLE));
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `mvn -q test -Dtest=ArangoSplitManagerTest`
Expected: PASS.

- [ ] **Step 5: Write the failing execution-path tests**

Add to `ArangoQueryFunctionTest` (new imports: `io.arango.trino.ArangoPageSourceProvider`, `io.arango.trino.aql.AqlBuilder`, `io.arango.trino.handle.ArangoSplit`, `io.trino.spi.connector.ConnectorPageSource`, `io.trino.spi.connector.SourcePage`, `java.util.Optional`; `io.trino.spi.type.BigintType` already present):

```java
    private ConnectorPageSource passthroughPageSource(ArangoQueryHandle handle) {
        return new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig())
                .createPageSource(
                        null,
                        null,
                        new ArangoSplit(List.of()),
                        handle,
                        Optional.empty(),
                        List.copyOf(handle.columns()),
                        null);
    }

    @Test
    void executionRunsTheStoredQueryVerbatim() {
        ArangoQueryHandle handle =
                new ArangoQueryHandle(
                        DB,
                        "FOR d IN users SORT d.age RETURN {age: d.age}",
                        List.of(
                                new ArangoColumnHandle(
                                        "age", BigintType.BIGINT, false, List.of("age"))));
        ConnectorPageSource source = passthroughPageSource(handle);
        try {
            SourcePage page = source.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(2);
            assertThat(BigintType.BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(36L);
        } finally {
            source.close();
        }
    }

    @Test
    void executionTimeNonObjectRowIsAUserErrorWithTheSameGuidance() {
        // derivation saw only objects; execution hits a later scalar (§4.1 last row / §9):
        // PassthroughCursor converts what would be a driver deserialization failure into the
        // same INVALID_FUNCTION_ARGUMENT message analyze() uses
        ArangoQueryHandle handle =
                new ArangoQueryHandle(
                        DB,
                        "FOR x IN [{a: 1}, {a: 2}, \"oops\"] RETURN x",
                        List.of(
                                new ArangoColumnHandle(
                                        "a", BigintType.BIGINT, false, List.of("a"))));
        ConnectorPageSource source = passthroughPageSource(handle);
        try {
            assertThatThrownBy(
                            () -> {
                                while (!source.isFinished()) {
                                    source.getNextSourcePage();
                                }
                            })
                    .isInstanceOf(TrinoException.class)
                    .hasMessageContaining("RETURN {");
        } finally {
            source.close();
        }
    }
```

- [ ] **Step 6: Run to verify they fail**

Run: `mvn -q test -Dtest=ArangoQueryFunctionTest`
Expected: the two new tests FAIL — `ClassCastException: ArangoQueryHandle cannot be cast to ArangoTableHandle` in `createPageSource`.

- [ ] **Step 7: Implement `PassthroughCursor`**

```java
package io.arango.trino.ptf;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoIterator;
import com.arangodb.entity.CursorStats;
import com.arangodb.entity.CursorWarning;
import io.trino.spi.TrinoException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Adapts the Object-typed passthrough cursor to the Map-typed one ArangoPageSource consumes
 * (which stays untouched, spec §8). A row that is not a JSON object surfaces as the same
 * INVALID_FUNCTION_ARGUMENT guidance analyze() gives (§4.1 last row / §9) — deterministically,
 * instead of as a driver deserialization failure whose shape we would have to sniff.
 */
@SuppressWarnings("rawtypes")
public final class PassthroughCursor implements ArangoCursor<Map> {
    private final ArangoCursor<Object> delegate;

    public PassthroughCursor(ArangoCursor<Object> delegate) {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Map next() {
        Object row = delegate.next();
        if (!(row instanceof Map<?, ?> document)) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT, ArangoQueryFunction.NON_OBJECT_ROW_MESSAGE);
        }
        return document;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public ArangoIterator<Map> iterator() {
        return this;
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public Class<Map> getType() {
        return Map.class;
    }

    @Override
    public Integer getCount() {
        return delegate.getCount();
    }

    @Override
    public CursorStats getStats() {
        return delegate.getStats();
    }

    @Override
    public Collection<CursorWarning> getWarnings() {
        return delegate.getWarnings();
    }

    @Override
    public boolean isCached() {
        return delegate.isCached();
    }

    @Override
    public boolean isPotentialDirtyRead() {
        return delegate.isPotentialDirtyRead();
    }

    @Override
    public String getNextBatchId() {
        return delegate.getNextBatchId();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
```

(If the compiler reports further unimplemented `ArangoCursor` methods, delegate each the same way — the interface was enumerated via `javap` and the list above matches `core-7.13.0`. If Step 10's SpotBugs gate flags `IT_NO_SUCH_ELEMENT` on `next()` — an `Iterator.next()` that seemingly cannot throw `NoSuchElementException` — the delegate's `next()` is what throws it; satisfy the rule with an explicit `if (!delegate.hasNext()) { throw new NoSuchElementException(); }` guard at the top of `next()` plus the `java.util.NoSuchElementException` import, rather than a suppression.)

- [ ] **Step 8: Implement the provider dispatch**

In `ArangoPageSourceProvider.createPageSource`, before the existing casts (new imports: `io.arango.trino.handle.ArangoQueryHandle`, `io.arango.trino.ptf.PassthroughCursor`):

```java
        if (table instanceof ArangoQueryHandle queryHandle) {
            // stored query verbatim — no AqlBuilder, no bind vars, no shard restriction (§5.1)
            List<ArangoColumnHandle> passthroughColumns =
                    columns.stream().map(ArangoColumnHandle.class::cast).toList();
            return new ArangoPageSource(
                    new PassthroughCursor(
                            client.queryPassthrough(queryHandle.database(), queryHandle.query())),
                    passthroughColumns,
                    config.getTypeCoercion());
        }
```

- [ ] **Step 9: Run to verify they pass**

Run: `mvn -q test -Dtest='ArangoQueryFunctionTest,ArangoSplitManagerTest'`
Expected: PASS.

- [ ] **Step 10: Static-analysis gates on the new/modified main files**

```bash
mvn checkstyle:check && mvn compile spotbugs:check
```

Expected: pass (`PassthroughCursor.java` and the split-manager/provider edits are not grandfathered). Fix findings now — see the `IT_NO_SUCH_ELEMENT` note above.

- [ ] **Step 11: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoSplitManager.java src/main/java/io/arango/trino/ptf/PassthroughCursor.java src/main/java/io/arango/trino/ArangoPageSourceProvider.java src/test/java/io/arango/trino/ArangoSplitManagerTest.java src/test/java/io/arango/trino/ptf/ArangoQueryFunctionTest.java
git commit -m "feat: passthrough execution — single split, verbatim query, object-row enforcement"
```

---

## Task 9: Module refactor + `Connector.getTableFunctions`

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoModule.java`
- Modify: `src/main/java/io/arango/trino/ArangoConnector.java`

**Interfaces:**
- Consumes: `ArangoConfig.isQueryFunctionEnabled` (Task 1), `ArangoQueryFunction` (Task 6).
- Produces: `ArangoConnector.getTableFunctions()` returning the injected `Set<ConnectorTableFunction>` — empty when the flag is off (disabled = unregistered, §7). `ArangoConnectorFactory` needs no change: Airlift `Bootstrap` handles `ConfigurationAwareModule`.

- [ ] **Step 1: Refactor `ArangoModule` to `AbstractConfigurationAwareModule`** (§8.2 — a plain `Module` cannot read config at `configure()` time)

```java
package io.arango.trino;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.ptf.ArangoQueryFunction;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.function.table.ConnectorTableFunction;

public class ArangoModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(ArangoConfig.class);
        binder.bind(ArangoClient.class).in(Scopes.SINGLETON);
        binder.bind(TypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(SchemaResolver.class).in(Scopes.SINGLETON);
        binder.bind(AqlBuilder.class).in(Scopes.SINGLETON);
        binder.bind(ArangoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(io.arango.trino.split.ShardFanoutCapability.class).in(Scopes.SINGLETON);
        binder.bind(ArangoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArangoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ArangoConnector.class).in(Scopes.SINGLETON);
        // The set must exist even when the flag is off: ArangoConnector injects it
        // unconditionally, and disabled-means-unregistered (spec §7) is an EMPTY set, not a
        // missing binding.
        newSetBinder(binder, ConnectorTableFunction.class);
        install(
                conditionalModule(
                        ArangoConfig.class,
                        ArangoConfig::isQueryFunctionEnabled,
                        inner ->
                                newSetBinder(inner, ConnectorTableFunction.class)
                                        .addBinding()
                                        .toProvider(ArangoQueryFunction.class)
                                        .in(Scopes.SINGLETON)));
    }
}
```

- [ ] **Step 2: Wire `ArangoConnector.getTableFunctions`**

```java
    private final Set<ConnectorTableFunction> tableFunctions;
```

constructor gains a parameter (imports `java.util.Set`, `io.trino.spi.function.table.ConnectorTableFunction`):

```java
    @Inject
    public ArangoConnector(
            LifeCycleManager lifeCycleManager,
            ArangoMetadata metadata,
            ArangoSplitManager splitManager,
            ArangoPageSourceProvider pageSourceProvider,
            Set<ConnectorTableFunction> tableFunctions) {
        this.lifeCycleManager = lifeCycleManager;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.tableFunctions = Set.copyOf(tableFunctions);
    }
```

and the override:

```java
    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return tableFunctions;
    }
```

- [ ] **Step 3: Verify the whole existing suite still boots the connector**

Run: `mvn -q test 2>&1 | tail -15`
Expected: BUILD SUCCESS — `ArangoConnectorQueryTest`/`ArangoConnectorPushdownTest`/`ArangoConnectorAggregationTest` all boot through `ArangoConnectorFactory` → `Bootstrap` → the refactored module, so a Guice wiring mistake fails loudly here.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoModule.java src/main/java/io/arango/trino/ArangoConnector.java
git commit -m "feat: conditional PTF registration — config-aware module, Connector.getTableFunctions"
```

---

## Task 10: End-to-end `ArangoConnectorQueryFunctionTest`

**Files:**
- Test: `src/test/java/io/arango/trino/ArangoConnectorQueryFunctionTest.java`

**Interfaces:**
- Consumes: everything above, through a real `DistributedQueryRunner` (which serializes both handles across the coordinator/worker boundary — the round-trip Task 5/6 tested in isolation).
- Produces: nothing further; this is spec §11's e2e row.

- [ ] **Step 1: Write the test**

```java
package io.arango.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingSession.testSessionBuilder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoConnectorQueryFunctionTest {
    private TestingArangoServer server;
    private QueryRunner queryRunner;

    @BeforeAll
    void setup() throws Exception {
        server = new TestingArangoServer();
        try (ArangoClient seed =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()))) {
            seed.createDatabaseForTest("shop");
            seed.createDocumentCollectionForTest("shop", "users");
            seed.insertForTest("shop", "users", Map.of("_key", "ada", "name", "ada", "age", 36L));
            seed.insertForTest("shop", "users", Map.of("_key", "bob", "name", "bob", "age", 41L));
            seed.createEdgeCollectionForTest("shop", "follows");
            seed.insertForTest(
                    "shop", "follows", Map.of("_from", "users/ada", "_to", "users/bob"));
        }

        queryRunner =
                DistributedQueryRunner.builder(
                                testSessionBuilder().setCatalog("arango").setSchema("shop").build())
                        .build();
        queryRunner.installPlugin(new ArangoPlugin());
        queryRunner.createCatalog(
                "arango",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts", server.hostPort(),
                        "arangodb.user", "root",
                        "arangodb.password", server.rootPassword()));
        // kill switch: same server, function unregistered (§7)
        queryRunner.createCatalog(
                "arango_off",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts", server.hostPort(),
                        "arangodb.user", "root",
                        "arangodb.password", server.rootPassword(),
                        "arangodb.query-function-enabled", "false"));
        // small planning sample: lets a test place a pathological row BEYOND the derivation batch
        queryRunner.createCatalog(
                "arango_k2",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts", server.hostPort(),
                        "arangodb.user", "root",
                        "arangodb.password", server.rootPassword(),
                        "arangodb.schema.sample-size", "2"));
    }

    @AfterAll
    void teardown() {
        if (queryRunner != null) queryRunner.close();
        if (server != null) server.close();
    }

    private long userCount() {
        return (long)
                queryRunner
                        .execute("SELECT count(*) FROM arango.shop.users")
                        .getOnlyValue();
    }

    @Test
    void traversalThroughQueryFunctionReturnsCorrectRows() {
        MaterializedResult r =
                queryRunner.execute(
                        "SELECT name FROM TABLE(arango.system.query("
                                + "database => 'shop', "
                                + "query => 'FOR v IN 1..1 OUTBOUND \"users/ada\" follows RETURN {name: v.name}'))");
        assertThat(r.getOnlyColumnAsSet()).containsExactly("bob");
    }

    @Test
    void projectionOverPassthroughWorks() {
        MaterializedResult r =
                queryRunner.execute(
                        "SELECT age FROM TABLE(arango.system.query("
                                + "database => 'shop', "
                                + "query => 'FOR d IN users RETURN {name: d.name, age: d.age}')) "
                                + "ORDER BY age");
        assertThat(r.getOnlyColumn()).containsExactly(36L, 41L);
    }

    @Test
    void trinoAggregationOverPassthroughIsCorrect() {
        // applyAggregation declines (§6); Trino computes it — and exactly once, proving the
        // single-split rule (a second split would double the count)
        MaterializedResult r =
                queryRunner.execute(
                        "SELECT count(*) FROM TABLE(arango.system.query("
                                + "database => 'shop', "
                                + "query => 'FOR d IN users RETURN {name: d.name}'))");
        assertThat(r.getOnlyValue()).isEqualTo(2L);
    }

    @Test
    void insertIsRejectedAndDidNotExecute() {
        long before = userCount();
        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango.system.query("
                                                + "database => 'shop', "
                                                + "query => 'INSERT {x: 1} INTO users'))"))
                .hasMessageContaining("read-only");
        // §3.1: the assertion that fails if gate/firstBatch ordering ever inverts
        assertThat(userCount()).isEqualTo(before);
    }

    @Test
    void explainOverPassthroughSucceeds() {
        // the ONLY caller of the getTableMetadata path for ArangoQueryHandle (§5.2)
        MaterializedResult r =
                queryRunner.execute(
                        "EXPLAIN SELECT * FROM TABLE(arango.system.query("
                                + "database => 'shop', "
                                + "query => 'FOR d IN users RETURN {name: d.name}'))");
        assertThat(r.getRowCount()).isGreaterThan(0);
    }

    @Test
    void disabledFlagUnregistersTheFunction() {
        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango_off.system.query("
                                                + "database => 'shop', "
                                                + "query => 'FOR d IN users RETURN {name: d.name}'))"))
                .hasMessageContaining("not registered");
    }

    @Test
    void nonObjectRowsAtPlanningAreAUserError() {
        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango.system.query("
                                                + "database => 'shop', "
                                                + "query => 'FOR d IN users RETURN d.name'))"))
                .hasMessageContaining("RETURN {");
    }

    @Test
    void lateNonObjectRowAtExecutionIsAUserError() {
        // derivation batch (k=2) sees only objects; execution hits the scalar
        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango_k2.system.query("
                                                + "database => 'shop', "
                                                + "query => 'FOR x IN [{a: 1}, {a: 2}, \"oops\"] RETURN x'))"))
                .hasMessageContaining("RETURN {");
    }

    @Test
    void emptyResultIsAUserError() {
        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango.system.query("
                                                + "database => 'shop', "
                                                + "query => 'FOR d IN users FILTER false RETURN d'))"))
                .hasMessageContaining("no rows");
    }
}
```

- [ ] **Step 2: Run it**

Run: `mvn -q test -Dtest=ArangoConnectorQueryFunctionTest`
Expected: PASS (9 tests). One assertion depends on engine wording: `disabledFlagUnregistersTheFunction` expects Trino's "Table function ... not registered" message — if the engine phrases it differently, pin the observed message (the *behavior* under test is that the function does not exist for that catalog).

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/arango/trino/ArangoConnectorQueryFunctionTest.java
git commit -m "test: e2e query() — traversal, gate ordering, EXPLAIN, kill switch, pathological rows"
```

---

## Task 11: `PassthroughClusterIT`

**Files:**
- Test: `src/test/java/io/arango/trino/PassthroughClusterIT.java`

**Interfaces:**
- Consumes: `SharedArangoClusterExtension` / `TestingArangoCluster` (existing; `cluster.config()` yields a connectable `ArangoConfig`), everything above.
- Produces: nothing — spec §11's cluster row: every §3 measurement was single-server; this converts the coordinator-distributed-plan inference into a measurement, and proves the `WITH` traversal (the case that killed §7's wrapper) end-to-end.

- [ ] **Step 1: Write the test**

```java
package io.arango.trino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.ptf.AqlReadOnlyGate;
import io.arango.trino.ptf.AqlReadOnlyGate.Kind;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

// @Tag("cluster"): excluded from the default failsafe run; run via `mvn verify -Pcluster-its`
// in the separate nightly CI job. See pom.xml it.excludedGroups and .github/workflows/ci.yml.
@Tag("cluster")
@ExtendWith(SharedArangoClusterExtension.class)
class PassthroughClusterIT {
    private static final String DB = "ptf_it";
    private static TestingArangoCluster cluster;
    private static ArangoClient client;
    private static QueryRunner queryRunner;

    @BeforeAll
    static void setup() throws Exception {
        cluster = SharedArangoClusterExtension.cluster();
        client = new ArangoClient(cluster.config());
        client.createDatabaseForTest(DB);
        client.createShardedCollectionForTest(DB, "users", 3);
        client.insertForTest(DB, "users", Map.of("_key", "a", "name", "ann"));
        client.insertForTest(DB, "users", Map.of("_key", "b", "name", "bob"));
        client.createEdgeCollectionForTest(DB, "follows");
        client.insertForTest(DB, "follows", Map.of("_from", "users/a", "_to", "users/b"));
        client.createGraphForTest(DB, "social", "follows", "users");

        ArangoConfig cfg = cluster.config();
        queryRunner =
                DistributedQueryRunner.builder(
                                io.trino.testing.TestingSession.testSessionBuilder()
                                        .setCatalog("arango")
                                        .setSchema(DB)
                                        .build())
                        .build();
        queryRunner.installPlugin(new ArangoPlugin());
        queryRunner.createCatalog(
                "arango",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts", cfg.getHosts(),
                        "arangodb.user", cfg.getUser(),
                        "arangodb.password", cfg.getPassword()));
    }

    @AfterAll
    static void teardown() {
        if (queryRunner != null) queryRunner.close();
        if (client != null) client.close();
        // Do NOT close the shared cluster here: SharedArangoClusterExtension stops it once at
        // the end of the test plan (see the other cluster ITs).
    }

    private static Optional<AqlReadOnlyGate.Rejection> verdict(String aql) {
        return AqlReadOnlyGate.check(client.explainPlan(DB, aql));
    }

    private long userCount() {
        return client.countWithShardIds(DB, "users", List.of());
    }

    // ---- the gate against a coordinator's DISTRIBUTED plan (§11: single-server measurements
    // do not automatically generalize to plans with scatter/gather nodes) ----

    @Test
    void coordinatorPlanStillTypesReadsAsRead() {
        assertThat(verdict("FOR d IN users RETURN d")).isEmpty();
        assertThat(verdict("WITH users FOR v IN 1..1 OUTBOUND \"users/a\" follows RETURN v"))
                .isEmpty();
    }

    @Test
    void namedGraphTraversalAdmitsOnCoordinator() {
        // §3's named-graph row (follows:read, users:read, and crucially NO _graphs entry) is a
        // single-server measurement; if a coordinator's distributed plan surfaced _graphs, the
        // gate's _-prefix rule would reject the motivating use case — measure it, don't infer
        assertThat(
                        verdict(
                                "WITH users FOR v IN 1..1 OUTBOUND \"users/a\" GRAPH \"social\" RETURN v"))
                .isEmpty();
    }

    @Test
    void coordinatorPlanStillTypesWritesAsWrite() {
        Optional<AqlReadOnlyGate.Rejection> v = verdict("FOR d IN users UPDATE d WITH {x: 1} IN users");
        assertThat(v).isPresent();
        assertThat(v.get().kind()).isEqualTo(Kind.NOT_READ_ONLY);
    }

    @Test
    void singleDocumentWriteOptimizationStillTypesAsWrite() {
        // the optimize-cluster-single-document-operations rule rewrites this plan shape (§11)
        Optional<AqlReadOnlyGate.Rejection> v = verdict("INSERT {_key: \"z\"} INTO users");
        assertThat(v).isPresent();
        assertThat(v.get().kind()).isEqualTo(Kind.NOT_READ_ONLY);
    }

    // ---- the motivating use case, end-to-end: a WITH-declared cluster traversal — the exact
    // query §7's subquery wrapper could not express ----

    @Test
    void withTraversalRunsEndToEnd() {
        MaterializedResult r =
                queryRunner.execute(
                        "SELECT name FROM TABLE(arango.system.query("
                                + "database => '" + DB + "', "
                                + "query => 'WITH users FOR v IN 1..1 OUTBOUND \"users/a\" follows RETURN {name: v.name}'))");
        assertThat(r.getOnlyColumnAsSet()).containsExactly("bob");
    }

    @Test
    void insertRejectedOnClusterWithCountUnchanged() {
        long before = userCount();
        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango.system.query("
                                                + "database => '" + DB + "', "
                                                + "query => 'INSERT {x: 1} INTO users'))"))
                .hasMessageContaining("read-only");
        assertThat(userCount()).isEqualTo(before);
    }
}
```

- [ ] **Step 2: Verify it compiles and is excluded from the default run**

Run: `mvn -q test-compile && mvn -q verify -DskipTests -Dsurefire.skip=true 2>&1 | tail -3`
Expected: compiles; default build does not run it (`@Tag("cluster")` is failsafe-excluded).

- [ ] **Step 3: Run it against a real cluster if the machine allows (optional locally — the nightly `cluster-its` CI job is the enforcement point)**

Run: `mvn verify -Pcluster-its -Dsurefire.skip=true 2>&1 | tail -20`
Expected: PASS (6 tests, alongside the two existing cluster ITs). If the local machine cannot boot the 4-container cluster, note that in the PR and let CI run it.

Note for the PR: this is the first cluster IT that boots a `DistributedQueryRunner` (in-JVM Trino coordinator + workers) alongside the 4-container ArangoDB cluster — the existing cluster ITs use only `ArangoClient`. On the 2-vCPU nightly runner this is real added pressure; if the nightly job starts timing out, the runner-side remedy is to split this class into its own job, not to shrink the test.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/io/arango/trino/PassthroughClusterIT.java
git commit -m "test: cluster IT — gate on distributed plans, WITH traversal end-to-end"
```

---

## Task 12: Docs, static-analysis gates, wrap-up

**Files:**
- Modify: `CLAUDE.md`, `README.md`

**Interfaces:** none — documentation and verification only.

- [ ] **Step 1: Update `CLAUDE.md`**

1. "What this is" paragraph: change the milestone sentence to end with `; and an AQL passthrough table function, arango.system.query (M6-B)`.
2. `ArangoConfig` paragraph: append `arangodb.query-function-enabled` (default `true` — registers `arango.system.query`; `false` removes the function entirely, so callers see Trino's "not registered" rather than a tailored error).
3. After read-path item 8, add item 9 summarizing M6-B: the `analyze()` ordering invariant (explain → `AqlReadOnlyGate` allowlist over `plan.collections[].type`, fail closed, `_`-prefix rejection → `firstBatch` with `stream(true)` and disposed cursor), schema derivation reusing `TypeMapper.merge` with the §4.1 reject rules (non-object/mixed/empty/empty-key), the separate `ArangoQueryHandle` (four hooks decline by `instanceof`, split manager short-circuits to one split before discovery), verbatim execution through `PassthroughCursor`, and the error routing (1228 → `SchemaNotFoundException`; 1203/15xx → `INVALID_FUNCTION_ARGUMENT`; write plan → `ARANGODB_QUERY_NOT_READ_ONLY`).
4. Package layout: add `io.arango.trino.ptf` — `ArangoQueryFunction` (PTF + `QueryFunctionHandle`), `AqlReadOnlyGate` (the one place M6-B's safety invariant lives), `PassthroughCursor`.
5. Note `ArangoModule` is now an `AbstractConfigurationAwareModule` (conditional PTF binding).

- [ ] **Step 2: Update `README.md`**

Read the file first; then add `arangodb.query-function-enabled` to whatever configuration table exists, and a short usage section:

```sql
SELECT *
FROM TABLE(arango.system.query(
    database => 'shop',
    query => 'WITH users FOR v IN 1..2 OUTBOUND "users/ada" follows RETURN {name: v.name}'));
```

with two sentences: read-only enforcement (explain-plan allowlist; deploy with a read-only ArangoDB user as the primary control) and the planning-time sampling caveat (the query runs once at planning to derive the schema and once at execution; it must return at least one row).

- [ ] **Step 3: Run the static-analysis gates**

```bash
mvn spotless:check && mvn checkstyle:check && mvn compile spotbugs:check
```

Expected: all pass. Fix any finding in the new files (they are not grandfathered); `mvn spotless:apply` for formatting.

- [ ] **Step 4: Full suite**

```bash
mvn -q test 2>&1 | tail -15
```

Expected: BUILD SUCCESS, previous count + the new tests.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: M6-B — arango.system.query passthrough (gate, derivation, kill switch)"
```

---

## Self-review notes (performed at planning time)

- **Spec coverage:** §3 → Tasks 2/4; §3.1 → Task 6 (`insertIsRejectedWithoutExecuting`) + Task 10/11 count-unchanged; §3.2 → Task 4 UDF test; §3.3 → Tasks 2/6; §3.4 → Task 4; §4/§4.1 → Tasks 3/6/8; §5/5.1/5.2 → Tasks 5/7/8 + Task 10 EXPLAIN; §6 → Tasks 7/8; §7 → Tasks 1/9 + Task 10 kill-switch; §8.1 → Task 3; §8.2 → Task 9; §8.3 → Task 0; §9 → Tasks 2/6/8; §11 → every listed test class exists (plus `ArangoClientPassthroughTest` beyond the spec's minimum).
- **Additions beyond the spec's §8 file table, deliberate:** `PassthroughCursor` (the spec's §9 execution-path error promise needs a deterministic seam; sniffing driver deserialization exceptions would be fragile) and the one-word `SchemaResolver.resolveUnknown` visibility change (§4.1 says derivation must infer "identical to `SchemaResolver`" — reusing the method is how that stays true by construction).
- **Known message-wording risks** (behavior is pinned, wording may need adjusting to observed values): the server's "syntax" wording (Task 6 Step 5), Trino's "not registered" wording (Task 10 Step 2), the view-explain shape, and the UDF result-vs-throw shape (both Task 4 Step 2). Each is called out inline at the step where it can surface.
- **Opus 5 review round (2026-08-03) applied:** C1 — `JsonMapperProvider` (airlift-439's `JsonCodecFactory` takes `Provider<JsonMapper>`, not `ObjectMapperProvider`); C2 — `ConnectorTableFunctionHandle` imports corrected to `io.trino.spi.function.table`; I1 — UDF test rewritten to match Appendix B's recorded returned-string measurement via `Object`-typed `firstBatch` (with a re-measure instruction if the server throws instead); I2 — `getErrorNum()` on the raw-`Request` path pinned in Task 3 (1228/1203/15xx), where the dependency is created; I3 — checkstyle+spotbugs gates added to Tasks 3/6/8 with concrete notes (`DE_MIGHT_IGNORE` → log the disposal failure; `IT_NO_SUCH_ELEMENT` → `hasNext` guard); I4 — `java.util.Optional` import note fixed; minors — test counts corrected (10/16), Task 7 Step 2 expected-failure wording fixed (`applyTableFunction` is an SPI default, so the class compiles), gate comment on non-string collection names, deliberate non-stream `queryPassthrough` comment, cluster IT gains the named-graph coordinator measurement and a runner resource-pressure note.
