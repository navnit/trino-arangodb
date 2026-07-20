# M2 Filter-Pushdown Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Widen filter pushdown from BOOLEAN-only to equality/IN for all scalar types plus guarded numeric range, and ship the `arangodb.type-coercion=lenient|strict` policy that makes it provably correct.

**Architecture:** The read path (`ArangoPageSource.appendValue`) is made *type-exact* so a type-mismatched stored value reads as NULL (lenient) or errors (strict) instead of being coerced. That closes the gap with AQL's type-strict comparisons, so `ArangoMetadata.isPushable` can safely claim equality/IN (no guard) and numeric range (`AqlBuilder` emits `IS_NUMBER`, plus `== FLOOR(...)` integrality for BIGINT). Strict mode declines all pushdown.

**Tech Stack:** Java 24, Trino connector SPI, Guice/Airlift config, ArangoDB Java driver 7.x, JUnit 5 + AssertJ, Testcontainers, Trino `AbstractTestQueryFramework`.

**Spec:** `docs/superpowers/specs/2026-07-19-m2-filter-pushdown-completion-design.md`

## Global Constraints

- **Core invariant (every pushdown decision must satisfy this):** the AQL-side guard must admit *exactly* the set of stored values that `appendValue` writes as non-NULL. If a predicate can only admit a *superset* (BIGINT range — the guard can't cleanly exclude integral values ≥ 2⁶³ that read as NULL), it is pushed as a **prefilter** and *also* kept in Trino's residual so the engine re-checks the superset (review finding C2, option 2). If it can't even guarantee a superset, it stays fully residual.
- **Build:** `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` is not found. `maven.compiler.release=24`.
- **Docker must be running** for any Testcontainers/`DistributedQueryRunner` test.
- **Default coercion is `LENIENT`.** `STRICT` raises `ARANGODB_TYPE_CONVERSION_ERROR`.
- **Stays residual (do NOT push):** IS NULL / IS NOT NULL, any null-allowed domain, VARCHAR/string range, and (in STRICT mode) *everything*.
- **Do not touch `pom.xml` dependency pins** (documented dependency-mediation workarounds).
- **No `ArangoModule` change:** `ArangoMetadata` and `ArangoPageSourceProvider` are `@Inject`-constructor singletons; adding an `ArangoConfig` parameter auto-wires because `ArangoConfig` is `configBinder`-bound.
- Commit after every task. TDD: write the failing test first.

## Test Flip Ledger

Existing tests encode the current BOOLEAN-only behavior and change as tasks land. Each row is the test's expected state **after** the named task. A per-task worker MUST bring these to the listed state in that task (this is how the suite stays green task-by-task).

| Test (file) | After Task 2 | After Task 3 | After Task 4 |
|---|---|---|---|
| `varcharEqualityIsResidualSoMixedTypeRowIsNotSilentlyDropped` (`ArangoConnectorPushdownTest`) | result → `{'str'}`, still `isNotFullyPushedDown` | → `isFullyPushedDown` | (unchanged) |
| `bigintEqualityFilterIsResidualButStillCorrect` (`ArangoConnectorPushdownTest`) | unchanged (residual) | → `isFullyPushedDown` | (unchanged) |
| `applyFilterKeepsBigintEqualityInResidual` (`ArangoMetadataTest`) | unchanged | → asserts pushed (rename) | (unchanged) |
| `bigintRangeFilterIsResidualButStillCorrect` (`ArangoConnectorPushdownTest`) | unchanged (residual) | unchanged (residual) | assertions **unchanged** (BIGINT range is prefilter-only → still `isNotFullyPushedDown`); comment updated |
| `residualFilterIsCorrectOnSampleTypeSkewedColumn` (`ArangoConnectorPushdownTest`) | result `{10,20}` unchanged | unchanged | result `{10,20}` unchanged; comment updated (AQL prefilter now engages) |
| `AqlBuilderTest` range-unreachable test (lines ~75-77) | unchanged | unchanged | → replaced by guarded-range render assertions |
| `ArangoPageSourceProviderTest` coercion tests | provider ctor gains 3rd arg; new mismatch tests added (no existing-assertion flips — fixtures are clean-typed) | (unchanged) | (unchanged) |

**Note (option 2):** BIGINT range is pushed to AQL as a wire-reducing *prefilter* but kept in Trino's residual, so it reports `isNotFullyPushedDown` — the two BIGINT-range tests above therefore keep their assertions and only get comment updates. DOUBLE range **is** fully enforced (`doubleRangeFilterIsFullyPushedDown`, new in Task 4).

**Unchanged throughout (keep):** `booleanEqualityFilterIsFullyPushedDown`, `isNotNullFilterIsNotPushedDownButStillCorrect`, `isNotNullFilterIsCorrectUnderSampleTypeSkew`, `limitIsFullyPushedDown`, `nestedProjectionReturnsCorrectValueProvingPushdownEngaged`, and `ArangoMetadataTest`'s BOOLEAN/IS-NULL/nullable/fixed-point tests.

---

## Task 0: AQL semantics assumptions spike

Confirms the AQL premises the guards rely on, against the real container, before any guard is written. Kept permanently as a regression guard against ArangoDB version drift.

**Files:**
- Create: `src/test/java/io/arango/trino/aql/AqlSemanticsAssumptionsTest.java`

**Interfaces:**
- Consumes: `TestingArangoServer.hostPort()/rootPassword()`, `ArangoClient(ArangoConfig)`, `ArangoClient.query(db, aql, binds)`, `ArangoClient.createDatabaseForTest/createDocumentCollectionForTest/insertForTest/sampleDocuments`.
- Produces: nothing consumed by later tasks (documentation only).

- [ ] **Step 1: Write the test**

```java
package io.arango.trino.aql;

import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.TestingArangoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AqlSemanticsAssumptionsTest {
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort())
                .setUser("root")
                .setPassword(server.rootPassword()));
        client.createDatabaseForTest("probe");
    }

    @AfterAll
    void teardown() {
        client.close();
        server.close();
    }

    private Object eval(String expr) {
        return client.query("probe", "RETURN { r: (" + expr + ") }", Map.of()).next().get("r");
    }

    @Test
    void aqlComparisonAndGuardPremisesHold() {
        assertThat(eval("42 == 42.0")).isEqualTo(true);      // numeric equality is cross-int/float
        assertThat(eval("42 == \"42\"")).isEqualTo(false);   // == is type-strict
        assertThat(eval("IS_NUMBER(42)")).isEqualTo(true);
        assertThat(eval("IS_NUMBER(\"x\")")).isEqualTo(false);
        assertThat(eval("IS_NUMBER(null)")).isEqualTo(false);
        assertThat(eval("IS_NUMBER(true)")).isEqualTo(false);
        assertThat(eval("42 == FLOOR(42.0)")).isEqualTo(true);
        assertThat(eval("42.5 == FLOOR(42.5)")).isEqualTo(false); // integrality guard rejects 42.5
        // total cross-type ordering null < bool < number < string
        assertThat(eval("null < false")).isEqualTo(true);
        assertThat(eval("false < 0")).isEqualTo(true);
        assertThat(eval("0 < \"a\"")).isEqualTo(true);
    }

    @Test
    void recordDriverNumericJavaTypes() {
        client.createDocumentCollectionForTest("probe", "nums");
        client.insertForTest("probe", "nums", Map.of("i", 7, "f", 7.5));
        Map<String, Object> doc = client.sampleDocuments("probe", "nums", 1, false).get(0);
        // On record for isIntegralInLongRange: what Java types the driver yields for int vs float.
        System.out.println("driver numeric types: i=" + doc.get("i").getClass().getName()
                + " f=" + doc.get("f").getClass().getName());
        assertThat(doc.get("i")).isInstanceOf(Number.class);
        assertThat(doc.get("f")).isInstanceOf(Number.class);
    }
}
```

- [ ] **Step 2: Run and verify it passes** (Docker required)

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=AqlSemanticsAssumptionsTest`
Expected: PASS. If any `assertThat(...).isEqualTo(...)` fails, STOP — a guard premise is wrong and the design needs revisiting before proceeding. Note the printed driver numeric types.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/arango/trino/aql/AqlSemanticsAssumptionsTest.java
git commit -m "test: pin AQL semantics assumptions for pushdown guards"
```

---

## Task 1: `arangodb.type-coercion` config

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Produces: `ArangoConfig.TypeCoercion { LENIENT, STRICT }`; `ArangoConfig.getTypeCoercion()`; `ArangoConfig.setTypeCoercion(TypeCoercion)`; config key `arangodb.type-coercion` (default `LENIENT`).

- [ ] **Step 1: Write the failing test** — add to `ArangoConfigTest`, following its existing Airlift `ConfigAssertions` style.

This file uses a single combined `testDefaults` (asserting `assertRecordedDefaults`) and `testExplicitPropertyMappings` (asserting `assertFullMapping`). `assertFullMapping` requires the property map to cover **every** non-deprecated `@Config` property, so a standalone one-key mapping test would fail — extend the existing combined tests instead:
- In the defaults assertion (`assertRecordedDefaults(new ArangoConfig()...)`), add the line `.setTypeCoercion(ArangoConfig.TypeCoercion.LENIENT)`.
- In `testExplicitPropertyMappings`, add `.put("arangodb.type-coercion", "STRICT")` to the properties map builder **and** `.setTypeCoercion(ArangoConfig.TypeCoercion.STRICT)` to the expected `new ArangoConfig()...` object, mirroring how the other properties are added.

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoConfigTest`
Expected: FAIL — `setTypeCoercion` / `getTypeCoercion` do not exist (compile error).

- [ ] **Step 3: Implement the config property** — in `ArangoConfig.java`.

Add the enum next to the existing `MixedTypeStrategy` enum:
```java
    public enum TypeCoercion { LENIENT, STRICT }
```
Add the field next to `mixedTypeStrategy`:
```java
    private TypeCoercion typeCoercion = TypeCoercion.LENIENT;
```
Add getter/setter (after the `mixedTypeStrategy` accessors):
```java
    @NotNull
    public TypeCoercion getTypeCoercion() { return typeCoercion; }

    @Config("arangodb.type-coercion")
    @ConfigDescription("Per-cell type-mismatch policy: LENIENT reads a mismatched value as NULL, STRICT raises an error")
    public ArangoConfig setTypeCoercion(TypeCoercion typeCoercion) {
        this.typeCoercion = typeCoercion; return this;
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoConfigTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoConfig.java src/test/java/io/arango/trino/ArangoConfigTest.java
git commit -m "feat: add arangodb.type-coercion config (lenient|strict)"
```

---

## Task 2: Type-exact read coercion + policy (the enabling change)

Makes `appendValue` type-exact, routes mismatches through the coercion policy, adds `ArangoErrorCode`, and threads the mode from config into the page source. After this task the read path (not pushdown) already changes the `mixed`/`code='42'` result to `{'str'}`.

**Files:**
- Create: `src/main/java/io/arango/trino/ArangoErrorCode.java`
- Modify: `src/main/java/io/arango/trino/ArangoPageSource.java`
- Modify: `src/main/java/io/arango/trino/ArangoPageSourceProvider.java:23-27,42-43`
- Modify: `src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java` (add `arangostrict` catalog + strict e2e test; retune `varchar…` test)
- Test/Modify: `src/test/java/io/arango/trino/ArangoPageSourceProviderTest.java`

**Interfaces:**
- Consumes: `ArangoConfig.TypeCoercion` (Task 1).
- Produces: `ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR`; `ArangoPageSource(ArangoCursor<Map>, List<ArangoColumnHandle>, ArangoConfig.TypeCoercion)` constructor (third param added).

- [ ] **Step 1: Write the failing e2e coercion tests** — in `ArangoConnectorPushdownTest`.

First, in `createQueryRunner()`, add a strict catalog next to the existing `arango`/`arangoskew` catalog registrations (mirror their `createCatalog(...)`/properties style, adding `arangodb.type-coercion=STRICT` and pointing at the same server/database as the default `arango` catalog):
```java
        queryRunner.createCatalog("arangostrict", "arangodb", Map.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword(),
                "arangodb.type-coercion", "STRICT"));
```
(Match the exact property keys/helper the file already uses for `arango`; copy that block and add the one extra property.)

Retune the existing `varcharEqualityIsResidualSoMixedTypeRowIsNotSilentlyDropped` to its **after-Task-2** state (result `{'str'}`, still residual) and rewrite its comment:
```java
    @Test
    void varcharEqualityIsResidualSoMixedTypeRowIsNotSilentlyDropped() {
        // shop.mixed.code holds numeric 42 (label 'num') and string "42" (label 'str'), inferring
        // VARCHAR. With type-exact coercion, the numeric 42 in a VARCHAR column now reads as NULL
        // (not "42"), so `code = '42'` correctly matches only the genuine string row. VARCHAR
        // equality is not yet pushed (Task 3), so this stays residual for now.
        assertThat(query("SELECT label FROM arango.shop.mixed WHERE code = '42'"))
                .matches("VALUES VARCHAR 'str'")
                .isNotFullyPushedDown(FilterNode.class);
    }
```
Add a strict-mode read test:
```java
    @Test
    void strictModeRaisesOnTypeMismatch() {
        // arango.shop.mixed.code infers VARCHAR; the numeric-42 doc is a type mismatch. Under the
        // strict catalog, reading it raises rather than NULLing. (Strict declines pushdown -- Task 3
        // -- so Trino reads every row, hitting the mismatch.)
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT code FROM arangostrict.shop.mixed"))
                .hasMessageContaining("expected varchar");
    }
```
Add the import `import static org.assertj.core.api.Assertions.assertThatThrownBy;` if not present. (Assert on the message text we control — `TrinoException.getMessage()` returns the supplied message, NOT the error-code name; asserting `"ARANGODB_TYPE_CONVERSION_ERROR"` would fail against a correct implementation. The exact substring must match `appendValue`'s message from Task 2 Step 4: `Column '%s' expected %s but a document held %s of type %s` → `"expected varchar"`.)

- [ ] **Step 2: Run to verify failure**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoConnectorPushdownTest`
Expected: FAIL — `ArangoPageSource` constructor arity (compile error) and/or old coercion still stringifies 42 → `"42"`.

- [ ] **Step 3: Add `ArangoErrorCode`**

```java
package io.arango.trino;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.USER_ERROR;

public enum ArangoErrorCode implements ErrorCodeSupplier {
    // 0x0100_0000 is this connector's private error-code base -- it only needs to be stable and
    // clear of Trino's StandardErrorCode range. Extend this enum as more error paths are built.
    ARANGODB_TYPE_CONVERSION_ERROR(0, USER_ERROR);

    private final ErrorCode errorCode;

    ArangoErrorCode(int code, ErrorType type) {
        this.errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
```

- [ ] **Step 4: Rewrite `ArangoPageSource` coercion**

Add imports (near the other `io.trino.spi` imports):
```java
import io.trino.spi.TrinoException;

import static io.arango.trino.ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR;
```
Add the field (with the other final fields):
```java
    private final ArangoConfig.TypeCoercion coercion;
```
Extend the constructor to accept and store the mode (keep the existing body, add the last line):
```java
    public ArangoPageSource(ArangoCursor<Map> cursor, List<ArangoColumnHandle> columns, ArangoConfig.TypeCoercion coercion) {
        // ... existing field assignments unchanged ...
        this.coercion = requireNonNull(coercion, "coercion is null");
    }
```
Change the call site (currently `appendValue(out, types.get(i), row.get(col.name()));`) to pass the column:
```java
                appendValue(out, col, types.get(i), row.get(col.name()));
```
Replace the whole `appendValue` method (and add the `isIntegralInLongRange` helper) with:
```java
    // Type-exact coercion (spec §4.2 / core invariant): a stored value whose runtime type does not
    // match the column's inferred Trino type is a *mismatch*, handled per this.coercion -- LENIENT
    // writes NULL, STRICT raises. No String.valueOf, no longValue() truncation: exactness is what
    // lets ArangoMetadata push equality/range filters safely, because the pushed AQL comparison and
    // this read path then admit exactly the same values.
    private void appendValue(BlockBuilder out, ArangoColumnHandle column, Type type, Object value) {
        if (value == null) {
            out.appendNull();
            return;
        }
        if (type.equals(BooleanType.BOOLEAN) && value instanceof Boolean b) {
            BooleanType.BOOLEAN.writeBoolean(out, b);
            return;
        }
        if (type.equals(BigintType.BIGINT) && isIntegralInLongRange(value)) {
            BigintType.BIGINT.writeLong(out, ((Number) value).longValue());
            return;
        }
        if (type.equals(DoubleType.DOUBLE) && value instanceof Number n) {
            DoubleType.DOUBLE.writeDouble(out, n.doubleValue());
            return;
        }
        if (type instanceof VarcharType && value instanceof String s) {
            type.writeSlice(out, utf8Slice(s));
            return;
        }
        // Mismatch (or an unanticipated structured type -- ArangoPageSourceProvider.checkMaterializable
        // already rejects ARRAY/ROW/DECIMAL columns before the query runs).
        if (coercion == ArangoConfig.TypeCoercion.STRICT) {
            throw new TrinoException(ARANGODB_TYPE_CONVERSION_ERROR,
                    "Column '%s' expected %s but a document held %s of type %s"
                            .formatted(column.name(), type, value, value.getClass().getSimpleName()));
        }
        out.appendNull();
    }

    // A BIGINT column accepts an integer-valued number within signed 64-bit range. 42.0 is accepted
    // (reads as 42); 42.5 is a mismatch -- truncating it (the old longValue() behavior) would
    // disagree with a pushed FILTER, the exact bug this milestone closes. Non-numbers are mismatches.
    private static boolean isIntegralInLongRange(Object value) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return true;
        }
        if (value instanceof Double d) {
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof Float f) {
            double d = f;
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof java.math.BigInteger bi) {
            return bi.bitLength() < 64;
        }
        return false;
    }
```
(`io.trino.spi.type.*` is already wildcard-imported, so `BigintType`/`DoubleType`/`VarcharType`/`BooleanType`/`Type` need no new import.)

**Deliberate deviation from spec §4.2:** the spec says "the existing `catch (RuntimeException)` → lenient-NULL fallback stays". This rewrite **removes** the try/catch entirely. That is intentional and correct: keeping it would swallow the strict-mode `TrinoException` (a `RuntimeException`) and silently NULL instead of raising. Every branch above is now guarded by an explicit `instanceof`/`isIntegralInLongRange` check before any cast, so none of them can throw for a type mismatch — the mismatch falls through to the explicit policy tail. (If the spec is treated as authoritative, note this deviation there; the plan is the correct behavior.)

- [ ] **Step 5: Thread the mode through `ArangoPageSourceProvider`**

Add the field and extend the constructor:
```java
    private final ArangoConfig config;

    @com.google.inject.Inject
    public ArangoPageSourceProvider(ArangoClient client, AqlBuilder aqlBuilder, ArangoConfig config) {
        this.client = client;
        this.aqlBuilder = aqlBuilder;
        this.config = config;
    }
```
Pass the mode when constructing the page source (line ~42-43):
```java
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars()), cols, config.getTypeCoercion());
```

- [ ] **Step 6: Audit + update `ArangoPageSourceProviderTest`**

Every existing `new ArangoPageSourceProvider(client, new AqlBuilder())` in this file (e.g. line ~91) gains a third `ArangoConfig` argument: `new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig())`. The existing clean-data tests (`items` collection: matching types + missing fields) keep their assertions — matching types and absent fields are unchanged by type-exact coercion.

Then add these mismatch tests plus a shared read helper (mirrors the file's own `getNextSourcePage` read loop; needs `import io.trino.spi.type.VarcharType;` and `import static org.assertj.core.api.Assertions.assertThatThrownBy;`):
```java
    @Test
    void numberInVarcharColumnReadsAsNullUnderLenient() throws Exception {
        client.createDocumentCollectionForTest("shop", "coercion");
        client.insertForTest("shop", "coercion", mapOf("_key", "c-num", "s", 42L));
        assertThat(readSingleColumn("coercion",
                new ArangoColumnHandle("s", VARCHAR, false, List.of("s")), new ArangoConfig())).isNull();
    }

    @Test
    void fractionalNumberInBigintColumnReadsAsNullUnderLenient() throws Exception {
        client.createDocumentCollectionForTest("shop", "coercion2");
        client.insertForTest("shop", "coercion2", mapOf("_key", "c-frac", "n", 42.5));
        assertThat(readSingleColumn("coercion2",
                new ArangoColumnHandle("n", BIGINT, false, List.of("n")), new ArangoConfig())).isNull();
    }

    @Test
    void strictModeRaisesOnTypeMismatch() {
        client.createDocumentCollectionForTest("shop", "coercion3");
        client.insertForTest("shop", "coercion3", mapOf("_key", "c-bad", "s", 42L));
        assertThatThrownBy(() -> readSingleColumn("coercion3",
                new ArangoColumnHandle("s", VARCHAR, false, List.of("s")),
                new ArangoConfig().setTypeCoercion(ArangoConfig.TypeCoercion.STRICT)))
                .isInstanceOfSatisfying(io.trino.spi.TrinoException.class,
                        e -> assertThat(e.getErrorCode().getName()).isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR"));
    }

    // Reads the single column `col` from a single-document collection, returning its one cell (or null).
    private Object readSingleColumn(String collection, ArangoColumnHandle col, ArangoConfig config) throws Exception {
        ArangoTableHandle handle = new ArangoTableHandle("shop", collection, false, TupleDomain.all(), OptionalLong.empty());
        ArangoPageSourceProvider provider = new ArangoPageSourceProvider(client, new AqlBuilder(), config);
        ConnectorPageSource source = provider.createPageSource(null, null, null, handle, List.of(col), null);
        Object result = null;
        while (!source.isFinished()) {
            SourcePage page = source.getNextSourcePage();
            if (page == null) {
                continue;
            }
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                result = page.getBlock(0).isNull(pos) ? null
                        : (col.type() instanceof VarcharType
                            ? VARCHAR.getSlice(page.getBlock(0), pos).toStringUtf8()
                            : BIGINT.getLong(page.getBlock(0), pos));
            }
        }
        return result;
    }
```
(Use whatever map-literal helper the file already uses in place of `mapOf(...)`; if the seeding helper takes a `Map`, pass `java.util.Map.of(...)`.)

- [ ] **Step 7: Run the affected tests**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoPageSourceProviderTest,ArangoConnectorPushdownTest`
Expected: PASS. The `varchar…` test now yields `{'str'}`; `strictModeRaisesOnTypeMismatch` errors as asserted.

- [ ] **Step 8: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoErrorCode.java \
        src/main/java/io/arango/trino/ArangoPageSource.java \
        src/main/java/io/arango/trino/ArangoPageSourceProvider.java \
        src/test/java/io/arango/trino/ArangoPageSourceProviderTest.java \
        src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java
git commit -m "feat: type-exact read coercion with lenient|strict policy"
```

---

## Task 3: Push equality/IN for all scalar types + decline in strict mode

`isPushable` becomes an instance method, declines everything in strict mode, and (in lenient mode) claims equality/IN discrete sets for BOOLEAN/VARCHAR/BIGINT/DOUBLE. `AqlBuilder`'s discrete-set rendering is already generic, so no `AqlBuilder` change is needed for eq/IN.

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java` (imports, constructor, `isPushable`)
- Modify: `src/test/java/io/arango/trino/ArangoMetadataTest.java`
- Modify: `src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java`

**Interfaces:**
- Consumes: `ArangoConfig.TypeCoercion` (Task 1); the type-exact read path (Task 2).
- Produces: widened `isPushable` (instance method); `ArangoMetadata(ArangoClient, SchemaResolver, ArangoConfig)` constructor (third param added).

- [ ] **Step 1: Write the failing unit tests** — in `ArangoMetadataTest`.

**First, mechanical:** **every** `new ArangoMetadata(...)` construction in this file gains a third argument — the lenient default `new ArangoMetadata(null, null, new ArangoConfig())` — so the file compiles against the new constructor. There are several call sites (roughly lines 125, 153, 196, 206, 216, 263, 278 in the current file); a compile error will point out any you miss, but update them all up front. Any test that specifically needs strict mode passes `new ArangoConfig().setTypeCoercion(ArangoConfig.TypeCoercion.STRICT)` instead. (This test constructs `ArangoMetadata` directly with `null` client/resolver, per CLAUDE.md's test-double approach; `handle` is the existing per-test `ArangoTableHandle` built with `TupleDomain.all()` and `OptionalLong.empty()`, mirror the `applyFilterPushesBooleanEqualityAndDropsFromResidual` setup.)

Rewrite `applyFilterKeepsBigintEqualityInResidual` to assert it is now pushed (rename to `applyFilterPushesBigintEquality`):
```java
    @Test
    void applyFilterPushesBigintEquality() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(age, Domain.singleValue(BIGINT, 30L))));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(null, handle, constraint);
        assertThat(result).isPresent();
        ArangoTableHandle newHandle = (ArangoTableHandle) result.orElseThrow().getHandle();
        assertThat(newHandle.constraint().getDomains().orElseThrow())
                .containsEntry(age, Domain.singleValue(BIGINT, 30L));
    }
```
(Confirm the `ArangoTableHandle` constructor arg order/arity against the file's existing usage; copy it verbatim from a neighboring test rather than trusting this signature.)

Add a VARCHAR-equality-pushed test (same shape, `VARCHAR`, `Domain.singleValue(VARCHAR, io.airlift.slice.Slices.utf8Slice("x"))`, asserting `result` is present) and a strict-decline test:
```java
    @Test
    void applyFilterPushesNothingInStrictMode() {
        ArangoMetadata strict = new ArangoMetadata(null, null,
                new ArangoConfig().setTypeCoercion(ArangoConfig.TypeCoercion.STRICT));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                Map.of(age, Domain.singleValue(BIGINT, 30L))));
        assertThat(strict.applyFilter(null, handle, constraint)).isEmpty();
    }
```
`static io.trino.spi.type.VarcharType.VARCHAR;` is already imported.

In `ArangoConnectorPushdownTest`, bring `bigintEqualityFilterIsResidualButStillCorrect` to its after-Task-3 state:
```java
    @Test
    void bigintEqualityFilterIsFullyPushedDown() {
        // BIGINT equality is now pushed: AQL `==` is type-strict and the type-exact read path agrees,
        // so no guard is needed. age = 30 matches only 'ada'.
        assertThat(query("SELECT name FROM arango.shop.users WHERE age = 30"))
                .matches("VALUES VARCHAR 'ada'")
                .isFullyPushedDown();
    }
```
Bring the `varchar…` test to `isFullyPushedDown` (result stays `{'str'}`):
```java
        assertThat(query("SELECT label FROM arango.shop.mixed WHERE code = '42'"))
                .matches("VALUES VARCHAR 'str'")
                .isFullyPushedDown();
```
Add a strict-declines-pushdown e2e check:
```java
    @Test
    void strictModeDeclinesPushdownLeavingResidual() {
        // Strict mode declines all pushdown, so age = 30 stays residual (Trino applies it post-read).
        // users is clean-typed, so reading it under strict raises nothing. Correct: age = 30 -> 'ada'.
        assertThat(query("SELECT name FROM arangostrict.shop.users WHERE age = 30"))
                .matches("VALUES VARCHAR 'ada'")
                .isNotFullyPushedDown(FilterNode.class);
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoMetadataTest,ArangoConnectorPushdownTest`
Expected: FAIL — `isPushable` still BOOLEAN-only; `ArangoMetadata` constructor arity.

- [ ] **Step 3: Extend `ArangoMetadata` constructor + imports**

Add imports:
```java
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
```
Add the field and the constructor parameter:
```java
    private final ArangoConfig config;

    @Inject
    public ArangoMetadata(ArangoClient client, SchemaResolver schemaResolver, ArangoConfig config) {
        this.client = client;
        this.schemaResolver = schemaResolver;
        this.config = config;
    }
```

- [ ] **Step 4: Rewrite `isPushable`** (replace the method and its doc comment)

```java
    // Widened M2 pushdown. The read path (ArangoPageSource.appendValue) is type-exact, so a pushed
    // AQL predicate and Trino's residual re-check admit exactly the same values (the core invariant).
    // Equality/IN need no guard (AQL ==/IN are type-strict); numeric range is guarded in AqlBuilder.
    private boolean isPushable(Type type, Domain domain) {
        if (domain.isAll()) {
            return false;
        }
        // STRICT coercion: a pushed filter would exclude a type-mismatched row server-side and thus
        // suppress the ARANGODB_TYPE_CONVERSION_ERROR strict mode must raise. Keep results (and
        // errors) independent of pushdown by pushing nothing (spec §5).
        if (config.getTypeCoercion() == ArangoConfig.TypeCoercion.STRICT) {
            return false;
        }
        // IS NULL / IS NOT NULL and any null-allowing domain stay residual: AQL == null/!= null test
        // the raw stored value, which diverges from Trino's post-coercion null-ness on a type mismatch.
        if (domain.isNullAllowed()) {
            return false;
        }
        ValueSet values = domain.getValues();
        if (values.isAll()) {
            return false;
        }
        // Equality / IN: pushable for every scalar type we materialize (no guard needed).
        // Numeric range is NOT pushed yet -- AqlBuilder.renderDomain still throws on non-discrete
        // domains until Task 4, so admitting a range here would make the scan throw at query time
        // (not fall to residual). Ranges are enabled in Task 4 together with the renderer.
        return values.isDiscreteSet() && (
                type.equals(BooleanType.BOOLEAN)
                || type instanceof VarcharType
                || type.equals(BigintType.BIGINT)
                || type.equals(DoubleType.DOUBLE));
    }
```

- [ ] **Step 5: Run to verify pass**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=ArangoMetadataTest,ArangoConnectorPushdownTest`
Expected: PASS. `bigintRangeFilterIsResidualButStillCorrect` still passes because `isPushable` gates on `values.isDiscreteSet()` — a range domain is not discrete, so it is NOT pushed onto the handle and never reaches `renderDomain` (which still throws on non-discrete domains until Task 4). It stays fully residual and `isNotFullyPushedDown`.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoMetadata.java \
        src/test/java/io/arango/trino/ArangoMetadataTest.java \
        src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java
git commit -m "feat: push equality/IN for all scalar types; decline in strict mode"
```

---

## Task 4: Numeric range pushdown (DOUBLE fully enforced, BIGINT as prefilter + residual)

Enables range pushdown. `isPushable` widens to admit BIGINT/DOUBLE ranges; `AqlBuilder.renderDomain` gains a guarded range branch. **DOUBLE range is fully enforced** (its `IS_NUMBER` guard admits exactly what `appendValue` accepts). **BIGINT range is prefilter-only** (option 2 from review finding C2): the guard `IS_NUMBER … == FLOOR` still admits integral values ≥ 2⁶³ that `appendValue` reads as NULL, so `applyFilter` pushes it to AQL for wire reduction **and** keeps it in the residual for Trino to re-check. This is unconditionally correct without depending on ArangoDB's int64/double boundary semantics.

**Files:**
- Modify: `src/main/java/io/arango/trino/aql/AqlBuilder.java` (imports + `renderDomain`)
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java` (`isPushable` range branch, new `isPrefilterOnly`, `applyFilter` dual-routing)
- Modify: `src/test/java/io/arango/trino/aql/AqlBuilderTest.java`
- Modify: `src/test/java/io/arango/trino/ArangoMetadataTest.java`
- Modify: `src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java` (new fixtures + range e2e tests)

**Interfaces:**
- Consumes: eq/IN pushdown + strict-decline `isPushable` (Task 3); type-exact read path (Task 2).
- Produces: guarded range AQL (single lower bound `d[..] > @v` → `GUARD AND d[..] > @v`; bounded → `GUARD AND (d[..] >= @a AND d[..] < @b)`; multi-range → `GUARD AND (r1 OR r2)`); `isPrefilterOnly(Type, Domain)` (BIGINT range only); `applyFilter` that adds prefilter-only domains to both the pushed handle and the residual.

- [ ] **Step 1: Write the failing unit tests** — in `AqlBuilderTest`.

Replace the range-unreachable test/comment (lines ~75-77) with real range-rendering assertions. Add:
```java
    @Test
    void bigintRangeRendersWithIntegralityGuard() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(Map.of(age,
                        Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 30L)), false))),
                OptionalLong.empty());
        AqlBuilder.AqlQuery q = new AqlBuilder().buildScan(handle, List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"age\"]) AND d[\"age\"] == FLOOR(d[\"age\"]) "
                        + "AND d[\"age\"] > @v0) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 30L);
    }

    @Test
    void doubleRangeRendersWithNumberGuardOnly() {
        ArangoColumnHandle price = new ArangoColumnHandle("price", DOUBLE, false, List.of("price"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "items", false,
                TupleDomain.withColumnDomains(Map.of(price,
                        Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 9.99)), false))),
                OptionalLong.empty());
        AqlBuilder.AqlQuery q = new AqlBuilder().buildScan(handle, List.of(price));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"price\"]) AND d[\"price\"] <= @v0) "
                        + "RETURN {\"price\": d[\"price\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 9.99);
    }
```
Add imports as needed: `import io.trino.spi.predicate.Range;`, `import io.trino.spi.predicate.ValueSet;`, and static type imports `BIGINT`/`DOUBLE` (match the file's existing import style). Adjust the exact expected `RETURN {...}` substring to whatever `buildReturnClause` currently emits for a single column (confirm against an existing discrete-set test in this same file and mirror its formatting precisely).

- [ ] **Step 2: Run to verify failure**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=AqlBuilderTest`
Expected: FAIL — `renderDomain` throws `IllegalStateException` on the range (non-discrete) domain.

- [ ] **Step 3: Implement the range branch** — in `AqlBuilder.java`.

Add imports:
```java
import io.trino.spi.predicate.Range;
import io.trino.spi.type.BigintType;
```
Replace the `throw new IllegalStateException(...)` tail of `renderDomain` with the range renderer (keep the discrete-set block above it unchanged):
```java
        // Numeric range (ArangoMetadata.isPushable only admits this for BIGINT/DOUBLE). AQL's <,>
        // use a total cross-type ordering (null<bool<number<string), so d.f>@v would also match
        // non-numbers; guard with IS_NUMBER. For BIGINT the read path accepts only integral values,
        // so the guard also requires d.f == FLOOR(d.f) -- otherwise a stored 35.5 would pass the AQL
        // filter yet read back NULL, diverging from the residual answer (the core invariant).
        String guard = column.type().equals(BigintType.BIGINT)
                ? "IS_NUMBER(" + accessor + ") AND " + accessor + " == FLOOR(" + accessor + ")"
                : "IS_NUMBER(" + accessor + ")";
        List<String> rangeClauses = new ArrayList<>();
        for (Range range : values.getRanges().getOrderedRanges()) {
            List<String> bounds = new ArrayList<>();
            if (!range.isLowUnbounded()) {
                String op = range.isLowInclusive() ? " >= @" : " > @";
                bounds.add(accessor + op + bindValue(bindVars, counter, toBindValue(range.getLowBoundedValue())));
            }
            if (!range.isHighUnbounded()) {
                String op = range.isHighInclusive() ? " <= @" : " < @";
                bounds.add(accessor + op + bindValue(bindVars, counter, toBindValue(range.getHighBoundedValue())));
            }
            // isPushable guarantees a real range (not all, not discrete), so bounds is non-empty.
            rangeClauses.add(bounds.size() == 1 ? bounds.get(0) : "(" + String.join(" AND ", bounds) + ")");
        }
        String ranges = rangeClauses.size() == 1 ? rangeClauses.get(0) : "(" + String.join(" OR ", rangeClauses) + ")";
        return guard + " AND " + ranges;
```

- [ ] **Step 4: Run unit tests to verify pass**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=AqlBuilderTest`
Expected: PASS. If an assertion mismatches on spacing/quoting, fix the *expected string* to match the builder's actual output (the builder is the source of truth for formatting).

- [ ] **Step 5: Wire range pushdown into `ArangoMetadata` (TDD)**

First, failing unit tests in `ArangoMetadataTest` (add `import io.trino.spi.predicate.Range;`, `import io.trino.spi.predicate.ValueSet;` if absent):
```java
    @Test
    void applyFilterPushesBigintRangeAsPrefilterKeepingResidual() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        Domain range = Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 30L)), false);
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(age, range)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);
        assertThat(result).isPresent();
        // Pushed onto the handle (-> AQL prefilter) AND kept in the remaining filter (-> Trino re-check).
        ArangoTableHandle newHandle = (ArangoTableHandle) result.orElseThrow().getHandle();
        assertThat(newHandle.constraint().getDomains().orElseThrow()).containsEntry(age, range);
        assertThat(result.orElseThrow().getRemainingFilter().getDomains().orElseThrow()).containsEntry(age, range);
    }

    @Test
    void applyFilterPushesDoubleRangeFullyEnforced() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig());
        ArangoTableHandle handle = new ArangoTableHandle("shop", "prices", false, TupleDomain.all(), OptionalLong.empty());
        ArangoColumnHandle amount = new ArangoColumnHandle("amount", DOUBLE, false, List.of("amount"));
        Domain range = Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 9.99)), false);
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(amount, range)));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result = metadata.applyFilter(null, handle, constraint);
        assertThat(result).isPresent();
        assertThat(result.orElseThrow().getRemainingFilter().isAll()).isTrue(); // fully enforced, no residual
    }
```
Run `mvn test -Dtest=ArangoMetadataTest`: FAIL (ranges not yet admitted / not dual-routed).

Then implement in `ArangoMetadata.java`. Widen `isPushable`'s tail (the discrete-only `return` from Task 3) to admit ranges:
```java
        // Equality / IN: pushable for every scalar type (no guard needed).
        if (values.isDiscreteSet()) {
            return type.equals(BooleanType.BOOLEAN)
                    || type instanceof VarcharType
                    || type.equals(BigintType.BIGINT)
                    || type.equals(DoubleType.DOUBLE);
        }
        // Numeric range: BIGINT and DOUBLE (guarded by AqlBuilder). DOUBLE is fully enforced; BIGINT
        // is prefilter-only (see isPrefilterOnly / applyFilter). String range stays residual.
        return type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE);
```
Add the helper:
```java
    // A pushable predicate whose AQL form admits a SUPERSET of what appendValue writes non-NULL, so
    // it must ALSO stay in Trino's residual for a post-read re-check. Only BIGINT range qualifies:
    // the IS_NUMBER + == FLOOR guard still admits integral values outside signed-64-bit range, which
    // appendValue reads as NULL. Equality/IN (bind values are in-range) and DOUBLE range (guard ==
    // read-path acceptance) are fully enforced, never prefilter-only. (Review finding C2, option 2.)
    private static boolean isPrefilterOnly(Type type, Domain domain) {
        return type.equals(BigintType.BIGINT) && !domain.getValues().isDiscreteSet();
    }
```
Dual-route in `applyFilter`'s loop — replace the `if (isPushable(...)) { pushedBuilder.put(...); } else { residualBuilder.put(...); }` body with:
```java
            if (isPushable(column.type(), domain)) {
                pushedBuilder.put(column, domain);
                if (isPrefilterOnly(column.type(), domain)) {
                    residualBuilder.put(column, domain); // Trino re-checks the AQL prefilter's superset
                }
            }
            else {
                residualBuilder.put(column, domain);
            }
```
Run `mvn test -Dtest=ArangoMetadataTest`: PASS. (Fixed-point guard still holds: on re-invocation the range is already on the handle, so `newHandleConstraint.equals(handle.constraint())` short-circuits to `Optional.empty()` — no infinite loop.)

- [ ] **Step 6: Add e2e fixtures + range/IN/DOUBLE tests** — in `ArangoConnectorPushdownTest`.

In `createQueryRunner()`, next to the existing seed calls, add (use the file's map-literal helper in place of `mapOf`; the sample-size-2 `arangoskew` catalog samples only the first two docs of a collection, so outliers must be inserted last):
```java
        client.createDocumentCollectionForTest("shop", "prices");
        client.insertForTest("shop", "prices", mapOf("_key", "p1", "amount", 10.5));
        client.insertForTest("shop", "prices", mapOf("_key", "p2", "amount", 20.0));

        client.createDocumentCollectionForTest("shop", "bigskew");
        client.insertForTest("shop", "bigskew", mapOf("_key", "b1", "big", 10L));
        client.insertForTest("shop", "bigskew", mapOf("_key", "b2", "big", 20L));
        client.insertForTest("shop", "bigskew", mapOf("_key", "b3", "big", 1e19)); // >= 2^63, out of sample

        client.insertForTest("shop", "skewed", mapOf("_key", "s4", "val", 42.5)); // fractional outlier, out of sample
```
Update the comments (only) on the two existing range tests `bigintRangeFilterIsResidualButStillCorrect` and `residualFilterIsCorrectOnSampleTypeSkewedColumn`: their assertions are unchanged (BIGINT range is prefilter-only → still `isNotFullyPushedDown` / result unchanged), but the AQL prefilter now engages under the hood — replace any "BIGINT ranges are no longer pushed at all" wording with "BIGINT range is pushed to AQL as a prefilter and re-checked residually". Then add:
```java
    @Test
    void varcharInFilterIsFullyPushedDown() {
        assertThat(query("SELECT name FROM arango.shop.users WHERE name IN ('ada', 'bob')"))
                .matches("VALUES (VARCHAR 'ada'), (VARCHAR 'bob')")
                .isFullyPushedDown();
    }

    @Test
    void doubleRangeFilterIsFullyPushedDown() {
        // DOUBLE range is fully enforced (IS_NUMBER guard == read-path acceptance).
        assertThat(query("SELECT amount FROM arango.shop.prices WHERE amount > 15.0"))
                .matches("VALUES DOUBLE '20.0'")
                .isFullyPushedDown();
    }

    @Test
    void bigintEqualityAgreesUnderSampleTypeSkew() {
        // val = 10 with the string "not-a-number" and fractional 42.5 outliers present: equality is
        // fully enforced (AQL == type-strict; read path agrees). Both outliers correctly excluded.
        assertThat(query("SELECT val FROM arangoskew.shop.skewed WHERE val = 10"))
                .matches("VALUES BIGINT '10'")
                .isFullyPushedDown();
    }

    @Test
    void bigintRangeAgreesOnOutOfLongRangeOutlier() {
        // Review finding C2: bigskew.big samples BIGINT from 10,20; a 3rd out-of-sample doc holds 1e19
        // (>= 2^63). The AQL guard admits 1e19 but appendValue reads it as NULL; the residual re-check
        // drops it, so pushed and residual agree -> 10,20. This is why BIGINT range keeps a residual.
        assertThat(query("SELECT big FROM arangoskew.shop.bigskew WHERE big > 5"))
                .matches("VALUES (BIGINT '10'), (BIGINT '20')")
                .isNotFullyPushedDown(FilterNode.class);
    }
```

- [ ] **Step 7: Run to verify pass**

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test -Dtest=AqlBuilderTest,ArangoMetadataTest,ArangoConnectorPushdownTest`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/main/java/io/arango/trino/aql/AqlBuilder.java \
        src/main/java/io/arango/trino/ArangoMetadata.java \
        src/test/java/io/arango/trino/aql/AqlBuilderTest.java \
        src/test/java/io/arango/trino/ArangoMetadataTest.java \
        src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java
git commit -m "feat: numeric range pushdown (DOUBLE enforced, BIGINT prefilter+residual)"
```

---

## Task 5: Docs + full-suite verification

**Files:**
- Modify: `CLAUDE.md` (read-path + pushdown sections)
- Verify: whole suite

- [ ] **Step 1: Update `CLAUDE.md`** — the architecture prose currently describes BOOLEAN-only pushdown and lenient String.valueOf coercion. Update:
  - The `ArangoPageSourceProvider`/`ArangoPageSource` bullet: coercion is now type-exact with an `arangodb.type-coercion` (`lenient`|`strict`) policy; a type-mismatched value reads as NULL (lenient) or raises `ARANGODB_TYPE_CONVERSION_ERROR` (strict); note the M1 read-behavior change (number-in-VARCHAR / fractional-in-BIGINT now NULL, not coerced).
  - The pushdown description: `applyFilter` pushes equality/IN for BOOLEAN/VARCHAR/BIGINT/DOUBLE (fully enforced) and numeric range for BIGINT/DOUBLE with `IS_NUMBER`(+`FLOOR`) guards — DOUBLE range fully enforced, **BIGINT range as an AQL prefilter that is also re-checked in Trino's residual** (so `isNotFullyPushedDown`, because the guard admits integral values ≥ 2⁶³ that read as NULL); IS NULL/IS NOT NULL and string range stay residual; strict mode declines all pushdown.
  - Add `arangodb.type-coercion` to the `ArangoConfig` settings list.

- [ ] **Step 2: Run the full suite** (Docker required)

Run: `source ~/.sdkman/bin/sdkman-init.sh && mvn test`
Expected: BUILD SUCCESS, all tests pass. Confirm the Flip Ledger tests are all in their final state and no BOOLEAN-only assertion remains.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update read-path coercion + widened pushdown in CLAUDE.md"
```

---

## Self-Review Notes (author)

- **Spec coverage:** §4.1 config → Task 1; §4.2 coercion + §4.5 error code → Task 2; §4.3 isPushable + §5 strict-decline → Task 3; §4.4 range guards → Task 4; §7 tests distributed across tasks (validation spike → Task 0, agreement/coercion/strict → Tasks 2-4); §8 files all touched; docs → Task 5.
- **Every flipping test has a per-task target state** (Flip Ledger + inline in Tasks 2-4).
- **Type consistency:** `ArangoPageSource` third ctor arg `ArangoConfig.TypeCoercion` used identically in provider (Task 2) and tests; `isPushable` signature (instance, `(Type, Domain)`) consistent Task 3; `renderDomain` range output format matches the AqlBuilderTest expectations in Task 4 (with the note that the builder is source-of-truth for exact spacing).
- **Known soft spots for the executor:** the exact `RETURN {...}` substring and catalog-registration helper differ only in mechanical formatting — each is resolved by mirroring an existing test/registration in the same file, and Task 0/Task 4 unit tests catch any `getRanges()`/format mismatch cheaply before container tests run.
- **Spec §9 refinement (review finding C2, option 2):** the spec's success criterion "numeric range (BIGINT/DOUBLE) reports `isFullyPushedDown()`" is amended — **DOUBLE range reports `isFullyPushedDown()`; BIGINT range is pushed as an AQL prefilter and re-checked residually (`isNotFullyPushedDown`)**. This trades the badge for unconditional correctness independent of ArangoDB's int64/double boundary semantics, while preserving the wire-reduction. The spec file predates this decision; treat this plan as authoritative on the BIGINT-range mechanism.
- **Review fixes applied (Fable review):** C1 — Task 3 `isPushable` is discrete-set-only; range admission + `AqlBuilder` renderer land together in Task 4 (prevents a scan-time throw between tasks). C2 — BIGINT range prefilter+residual (above). M1 — strict tests assert the message text (`"expected varchar"`) / error-code name, not the code name in the message. M2 — added e2e IN (`varcharInFilterIsFullyPushedDown`), DOUBLE (`doubleRangeFilterIsFullyPushedDown`), BIGINT eq under skew, and the C2 out-of-range agreement test. Minors — combined config-mapping form, try/catch-removal deviation noted, full constructor-call-site list, all in place.
