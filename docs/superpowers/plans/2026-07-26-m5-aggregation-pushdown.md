# M5 — Aggregation pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Push `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` and single-grouping-set `GROUP BY` into AQL, on a single split, computing over exactly the values the read path would have emitted.

**Architecture:** `ArangoMetadata.applyAggregation` hands the request to a pure gate (`AggregatePushdown`) that either declines or returns an `ArangoAggregation` descriptor stored on `ArangoTableHandle`. `AqlBuilder.buildAggregate` renders that descriptor as `FOR → FILTER → COLLECT/AGGREGATE → LIMIT → RETURN`, wrapping every aggregate input and grouping key in a `ColumnGuard` expression that reproduces `ValueMaterializer`'s coercion exactly. An aggregated handle always produces one split. `ArangoPageSource` and `ValueMaterializer` are untouched — aggregate outputs are ordinary `ArangoColumnHandle`s.

**Tech Stack:** Java 25, Maven, Trino SPI 483, ArangoDB Java driver 7.x, JUnit 6 + AssertJ, Testcontainers (ArangoDB 3.12). No mocking framework — test doubles are hand-written `ArangoClient` subclasses.

**Design spec:** `docs/superpowers/specs/2026-07-26-m5-aggregation-pushdown-design.md`. Section references below (§4/18 etc.) point into it.

## Global Constraints

- **Build:** Java 25 (`maven.compiler.release=25`). `source ~/.sdkman/bin/sdkman-init.sh` first if `mvn` is not found.
- **Docker must be running** for any test that uses `TestingArangoServer`.
- **No mocking framework.** Test doubles are hand-written subclasses (`class X extends ArangoClient { X() { super(new ArangoConfig()); } ... }` — the constructor does not connect).
- **Exactness is the acceptance bar.** A pushed aggregate has no residual re-check. If a rendering cannot be proven identical to the read path, it declines.
- **Every grouping-related AQL assertion runs under both `OPTIONS { method: "hash" }` and `OPTIONS { method: "sorted" }` and asserts they agree** (§4/18 — the two methods genuinely disagree on a bare accessor).
- **New files are enforced by the static-analysis gates** (`mvn spotless:check`, `mvn checkstyle:check`, `mvn compile spotbugs:check`) — they are not grandfathered. Run all three before the final commit.
- **Numeric literals used in guards, verbatim:** `-9223372036854775808` (long min), `9223372036854775808` (2⁶³, exclusive upper bound), `9007199254740992` (2⁵³).

---

## Task 0: Formatting-only prep commit

**Why first:** Spotless is ratcheted `ratchetFrom=origin/master` and is *file-granular*. The six pre-existing main files M5 modifies were hand-formatted during M1–M3 and will be fully reflowed by google-java-format the moment they are touched. Doing that inside the logic commits buries the real diff. A bare `mvn spotless:apply` at the branch point is a **no-op** — the ratchet restricts Spotless to files differing from `origin/master`, and at the branch point none do. The ratchet must be neutralized for this one commit.

**Files:**
- Modify (temporarily): `pom.xml:341-359` (the `spotless-maven-plugin` `<configuration>` block)
- Reformat: `src/main/java/io/arango/trino/ArangoConfig.java`, `src/main/java/io/arango/trino/ArangoMetadata.java`, `src/main/java/io/arango/trino/ArangoSplitManager.java`, `src/main/java/io/arango/trino/ArangoPageSourceProvider.java`, `src/main/java/io/arango/trino/aql/AqlBuilder.java`, `src/main/java/io/arango/trino/handle/ArangoTableHandle.java`, `src/test/java/io/arango/trino/ArangoMetadataTest.java`, `src/test/java/io/arango/trino/ArangoMetadataLimitTest.java`, `src/test/java/io/arango/trino/ArangoSplitManagerTest.java`, `src/test/java/io/arango/trino/ArangoConfigTest.java`, `src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java`, `src/test/java/io/arango/trino/aql/AqlBuilderTest.java`, `src/test/java/io/arango/trino/aql/AqlSemanticsAssumptionsTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing. Behavior must be **byte-identical** — this task changes only whitespace, line wrapping, and import order.

- [ ] **Step 1: Record the pre-formatting test baseline**

```bash
source ~/.sdkman/bin/sdkman-init.sh 2>/dev/null
mvn -q test 2>&1 | tail -20
```

Expected: BUILD SUCCESS. Note the test count — it must be identical at the end of this task.

- [ ] **Step 2: Temporarily neutralize the ratchet and scope Spotless to the M5 files**

In `pom.xml`, inside the `spotless-maven-plugin` `<configuration>` block, comment out the ratchet line and add an `<includes>` list. `<includes>` paths are relative to the project root:

```xml
<configuration>
    <!-- TEMPORARY (M5 Task 0): ratchet disabled + includes scoped so spotless:apply
         reformats exactly the M5 files. REVERT before committing. -->
    <!-- <ratchetFrom>origin/master</ratchetFrom> -->
    <java>
        <includes>
            <include>src/main/java/io/arango/trino/ArangoConfig.java</include>
            <include>src/main/java/io/arango/trino/ArangoMetadata.java</include>
            <include>src/main/java/io/arango/trino/ArangoSplitManager.java</include>
            <include>src/main/java/io/arango/trino/ArangoPageSourceProvider.java</include>
            <include>src/main/java/io/arango/trino/aql/AqlBuilder.java</include>
            <include>src/main/java/io/arango/trino/handle/ArangoTableHandle.java</include>
            <include>src/test/java/io/arango/trino/ArangoMetadataTest.java</include>
            <include>src/test/java/io/arango/trino/ArangoMetadataLimitTest.java</include>
            <include>src/test/java/io/arango/trino/ArangoSplitManagerTest.java</include>
            <include>src/test/java/io/arango/trino/ArangoConfigTest.java</include>
            <include>src/test/java/io/arango/trino/ArangoConnectorPushdownTest.java</include>
            <include>src/test/java/io/arango/trino/aql/AqlBuilderTest.java</include>
            <include>src/test/java/io/arango/trino/aql/AqlSemanticsAssumptionsTest.java</include>
        </includes>
        <googleJavaFormat>
            <version>1.35.0</version>
            <style>AOSP</style>
        </googleJavaFormat>
        <removeUnusedImports/>
        <importOrder/>
        <trimTrailingWhitespace/>
        <endWithNewline/>
    </java>
</configuration>
```

- [ ] **Step 3: Apply the formatter**

```bash
mvn spotless:apply
git diff --stat
```

Expected: several of the 13 files listed with changes. Files already in AOSP style (e.g. `ArangoSplitManagerTest.java`) may show no change — that is correct, not a failure.

- [ ] **Step 4: Restore `pom.xml` exactly**

```bash
git checkout -- pom.xml
git diff --stat -- pom.xml
```

Expected: empty output. `pom.xml` must carry **no** change into this commit.

- [ ] **Step 5: Verify the reformatting changed no behavior**

```bash
mvn -q test 2>&1 | tail -20
```

Expected: BUILD SUCCESS with the same test count as Step 1.

- [ ] **Step 6: Commit**

```bash
git add -A src
git commit -m "style: reformat M5-touched files with google-java-format (AOSP)

No behavior change. The Spotless ratchet is file-granular, so M5's edits to
these hand-formatted M1-M3 files would reflow them anyway; doing it in one
formatting-only commit keeps the M5 logic diff reviewable. A bare
spotless:apply is a no-op at the branch point (the ratchet sees no changed
files), so this was run with ratchetFrom temporarily disabled and includes
scoped to exactly these files."
```

---

## Task 1: Config flag `arangodb.aggregation-pushdown-enabled`

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoConfig.java`
- Test: `src/test/java/io/arango/trino/ArangoConfigTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `ArangoConfig.isAggregationPushdownEnabled()` → `boolean` (default `true`), `ArangoConfig.setAggregationPushdownEnabled(boolean)` → `ArangoConfig` (fluent, for tests). Consumed by Task 5.

- [ ] **Step 1: Write failing test**

Add to `ArangoConfigTest`:

```java
@Test
void aggregationPushdownDefaultsToEnabled() {
    assertThat(new ArangoConfig().isAggregationPushdownEnabled()).isTrue();
}

@Test
void aggregationPushdownCanBeDisabled() {
    assertThat(new ArangoConfig().setAggregationPushdownEnabled(false).isAggregationPushdownEnabled())
            .isFalse();
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=ArangoConfigTest`
Expected: compilation failure — `cannot find symbol: method isAggregationPushdownEnabled()`.

- [ ] **Step 3: Implement**

In `ArangoConfig`, beside `shardParallelismEnabled`:

```java
private boolean aggregationPushdownEnabled = true;

public boolean isAggregationPushdownEnabled() {
    return aggregationPushdownEnabled;
}

@Config("arangodb.aggregation-pushdown-enabled")
@ConfigDescription(
        "Push COUNT/SUM/MIN/MAX/AVG and GROUP BY into AQL on a single split; set false to"
                + " aggregate entirely in Trino")
public ArangoConfig setAggregationPushdownEnabled(boolean aggregationPushdownEnabled) {
    this.aggregationPushdownEnabled = aggregationPushdownEnabled;
    return this;
}
```

- [ ] **Step 4: Run test and verify it passes**

Run: `mvn test -Dtest=ArangoConfigTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoConfig.java src/test/java/io/arango/trino/ArangoConfigTest.java
git commit -m "feat(config): add arangodb.aggregation-pushdown-enabled (default true)"
```

---

## Task 2: `ColumnGuard` — the read-path/pushdown invariant in one place

**Why this is the load-bearing unit:** every exactness claim in the milestone reduces to this class rendering `ValueMaterializer`'s coercion faithfully. It is pure string generation, so it is fully unit-testable without a container.

**Files:**
- Create: `src/main/java/io/arango/trino/aggregation/ColumnGuard.java`
- Test: `src/test/java/io/arango/trino/aggregation/ColumnGuardTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `enum ColumnGuard.Purpose { GROUPING_KEY, SUM_AVG, MIN_MAX }`
  - `static Optional<String> ColumnGuard.predicate(Type type, String accessor)` — boolean AQL expression, empty ⇒ type unsupported
  - `static String ColumnGuard.value(Type type, String accessor, Purpose purpose)` — the value expression
  - `static Optional<String> ColumnGuard.coerce(Type type, String accessor, Purpose purpose)` — `((predicate) ? value : null)`, empty ⇒ decline

  Consumed by Tasks 5 and 6.

- [ ] **Step 1: Write failing test**

Create `src/test/java/io/arango/trino/aggregation/ColumnGuardTest.java`:

```java
package io.arango.trino.aggregation;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.ColumnGuard.Purpose;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ColumnGuardTest {
    private static final String A = "d[\"v\"]";

    @Test
    void predicatesMatchTheReadPathPerType() {
        assertThat(ColumnGuard.predicate(BOOLEAN, A)).contains("IS_BOOL(" + A + ")");
        assertThat(ColumnGuard.predicate(VARCHAR, A)).contains("IS_STRING(" + A + ")");
        assertThat(ColumnGuard.predicate(DOUBLE, A)).contains("IS_NUMBER(" + A + ")");
    }

    // The BIGINT predicate transliterates ValueMaterializer.isIntegralInLongRange: a number,
    // within [-2^63, 2^63), and integral. Integrality cannot use a bare FLOOR test (review
    // finding C3: FLOOR returns a double, so a stored int64 > 2^53 fails it) -- above 2^53 no
    // double can carry a fractional part, so everything there is integral by construction.
    @Test
    void bigintPredicateGuardsRangeAndIntegralityWithoutABareFloorTest() {
        String p = ColumnGuard.predicate(BIGINT, A).orElseThrow();
        assertThat(p)
                .isEqualTo(
                        "IS_NUMBER(d[\"v\"]) AND d[\"v\"] >= -9223372036854775808 AND d[\"v\"] <"
                            + " 9223372036854775808 AND (ABS(d[\"v\"]) >= 9007199254740992 OR"
                            + " d[\"v\"] == FLOOR(d[\"v\"]))");
    }

    // §4/10: COLLECT separates -0.0 from 0.0. The DOUBLE grouping key's `+ 0.0` collapses them,
    // matching Trino's normalization.
    @Test
    void doubleGroupingKeyPromotesToCollapseSignedZero() {
        assertThat(ColumnGuard.value(DOUBLE, A, Purpose.GROUPING_KEY)).isEqualTo("(" + A + " + 0.0)");
        assertThat(ColumnGuard.value(DOUBLE, A, Purpose.SUM_AVG)).isEqualTo("(" + A + " + 0.0)");
    }

    // Review finding S1 / §4/22: `+ 0.0` would turn a stored -0.0 into 0.0, so min/max would
    // disagree with the unpushed plan. Rounding is monotone, so the promotion is unnecessary here.
    @Test
    void minMaxUsesTheBareAccessorSoSignedZeroSurvives() {
        assertThat(ColumnGuard.value(DOUBLE, A, Purpose.MIN_MAX)).isEqualTo(A);
        assertThat(ColumnGuard.value(BIGINT, A, Purpose.MIN_MAX)).isEqualTo(A);
    }

    // Review finding B1 / §4/18-19: a stored double -0.0 passes the BIGINT guard and reads back
    // as 0, but hash-COLLECT groups it separately -- two AQL groups, one Trino key, duplicate
    // output rows. Normalizing by exact numeric equality fixes it under both COLLECT methods.
    @Test
    void bigintGroupingKeyNormalizesSignedZero() {
        assertThat(ColumnGuard.value(BIGINT, A, Purpose.GROUPING_KEY))
                .isEqualTo("(" + A + " == 0 ? 0 : " + A + ")");
    }

    @Test
    void coerceWrapsValueInTheGuardTernary() {
        assertThat(ColumnGuard.coerce(VARCHAR, A, Purpose.GROUPING_KEY))
                .contains("((IS_STRING(" + A + ")) ? " + A + " : null)");
    }

    @Test
    void structuredAndDecimalTypesDecline() {
        assertThat(ColumnGuard.predicate(new ArrayType(BIGINT), A)).isEmpty();
        assertThat(ColumnGuard.predicate(RowType.rowType(RowType.field("f", BIGINT)), A)).isEmpty();
        assertThat(ColumnGuard.predicate(DecimalType.createDecimalType(38, 0), A)).isEmpty();
        assertThat(ColumnGuard.coerce(new ArrayType(BIGINT), A, Purpose.MIN_MAX))
                .isEqualTo(Optional.empty());
    }
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=ColumnGuardTest`
Expected: compilation failure — `package io.arango.trino.aggregation does not exist`.

- [ ] **Step 3: Implement**

Create `src/main/java/io/arango/trino/aggregation/ColumnGuard.java`:

```java
package io.arango.trino.aggregation;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.Optional;

/**
 * The AQL rendering of {@link io.arango.trino.type.ValueMaterializer}'s coercion, and the single
 * place M5's exactness invariant lives: a pushed aggregate must compute over exactly the values the
 * read path would have emitted. {@link #predicate} admits precisely what the read path materializes
 * non-NULL; {@link #coerce} maps everything else to AQL {@code null}, which AQL's aggregates ignore
 * -- matching Trino's own NULL handling.
 *
 * <p>Unlike filter pushdown, aggregation has no residual re-check (Trino replaces the aggregation
 * node outright), so anything rendered here must be exact rather than a superset.
 */
public final class ColumnGuard {
    /**
     * Why the value expression depends on context, when the predicate does not:
     *
     * <ul>
     *   <li>{@code GROUPING_KEY} -- DOUBLE promotes with {@code + 0.0} and BIGINT normalizes signed
     *       zero, because COLLECT puts a stored {@code -0.0} in its own group (design §4/10, §4/18)
     *       while both read back as the same Trino value. Without this, one Trino group would be
     *       emitted as two final rows.
     *   <li>{@code SUM_AVG} -- DOUBLE keeps the {@code + 0.0} promotion, which is the M2 finding-C1
     *       argument unchanged: it makes AQL compare and accumulate in the same double space the
     *       read path's {@code doubleValue()} produces.
     *   <li>{@code MIN_MAX} -- bare accessor. Promotion would turn a stored {@code -0.0} into {@code
     *       0.0} and disagree with the unpushed plan, and it is unnecessary because double rounding
     *       is monotone: the bare extremum, rounded on read, equals the extremum of the rounded
     *       values (design §4/22).
     * </ul>
     */
    public enum Purpose {
        GROUPING_KEY,
        SUM_AVG,
        MIN_MAX
    }

    private ColumnGuard() {}

    /**
     * A boolean AQL expression true for exactly the values the read path materializes non-NULL, or
     * empty when the column's type supports no exact guard (ARRAY/ROW/DECIMAL).
     */
    public static Optional<String> predicate(Type type, String accessor) {
        if (type.equals(BooleanType.BOOLEAN)) {
            return Optional.of("IS_BOOL(" + accessor + ")");
        }
        if (type instanceof VarcharType) {
            return Optional.of("IS_STRING(" + accessor + ")");
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return Optional.of("IS_NUMBER(" + accessor + ")");
        }
        if (type.equals(BigintType.BIGINT)) {
            // ValueMaterializer.isIntegralInLongRange, transliterated: a number, within
            // [-2^63, 2^63), and integral. The integrality test cannot be a bare
            // `v == FLOOR(v)` -- FLOOR returns a double, so a stored int64 above 2^53 fails it
            // (finding C3). Above 2^53 no double can carry a fractional part, so every value
            // there is integral and the FLOOR test is only needed below that threshold.
            return Optional.of(
                    "IS_NUMBER(%s) AND %s >= -9223372036854775808 AND %s < 9223372036854775808"
                            .formatted(accessor, accessor, accessor)
                            + " AND (ABS(%s) >= 9007199254740992 OR %s == FLOOR(%s))"
                                    .formatted(accessor, accessor, accessor));
        }
        return Optional.empty();
    }

    /** The value expression to aggregate or group by, for a type {@link #predicate} admits. */
    public static String value(Type type, String accessor, Purpose purpose) {
        if (type.equals(DoubleType.DOUBLE) && purpose != Purpose.MIN_MAX) {
            return "(" + accessor + " + 0.0)";
        }
        if (type.equals(BigintType.BIGINT) && purpose == Purpose.GROUPING_KEY) {
            return "(" + accessor + " == 0 ? 0 : " + accessor + ")";
        }
        return accessor;
    }

    /** {@code ((predicate) ? value : null)}, or empty when the type supports no exact guard. */
    public static Optional<String> coerce(Type type, String accessor, Purpose purpose) {
        return predicate(type, accessor)
                .map(p -> "((" + p + ") ? " + value(type, accessor, purpose) + " : null)");
    }
}
```

- [ ] **Step 4: Run test and verify it passes**

Run: `mvn test -Dtest=ColumnGuardTest`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/aggregation/ColumnGuard.java src/test/java/io/arango/trino/aggregation/ColumnGuardTest.java
git commit -m "feat(aggregation): add ColumnGuard, the AQL rendering of ValueMaterializer coercion"
```

---

## Task 3: `AggregateSpec` and `ArangoAggregation` descriptor records

**Files:**
- Create: `src/main/java/io/arango/trino/aggregation/AggregateSpec.java`
- Create: `src/main/java/io/arango/trino/aggregation/ArangoAggregation.java`
- Test: `src/test/java/io/arango/trino/aggregation/ArangoAggregationTest.java`

**Interfaces:**
- Consumes: `ArangoColumnHandle` (existing, `io.arango.trino.handle`).
- Produces:
  - `enum AggregateSpec.Kind { COUNT_STAR, COUNT_COLUMN, SUM, MIN, MAX, AVG }`
  - `record AggregateSpec(Kind kind, Optional<ArangoColumnHandle> input, String outputName, Type outputType)`
  - `record ArangoAggregation(List<ArangoColumnHandle> groupingColumns, List<AggregateSpec> aggregates)`

  Both Jackson-serializable — they ride inside `ArangoTableHandle`, which Trino serializes between coordinator and worker. Consumed by Tasks 4, 5, 6, 7.

- [ ] **Step 1: Write failing test**

Create `src/test/java/io/arango/trino/aggregation/ArangoAggregationTest.java`:

```java
package io.arango.trino.aggregation;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.handle.ArangoColumnHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ArangoAggregationTest {
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BIGINT, false, List.of("age"));

    @Test
    void countStarCarriesNoInputColumn() {
        AggregateSpec spec =
                new AggregateSpec(
                        AggregateSpec.Kind.COUNT_STAR, Optional.empty(), "agg_0", BIGINT);
        assertThat(spec.input()).isEmpty();
        assertThat(spec.outputType()).isEqualTo(BIGINT);
    }

    // The kind/input pairing is an invariant AqlBuilder relies on when it switches on the kind:
    // a COUNT_STAR with an input, or a MAX without one, would render nonsense AQL.
    @Test
    void kindAndInputMustAgree() {
        assertThatThrownBy(
                        () ->
                                new AggregateSpec(
                                        AggregateSpec.Kind.COUNT_STAR,
                                        Optional.of(AGE),
                                        "agg_0",
                                        BIGINT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new AggregateSpec(
                                        AggregateSpec.Kind.MAX, Optional.empty(), "agg_0", BIGINT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void componentsAreDefensivelyCopiedAndNonNull() {
        List<ArangoColumnHandle> mutable = new ArrayList<>();
        mutable.add(new ArangoColumnHandle("city", VARCHAR, false, List.of("city")));
        ArangoAggregation aggregation = new ArangoAggregation(mutable, List.of());
        mutable.clear();
        assertThat(aggregation.groupingColumns()).hasSize(1);

        assertThatThrownBy(() -> new ArangoAggregation(null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("groupingColumns");
    }
}
```

**On JSON serialization:** the descriptor rides inside `ArangoTableHandle`, which Trino serializes between coordinator and worker. A unit round-trip would need a `TypeManager` to deserialize the `Type` properties, and `trino-spi` 483 ships no testing deserializer for it (verified: no `TestingTypeDeserializer`/`TestingTypeManager` in the jar) — the real one lives in `trino-main`, which plugins must not depend on. The existing suite has the same gap: `ArangoSplitTest` round-trips `ArangoSplit` only because it carries no `Type`. Serialization is instead covered end-to-end by Task 10's `DistributedQueryRunner` ITs, which serialize handles across in-process worker nodes for real. Do not add a `trino-main` dependency to close this.

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=ArangoAggregationTest`
Expected: compilation failure — `cannot find symbol: class AggregateSpec`.

If `io.trino.spi.type.TestingTypeDeserializer` is not resolvable in this SPI version, replace the codec helper with the deserializer the existing suite already uses for handles; check with `grep -rn "TestingTypeDeserializer\|TypeDeserializer" src/test` and mirror that.

- [ ] **Step 3: Implement**

Create `src/main/java/io/arango/trino/aggregation/AggregateSpec.java`:

```java
package io.arango.trino.aggregation;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.arango.trino.handle.ArangoColumnHandle;
import io.trino.spi.type.Type;
import java.util.Optional;

/**
 * One pushed aggregate: what to compute, over which column, under what output name and type. The
 * output type comes from Trino's {@code AggregateFunction.getOutputType()}, never from the inferred
 * column type -- {@code count} over a VARCHAR column outputs BIGINT.
 */
public record AggregateSpec(
        @JsonProperty("kind") Kind kind,
        @JsonProperty("input") Optional<ArangoColumnHandle> input,
        @JsonProperty("outputName") String outputName,
        @JsonProperty("outputType") Type outputType) {
    public enum Kind {
        /** {@code count(*)} -- no input column, so no coercion surface. */
        COUNT_STAR,
        /** {@code count(col)} -- counts values the read path materializes non-NULL. */
        COUNT_COLUMN,
        SUM,
        MIN,
        MAX,
        AVG
    }

    @JsonCreator
    public AggregateSpec {
        requireNonNull(kind, "kind is null");
        requireNonNull(input, "input is null");
        requireNonNull(outputName, "outputName is null");
        requireNonNull(outputType, "outputType is null");
        // AqlBuilder switches on the kind and dereferences input() for every kind but COUNT_STAR,
        // so the pairing is an invariant rather than a convention.
        boolean expectsInput = kind != Kind.COUNT_STAR;
        if (expectsInput != input.isPresent()) {
            throw new IllegalArgumentException(
                    "COUNT_STAR takes no input column; every other kind requires one (kind=%s,"
                            .formatted(kind)
                            + " input present=%s)".formatted(input.isPresent()));
        }
    }
}
```

Create `src/main/java/io/arango/trino/aggregation/ArangoAggregation.java`:

```java
package io.arango.trino.aggregation;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.arango.trino.handle.ArangoColumnHandle;
import java.util.List;

/**
 * The aggregation pushed onto a table handle. Its presence on {@link
 * io.arango.trino.handle.ArangoTableHandle} <em>is</em> the "aggregated" flag -- there is no
 * separate boolean to keep in sync -- and it is what makes the split manager emit exactly one
 * split: Trino treats connector aggregation output as final, so N splits would emit N duplicate
 * final rows (master spec §6.4).
 *
 * <p>An empty {@code aggregates} list with a non-empty {@code groupingColumns} list is a pushed
 * {@code SELECT DISTINCT} / bare {@code GROUP BY}.
 */
public record ArangoAggregation(
        @JsonProperty("groupingColumns") List<ArangoColumnHandle> groupingColumns,
        @JsonProperty("aggregates") List<AggregateSpec> aggregates) {
    @JsonCreator
    public ArangoAggregation {
        requireNonNull(groupingColumns, "groupingColumns is null");
        requireNonNull(aggregates, "aggregates is null");
        groupingColumns = List.copyOf(groupingColumns);
        aggregates = List.copyOf(aggregates);
    }
}
```

- [ ] **Step 4: Run test and verify it passes**

Run: `mvn test -Dtest=ArangoAggregationTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/aggregation src/test/java/io/arango/trino/aggregation/ArangoAggregationTest.java
git commit -m "feat(aggregation): add AggregateSpec and ArangoAggregation descriptor records"
```

---

## Task 4: Carry the descriptor on `ArangoTableHandle`

**Note on blast radius:** adding a record component breaks every construction site. Task 0 already reformatted these files, so this diff is logic-only.

**Files:**
- Modify: `src/main/java/io/arango/trino/handle/ArangoTableHandle.java`
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java` (the `getTableHandle` construction site)
- Modify (construction sites): `src/test/java/io/arango/trino/ArangoMetadataTest.java`, `src/test/java/io/arango/trino/ArangoMetadataLimitTest.java`, `src/test/java/io/arango/trino/ArangoSplitManagerTest.java`, `src/test/java/io/arango/trino/aql/AqlBuilderTest.java`
- Test: `src/test/java/io/arango/trino/handle/ArangoTableHandleTest.java` (create if absent)

**Interfaces:**
- Consumes: `ArangoAggregation` (Task 3).
- Produces:
  - `ArangoTableHandle(String schema, String table, boolean edge, TupleDomain<ColumnHandle> constraint, OptionalLong limit, Optional<ArangoAggregation> aggregation)` — **6 components now**
  - `ArangoTableHandle.withAggregation(ArangoAggregation)` → `ArangoTableHandle`
  - existing `withConstraint`/`withLimit`/`schemaTableName` unchanged in signature, now preserving `aggregation`

  Consumed by Tasks 5, 6, 7.

- [ ] **Step 1: Write failing test**

Create `src/test/java/io/arango/trino/handle/ArangoTableHandleTest.java`:

```java
package io.arango.trino.handle;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ArangoTableHandleTest {
    private static ArangoTableHandle base() {
        return new ArangoTableHandle(
                "shop",
                "users",
                false,
                TupleDomain.<ColumnHandle>all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    private static ArangoAggregation countStar() {
        return new ArangoAggregation(
                List.of(),
                List.of(
                        new AggregateSpec(
                                AggregateSpec.Kind.COUNT_STAR,
                                Optional.empty(),
                                "agg_0",
                                BigintType.BIGINT)));
    }

    @Test
    void aggregationDefaultsToAbsent() {
        assertThat(base().aggregation()).isEmpty();
    }

    @Test
    void withAggregationSetsItAndPreservesEverythingElse() {
        ArangoTableHandle aggregated = base().withLimit(10).withAggregation(countStar());
        assertThat(aggregated.aggregation()).contains(countStar());
        assertThat(aggregated.schema()).isEqualTo("shop");
        assertThat(aggregated.table()).isEqualTo("users");
        assertThat(aggregated.limit()).hasValue(10);
    }

    // withConstraint/withLimit predate the aggregation component; if either dropped it, an
    // aggregated handle could silently fan out into multiple splits and emit duplicate final rows.
    @Test
    void existingWithersPreserveTheAggregation() {
        ArangoTableHandle aggregated = base().withAggregation(countStar());
        assertThat(aggregated.withLimit(5).aggregation()).contains(countStar());
        assertThat(aggregated.withConstraint(TupleDomain.all()).aggregation()).contains(countStar());
    }
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=ArangoTableHandleTest`
Expected: compilation failure — constructor takes 5 arguments, 6 given.

- [ ] **Step 3: Implement**

Rewrite `ArangoTableHandle` to add the component and the wither, preserving it in the existing withers:

```java
public record ArangoTableHandle(
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("edge") boolean edge,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("limit") OptionalLong limit,
        @JsonProperty("aggregation") Optional<ArangoAggregation> aggregation)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoTableHandle {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(aggregation, "aggregation is null");
    }

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schema, table);
    }

    public ArangoTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new ArangoTableHandle(schema, table, edge, newConstraint, limit, aggregation);
    }

    public ArangoTableHandle withLimit(long newLimit) {
        return new ArangoTableHandle(
                schema, table, edge, constraint, OptionalLong.of(newLimit), aggregation);
    }

    public ArangoTableHandle withAggregation(ArangoAggregation newAggregation) {
        return new ArangoTableHandle(
                schema,
                table,
                edge,
                constraint,
                limit,
                Optional.of(requireNonNull(newAggregation, "newAggregation is null")));
    }
}
```

Add `import io.arango.trino.aggregation.ArangoAggregation;` and `import java.util.Optional;`.

- [ ] **Step 4: Fix every construction site**

```bash
grep -rn "new ArangoTableHandle(" src/
```

Append `Optional.empty()` as the sixth argument at each site — in `ArangoMetadata.getTableHandle` and in the four test files listed above. Add `import java.util.Optional;` where missing.

- [ ] **Step 5: Run the full suite and verify it passes**

Run: `mvn test`
Expected: BUILD SUCCESS, test count = Task 0's baseline + the new tests.

- [ ] **Step 6: Commit**

```bash
git add -A src
git commit -m "feat(handle): carry an optional ArangoAggregation on ArangoTableHandle

Its presence is the 'aggregated' flag -- no separate boolean to keep in sync.
withConstraint/withLimit preserve it: dropping it would let an aggregated
handle fan out into multiple splits and emit duplicate final rows."
```

---

## Task 5: `AggregatePushdown` — the decline gate

**Files:**
- Create: `src/main/java/io/arango/trino/aggregation/AggregatePushdown.java`
- Test: `src/test/java/io/arango/trino/aggregation/AggregatePushdownTest.java`

**Interfaces:**
- Consumes: `ColumnGuard` (Task 2), `AggregateSpec`/`ArangoAggregation` (Task 3), `ArangoTableHandle` (Task 4), `ArangoConfig` (Task 1).
- Produces: `static Optional<ArangoAggregation> AggregatePushdown.plan(ArangoConfig config, ArangoTableHandle handle, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)`. Consumed by Task 7.

- [ ] **Step 1: Write failing test**

Create `src/test/java/io/arango/trino/aggregation/AggregatePushdownTest.java`:

```java
package io.arango.trino.aggregation;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.ArangoConfig;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class AggregatePushdownTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VARCHAR, false, List.of("city"));
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
    private static final ArangoColumnHandle SCORE =
            new ArangoColumnHandle("score", DOUBLE, false, List.of("score"));
    private static final ArangoColumnHandle ACTIVE =
            new ArangoColumnHandle("active", BOOLEAN, false, List.of("active"));

    private static final Map<String, ColumnHandle> ASSIGNMENTS =
            Map.of("city", CITY, "age", AGE, "score", SCORE, "active", ACTIVE);

    private static ArangoTableHandle handle() {
        return new ArangoTableHandle(
                "shop",
                "users",
                false,
                TupleDomain.<ColumnHandle>all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    private static AggregateFunction fn(String name, Type outputType, ArangoColumnHandle input) {
        List<ConnectorExpression> args =
                input == null ? List.of() : List.of(new Variable(input.name(), input.type()));
        return new AggregateFunction(name, outputType, args, List.of(), false, Optional.empty());
    }

    private static Optional<ArangoAggregation> plan(
            List<AggregateFunction> aggregates, List<List<ColumnHandle>> groupingSets) {
        return AggregatePushdown.plan(
                new ArangoConfig(), handle(), aggregates, ASSIGNMENTS, groupingSets);
    }

    private static Optional<ArangoAggregation> global(List<AggregateFunction> aggregates) {
        return plan(aggregates, List.of(List.of()));
    }

    // --- claimed shapes -------------------------------------------------------------------

    @Test
    void countStarPushesGlobally() {
        ArangoAggregation agg = global(List.of(fn("count", BIGINT, null))).orElseThrow();
        assertThat(agg.groupingColumns()).isEmpty();
        assertThat(agg.aggregates())
                .singleElement()
                .satisfies(
                        s -> {
                            assertThat(s.kind()).isEqualTo(AggregateSpec.Kind.COUNT_STAR);
                            assertThat(s.outputName()).isEqualTo("agg_0");
                            assertThat(s.outputType()).isEqualTo(BIGINT);
                        });
    }

    @Test
    void countOfEveryGuardableColumnTypePushes() {
        for (ArangoColumnHandle column : List.of(CITY, AGE, SCORE, ACTIVE)) {
            assertThat(global(List.of(fn("count", BIGINT, column))))
                    .as("count(%s)", column.name())
                    .isPresent();
        }
    }

    @Test
    void minMaxPushOnBigintAndDouble() {
        assertThat(global(List.of(fn("min", BIGINT, AGE)))).isPresent();
        assertThat(global(List.of(fn("max", DOUBLE, SCORE)))).isPresent();
    }

    @Test
    void sumAndAvgPushOnDouble() {
        assertThat(global(List.of(fn("sum", DOUBLE, SCORE)))).isPresent();
        assertThat(global(List.of(fn("avg", DOUBLE, SCORE)))).isPresent();
    }

    @Test
    void groupedAggregatePushesAndKeepsGroupingColumns() {
        ArangoAggregation agg =
                plan(List.of(fn("count", BIGINT, null)), List.of(List.of(CITY))).orElseThrow();
        assertThat(agg.groupingColumns()).containsExactly(CITY);
    }

    // SELECT DISTINCT city / bare GROUP BY city: zero aggregates, one grouping set (design §5).
    @Test
    void zeroAggregateGroupingPushes() {
        ArangoAggregation agg = plan(List.of(), List.of(List.of(CITY))).orElseThrow();
        assertThat(agg.aggregates()).isEmpty();
        assertThat(agg.groupingColumns()).containsExactly(CITY);
    }

    @Test
    void aggregateOutputNamesAreUniqueAndDoNotCollideWithGroupingColumns() {
        ArangoColumnHandle agg0 = new ArangoColumnHandle("agg_0", VARCHAR, false, List.of("agg_0"));
        Optional<ArangoAggregation> planned =
                AggregatePushdown.plan(
                        new ArangoConfig(),
                        handle(),
                        List.of(fn("count", BIGINT, null)),
                        Map.of("agg_0", agg0),
                        List.of(List.of(agg0)));
        assertThat(planned.orElseThrow().aggregates().get(0).outputName()).isNotEqualTo("agg_0");
    }

    // --- declines -------------------------------------------------------------------------

    @Test
    void declinesWhenDisabledByConfig() {
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig().setAggregationPushdownEnabled(false),
                                handle(),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // Strict mode must raise ARANGODB_TYPE_CONVERSION_ERROR on a mismatch; a pushed aggregate
    // would silently absorb it. Mirrors ArangoMetadata.isPushable's strict-mode decline.
    @Test
    void declinesUnderStrictCoercion() {
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig().setTypeCoercion(ArangoConfig.TypeCoercion.STRICT),
                                handle(),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    @Test
    void declinesOnAnAlreadyAggregatedHandle() {
        ArangoTableHandle aggregated =
                handle().withAggregation(
                                new ArangoAggregation(
                                        List.of(),
                                        List.of(
                                                new AggregateSpec(
                                                        AggregateSpec.Kind.COUNT_STAR,
                                                        Optional.empty(),
                                                        "agg_0",
                                                        BIGINT))));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                aggregated,
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // LIMIT-then-GROUP BY is not GROUP BY-then-LIMIT, and the single-FOR AQL body expresses only
    // the latter.
    @Test
    void declinesWhenALimitIsAlreadyPushed() {
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle().withLimit(10),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // A prefilter-only domain (BIGINT range) is enforced jointly by AQL and Trino's residual
    // re-check; aggregating over the AQL side alone would include rows the residual would drop.
    @Test
    void declinesOverAPrefilterOnlyConstraint() {
        TupleDomain<ColumnHandle> bigintRange =
                TupleDomain.withColumnDomains(
                        Map.of(
                                AGE,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(BIGINT, 21L)), false)));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle().withConstraint(bigintRange),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    @Test
    void declinesMultipleGroupingSets() {
        assertThat(plan(List.of(fn("count", BIGINT, null)), List.of(List.of(CITY), List.of())))
                .isEmpty();
    }

    @Test
    void declinesGlobalAggregationWithNoAggregateFunctions() {
        assertThat(plan(List.of(), List.of(List.of()))).isEmpty();
    }

    @Test
    void declinesDistinctFilteredAndOrderedAggregates() {
        AggregateFunction distinct =
                new AggregateFunction(
                        "count",
                        BIGINT,
                        List.of(new Variable("age", BIGINT)),
                        List.of(),
                        true,
                        Optional.empty());
        AggregateFunction filtered =
                new AggregateFunction(
                        "count",
                        BIGINT,
                        List.of(new Variable("age", BIGINT)),
                        List.of(),
                        false,
                        Optional.of(new Variable("active", BOOLEAN)));
        AggregateFunction ordered =
                new AggregateFunction(
                        "max",
                        BIGINT,
                        List.of(new Variable("age", BIGINT)),
                        List.of(new SortItem(AGE, SortItem.SortOrder.ASC_NULLS_FIRST)),
                        false,
                        Optional.empty());
        assertThat(global(List.of(distinct))).isEmpty();
        assertThat(global(List.of(filtered))).isEmpty();
        assertThat(global(List.of(ordered))).isEmpty();
    }

    @Test
    void declinesUnknownFunctionNames() {
        assertThat(global(List.of(fn("approx_distinct", BIGINT, AGE)))).isEmpty();
        assertThat(global(List.of(fn("count_if", BIGINT, ACTIVE)))).isEmpty();
    }

    @Test
    void declinesNonVariableArguments() {
        AggregateFunction constantArg =
                new AggregateFunction(
                        "count",
                        BIGINT,
                        List.of(new Constant(1L, BIGINT)),
                        List.of(),
                        false,
                        Optional.empty());
        assertThat(global(List.of(constantArg))).isEmpty();
    }

    // ArangoDB orders strings by the server's collation; Trino orders by codepoint (design §5).
    @Test
    void declinesMinMaxOnVarchar() {
        assertThat(global(List.of(fn("min", VARCHAR, CITY)))).isEmpty();
        assertThat(global(List.of(fn("max", VARCHAR, CITY)))).isEmpty();
    }

    // AQL accumulates sums in double: precision is lost past 2^53 and Trino's loud
    // sum(bigint) overflow becomes silent (design §4/15).
    @Test
    void declinesSumAndAvgOnBigint() {
        assertThat(global(List.of(fn("sum", BIGINT, AGE)))).isEmpty();
        assertThat(global(List.of(fn("avg", DOUBLE, AGE)))).isEmpty();
    }

    @Test
    void declinesMinMaxOnBoolean() {
        assertThat(global(List.of(fn("min", BOOLEAN, ACTIVE)))).isEmpty();
    }

    @Test
    void declinesUnguardableGroupingKeyTypes() {
        ArangoColumnHandle tags =
                new ArangoColumnHandle(
                        "tags", new io.trino.spi.type.ArrayType(BIGINT), false, List.of("tags"));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle(),
                                List.of(fn("count", BIGINT, null)),
                                Map.of("tags", tags),
                                List.of(List.of(tags))))
                .isEmpty();
    }

    // All-or-nothing: one unsupported aggregate declines the whole call (base-JDBC's contract).
    @Test
    void oneUnsupportedAggregateDeclinesTheWholeCall() {
        assertThat(global(List.of(fn("count", BIGINT, null), fn("sum", BIGINT, AGE)))).isEmpty();
    }
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=AggregatePushdownTest`
Expected: compilation failure — `cannot find symbol: class AggregatePushdown`.

- [ ] **Step 3: Implement**

Create `src/main/java/io/arango/trino/aggregation/AggregatePushdown.java`:

```java
package io.arango.trino.aggregation;

import io.arango.trino.ArangoConfig;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Decides what aggregation may be pushed, and declines everything else. Pure: no client, no session,
 * no I/O, so the entire decline matrix is unit-testable.
 *
 * <p>The bar is higher than for filters. {@code applyFilter} may push a prefilter and let Trino
 * re-check the residual; aggregation has no residual, because Trino replaces the aggregation node
 * and treats the connector's output as final. Anything claimed here must therefore be exactly right
 * -- a wrong aggregate is simply a wrong answer.
 */
public final class AggregatePushdown {
    private AggregatePushdown() {}

    public static Optional<ArangoAggregation> plan(
            ArangoConfig config,
            ArangoTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        if (!config.isAggregationPushdownEnabled()) {
            return Optional.empty();
        }
        // Strict coercion: a pushed aggregate silently absorbs the type mismatch that strict mode
        // exists to raise as ARANGODB_TYPE_CONVERSION_ERROR. Mirrors ArangoMetadata.isPushable.
        if (config.getTypeCoercion() == ArangoConfig.TypeCoercion.STRICT) {
            return Optional.empty();
        }
        // Trino may call the hook again on the handle it just returned; a second push would
        // aggregate an aggregate.
        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }
        // LIMIT n then GROUP BY is not GROUP BY then LIMIT n, and the single-FOR AQL body can
        // only express the latter (the same reason applyFilter declines on a limited handle).
        if (handle.limit().isPresent()) {
            return Optional.empty();
        }
        // A prefilter-only domain is enforced jointly by the pushed AQL and Trino's residual
        // re-check. Aggregating over the AQL side alone would include rows -- fractional or
        // out-of-long-range values in the filter column -- that the residual would have dropped.
        // Trino's planner is not expected to offer this shape, but this makes it a local
        // guarantee rather than one that depends on PushAggregationIntoTableScan's pattern.
        if (hasPrefilterOnlyDomain(handle)) {
            return Optional.empty();
        }
        if (groupingSets.size() != 1) {
            return Optional.empty();
        }

        List<ColumnHandle> groupingSet = groupingSets.get(0);
        if (aggregates.isEmpty() && groupingSet.isEmpty()) {
            return Optional.empty(); // global aggregation with nothing to aggregate
        }

        List<ArangoColumnHandle> groupingColumns = new ArrayList<>();
        for (ColumnHandle column : groupingSet) {
            if (!(column instanceof ArangoColumnHandle arangoColumn)
                    || ColumnGuard.predicate(arangoColumn.type(), "x").isEmpty()) {
                return Optional.empty();
            }
            groupingColumns.add(arangoColumn);
        }

        Set<String> takenNames = new LinkedHashSet<>();
        groupingColumns.forEach(c -> takenNames.add(c.name()));

        List<AggregateSpec> specs = new ArrayList<>();
        for (int i = 0; i < aggregates.size(); i++) {
            Optional<AggregateSpec> spec =
                    specFor(aggregates.get(i), assignments, uniqueName(i, takenNames));
            if (spec.isEmpty()) {
                return Optional.empty(); // all-or-nothing, as base-JDBC does
            }
            takenNames.add(spec.get().outputName());
            specs.add(spec.get());
        }
        return Optional.of(new ArangoAggregation(groupingColumns, specs));
    }

    private static boolean hasPrefilterOnlyDomain(ArangoTableHandle handle) {
        return handle.constraint()
                .getDomains()
                .map(
                        domains ->
                                domains.entrySet().stream()
                                        .anyMatch(
                                                e ->
                                                        e.getKey() instanceof ArangoColumnHandle c
                                                                && isPrefilterOnly(
                                                                        c.type(), e.getValue())))
                .orElse(false);
    }

    // Kept in step with ArangoMetadata.isPrefilterOnly: BIGINT range is the only pushed shape
    // whose AQL form admits a superset of what the read path materializes.
    private static boolean isPrefilterOnly(Type type, Domain domain) {
        return type.equals(BigintType.BIGINT) && !domain.getValues().isDiscreteSet();
    }

    private static String uniqueName(int ordinal, Set<String> taken) {
        String candidate = "agg_" + ordinal;
        for (int suffix = 1; taken.contains(candidate); suffix++) {
            candidate = "agg_" + ordinal + "_" + suffix;
        }
        return candidate;
    }

    private static Optional<AggregateSpec> specFor(
            AggregateFunction function, Map<String, ColumnHandle> assignments, String outputName) {
        if (function.isDistinct()
                || function.getFilter().isPresent()
                || !function.getSortItems().isEmpty()) {
            return Optional.empty();
        }
        List<ConnectorExpression> arguments = function.getArguments();
        String name = function.getFunctionName();

        if (arguments.isEmpty()) {
            return "count".equals(name)
                    ? Optional.of(
                            new AggregateSpec(
                                    AggregateSpec.Kind.COUNT_STAR,
                                    Optional.empty(),
                                    outputName,
                                    function.getOutputType()))
                    : Optional.empty();
        }
        if (arguments.size() != 1
                || !(arguments.get(0) instanceof Variable variable)
                || !(assignments.get(variable.getName()) instanceof ArangoColumnHandle input)) {
            return Optional.empty();
        }

        Type type = input.type();
        // An explicit allowlist rather than a fall-through chain, so an unrecognized function
        // (approx_distinct, count_if, arbitrary, ...) declines by construction.
        AggregateSpec.Kind kind =
                switch (name) {
                    // count needs only a guard predicate: no ordering, no accumulation.
                    case "count" ->
                            ColumnGuard.predicate(type, "x").isPresent()
                                    ? AggregateSpec.Kind.COUNT_COLUMN
                                    : null;
                    // min/max compare, so VARCHAR is out (server collation vs Trino codepoint)
                    // and BOOLEAN is out by scope decision.
                    case "min" -> isMinMaxable(type) ? AggregateSpec.Kind.MIN : null;
                    case "max" -> isMinMaxable(type) ? AggregateSpec.Kind.MAX : null;
                    // sum/avg accumulate, and AQL accumulates in double: BIGINT would lose
                    // precision past 2^53 and turn Trino's loud overflow into a silent one.
                    case "sum" -> type.equals(DoubleType.DOUBLE) ? AggregateSpec.Kind.SUM : null;
                    case "avg" -> type.equals(DoubleType.DOUBLE) ? AggregateSpec.Kind.AVG : null;
                    default -> null;
                };
        return kind == null
                ? Optional.empty()
                : Optional.of(
                        new AggregateSpec(
                                kind, Optional.of(input), outputName, function.getOutputType()));
    }

    private static boolean isMinMaxable(Type type) {
        return type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE);
    }
}
```

- [ ] **Step 4: Run test and verify it passes**

Run: `mvn test -Dtest=AggregatePushdownTest`
Expected: PASS. If `SortItem.SortOrder.ASC_NULLS_FIRST` does not resolve, check the actual enum with `javap -cp ~/.m2/repository/io/trino/trino-spi/483/trino-spi-483.jar io.trino.spi.connector.SortItem` and adjust the test.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/aggregation/AggregatePushdown.java src/test/java/io/arango/trino/aggregation/AggregatePushdownTest.java
git commit -m "feat(aggregation): add AggregatePushdown, the exactness/decline gate"
```

---

## Task 6: `AqlBuilder.buildAggregate`

**Files:**
- Modify: `src/main/java/io/arango/trino/aql/AqlBuilder.java`
- Test: `src/test/java/io/arango/trino/aql/AqlBuilderAggregateTest.java` (create)

**Interfaces:**
- Consumes: `ArangoAggregation`/`AggregateSpec` (Task 3), `ColumnGuard` (Task 2), `ArangoTableHandle` (Task 4).
- Produces: `AqlQuery AqlBuilder.buildAggregate(ArangoTableHandle table, List<ArangoColumnHandle> columns)`. Consumed by Task 7.

**Rendering contract (design §7):**

| Trino | `AGGREGATE` term | `RETURN` expression |
|---|---|---|
| `count()` | `aN = LENGTH(1)` | `aN` |
| `count(col)` | `aN = SUM(pred ? 1 : 0)` | `(aN == null ? 0 : aN)` |
| `min(col)` | `aN = MIN(coerce)` | `aN` |
| `max(col)` | `aN = MAX(coerce)` | `aN` |
| `avg(col)` | `aN = AVERAGE(coerce)` | `aN` |
| `sum(col)` | `aN = SUM(coerce)`, `aNn = SUM(pred ? 1 : 0)` | `(aNn > 0 ? aN : null)` |

- [ ] **Step 1: Write failing test**

Create `src/test/java/io/arango/trino/aql/AqlBuilderAggregateTest.java`:

```java
package io.arango.trino.aql;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class AqlBuilderAggregateTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VARCHAR, false, List.of("city"));
    private static final ArangoColumnHandle SCORE =
            new ArangoColumnHandle("score", DOUBLE, false, List.of("score"));
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BIGINT, false, List.of("age"));

    private static ArangoColumnHandle output(String name) {
        return new ArangoColumnHandle(name, BIGINT, false, List.of(name));
    }

    private static ArangoTableHandle aggregated(ArangoAggregation aggregation) {
        return new ArangoTableHandle(
                        "shop",
                        "users",
                        false,
                        TupleDomain.<ColumnHandle>all(),
                        OptionalLong.empty(),
                        Optional.empty())
                .withAggregation(aggregation);
    }

    private static AggregateSpec spec(
            AggregateSpec.Kind kind, ArangoColumnHandle input, String name) {
        return new AggregateSpec(
                kind, Optional.ofNullable(input), name, kind == AggregateSpec.Kind.AVG ? DOUBLE : BIGINT);
    }

    @Test
    void globalCountStarUsesLengthAndReturnsItDirectly() {
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(
                                aggregated(
                                        new ArangoAggregation(
                                                List.of(),
                                                List.of(
                                                        spec(
                                                                AggregateSpec.Kind.COUNT_STAR,
                                                                null,
                                                                "agg_0")))),
                                List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = LENGTH(1) RETURN {\"agg_0\": a0}");
        assertThat(q.bindVars()).containsEntry("@col", "users");
    }

    // AQL COUNT is an alias of LENGTH and counts nulls, so count(col) must sum a guard predicate.
    // AQL SUM over zero rows is null, so an empty table would report NULL instead of 0 without
    // the wrap (design §4/1, §4/3).
    @Test
    void countOfColumnSumsTheGuardAndWrapsAgainstNull() {
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(
                                aggregated(
                                        new ArangoAggregation(
                                                List.of(),
                                                List.of(
                                                        spec(
                                                                AggregateSpec.Kind.COUNT_COLUMN,
                                                                CITY,
                                                                "agg_0")))),
                                List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = SUM((IS_STRING(d[\"city\"])) ? 1 :"
                            + " 0) RETURN {\"agg_0\": (a0 == null ? 0 : a0)}");
    }

    // AQL SUM of an all-null group is 0 where SQL says NULL, so a companion count converts it
    // (design §4/2). null > 0 is false, which also covers the empty-table case (§4/23).
    @Test
    void sumCarriesACompanionCountToDistinguishZeroFromNull() {
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(
                                aggregated(
                                        new ArangoAggregation(
                                                List.of(),
                                                List.of(
                                                        spec(
                                                                AggregateSpec.Kind.SUM,
                                                                SCORE,
                                                                "agg_0")))),
                                List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = SUM((IS_NUMBER(d[\"score\"])) ?"
                            + " (d[\"score\"] + 0.0) : null), a0n = SUM((IS_NUMBER(d[\"score\"])) ?"
                            + " 1 : 0) RETURN {\"agg_0\": (a0n > 0 ? a0 : null)}");
    }

    @Test
    void minMaxOnDoubleDoNotPromote() {
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(
                                aggregated(
                                        new ArangoAggregation(
                                                List.of(),
                                                List.of(
                                                        spec(
                                                                AggregateSpec.Kind.MIN,
                                                                SCORE,
                                                                "agg_0")))),
                                List.of(output("agg_0")));
        assertThat(q.aql()).contains("MIN((IS_NUMBER(d[\"score\"])) ? d[\"score\"] : null)");
        assertThat(q.aql()).doesNotContain("+ 0.0");
    }

    // Review finding B1: a bare BIGINT grouping key puts a stored -0.0 in its own hash-COLLECT
    // group, which then materializes to the same Trino key 0 -- duplicate output rows.
    @Test
    void bigintGroupingKeyNormalizesSignedZero() {
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(
                                aggregated(
                                        new ArangoAggregation(
                                                List.of(AGE),
                                                List.of(
                                                        spec(
                                                                AggregateSpec.Kind.COUNT_STAR,
                                                                null,
                                                                "agg_0")))),
                                List.of(AGE, output("agg_0")));
        assertThat(q.aql())
                .contains("COLLECT g0 = ((" )
                .contains("d[\"age\"] == 0 ? 0 : d[\"age\"]")
                .contains("RETURN {\"age\": g0, \"agg_0\": a0}");
    }

    // Synthetic COLLECT variable names exist so a column name that is not a legal AQL identifier
    // (applyProjection's nested address$city) never has to be one.
    @Test
    void usesSyntheticVariableNamesButRealObjectKeys() {
        ArangoColumnHandle nested =
                new ArangoColumnHandle(
                        "address$city", VARCHAR, false, List.of("address", "city"));
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(
                                aggregated(new ArangoAggregation(List.of(nested), List.of())),
                                List.of(nested));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT g0 = ((IS_STRING(d[\"address\"][\"city\"])) ?"
                            + " d[\"address\"][\"city\"] : null) RETURN {\"address$city\": g0}");
    }

    @Test
    void clauseOrderIsFilterThenCollectThenLimit() {
        TupleDomain<ColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        Map.of(
                                SCORE,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false)));
        ArangoTableHandle handle =
                aggregated(
                                new ArangoAggregation(
                                        List.of(CITY),
                                        List.of(spec(AggregateSpec.Kind.COUNT_STAR, null, "agg_0"))))
                        .withConstraint(constraint)
                        .withLimit(5);
        AqlQuery q = new AqlBuilder().buildAggregate(handle, List.of(CITY, output("agg_0")));
        assertThat(q.aql())
                .matches("FOR d IN @@col FILTER .*COLLECT g0 = .*AGGREGATE a0 = LENGTH\\(1\\) LIMIT 5 RETURN \\{.*\\}");
        assertThat(q.bindVars()).containsEntry("v0", 1.0);
    }

    // Trino may prune aggregate outputs it does not need, and the sum companion count is never a
    // requested column -- so RETURN is driven by the requested columns, not by the descriptor.
    @Test
    void returnsOnlyRequestedColumnsInRequestedOrder() {
        ArangoAggregation aggregation =
                new ArangoAggregation(
                        List.of(CITY),
                        List.of(
                                spec(AggregateSpec.Kind.COUNT_STAR, null, "agg_0"),
                                spec(AggregateSpec.Kind.MAX, AGE, "agg_1")));
        AqlQuery q =
                new AqlBuilder()
                        .buildAggregate(aggregated(aggregation), List.of(output("agg_1"), CITY));
        assertThat(q.aql()).endsWith("RETURN {\"agg_1\": a1, \"city\": g0}");
        assertThat(q.aql()).contains("AGGREGATE a0 = LENGTH(1), a1 = MAX(");
    }
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=AqlBuilderAggregateTest`
Expected: compilation failure — `cannot find symbol: method buildAggregate`.

- [ ] **Step 3: Implement**

In `AqlBuilder`, first extract the shared `FOR`/`FILTER` prefix out of `buildScan` so both paths use it, then add `buildAggregate`:

```java
    // Shared by buildScan and buildAggregate: "FOR d IN @@col" plus the pushed FILTER, if any.
    private StringBuilder scanPrefix(
            ArangoTableHandle table, Map<String, Object> bindVars, int[] counter) {
        bindVars.put("@col", table.table());
        StringBuilder aql = new StringBuilder("FOR d IN @@col");
        List<String> filters = new ArrayList<>();
        table.constraint()
                .getDomains()
                .ifPresent(
                        domains -> {
                            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                                ArangoColumnHandle column = (ArangoColumnHandle) entry.getKey();
                                filters.add(
                                        "("
                                                + renderDomain(
                                                        column, entry.getValue(), bindVars, counter)
                                                + ")");
                            }
                        });
        if (!filters.isEmpty()) {
            aql.append(" FILTER ").append(String.join(" AND ", filters));
        }
        return aql;
    }

    /**
     * Renders a pushed aggregation as {@code FOR -> FILTER -> COLLECT/AGGREGATE -> LIMIT ->
     * RETURN}. Every aggregate input and grouping key goes through {@link ColumnGuard}, so the
     * query computes over exactly the values {@code ValueMaterializer} would have emitted.
     *
     * <p>The AGGREGATE terms come from the handle's descriptor, not from {@code columns}: the sum
     * companion counts are never requested columns, and Trino may prune aggregate outputs. The
     * RETURN object is built from {@code columns} instead, in their order, because {@code
     * ArangoPageSource} looks each value up by the column's name.
     */
    public AqlQuery buildAggregate(ArangoTableHandle table, List<ArangoColumnHandle> columns) {
        ArangoAggregation aggregation =
                table.aggregation()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "buildAggregate called on a non-aggregated handle"));
        Map<String, Object> bindVars = new LinkedHashMap<>();
        int[] counter = {0};
        StringBuilder aql = scanPrefix(table, bindVars, counter);

        // Synthetic variable names (g0..., a0...) so a column name that is not a legal AQL
        // identifier -- e.g. applyProjection's nested "address$city" -- never has to be one.
        Map<String, String> returnExpressions = new LinkedHashMap<>();
        List<String> groupTerms = new ArrayList<>();
        List<ArangoColumnHandle> groupingColumns = aggregation.groupingColumns();
        for (int i = 0; i < groupingColumns.size(); i++) {
            ArangoColumnHandle column = groupingColumns.get(i);
            String variable = "g" + i;
            groupTerms.add(
                    variable
                            + " = "
                            + ColumnGuard.coerce(
                                            column.type(),
                                            documentAccessor(column.path()),
                                            ColumnGuard.Purpose.GROUPING_KEY)
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            "unguardable grouping column: "
                                                                    + column.name())));
            returnExpressions.put(column.name(), variable);
        }

        List<String> aggregateTerms = new ArrayList<>();
        List<AggregateSpec> specs = aggregation.aggregates();
        for (int i = 0; i < specs.size(); i++) {
            AggregateSpec spec = specs.get(i);
            String variable = "a" + i;
            String accessor = spec.input().map(c -> documentAccessor(c.path())).orElse(null);
            Type inputType = spec.input().map(ArangoColumnHandle::type).orElse(null);
            switch (spec.kind()) {
                case COUNT_STAR -> {
                    aggregateTerms.add(variable + " = LENGTH(1)");
                    returnExpressions.put(spec.outputName(), variable);
                }
                case COUNT_COLUMN -> {
                    aggregateTerms.add(
                            variable + " = SUM(" + countTerm(inputType, accessor) + ")");
                    // AQL SUM over zero rows is null; Trino's count of an empty table is 0.
                    returnExpressions.put(
                            spec.outputName(),
                            "(" + variable + " == null ? 0 : " + variable + ")");
                }
                case SUM -> {
                    String companion = variable + "n";
                    aggregateTerms.add(
                            variable
                                    + " = SUM("
                                    + coerceOrThrow(inputType, accessor, ColumnGuard.Purpose.SUM_AVG)
                                    + ")");
                    aggregateTerms.add(companion + " = SUM(" + countTerm(inputType, accessor) + ")");
                    // AQL SUM of an all-null group is 0 where SQL says NULL; the companion count
                    // distinguishes them. `null > 0` is false, so the empty-table case is covered.
                    returnExpressions.put(
                            spec.outputName(),
                            "(" + companion + " > 0 ? " + variable + " : null)");
                }
                case AVG -> {
                    aggregateTerms.add(
                            variable
                                    + " = AVERAGE("
                                    + coerceOrThrow(inputType, accessor, ColumnGuard.Purpose.SUM_AVG)
                                    + ")");
                    returnExpressions.put(spec.outputName(), variable);
                }
                case MIN, MAX -> {
                    String function = spec.kind() == AggregateSpec.Kind.MIN ? "MIN" : "MAX";
                    aggregateTerms.add(
                            variable
                                    + " = "
                                    + function
                                    + "("
                                    + coerceOrThrow(inputType, accessor, ColumnGuard.Purpose.MIN_MAX)
                                    + ")");
                    returnExpressions.put(spec.outputName(), variable);
                }
            }
        }

        aql.append(" COLLECT");
        if (!groupTerms.isEmpty()) {
            aql.append(' ').append(String.join(", ", groupTerms));
        }
        if (!aggregateTerms.isEmpty()) {
            aql.append(" AGGREGATE ").append(String.join(", ", aggregateTerms));
        }
        table.limit().ifPresent(limit -> aql.append(" LIMIT ").append(limit));

        StringBuilder returnClause = new StringBuilder("{");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                returnClause.append(", ");
            }
            ArangoColumnHandle column = columns.get(i);
            String expression = returnExpressions.get(column.name());
            if (expression == null) {
                throw new IllegalArgumentException(
                        "column not produced by the pushed aggregation: " + column.name());
            }
            returnClause.append(quoteAqlString(column.name())).append(": ").append(expression);
        }
        aql.append(" RETURN ").append(returnClause.append("}"));
        return new AqlQuery(aql.toString(), bindVars);
    }

    // The 0/1 term a count sums: AQL COUNT is an alias of LENGTH and would count nulls.
    private static String countTerm(Type type, String accessor) {
        return "("
                + ColumnGuard.predicate(type, accessor)
                        .orElseThrow(
                                () -> new IllegalArgumentException("unguardable count input"))
                + ") ? 1 : 0";
    }

    private static String coerceOrThrow(Type type, String accessor, ColumnGuard.Purpose purpose) {
        return ColumnGuard.coerce(type, accessor, purpose)
                .orElseThrow(() -> new IllegalArgumentException("unguardable aggregate input"));
    }
```

Rewrite `buildScan`'s opening to use `scanPrefix`:

```java
    public AqlQuery buildScan(ArangoTableHandle table, List<ArangoColumnHandle> columns) {
        Map<String, Object> bindVars = new LinkedHashMap<>();
        int[] counter = {0};
        StringBuilder aql = scanPrefix(table, bindVars, counter);
        table.limit().ifPresent(limit -> aql.append(" LIMIT ").append(limit));
        aql.append(" RETURN ").append(buildReturnClause(columns));
        return new AqlQuery(aql.toString(), bindVars);
    }
```

Add imports: `io.arango.trino.aggregation.AggregateSpec`, `io.arango.trino.aggregation.ArangoAggregation`, `io.arango.trino.aggregation.ColumnGuard`, `io.trino.spi.type.Type`.

- [ ] **Step 4: Run tests and verify they pass**

Run: `mvn test -Dtest='AqlBuilderTest+AqlBuilderAggregateTest'`
Expected: PASS for both — `AqlBuilderTest` proves the `scanPrefix` extraction changed no existing output.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/aql/AqlBuilder.java src/test/java/io/arango/trino/aql/AqlBuilderAggregateTest.java
git commit -m "feat(aql): render pushed aggregation as COLLECT/AGGREGATE with guarded inputs"
```

---

## Task 7: `ArangoMetadata.applyAggregation` and the reciprocal declines

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoMetadata.java`
- Test: `src/test/java/io/arango/trino/ArangoMetadataAggregationTest.java` (create)

**Interfaces:**
- Consumes: `AggregatePushdown.plan` (Task 5), `ArangoTableHandle.withAggregation` (Task 4).
- Produces: `applyAggregation` override; `applyFilter`/`applyProjection` declining on aggregated handles; `applyLimit` reporting `limitGuaranteed = true` for aggregated handles.

- [ ] **Step 1: Write failing test**

Create `src/test/java/io/arango/trino/ArangoMetadataAggregationTest.java`:

```java
package io.arango.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ArangoMetadataAggregationTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VARCHAR, false, List.of("city"));

    private static ArangoMetadata metadata(ArangoConfig config) {
        return new ArangoMetadata(new ArangoClientStub(), null, config);
    }

    // No container needed: these paths never touch the client. The hand-written subclass matches
    // the suite's convention of test doubles over a mocking framework.
    private static final class ArangoClientStub extends io.arango.trino.client.ArangoClient {
        ArangoClientStub() {
            super(new ArangoConfig());
        }
    }

    private static ArangoTableHandle handle() {
        return new ArangoTableHandle(
                "shop",
                "users",
                false,
                TupleDomain.<ColumnHandle>all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    private static ArangoTableHandle aggregatedHandle() {
        return handle().withAggregation(
                        new ArangoAggregation(
                                List.of(),
                                List.of(
                                        new AggregateSpec(
                                                AggregateSpec.Kind.COUNT_STAR,
                                                Optional.empty(),
                                                "agg_0",
                                                BIGINT))));
    }

    private static AggregateFunction countStar() {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty());
    }

    @Test
    void applyAggregationProducesAnAggregatedHandleWithMatchingProjections() {
        AggregationApplicationResult<ConnectorTableHandle> result =
                metadata(new ArangoConfig())
                        .applyAggregation(
                                null,
                                handle(),
                                List.of(countStar()),
                                Map.of("city", CITY),
                                List.of(List.of(CITY)))
                        .orElseThrow();

        ArangoTableHandle newHandle = (ArangoTableHandle) result.getHandle();
        assertThat(newHandle.aggregation()).isPresent();
        assertThat(newHandle.aggregation().orElseThrow().groupingColumns()).containsExactly(CITY);
        assertThat(result.getProjections())
                .containsExactly(new Variable("agg_0", BIGINT));
        assertThat(result.getAssignments()).singleElement()
                .satisfies(a -> assertThat(a.getVariable()).isEqualTo("agg_0"));
        // Grouping columns keep their own handles, as base-JDBC does.
        assertThat(result.getGroupingColumnMapping()).isEmpty();
        assertThat(result.isPrecalculateStatistics()).isFalse();
    }

    @Test
    void applyAggregationDeclinesWhenTheGateDeclines() {
        assertThat(
                        metadata(new ArangoConfig().setAggregationPushdownEnabled(false))
                                .applyAggregation(
                                        null,
                                        handle(),
                                        List.of(countStar()),
                                        Map.of(),
                                        List.of(List.of())))
                .isEmpty();
    }

    // A filter arriving after aggregation is a HAVING, but AqlBuilder renders pushed filters
    // BEFORE the COLLECT -- pushing it would silently turn HAVING into WHERE.
    @Test
    void applyFilterDeclinesOnAnAggregatedHandle() {
        Constraint constraint =
                new Constraint(
                        TupleDomain.withColumnDomains(
                                Map.<ColumnHandle, Domain>of(
                                        CITY, Domain.singleValue(VARCHAR, utf8("nyc")))));
        assertThat(metadata(new ArangoConfig()).applyFilter(null, aggregatedHandle(), constraint))
                .isEmpty();
    }

    private static io.airlift.slice.Slice utf8(String s) {
        return io.airlift.slice.Slices.utf8Slice(s);
    }

    // Declines explicitly rather than by coincidence of the !progress exit, so a later widening
    // of the grouping-key matrix cannot turn it into a dereference pushed at a COLLECT variable.
    @Test
    void applyProjectionDeclinesOnAnAggregatedHandle() {
        assertThat(
                        metadata(new ArangoConfig())
                                .applyProjection(
                                        null,
                                        aggregatedHandle(),
                                        List.of(new Variable("agg_0", (Type) BIGINT)),
                                        Map.of("agg_0", CITY)))
                .isEmpty();
    }

    // An aggregated handle is always one split, so a LIMIT after COLLECT is the final limit.
    @Test
    void applyLimitIsGuaranteedOnAnAggregatedHandleEvenWithShardParallelismEnabled() {
        assertThat(
                        metadata(new ArangoConfig())
                                .applyLimit(null, aggregatedHandle(), 10)
                                .orElseThrow()
                                .isLimitGuaranteed())
                .isTrue();
        assertThat(
                        metadata(new ArangoConfig())
                                .applyLimit(null, handle(), 10)
                                .orElseThrow()
                                .isLimitGuaranteed())
                .isFalse();
    }
}
```

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=ArangoMetadataAggregationTest`
Expected: compilation failure — `applyAggregation` is not overridden (the SPI default returns empty, so `orElseThrow` would fail even once it compiles).

- [ ] **Step 3: Implement**

In `ArangoMetadata`, add the override:

```java
    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        Optional<ArangoAggregation> planned =
                AggregatePushdown.plan(config, handle, aggregates, assignments, groupingSets);
        if (planned.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> newAssignments = ImmutableList.builder();
        for (AggregateSpec spec : planned.get().aggregates()) {
            // The output type is the aggregate's, never the inferred column's: count over a
            // VARCHAR column outputs BIGINT. ArangoPageSource materializes it like any column.
            ArangoColumnHandle output =
                    new ArangoColumnHandle(
                            spec.outputName(), spec.outputType(), false, List.of(spec.outputName()));
            projections.add(new Variable(output.name(), output.type()));
            newAssignments.add(new Assignment(output.name(), output, output.type()));
        }

        return Optional.of(
                new AggregationApplicationResult<>(
                        handle.withAggregation(planned.get()),
                        projections.build(),
                        newAssignments.build(),
                        // Grouping columns keep their own handles, so no remapping is needed.
                        ImmutableMap.of(),
                        false));
    }
```

Add the two reciprocal declines. In `applyFilter`, immediately after the cast:

```java
        // A filter arriving after aggregation is a HAVING, but pushed filters render BEFORE the
        // COLLECT -- pushing it would silently evaluate it as a WHERE.
        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }
```

In `applyProjection`, at the top of the method:

```java
        // Explicit rather than incidental: today the !progress exit below happens to catch this
        // because every aggregate output and grouping key is scalar, so no FieldDereference can
        // resolve. That is safety by coincidence.
        if (((ArangoTableHandle) table).aggregation().isPresent()) {
            return Optional.empty();
        }
```

In `applyLimit`, widen the guarantee:

```java
        // Exact for a single-split scan. An aggregated handle is always one split (the split
        // manager short-circuits), so a LIMIT after COLLECT is the final limit; otherwise shard
        // fan-out means each split applies LIMIT n independently and Trino must re-apply it.
        boolean limitGuaranteed =
                !config.isShardParallelismEnabled() || handle.aggregation().isPresent();
```

Add imports: `io.arango.trino.aggregation.AggregatePushdown`, `io.arango.trino.aggregation.AggregateSpec`, `io.arango.trino.aggregation.ArangoAggregation`.

- [ ] **Step 4: Run tests and verify they pass**

Run: `mvn test -Dtest='ArangoMetadataAggregationTest+ArangoMetadataTest+ArangoMetadataLimitTest'`
Expected: PASS for all three.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoMetadata.java src/test/java/io/arango/trino/ArangoMetadataAggregationTest.java
git commit -m "feat(metadata): implement applyAggregation and the reciprocal hook declines"
```

---

## Task 8: Single split for aggregated handles, and page-source dispatch

**Files:**
- Modify: `src/main/java/io/arango/trino/ArangoSplitManager.java`
- Modify: `src/main/java/io/arango/trino/ArangoPageSourceProvider.java`
- Test: `src/test/java/io/arango/trino/ArangoSplitManagerTest.java` (extend)

**Interfaces:**
- Consumes: `ArangoTableHandle.aggregation()` (Task 4), `AqlBuilder.buildAggregate` (Task 6).
- Produces: no new public surface.

- [ ] **Step 1: Write failing test**

Add to `ArangoSplitManagerTest`:

```java
    // Trino treats connector aggregation output as final, so N splits would emit N duplicate
    // final rows. The check must also come FIRST: the shard pipeline's capability probe costs a
    // round trip per collection and is meaningless for a query that will not fan out. This double
    // fails the test if the pipeline is consulted at all.
    private static final class ShardingForbiddenClient extends ArangoClient {
        ShardingForbiddenClient() {
            super(new ArangoConfig());
        }

        @Override
        public ShardingInfo getShardingInfo(String database, String collection) {
            throw new AssertionError("shard discovery must not run for an aggregated handle");
        }
    }

    @Test
    void aggregatedHandleAlwaysProducesExactlyOneSplitWithoutShardDiscovery() {
        ArangoClient forbidden = new ShardingForbiddenClient();
        ArangoSplitManager mgr =
                new ArangoSplitManager(
                        forbidden, new ArangoConfig(), new ShardFanoutCapability(forbidden));
        ArangoTableHandle aggregated =
                handle().withAggregation(
                                new ArangoAggregation(
                                        List.of(),
                                        List.of(
                                                new AggregateSpec(
                                                        AggregateSpec.Kind.COUNT_STAR,
                                                        java.util.Optional.empty(),
                                                        "agg_0",
                                                        io.trino.spi.type.BigintType.BIGINT))));
        ConnectorSplitSource source =
                mgr.getSplits(null, null, aggregated, Set.of(), Constraint.alwaysTrue());
        List<ArangoSplit> splits =
                source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).getNow(null).stream()
                        .map(ArangoSplit.class::cast)
                        .toList();
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }
```

Add imports for `ArangoAggregation`, `AggregateSpec`, `ShardingInfo`, and `List`/`Optional` as needed.

- [ ] **Step 2: Run test and verify it fails**

Run: `mvn test -Dtest=ArangoSplitManagerTest`
Expected: FAIL with `AssertionError: shard discovery must not run for an aggregated handle` — the split manager currently runs the pipeline unconditionally.

- [ ] **Step 3: Implement**

In `ArangoSplitManager.splitsFor`, make the aggregated check the very first statement:

```java
    private List<ArangoSplit> splitsFor(ArangoTableHandle handle) {
        // Aggregated handles are always exactly one split: Trino replaces the aggregation node
        // and treats this output as final, so N shard-splits would emit N duplicate final rows
        // (master spec §6.4). Checked before shard discovery so the capability probe's round trip
        // is not paid for a query that cannot fan out. ArangoDB still parallelizes the single AQL
        // across its own shards internally.
        if (handle.aggregation().isPresent()) {
            return List.of(SINGLE);
        }
        if (!config.isShardParallelismEnabled()) {
            return List.of(SINGLE);
        }
        // ... existing body unchanged
    }
```

In `ArangoPageSourceProvider.createPageSource`, dispatch on the handle:

```java
        AqlQuery q =
                handle.aggregation().isPresent()
                        ? aqlBuilder.buildAggregate(handle, cols)
                        : aqlBuilder.buildScan(handle, cols);
```

- [ ] **Step 4: Run tests and verify they pass**

Run: `mvn test -Dtest='ArangoSplitManagerTest+ArangoPageSourceProviderTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/arango/trino/ArangoSplitManager.java src/main/java/io/arango/trino/ArangoPageSourceProvider.java src/test/java/io/arango/trino/ArangoSplitManagerTest.java
git commit -m "feat(split): force one split for aggregated handles; dispatch to buildAggregate"
```

---

## Task 9: Pin the AQL semantics in `AqlSemanticsAssumptionsTest`

**Why:** every exactness claim rests on measured ArangoDB behavior. Pinning it means a server upgrade that changes any of it fails loudly instead of silently changing query results. This is the instrument that caught findings C1 and C3.

**Files:**
- Modify: `src/test/java/io/arango/trino/aql/AqlSemanticsAssumptionsTest.java`

**Interfaces:**
- Consumes: the existing `eval(String)` helper and `client` field in that class.
- Produces: no production surface.

- [ ] **Step 1: Write the failing (or immediately-passing) pinning tests**

Add to `AqlSemanticsAssumptionsTest`. The class already has `client` and `eval`; add a seeded collection and a grouping helper that runs a query under **both** COLLECT methods and asserts they agree — §4/18 is the proof that one method's result does not bind the other.

```java
    private static final String NUMS = "aggnums";

    private void seedAggregationFixtures() {
        client.createDocumentCollectionForTest("probe", NUMS);
        // Inserted through the driver; the values that matter (int64 beyond 2^53, uint64, a
        // fraction, a stored -0.0) are the ones the guards must classify exactly.
        client.insertForTest("probe", NUMS, Map.of("v", 42L));
        client.insertForTest("probe", NUMS, Map.of("v", 42.5));
        client.insertForTest("probe", NUMS, Map.of("v", 9007199254740993L));
        client.insertForTest("probe", NUMS, Map.of("v", Long.MAX_VALUE));
        client.insertForTest("probe", NUMS, Map.of("v", "x"));
        client.insertForTest("probe", NUMS, Map.of("v", true));
    }

    // §4/1: AQL COUNT is an alias of LENGTH -- it counts nulls, so Trino's count(col) cannot map
    // to it and must sum a guard predicate instead.
    @Test
    void aqlCountIsLengthAndCountsNulls() {
        assertThat(eval("COUNT([1,null,2])")).isEqualTo(3L);
        assertThat(eval("LENGTH([1,null,2])")).isEqualTo(3L);
    }

    // §4/2, §4/5: aggregates ignore nulls, but SUM of an all-null group is 0 where SQL says NULL.
    // That gap is exactly what the companion count in AqlBuilder exists to close.
    @Test
    void aqlSumIgnoresNullsButReturnsZeroWhenEverythingIsNull() {
        assertThat(eval("SUM([1,2,null])")).isEqualTo(3L);
        assertThat(eval("SUM([null,null])")).isEqualTo(0L);
        assertThat(eval("AVERAGE([null,null])")).isNull();
        assertThat(eval("MIN([null,null])")).isNull();
    }

    // §4/23: load-bearing for the sum fix-up `(companion > 0 ? sum : null)` on an empty table,
    // where the companion itself is null.
    @Test
    void nullComparesFalseAgainstZero() {
        assertThat(eval("null > 0")).isEqualTo(false);
    }

    // §4/3: SUM over zero rows is null, which is why count(col) is wrapped `== null ? 0`.
    @Test
    void aggregateOverZeroRowsGivesNullSumButOneRow() {
        client.createDocumentCollectionForTest("probe", "emptyprobe");
        List<Map<String, Object>> rows = new ArrayList<>();
        client.query(
                        "probe",
                        "FOR d IN @@col COLLECT AGGREGATE s = SUM(d.v), n = LENGTH(1)"
                                + " RETURN { s, n }",
                        Map.of("@col", "emptyprobe"))
                .forEachRemaining(r -> rows.add(r));
        assertThat(rows).hasSize(1); // §4/6: global aggregation still emits exactly one row
        assertThat(rows.get(0).get("s")).isNull();
        assertThat(rows.get(0).get("n")).isEqualTo(0L);
    }

    // §4/8-9: the compound integrality guard is correct where a bare FLOOR test (finding C3) is
    // not, because no double at or above 2^53 can carry a fractional part; and the long-range
    // bound is exact because ArangoDB compares int64 against double by exact mathematical value.
    @Test
    void bigintGuardMatchesTheReadPathAtEveryBoundary() {
        String guard =
                "IS_NUMBER(%1$s) AND %1$s >= -9223372036854775808 AND %1$s < 9223372036854775808"
                        + " AND (ABS(%1$s) >= 9007199254740992 OR %1$s == FLOOR(%1$s))";
        assertThat(eval(guard.formatted("42"))).isEqualTo(true);
        assertThat(eval(guard.formatted("42.5"))).isEqualTo(false);
        assertThat(eval(guard.formatted("9007199254740993"))).isEqualTo(true);
        assertThat(eval(guard.formatted("-9007199254740993"))).isEqualTo(true);
        assertThat(eval(guard.formatted("9223372036854775807"))).isEqualTo(true);
        assertThat(eval(guard.formatted("1e19"))).isEqualTo(false);
        assertThat(eval(guard.formatted("\"x\""))).isEqualTo(false);
        assertThat(eval(guard.formatted("null"))).isEqualTo(false);
        assertThat(eval(guard.formatted("true"))).isEqualTo(false);
        // The exact-value comparison the range bound depends on:
        assertThat(eval("9223372036854775807 < 9223372036854775808")).isEqualTo(true);
    }

    // §4/18-19: the two COLLECT methods disagree on a bare accessor -- a stored -0.0 gets its own
    // group under `hash`, which the optimizer picks for M5's shape. Normalizing by exact numeric
    // equality makes the result identical under both. This is review finding B1's regression pin.
    @Test
    void bigintGroupingNeedsSignedZeroNormalizationAndAgreesUnderBothCollectMethods() {
        client.createDocumentCollectionForTest("probe", "zeroprobe");
        client.insertForTest("probe", "zeroprobe", Map.of("v", 0L));
        client.insertForTest("probe", "zeroprobe", Map.of("v", -0.0d));
        client.insertForTest("probe", "zeroprobe", Map.of("v", 0.0d));

        String bareHash = groupCount("zeroprobe", "d.v", "hash");
        String bareSorted = groupCount("zeroprobe", "d.v", "sorted");
        assertThat(bareHash).isNotEqualTo(bareSorted); // the divergence itself is the finding

        String normalized = "d.v == 0 ? 0 : d.v";
        assertThat(groupCount("zeroprobe", normalized, "hash")).isEqualTo("1");
        assertThat(groupCount("zeroprobe", normalized, "sorted")).isEqualTo("1");
    }

    private String groupCount(String collection, String keyExpression, String method) {
        String aql =
                "FOR d IN @@col COLLECT g = (%s) OPTIONS { method: \"%s\" } RETURN g"
                        .formatted(keyExpression, method);
        int groups = 0;
        var cursor = client.query("probe", aql, Map.of("@col", collection));
        while (cursor.hasNext()) {
            cursor.next();
            groups++;
        }
        return String.valueOf(groups);
    }

    // §4/24: COLLECT groups by numeric value, not stored representation -- so a BIGINT key needs
    // no canonicalizer beyond the signed-zero normalization. Verified at 2^53, where the double's
    // ".0" survives storage (at 42 it does not, which is why the naive probe was vacuous).
    @Test
    void int64AndDoubleOfEqualValueShareAGroupUnderBothMethods() {
        client.createDocumentCollectionForTest("probe", "repprobe");
        client.insertForTest("probe", "repprobe", Map.of("v", 9007199254740992L));
        client.insertForTest("probe", "repprobe", Map.of("v", 9007199254740992.0d));
        assertThat(groupCount("repprobe", "d.v == 0 ? 0 : d.v", "hash")).isEqualTo("1");
        assertThat(groupCount("repprobe", "d.v == 0 ? 0 : d.v", "sorted")).isEqualTo("1");
    }

    // §4/21: AQL equality is byte-exact, not collation-based. This is what makes VARCHAR grouping
    // keys safe, and it independently confirms M2's shipped VARCHAR equality pushdown.
    @Test
    void stringEqualityIsByteExactNotCollationBased() {
        assertThat(eval("\"ab\" == \"a\\u00ADb\"")).isEqualTo(false); // soft hyphen
        assertThat(eval("\"\\u00E9\" == \"e\\u0301\"")).isEqualTo(false); // NFC vs NFD
        assertThat(eval("\"a\" == \"A\"")).isEqualTo(false);
    }

    // §4/15: AQL accumulates sums in double, so sum(BIGINT) is not claimable -- precision is lost
    // past 2^53 and Trino's loud overflow becomes silent.
    @Test
    void aqlSumAccumulatesInDoubleSoBigintSumIsNotClaimable() {
        assertThat(eval("SUM([9007199254740993, 1]) == 9007199254740992")).isEqualTo(true);
    }

    // §4/14: a double sum that overflows reads back as 0, not Infinity -- JSON/VelocyPack cannot
    // carry non-finite doubles. Accepted limitation (design §10/1), pinned so it stays visible.
    @Test
    void doubleSumOverflowReadsAsZeroNotInfinity() {
        assertThat(eval("SUM([1.7976931348623157e308, 1.7976931348623157e308])")).isEqualTo(0L);
    }

    // §4/13: count(*) needs no special AQL form -- AGGREGATE LENGTH(1) plans identically to
    // COLLECT WITH COUNT INTO, so one code path serves both.
    @Test
    void lengthAggregateCountsRowsPerGroup() {
        seedAggregationFixtures();
        List<Map<String, Object>> rows = new ArrayList<>();
        client.query(
                        "probe",
                        "FOR d IN @@col COLLECT AGGREGATE n = LENGTH(1) RETURN { n }",
                        Map.of("@col", NUMS))
                .forEachRemaining(r -> rows.add(r));
        assertThat(rows.get(0).get("n")).isEqualTo(6L);
    }

    // §3: the unguarded danger, measured -- AQL's total cross-type ordering (null < bool < number
    // < string) makes MIN return a boolean and MAX a string over a mixed column.
    @Test
    void unguardedMinMaxCrossTypeOrderingIsWhyGuardsExist() {
        assertThat(eval("MIN([5,\"a\",null,true])")).isEqualTo(true);
        assertThat(eval("MAX([5,\"a\",null,true])")).isEqualTo("a");
    }
```

- [ ] **Step 2: Run and verify**

Run: `mvn test -Dtest=AqlSemanticsAssumptionsTest`
Expected: PASS. These pin observed behavior, so they should pass immediately; a failure means either a helper mismatch (adjust the helper) or that the server behaves differently from the design's measurements — in which case **stop and re-derive the affected rendering**, because a §4 row is the premise of a §7 rendering.

Numeric return types come back from the driver as `Long`/`Double`/`BigInteger`; if an assertion fails only on boxing (e.g. `Integer` vs `Long`), compare with `assertThat(((Number) eval(...)).longValue())`.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/io/arango/trino/aql/AqlSemanticsAssumptionsTest.java
git commit -m "test(aql): pin the M5 aggregate and COLLECT-grouping semantics

Every grouping assertion runs under both COLLECT methods and asserts they
agree -- the two genuinely disagree on a bare accessor, which is how the
signed-zero grouping defect (review finding B1) was found."
```

---

## Task 10: End-to-end correctness ITs

**Why this is the decisive task:** the milestone's exit criterion is "aggregates correct vs reference." The reference is the same query with pushdown disabled.

**Files:**
- Create: `src/test/java/io/arango/trino/ArangoConnectorAggregationTest.java`

**Interfaces:**
- Consumes: the whole stack.
- Produces: no production surface.

- [ ] **Step 1: Write the residual-filter interaction test first**

This one is written before the rest: if a `BIGINT`-range predicate prevents aggregation pushdown, every later `isFullyPushedDown()` assertion that combines a filter with an aggregate must be written accordingly (design §10/8).

```java
package io.arango.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

class ArangoConnectorAggregationTest extends AbstractTestQueryFramework {
    private TestingArangoServer server;

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = new TestingArangoServer();
        try (ArangoClient seed =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()))) {
            seed.createDatabaseForTest("agg");

            // Clean fixture: every value matches its inferred type, so pushed and unpushed
            // results must agree exactly.
            seed.createDocumentCollectionForTest("agg", "sales");
            seed.insertForTest("agg", "sales", Map.of("city", "nyc", "qty", 3L, "price", 10.5));
            seed.insertForTest("agg", "sales", Map.of("city", "nyc", "qty", 5L, "price", 2.5));
            seed.insertForTest("agg", "sales", Map.of("city", "sfo", "qty", 7L, "price", 4.0));

            // Dirty fixture: the first two documents type the columns (sample-size 2), so the
            // later mismatched/absent/out-of-range values are invisible to inference and exercise
            // the guards. Depends on sampleDocuments' unsorted LIMIT landing on insertion order --
            // the same assumption ArangoConnectorPushdownTest already documents and relies on.
            seed.createDocumentCollectionForTest("agg", "dirty");
            seed.insertForTest("agg", "dirty", Map.of("g", "a", "n", 10L, "x", 1.0));
            seed.insertForTest("agg", "dirty", Map.of("g", "b", "n", 20L, "x", 2.0));
            seed.insertForTest("agg", "dirty", Map.of("g", "a", "n", 42.5, "x", "not-a-number"));
            seed.insertForTest("agg", "dirty", Map.of("g", "b", "n", 1e19, "x", 4.0));
            seed.insertForTest("agg", "dirty", Map.of("g", "a"));
            seed.insertForTest("agg", "dirty", new HashMap<>(Map.of("g", "c")) {
                {
                    put("n", null);
                    put("x", null);
                }
            });

            seed.createDocumentCollectionForTest("agg", "zeros");
            seed.insertForTest("agg", "zeros", Map.of("z", 0L));
            seed.insertForTest("agg", "zeros", Map.of("z", -0.0d));
            seed.insertForTest("agg", "zeros", Map.of("z", 0.0d));

            seed.createDocumentCollectionForTest("agg", "empty");
        }

        DistributedQueryRunner runner =
                DistributedQueryRunner.builder(
                                testSessionBuilder().setCatalog("arango").setSchema("agg").build())
                        .build();
        runner.installPlugin(new ArangoPlugin());
        runner.createCatalog(
                "arango",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts", server.hostPort(),
                        "arangodb.user", "root",
                        "arangodb.password", server.rootPassword(),
                        "arangodb.schema.sample-size", "2"));
        // Second catalog, identical except that pushdown is off: the reference for every
        // correctness comparison below.
        runner.createCatalog(
                "noagg",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts", server.hostPort(),
                        "arangodb.user", "root",
                        "arangodb.password", server.rootPassword(),
                        "arangodb.schema.sample-size", "2",
                        "arangodb.aggregation-pushdown-enabled", "false"));
        return runner;
    }

    @AfterAll
    void stopServer() {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Runs the same query against the pushdown-enabled and pushdown-disabled catalogs and asserts
     * identical rows. {@code template} carries one {@code %s} where the catalog-qualified schema
     * goes, e.g. {@code "SELECT count(*) FROM %s.sales"}.
     */
    private void assertSameAsReference(String template) {
        MaterializedResult pushed = computeActual(template.formatted("arango.agg"));
        MaterializedResult reference = computeActual(template.formatted("noagg.agg"));
        assertThat(pushed.getMaterializedRows())
                .as("pushed vs reference for: %s", template)
                .containsExactlyInAnyOrderElementsOf(reference.getMaterializedRows());
    }

    // Written first: a BIGINT range predicate stays in Trino's residual (prefilter-only), which
    // leaves a FilterNode between the AggregationNode and the TableScanNode, and
    // PushAggregationIntoTableScan matches only aggregation(tableScan()) /
    // aggregation(project(tableScan())). Whatever the planner actually does must be pinned here --
    // every other pushdown assertion in this class depends on knowing it (design §10/8).
    @Test
    void residualFilterInteractionIsPinned() {
        // DOUBLE range is fully enforced -- no residual -- so the aggregate pushes.
        assertThat(query("SELECT count(*) FROM arango.agg.sales WHERE price > 3.0"))
                .matches("VALUES BIGINT '2'")
                .isFullyPushedDown();

        // BIGINT range is prefilter-only, so its residual FilterNode remains. The answer must be
        // correct either way; only the plan differs.
        assertSameAsReference("SELECT count(*) FROM %s.sales WHERE qty > 4");
        assertThat(query("SELECT count(*) FROM arango.agg.sales WHERE qty > 4"))
                .matches("VALUES BIGINT '2'")
                .isNotFullyPushedDown(AggregationNode.class, FilterNode.class);
    }
```

If that last assertion fails, the planner pushed the aggregate anyway — **invert it to `.isFullyPushedDown()` and correct design §10/8's wording to match.** The purpose of this test is to pin what the planner truly does, not to defend a prediction. Either outcome is safe: `AggregatePushdown` rule 10 already declines over a prefilter-only constraint, so the aggregate cannot be computed over the unfiltered superset regardless of plan shape.

Import `io.trino.sql.planner.plan.AggregationNode` and `io.trino.sql.planner.plan.FilterNode` — `ArangoConnectorPushdownTest` already imports the latter for the same purpose.

- [ ] **Step 2: Run it and record the answer**

Run: `mvn test -Dtest=ArangoConnectorAggregationTest#residualFilterInteractionIsPinned`
Expected: the correctness half passes. If the pushdown assertion fails, **invert it to match reality** and update design §10/8's wording to match — the point of this test is to pin the truth, not a guess.

- [ ] **Step 3: Write the remaining correctness tests**

```java
    @Test
    void globalAggregatesMatchTheReference() {
        assertSameAsReference("SELECT count(*) FROM %s.sales");
        assertSameAsReference("SELECT count(city) FROM %s.sales");
        assertSameAsReference("SELECT min(qty), max(qty) FROM %s.sales");
        assertSameAsReference("SELECT sum(price), avg(price) FROM %s.sales");
    }

    @Test
    void groupedAggregatesMatchTheReference() {
        assertSameAsReference("SELECT city, count(*) FROM %s.sales GROUP BY city");
        assertSameAsReference("SELECT city, sum(price) FROM %s.sales GROUP BY city");
        assertSameAsReference("SELECT qty, count(*) FROM %s.sales GROUP BY qty");
        assertSameAsReference("SELECT DISTINCT city FROM %s.sales");
    }

    // The guards' reason for existing: every one of these columns holds values invisible to
    // inference that the read path materializes as NULL.
    @Test
    void aggregatesOverDirtyDataMatchTheReference() {
        assertSameAsReference("SELECT count(n), min(n), max(n) FROM %s.dirty");
        assertSameAsReference("SELECT sum(x), avg(x), count(x) FROM %s.dirty");
        assertSameAsReference("SELECT g, count(*), count(n) FROM %s.dirty GROUP BY g");
    }

    // Trino's count of an empty table is 0 and its sum is NULL; AQL's SUM over zero rows is null
    // and over an all-null group is 0, which is why the renderings wrap both (§4/2, §4/3).
    @Test
    void emptyTableAndAllNullGroupFollowSqlNullSemantics() {
        MaterializedResult empty = computeActual("SELECT count(*), count(z), sum(z) FROM arango.agg.empty");
        assertThat(empty.getMaterializedRows()).hasSize(1);
        assertThat(empty.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        assertThat(empty.getMaterializedRows().get(0).getField(1)).isEqualTo(0L);
        assertThat(empty.getMaterializedRows().get(0).getField(2)).isNull();

        // Group "c" holds only nulls: count = 0, sum = NULL.
        assertSameAsReference("SELECT g, count(x), sum(x) FROM %s.dirty GROUP BY g");
    }

    // Review finding B1's end-to-end regression: 0, -0.0 and 0.0 must be ONE group, not two.
    @Test
    void signedZeroDoesNotSplitAGroup() {
        MaterializedResult grouped = computeActual("SELECT z, count(*) FROM arango.agg.zeros GROUP BY z");
        assertThat(grouped.getMaterializedRows()).hasSize(1);
        assertSameAsReference("SELECT z, count(*) FROM %s.zeros GROUP BY z");
    }

    @Test
    void declinedShapesStillReturnCorrectResults() {
        // min/max on VARCHAR (collation), sum/avg on BIGINT (double accumulation), count DISTINCT.
        assertSameAsReference("SELECT min(city), max(city) FROM %s.sales");
        assertSameAsReference("SELECT sum(qty), avg(qty) FROM %s.sales");
        assertSameAsReference("SELECT count(DISTINCT city) FROM %s.sales");
    }

    @Test
    void disablingPushdownChangesNothingButThePlan() {
        assertSameAsReference("SELECT city, count(*), sum(price) FROM %s.sales GROUP BY city");
    }
```

- [ ] **Step 4: Run the full IT class**

Run: `mvn test -Dtest=ArangoConnectorAggregationTest`
Expected: PASS. A failure here is a genuine correctness defect — trace it back to the §4 row and §7 rendering it contradicts before changing any assertion.

- [ ] **Step 5: Run the whole suite**

Run: `mvn test`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add src/test/java/io/arango/trino/ArangoConnectorAggregationTest.java
git commit -m "test(e2e): aggregation correctness against a pushdown-disabled reference catalog"
```

---

## Task 11: Documentation and static-analysis gates

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-18-arangodb-trino-connector-design.md` (§6.4 note)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

- [ ] **Step 1: Update `CLAUDE.md`**

Update the "What this is" paragraph to say the milestone is M5 ("aggregation pushdown") and add a read-path/pushdown item describing `applyAggregation`: the exactness matrix, the single-split rule, `ColumnGuard`, and the `io.arango.trino.aggregation` package. Add `arangodb.aggregation-pushdown-enabled` to the `ArangoConfig` list. Add `io.arango.trino.aggregation` to the package layout section.

- [ ] **Step 2: Update `README.md`**

Document the new config option and which aggregates push (and which deliberately do not, with the one-line reason each: `min`/`max` on `VARCHAR` = collation; `sum`/`avg` on `BIGINT` = AQL's double accumulation).

- [ ] **Step 3: Add the master-spec §6.4 note**

In `docs/superpowers/specs/2026-07-18-arangodb-trino-connector-design.md` §6.4, immediately after "all of `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` are safe to push":

```markdown
> **Note (2026-07-26, M5).** This "all five are safe" claim is about *re-aggregation only* — single-split execution means no partial/final decomposition is needed. It says nothing about value coercion, and M5 found that `sum`/`avg` over `BIGINT` and `min`/`max` over `VARCHAR` cannot be pushed exactly. See `2026-07-26-m5-aggregation-pushdown-design.md` §5 for the governing matrix.
```

- [ ] **Step 4: Run all three static-analysis gates**

```bash
mvn spotless:check
mvn checkstyle:check
mvn compile spotbugs:check
```

Expected: all pass. New files are enforced, not grandfathered. If Spotless flags a new file, run `mvn spotless:apply` and re-commit; if Checkstyle or SpotBugs flags one, **fix the code** — do not add a suppression, since suppressions are reserved for the grandfathered M1–M3 files.

- [ ] **Step 5: Run the full suite one final time**

```bash
mvn test
```

Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add CLAUDE.md README.md docs/superpowers/specs/2026-07-18-arangodb-trino-connector-design.md
git commit -m "docs: document M5 aggregation pushdown and its declined shapes"
```

---

## Done criteria

- [ ] `mvn test` green, with Docker running.
- [ ] `mvn spotless:check`, `mvn checkstyle:check`, `mvn compile spotbugs:check` all green.
- [ ] `SELECT count(*) FROM t`, `GROUP BY`, and `SELECT DISTINCT` push; declined shapes return identical results unpushed.
- [ ] Every claimed aggregate matches the pushdown-disabled reference catalog, including over the dirty fixture.
- [ ] Grouping on a column containing `0`, `-0.0`, and `0.0` yields exactly one row.
- [ ] An aggregated handle produces exactly one split, and shard discovery never runs for it.
