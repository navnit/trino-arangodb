package io.arango.trino.aql;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AqlSemanticsAssumptionsTest {
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
        assertThat(eval("42 == 42.0")).isEqualTo(true); // numeric equality is cross-int/float
        assertThat(eval("42 == \"42\"")).isEqualTo(false); // == is type-strict
        assertThat(eval("IS_NUMBER(42)")).isEqualTo(true);
        assertThat(eval("IS_NUMBER(\"x\")")).isEqualTo(false);
        assertThat(eval("IS_NUMBER(null)")).isEqualTo(false);
        assertThat(eval("IS_NUMBER(true)")).isEqualTo(false);
        // FLOOR() returns a DOUBLE, so `v == FLOOR(v)` is NOT a usable int64 integrality guard: for
        // a
        // stored int64 that isn't exactly double-representable it is false even though v is
        // integral
        // (review finding C3). AqlBuilder therefore uses a bare IS_NUMBER guard for BIGINT range
        // and lets
        // the residual re-check drop fractionals -- this pins why FLOOR was removed.
        assertThat(eval("42 == FLOOR(42.0)")).isEqualTo(true); // fine for small values
        assertThat(eval("9007199254740993 == FLOOR(9007199254740993)"))
                .isEqualTo(false); // 2^53+1 wrongly rejected
        assertThat(eval("42.5 == FLOOR(42.5)")).isEqualTo(false); // fractional correctly rejected
        // total cross-type ordering null < bool < number < string
        assertThat(eval("null < false")).isEqualTo(true);
        assertThat(eval("false < 0")).isEqualTo(true);
        assertThat(eval("0 < \"a\"")).isEqualTo(true);
    }

    @Test
    void recordDriverNumericJavaTypes() {
        client.createDocumentCollectionForTest("probe", "nums");
        client.insertForTest("probe", "nums", Map.of("i", 7, "big", 5_000_000_000L, "f", 7.5));
        Map<String, Object> doc = client.sampleDocuments("probe", "nums", 1, false).get(0);
        // On record for isIntegralInLongRange / TypeMapper.inferType: what Java types the driver
        // yields for a small int ("i"), an int too large for a 32-bit int ("big"), and a
        // fractional value ("f"). A small integral JSON value comes back as Integer, not Long as
        // one might assume -- isIntegralInLongRange and TypeMapper.inferType both handle
        // Integer and Long alike, so this is benign, but the concrete type must be pinned rather
        // than guessed. In no case does the driver yield BigDecimal -- this is why
        // ArangoPageSource.isIntegralInLongRange correctly has no BigDecimal branch (its missing
        // branch is unreachable, not an oversight).
        System.out.println(
                "driver numeric types: i="
                        + doc.get("i").getClass().getName()
                        + " big="
                        + doc.get("big").getClass().getName()
                        + " f="
                        + doc.get("f").getClass().getName());
        assertThat(doc.get("i")).isInstanceOf(Integer.class);
        assertThat(doc.get("big")).isInstanceOf(Long.class);
        assertThat(doc.get("f")).isInstanceOf(Double.class);
    }

    @Test
    void doubleComparisonAgreesWithJavaAtInt64DoublePrecisionBoundary() {
        // Beyond 2^53, not every long has an exact double representation, so int64 and double
        // comparison can diverge. This pins the STORED-DOUBLE case: AQL's `<`/`>` over stored
        // double
        // values agrees with plain Java double comparison. The riskier
        // STORED-INT64-in-a-DOUBLE-column
        // case (where ArangoDB compares exactly but the read path rounds) is characterized
        // separately in
        // mixedInt64DoubleComparisonNeedsDoublePromotion -- together they cover why DOUBLE pushdown
        // must
        // promote the operand with `+ 0.0` yet can still be fully enforced by the pushed AQL alone.
        client.createDocumentCollectionForTest("probe", "boundary");
        double justBelow = 9_007_199_254_740_990.0; // < 2^53
        double justAbove =
                9_007_199_254_740_994.0; // > 2^53, straddling the boundary with justBelow
        client.insertForTest("probe", "boundary", Map.of("tag", "below", "val", justBelow));
        client.insertForTest("probe", "boundary", Map.of("tag", "above", "val", justAbove));

        double bound = 9_007_199_254_740_992.0; // 2^53, strictly between the two stored values
        Object matched =
                client.query(
                                "probe",
                                "RETURN { r: (FOR d IN boundary FILTER d.val > @b SORT d.tag RETURN d.tag) }",
                                Map.of("b", bound))
                        .next()
                        .get("r");

        assertThat(matched).isEqualTo(List.of("above"));
        assertThat(justAbove > bound).isTrue();
        assertThat(justBelow > bound).isFalse();
    }

    @Test
    void mixedInt64DoubleComparisonNeedsDoublePromotion() {
        // THE C1 PREMISE. A DOUBLE column legitimately holds stored int64 values (it is inferred
        // DOUBLE
        // because the sample saw both ints and floats). ArangoPageSource.appendValue reads such a
        // value
        // ROUNDED to double (n.doubleValue()), but ArangoDB compares int64-vs-double by EXACT
        // mathematical
        // value, not in double space -- so a bare `d.val <op> @bind` diverges from the read path
        // beyond 2^53.
        // AqlBuilder promotes a DOUBLE operand with `+ 0.0`, forcing double-space comparison that
        // matches the
        // read path. Both divergence directions are pinned here so a driver/engine change fails
        // loudly.

        // Direction 1 -- false INCLUDE: stored int64 2^53+1 reads back rounded to 2^53.
        client.createDocumentCollectionForTest("probe", "mixedGt");
        long v1 = 9_007_199_254_740_993L; // 2^53 + 1, exact as int64
        client.insertForTest("probe", "mixedGt", Map.of("val", v1));
        double b1 = 9_007_199_254_740_992.0; // 2^53
        assertThat((double) v1).isEqualTo(b1); // Java rounds the stored value to 2^53
        assertThat(countMatches("mixedGt", "d.val > @b", b1))
                .as("bare: exact int64 wrongly includes")
                .isEqualTo(1);
        assertThat(countMatches("mixedGt", "(d.val + 0.0) > @b", b1))
                .as("promoted: matches read path")
                .isEqualTo(0);
        assertThat((double) v1 > b1).as("Java read path excludes").isFalse();

        // Direction 2 -- false MISS: stored int64 2^54-1 reads back rounded to 2^54.
        client.createDocumentCollectionForTest("probe", "mixedEq");
        long v2 = 18_014_398_509_481_983L; // 2^54 - 1, exact as int64
        client.insertForTest("probe", "mixedEq", Map.of("val", v2));
        double b2 = 18_014_398_509_481_984.0; // 2^54
        assertThat((double) v2).isEqualTo(b2); // Java rounds the stored value to 2^54
        assertThat(countMatches("mixedEq", "d.val == @b", b2))
                .as("bare: exact int64 wrongly misses")
                .isEqualTo(0);
        assertThat(countMatches("mixedEq", "(d.val + 0.0) == @b", b2))
                .as("promoted: matches read path")
                .isEqualTo(1);

        // Guard safety: `+ 0.0` coerces non-numbers ("abc" + 0.0 == 0.0), so the IS_NUMBER guard
        // AqlBuilder emits ahead of the promotion is load-bearing, not decorative.
        client.createDocumentCollectionForTest("probe", "mixedStr");
        client.insertForTest("probe", "mixedStr", Map.of("val", "abc"));
        assertThat(countMatches("mixedStr", "(d.val + 0.0) == @b", 0.0))
                .as("unguarded coerces \"abc\"->0")
                .isEqualTo(1);
        assertThat(countMatches("mixedStr", "IS_NUMBER(d.val) AND (d.val + 0.0) == @b", 0.0))
                .as("guard excludes it")
                .isEqualTo(0);
    }

    @Test
    void bigintExactComparisonMustNotBePromoted() {
        // The MIRROR premise: BIGINT must stay bare. Its read path is exact (longValue() on an
        // integral
        // value, no rounding) against long binds, so ArangoDB's exact int64 comparison is what
        // agrees.
        // Promoting BIGINT with `+ 0.0` would round the stored value and reintroduce C1 in reverse.
        // Pinned
        // so a future edit that leaks the promotion onto the BIGINT path fails here.
        client.createDocumentCollectionForTest("probe", "bigints");
        long v = 9_007_199_254_740_993L; // 2^53 + 1, exact as int64
        client.insertForTest("probe", "bigints", Map.of("val", v));
        // Bare exact comparison against the exact long bind agrees with the exact read path.
        assertThat(countMatches("bigints", "d.val == @b", v))
                .as("bare exact matches (correct)")
                .isEqualTo(1);
        // Promotion would round the stored 2^53+1 to 2^53 and miss the long bind -- the behavior to
        // avoid.
        assertThat(countMatches("bigints", "(d.val + 0.0) == @b", v))
                .as("promoted would wrongly miss it")
                .isEqualTo(0);
    }

    // ---------------------------------------------------------------------------------------
    // M5 (aggregation pushdown). Every rendering in AqlBuilder.buildAggregate and ColumnGuard
    // rests on one of the facts below; pinning them means an ArangoDB upgrade that changes any
    // of them fails loudly here instead of silently changing query results.
    // ---------------------------------------------------------------------------------------

    // §4/1: AQL COUNT is an alias of LENGTH -- it counts nulls, so Trino's count(col) cannot map
    // to it and must sum a guard predicate instead.
    @Test
    void aqlCountIsLengthAndCountsNulls() {
        assertThat(((Number) eval("COUNT([1,null,2])")).longValue()).isEqualTo(3);
        assertThat(((Number) eval("LENGTH([1,null,2])")).longValue()).isEqualTo(3);
    }

    // §4/2, §4/5: aggregates ignore nulls, but SUM of an all-null input is 0 where SQL says NULL.
    // That gap is exactly what the companion count in buildAggregate exists to close.
    @Test
    void aqlSumIgnoresNullsButReturnsZeroWhenEverythingIsNull() {
        assertThat(((Number) eval("SUM([1,2,null])")).longValue()).isEqualTo(3);
        assertThat(((Number) eval("SUM([null,null])")).longValue()).isEqualTo(0);
        assertThat(eval("AVERAGE([null,null])")).isNull();
        assertThat(eval("MIN([null,null])")).isNull();
    }

    // §4/23: load-bearing for the sum fix-up `(companion > 0 ? sum : null)` on an empty table,
    // where the companion itself comes back null.
    @Test
    void nullComparesFalseAgainstZero() {
        assertThat(eval("null > 0")).isEqualTo(false);
    }

    // §3: the unguarded danger, measured. AQL's total cross-type ordering (null < bool < number <
    // string) makes MIN return a boolean and MAX a string over a mixed column, and a single
    // string poisons SUM to null -- none of which matches what the read path would compute.
    @Test
    void unguardedMinMaxCrossTypeOrderingIsWhyGuardsExist() {
        assertThat(eval("MIN([5,\"a\",null,true])")).isEqualTo(true);
        assertThat(eval("MAX([5,\"a\",null,true])")).isEqualTo("a");
        assertThat(eval("SUM([1,2,\"3\"])")).isNull();
    }

    // §4/3, §4/6: a global aggregation over an empty collection still emits exactly one row, but
    // its SUM is null -- which is why count(col) is wrapped `== null ? 0`.
    @Test
    void globalAggregateOverZeroRowsEmitsOneRowWithNullSum() {
        client.createDocumentCollectionForTest("probe", "m5empty");
        Map<String, Object> row =
                client.query(
                                "probe",
                                "FOR d IN m5empty COLLECT AGGREGATE s = SUM(d.v), n = LENGTH(1)"
                                        + " RETURN { s, n }",
                                Map.of())
                        .next();
        assertThat(row.get("s")).isNull();
        assertThat(((Number) row.get("n")).longValue()).isEqualTo(0);
    }

    // §4/8-9: the compound integrality guard is correct where a bare FLOOR test (finding C3) is
    // not, because no double at or above 2^53 can carry a fractional part; and the long-range
    // bound is exact because ArangoDB compares int64 against double by exact mathematical value.
    // This is ColumnGuard.predicate(BIGINT) verbatim.
    @Test
    void bigintGuardMatchesTheReadPathAtEveryBoundary() {
        String guard =
                "IS_NUMBER(%1$s) AND %1$s >= -9223372036854775808 AND %1$s < 9223372036854775808"
                        + " AND (ABS(%1$s) >= 9007199254740992 OR %1$s == FLOOR(%1$s))";
        assertThat(eval(guard.formatted("42"))).as("small int").isEqualTo(true);
        assertThat(eval(guard.formatted("0"))).as("zero").isEqualTo(true);
        assertThat(eval(guard.formatted("42.5"))).as("fraction").isEqualTo(false);
        assertThat(eval(guard.formatted("-0.5"))).as("negative fraction").isEqualTo(false);
        assertThat(eval(guard.formatted("9007199254740993"))).as("2^53+1").isEqualTo(true);
        assertThat(eval(guard.formatted("-9007199254740993"))).as("-(2^53+1)").isEqualTo(true);
        assertThat(eval(guard.formatted("9223372036854775807"))).as("int64 max").isEqualTo(true);
        // int64 min is the exact lower bound the guard hardcodes, and the only literal in it that
        // AQL must parse as the negation of a value too large for int64 (2^63 is exactly
        // representable as a double, so the negation is exact). ValueMaterializer accepts it --
        // BigInteger(-2^63).bitLength() is 63 -- so the guard must too.
        assertThat(eval(guard.formatted("-9223372036854775808"))).as("int64 min").isEqualTo(true);
        assertThat(eval(guard.formatted("-9223372036854775807"))).as("int64 min+1").isEqualTo(true);
        assertThat(eval(guard.formatted("1e19"))).as("above 2^63").isEqualTo(false);
        assertThat(eval(guard.formatted("-1e19"))).as("below -2^63").isEqualTo(false);
        assertThat(eval(guard.formatted("\"x\""))).as("string").isEqualTo(false);
        assertThat(eval(guard.formatted("null"))).as("null").isEqualTo(false);
        assertThat(eval(guard.formatted("true"))).as("bool").isEqualTo(false);
        // The exact-value int64-vs-double comparison the range bound depends on:
        assertThat(eval("9223372036854775807 < 9223372036854775808")).isEqualTo(true);
    }

    // §4/15: AQL accumulates sums in double, so sum(BIGINT) is not claimable -- precision is lost
    // past 2^53 and Trino's loud sum(bigint) overflow would become silent.
    @Test
    void aqlSumAccumulatesInDoubleSoBigintSumIsNotClaimable() {
        assertThat(eval("SUM([9007199254740993, 1]) == 9007199254740992")).isEqualTo(true);
    }

    // §4/14: a double sum that overflows reads back as 0, not Infinity -- JSON/VelocyPack cannot
    // carry non-finite doubles. Accepted limitation (design §10/1), pinned so it stays visible.
    @Test
    void doubleSumOverflowReadsAsZeroNotInfinity() {
        assertThat(
                        ((Number) eval("SUM([1.7976931348623157e308, 1.7976931348623157e308])"))
                                .doubleValue())
                .isEqualTo(0.0);
    }

    // §4/21: AQL equality is byte-exact, not collation-based. This is what makes VARCHAR grouping
    // keys safe, and it independently confirms M2's already-shipped VARCHAR equality pushdown --
    // a contrary result would have been a defect in released behavior.
    @Test
    void stringEqualityIsByteExactNotCollationBased() {
        assertThat(eval("\"ab\" == \"a\\u00ADb\"")).as("soft hyphen").isEqualTo(false);
        assertThat(eval("\"\\u00E9\" == \"e\\u0301\"")).as("NFC vs NFD").isEqualTo(false);
        assertThat(eval("\"a\" == \"A\"")).as("case").isEqualTo(false);
    }

    // §4/18-19: THE review-finding-B1 pin. The two COLLECT methods disagree on a bare accessor --
    // a stored -0.0 gets its own group under `hash`, which is the method the optimizer picks for
    // M5's shape -- so one Trino group would be emitted as two final rows. Normalizing by exact
    // numeric equality (ColumnGuard's BIGINT grouping value) makes both methods agree on one.
    @Test
    void bigintGroupingNeedsSignedZeroNormalizationAndAgreesUnderBothCollectMethods() {
        client.createDocumentCollectionForTest("probe", "m5zeros");
        client.insertForTest("probe", "m5zeros", Map.of("v", 0L));
        client.insertForTest("probe", "m5zeros", Map.of("v", -0.0d));
        client.insertForTest("probe", "m5zeros", Map.of("v", 0.0d));

        assertThat(groupCount("m5zeros", "d.v", "hash"))
                .as("bare key under hash splits signed zero -- the defect")
                .isEqualTo(2);
        assertThat(groupCount("m5zeros", "d.v", "sorted"))
                .as("...and the methods disagree, so one method's result binds nothing")
                .isEqualTo(1);

        String normalized = "d.v == 0 ? 0 : d.v";
        assertThat(groupCount("m5zeros", normalized, "hash")).isEqualTo(1);
        assertThat(groupCount("m5zeros", normalized, "sorted")).isEqualTo(1);
    }

    // §4/24: COLLECT groups by numeric value, not stored representation, so a BIGINT key needs no
    // canonicalizer beyond the signed-zero normalization. Verified at 2^53, where a stored
    // double's ".0" survives -- at 42 VelocyPack normalizes 42.0 to int, which would make the
    // test vacuous.
    @Test
    void int64AndDoubleOfEqualValueShareAGroupUnderBothMethods() {
        client.createDocumentCollectionForTest("probe", "m5reps");
        client.insertForTest("probe", "m5reps", Map.of("v", 9007199254740992L));
        client.insertForTest("probe", "m5reps", Map.of("v", 9007199254740992.0d));
        assertThat(eval("TO_STRING(9007199254740992.0)"))
                .as("the two representations really are distinct at this magnitude")
                .isEqualTo("9007199254740992.0");

        assertThat(groupCount("m5reps", "d.v == 0 ? 0 : d.v", "hash")).isEqualTo(1);
        assertThat(groupCount("m5reps", "d.v == 0 ? 0 : d.v", "sorted")).isEqualTo(1);
    }

    // §4/20: distinct values stay distinct at the 2^53 boundary -- BIGINT grouping is exact.
    @Test
    void distinctInt64AndDoubleStaySeparateGroups() {
        client.createDocumentCollectionForTest("probe", "m5boundary");
        client.insertForTest("probe", "m5boundary", Map.of("v", 9007199254740993L));
        client.insertForTest("probe", "m5boundary", Map.of("v", 9007199254740992.0d));
        assertThat(groupCount("m5boundary", "d.v == 0 ? 0 : d.v", "hash")).isEqualTo(2);
        assertThat(groupCount("m5boundary", "d.v == 0 ? 0 : d.v", "sorted")).isEqualTo(2);
    }

    // §4/13: count(*) needs no special AQL form -- AGGREGATE LENGTH(1) plans to the same
    // CollectNode as COLLECT WITH COUNT INTO, so one code path serves both.
    @Test
    void lengthAggregateCountsRows() {
        client.createDocumentCollectionForTest("probe", "m5rows");
        client.insertForTest("probe", "m5rows", Map.of("v", 1L));
        client.insertForTest("probe", "m5rows", Map.of("v", 2L));
        Object n =
                client.query(
                                "probe",
                                "FOR d IN m5rows COLLECT AGGREGATE n = LENGTH(1) RETURN { n }",
                                Map.of())
                        .next()
                        .get("n");
        assertThat(((Number) n).longValue()).isEqualTo(2);
    }

    // §4/22: rounding is monotone, so the bare MAX of int64s -- rounded when the read path applies
    // doubleValue() -- equals the promoted MAX. That is why min/max(DOUBLE) must NOT promote
    // (promotion would turn a stored -0.0 into 0.0; review finding S1).
    @Test
    void bareExtremumThenRoundingEqualsPromotedExtremum() {
        assertThat(
                        eval(
                                "(MAX([9007199254740993, 9007199254740995]) + 0.0) =="
                                        + " MAX([9007199254740993 + 0.0, 9007199254740995 + 0.0])"))
                .isEqualTo(true);
    }

    /** Number of groups a COLLECT on {@code keyExpression} produces under {@code method}. */
    private int groupCount(String collection, String keyExpression, String method) {
        // RETURN an object, not the bare group key: ArangoClient.query deserializes rows as Map.
        String aql =
                "FOR d IN %s COLLECT g = (%s) OPTIONS { method: \"%s\" } RETURN { g }"
                        .formatted(collection, keyExpression, method);
        int groups = 0;
        var cursor = client.query("probe", aql, Map.of());
        while (cursor.hasNext()) {
            cursor.next();
            groups++;
        }
        return groups;
    }

    private long countMatches(String collection, String predicate, Object bind) {
        Object r =
                client.query(
                                "probe",
                                "RETURN { r: LENGTH(FOR d IN "
                                        + collection
                                        + " FILTER "
                                        + predicate
                                        + " RETURN 1) }",
                                Map.of("b", bind))
                        .next()
                        .get("r");
        return ((Number) r).longValue();
    }
}
