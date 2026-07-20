package io.arango.trino.aql;

import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.TestingArangoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
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
        // FLOOR() returns a DOUBLE, so `v == FLOOR(v)` is NOT a usable int64 integrality guard: for a
        // stored int64 that isn't exactly double-representable it is false even though v is integral
        // (review finding C3). AqlBuilder therefore uses a bare IS_NUMBER guard for BIGINT range and lets
        // the residual re-check drop fractionals -- this pins why FLOOR was removed.
        assertThat(eval("42 == FLOOR(42.0)")).isEqualTo(true);                            // fine for small values
        assertThat(eval("9007199254740993 == FLOOR(9007199254740993)")).isEqualTo(false); // 2^53+1 wrongly rejected
        assertThat(eval("42.5 == FLOOR(42.5)")).isEqualTo(false);                         // fractional correctly rejected
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
        System.out.println("driver numeric types: i=" + doc.get("i").getClass().getName()
                + " big=" + doc.get("big").getClass().getName()
                + " f=" + doc.get("f").getClass().getName());
        assertThat(doc.get("i")).isInstanceOf(Integer.class);
        assertThat(doc.get("big")).isInstanceOf(Long.class);
        assertThat(doc.get("f")).isInstanceOf(Double.class);
    }

    @Test
    void doubleComparisonAgreesWithJavaAtInt64DoublePrecisionBoundary() {
        // Beyond 2^53, not every long has an exact double representation, so int64 and double
        // comparison can diverge. This pins the STORED-DOUBLE case: AQL's `<`/`>` over stored double
        // values agrees with plain Java double comparison. The riskier STORED-INT64-in-a-DOUBLE-column
        // case (where ArangoDB compares exactly but the read path rounds) is characterized separately in
        // mixedInt64DoubleComparisonNeedsDoublePromotion -- together they cover why DOUBLE pushdown must
        // promote the operand with `+ 0.0` yet can still be fully enforced by the pushed AQL alone.
        client.createDocumentCollectionForTest("probe", "boundary");
        double justBelow = 9_007_199_254_740_990.0; // < 2^53
        double justAbove = 9_007_199_254_740_994.0; // > 2^53, straddling the boundary with justBelow
        client.insertForTest("probe", "boundary", Map.of("tag", "below", "val", justBelow));
        client.insertForTest("probe", "boundary", Map.of("tag", "above", "val", justAbove));

        double bound = 9_007_199_254_740_992.0; // 2^53, strictly between the two stored values
        Object matched = client.query("probe",
                "RETURN { r: (FOR d IN boundary FILTER d.val > @b SORT d.tag RETURN d.tag) }",
                Map.of("b", bound)).next().get("r");

        assertThat(matched).isEqualTo(List.of("above"));
        assertThat(justAbove > bound).isTrue();
        assertThat(justBelow > bound).isFalse();
    }

    @Test
    void mixedInt64DoubleComparisonNeedsDoublePromotion() {
        // THE C1 PREMISE. A DOUBLE column legitimately holds stored int64 values (it is inferred DOUBLE
        // because the sample saw both ints and floats). ArangoPageSource.appendValue reads such a value
        // ROUNDED to double (n.doubleValue()), but ArangoDB compares int64-vs-double by EXACT mathematical
        // value, not in double space -- so a bare `d.val <op> @bind` diverges from the read path beyond 2^53.
        // AqlBuilder promotes a DOUBLE operand with `+ 0.0`, forcing double-space comparison that matches the
        // read path. Both divergence directions are pinned here so a driver/engine change fails loudly.

        // Direction 1 -- false INCLUDE: stored int64 2^53+1 reads back rounded to 2^53.
        client.createDocumentCollectionForTest("probe", "mixedGt");
        long v1 = 9_007_199_254_740_993L;            // 2^53 + 1, exact as int64
        client.insertForTest("probe", "mixedGt", Map.of("val", v1));
        double b1 = 9_007_199_254_740_992.0;         // 2^53
        assertThat((double) v1).isEqualTo(b1);       // Java rounds the stored value to 2^53
        assertThat(countMatches("mixedGt", "d.val > @b", b1)).as("bare: exact int64 wrongly includes").isEqualTo(1);
        assertThat(countMatches("mixedGt", "(d.val + 0.0) > @b", b1)).as("promoted: matches read path").isEqualTo(0);
        assertThat((double) v1 > b1).as("Java read path excludes").isFalse();

        // Direction 2 -- false MISS: stored int64 2^54-1 reads back rounded to 2^54.
        client.createDocumentCollectionForTest("probe", "mixedEq");
        long v2 = 18_014_398_509_481_983L;           // 2^54 - 1, exact as int64
        client.insertForTest("probe", "mixedEq", Map.of("val", v2));
        double b2 = 18_014_398_509_481_984.0;        // 2^54
        assertThat((double) v2).isEqualTo(b2);       // Java rounds the stored value to 2^54
        assertThat(countMatches("mixedEq", "d.val == @b", b2)).as("bare: exact int64 wrongly misses").isEqualTo(0);
        assertThat(countMatches("mixedEq", "(d.val + 0.0) == @b", b2)).as("promoted: matches read path").isEqualTo(1);

        // Guard safety: `+ 0.0` coerces non-numbers ("abc" + 0.0 == 0.0), so the IS_NUMBER guard
        // AqlBuilder emits ahead of the promotion is load-bearing, not decorative.
        client.createDocumentCollectionForTest("probe", "mixedStr");
        client.insertForTest("probe", "mixedStr", Map.of("val", "abc"));
        assertThat(countMatches("mixedStr", "(d.val + 0.0) == @b", 0.0)).as("unguarded coerces \"abc\"->0").isEqualTo(1);
        assertThat(countMatches("mixedStr", "IS_NUMBER(d.val) AND (d.val + 0.0) == @b", 0.0)).as("guard excludes it").isEqualTo(0);
    }

    @Test
    void bigintExactComparisonMustNotBePromoted() {
        // The MIRROR premise: BIGINT must stay bare. Its read path is exact (longValue() on an integral
        // value, no rounding) against long binds, so ArangoDB's exact int64 comparison is what agrees.
        // Promoting BIGINT with `+ 0.0` would round the stored value and reintroduce C1 in reverse. Pinned
        // so a future edit that leaks the promotion onto the BIGINT path fails here.
        client.createDocumentCollectionForTest("probe", "bigints");
        long v = 9_007_199_254_740_993L;             // 2^53 + 1, exact as int64
        client.insertForTest("probe", "bigints", Map.of("val", v));
        // Bare exact comparison against the exact long bind agrees with the exact read path.
        assertThat(countMatches("bigints", "d.val == @b", v)).as("bare exact matches (correct)").isEqualTo(1);
        // Promotion would round the stored 2^53+1 to 2^53 and miss the long bind -- the behavior to avoid.
        assertThat(countMatches("bigints", "(d.val + 0.0) == @b", v)).as("promoted would wrongly miss it").isEqualTo(0);
    }

    private long countMatches(String collection, String predicate, Object bind) {
        Object r = client.query("probe",
                "RETURN { r: LENGTH(FOR d IN " + collection + " FILTER " + predicate + " RETURN 1) }",
                Map.of("b", bind)).next().get("r");
        return ((Number) r).longValue();
    }
}
