package io.arango.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * The decisive M5 tests: every claimed aggregate must return exactly what the same query returns
 * with pushdown disabled. The "noagg" catalog is that reference -- identical configuration except
 * {@code arangodb.aggregation-pushdown-enabled=false}.
 */
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

            // Clean fixture: every value matches its inferred type, so pushed and unpushed results
            // must agree exactly.
            seed.createDocumentCollectionForTest("agg", "sales");
            seed.insertForTest(
                    "agg", "sales", Map.of("city", "nyc", "qty", 3L, "price", 10.5, "vip", true));
            seed.insertForTest(
                    "agg", "sales", Map.of("city", "nyc", "qty", 5L, "price", 2.5, "vip", false));
            seed.insertForTest(
                    "agg", "sales", Map.of("city", "sfo", "qty", 7L, "price", 4.0, "vip", true));

            // Dirty fixture: only the CLEAN documents exist at schema-resolution time, so the
            // columns infer as VARCHAR/BIGINT/DOUBLE. The mismatched, absent and out-of-range
            // values are added after the schema is resolved and cached (see below), which is what
            // makes them invisible to inference and forces the guards to do the work.
            //
            // Determinism note: an earlier version simply inserted the dirty documents last and
            // relied on sampleDocuments' unsorted `LIMIT n` returning insertion order -- stable in
            // practice but not an AQL guarantee. Warming the schema cache instead removes the
            // dependency on scan order entirely.
            seed.createDocumentCollectionForTest("agg", "dirty");
            seed.insertForTest("agg", "dirty", Map.of("g", "a", "n", 10L, "x", 1.5));
            seed.insertForTest("agg", "dirty", Map.of("g", "b", "n", 20L, "x", 2.5));

            // Nested-field fixture: applyProjection rewrites `addr.city` into a synthetic
            // `addr$city` column handle, which applyAggregation then groups by -- two pushdowns
            // composing, which nothing else exercises through SQL.
            seed.createDocumentCollectionForTest("agg", "people");
            seed.insertForTest(
                    "agg", "people", Map.of("addr", Map.of("city", "nyc", "zip", "10001")));
            seed.insertForTest(
                    "agg", "people", Map.of("addr", Map.of("city", "nyc", "zip", "10002")));
            seed.insertForTest(
                    "agg", "people", Map.of("addr", Map.of("city", "sfo", "zip", "94101")));

            // Review finding B1: a BIGINT column holding 0, -0.0 and 0.0. All three read back as
            // BIGINT 0, so SQL sees exactly one group.
            seed.createDocumentCollectionForTest("agg", "zeros");
            seed.insertForTest("agg", "zeros", Map.of("z", 0L));
            seed.insertForTest("agg", "zeros", Map.of("z", -0.0d));
            seed.insertForTest("agg", "zeros", Map.of("z", 0.0d));

            // Declared-type fixture (M6-C task 9): a schema-override doc types "amount" DECIMAL
            // and "at" TIMESTAMP, neither of which ColumnGuard.predicate/AggregatePushdown's
            // BIGINT/DOUBLE-only allowlists admit. Used to pin that aggregation pushdown
            // auto-declines both across min/max, sum, count(col) and grouping-key, while an
            // ordinary VARCHAR column on the same override-driven table still pushes.
            seed.createDocumentCollectionForTest("agg", "declared");
            seed.insertForTest(
                    "agg",
                    "declared",
                    Map.of("tag", "x", "amount", "12.34", "at", "2026-01-02T03:04:05.678"));
            seed.createDocumentCollectionForTest("agg", "trino_schema");
            seed.insertForTest(
                    "agg",
                    "trino_schema",
                    Map.of(
                            "table",
                            "declared",
                            "fields",
                            List.of(
                                    Map.of("name", "tag", "type", "varchar"),
                                    Map.of("name", "amount", "type", "decimal(12,2)"),
                                    Map.of("name", "at", "type", "timestamp(3)"))));
        }

        QueryRunner runner =
                DistributedQueryRunner.builder(
                                testSessionBuilder().setCatalog("arango").setSchema("agg").build())
                        .build();
        runner.installPlugin(new ArangoPlugin());
        Map<String, String> base =
                ImmutableMap.of(
                        "arangodb.hosts",
                        server.hostPort(),
                        "arangodb.user",
                        "root",
                        "arangodb.password",
                        server.rootPassword(),
                        "arangodb.schema.sample-size",
                        "2");
        runner.createCatalog("arango", "arangodb", base);
        runner.createCatalog(
                "noagg",
                "arangodb",
                ImmutableMap.<String, String>builder()
                        .putAll(base)
                        .put("arangodb.aggregation-pushdown-enabled", "false")
                        .buildOrThrow());

        // Resolve and cache the "dirty" schema from the clean documents only -- one query per
        // catalog, since each has its own connector and therefore its own schema cache (TTL 5m,
        // far longer than this class runs). Only then add the documents that contradict it.
        runner.execute("SELECT g, n, x FROM arango.agg.dirty");
        runner.execute("SELECT g, n, x FROM noagg.agg.dirty");
        try (ArangoClient seed =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()))) {
            seed.insertForTest("agg", "dirty", Map.of("g", "a", "n", 42.5, "x", "not-a-number"));
            seed.insertForTest("agg", "dirty", Map.of("g", "b", "n", 1e19, "x", 4.5));
            seed.insertForTest("agg", "dirty", Map.of("g", "a"));
            Map<String, Object> allNullGroup = new HashMap<>();
            allNullGroup.put("g", "c");
            allNullGroup.put("n", null);
            allNullGroup.put("x", null);
            seed.insertForTest("agg", "dirty", allNullGroup);
        }
        return runner;
    }

    @AfterAll
    void teardown() {
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

    // Written first, because every other pushdown assertion in this class depends on knowing this
    // (design §10/8). The measured plans correct the design's assumption:
    //
    //   - A filter does NOT block aggregation pushdown. Trino fuses the predicate into a
    //     ScanFilterProject rather than leaving a standalone FilterNode, and the DOUBLE case below
    //     pushes fully -- so PushAggregationIntoTableScan is not the thing that stops the BIGINT
    //     case, contrary to what §10/8 originally reasoned.
    //   - What stops it is AggregatePushdown's decline over a prefilter-only constraint. That rule
    //     is therefore load-bearing, not defense-in-depth: a BIGINT range is enforced JOINTLY by
    //     the pushed AQL and Trino's residual re-check, so an aggregate computed on the AQL side
    //     alone would count rows -- fractional or out-of-long-range qty values -- that the residual
    //     drops. Deleting the rule would produce a silently wrong count here.
    @Test
    void residualFilterInteractionIsPinned() {
        // DOUBLE range is fully enforced -- no residual -- so the aggregate pushes straight
        // through the filter.
        assertThat(query("SELECT count(*) FROM arango.agg.sales WHERE price > 3.0"))
                .matches("VALUES BIGINT '2'")
                .isFullyPushedDown();

        // BIGINT range is prefilter-only, so the aggregate is declined and Trino computes it.
        // The answer is correct either way; only the plan differs.
        assertSameAsReference("SELECT count(*) FROM %s.sales WHERE qty > 4");
        assertThat(query("SELECT count(*) FROM arango.agg.sales WHERE qty > 4"))
                .matches("VALUES BIGINT '2'")
                .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class, FilterNode.class);
    }

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

    @Test
    void claimedShapesAreFullyPushedDown() {
        assertThat(query("SELECT count(*) FROM arango.agg.sales")).isFullyPushedDown();
        assertThat(query("SELECT city, count(*) FROM arango.agg.sales GROUP BY city"))
                .isFullyPushedDown();
        assertThat(query("SELECT sum(price) FROM arango.agg.sales")).isFullyPushedDown();
    }

    // The guards' reason for existing: each of these columns holds values invisible to inference
    // that the read path materializes as NULL.
    @Test
    void aggregatesOverDirtyDataMatchTheReference() {
        assertSameAsReference("SELECT count(n), min(n), max(n) FROM %s.dirty");
        assertSameAsReference("SELECT sum(x), avg(x), count(x) FROM %s.dirty");
        assertSameAsReference("SELECT g, count(*), count(n) FROM %s.dirty GROUP BY g");
    }

    // Trino's count of an empty table is 0 and its sum is NULL; AQL's SUM over zero rows is null
    // and over an all-null group is 0, which is why both renderings wrap (design §4/2, §4/3).
    @Test
    void emptyTableAndAllNullGroupFollowSqlNullSemantics() {
        // Zero input rows reach the aggregate: `price = 999.0` is a discrete set, so it is fully
        // enforced in AQL and the aggregate still pushes. (A genuinely empty collection cannot be
        // used -- schema inference samples documents, so it would have no resolvable columns.)
        // count must report 0 where AQL's SUM over zero rows is null, and sum must report NULL.
        MaterializedResult empty =
                computeActual(
                        "SELECT count(*), count(price), sum(price) FROM arango.agg.sales"
                                + " WHERE price = 999.0");
        assertThat(empty.getMaterializedRows()).hasSize(1);
        assertThat(empty.getMaterializedRows().get(0).getField(0)).isEqualTo(0L);
        assertThat(empty.getMaterializedRows().get(0).getField(1)).isEqualTo(0L);
        assertThat(empty.getMaterializedRows().get(0).getField(2)).isNull();
        assertThat(
                        query(
                                "SELECT count(price), sum(price) FROM arango.agg.sales WHERE price = 999.0"))
                .isFullyPushedDown();

        // Group "c" holds only nulls: count = 0, sum = NULL.
        assertSameAsReference("SELECT g, count(x), sum(x) FROM %s.dirty GROUP BY g");
    }

    // Review finding B1's end-to-end regression: 0, -0.0 and 0.0 must be ONE group, not two.
    //
    // This assertion is only non-vacuous because AqlSemanticsAssumptionsTest
    // .bigintGroupingNeedsSignedZeroNormalizationAndAgreesUnderBothCollectMethods proves -- through
    // this same insertForTest path -- that a BARE accessor yields 2 groups under hash-COLLECT. If
    // VelocyPack ever normalized -0.0 to 0 at insert, that test would fail first and tell us this
    // one had stopped testing anything (the trap that made the original §4/12 probe vacuous).
    @Test
    void signedZeroDoesNotSplitAGroup() {
        MaterializedResult grouped =
                computeActual("SELECT z, count(*) FROM arango.agg.zeros GROUP BY z");
        assertThat(grouped.getMaterializedRows()).hasSize(1);
        assertSameAsReference("SELECT z, count(*) FROM %s.zeros GROUP BY z");
    }

    // Declined shapes must still return correct results -- Trino just computes them itself.
    @Test
    void declinedShapesStillReturnCorrectResults() {
        assertSameAsReference("SELECT min(city), max(city) FROM %s.sales");
        assertSameAsReference("SELECT sum(qty), avg(qty) FROM %s.sales");
        assertSameAsReference("SELECT count(DISTINCT city) FROM %s.sales");
    }

    @Test
    void declinedShapesAreNotPushedDown() {
        assertThat(query("SELECT min(city) FROM arango.agg.sales"))
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT sum(qty) FROM arango.agg.sales"))
                .isNotFullyPushedDown(AggregationNode.class);
    }

    // Review finding M3: AggregatePushdown declines any aggregate whose argument is not a plain
    // column reference, so count(1) pushes only if Trino canonicalizes the constant argument away
    // before the connector sees it. Pinned because it is a common query shape and the answer
    // decides whether widening the rule to accept a non-null Constant is worth doing.
    @Test
    void countOfAConstantIsPinned() {
        assertThat(query("SELECT count(1) FROM arango.agg.sales"))
                .matches("VALUES BIGINT '3'")
                .isFullyPushedDown();
    }

    // The g0/g1/... rendering loop in buildAggregate had never executed with more than one
    // grouping column anywhere in the suite -- unit tests included.
    @Test
    void multiColumnGroupByMatchesTheReference() {
        assertSameAsReference("SELECT city, qty, count(*) FROM %s.sales GROUP BY city, qty");
        assertSameAsReference(
                "SELECT city, vip, count(*), sum(price) FROM %s.sales GROUP BY city, vip");
        assertThat(query("SELECT city, qty, count(*) FROM arango.agg.sales GROUP BY city, qty"))
                .isFullyPushedDown();
    }

    // HAVING is the reason applyFilter declines on an aggregated handle: pushed filters render
    // BEFORE the COLLECT, so a pushed HAVING would silently become a WHERE.
    @Test
    void havingIsEvaluatedByTrinoAndStaysCorrect() {
        assertSameAsReference(
                "SELECT city, count(*) FROM %s.sales GROUP BY city HAVING count(*) > 1");
        assertSameAsReference(
                "SELECT city, sum(price) FROM %s.sales GROUP BY city HAVING sum(price) > 5.0");
    }

    // limitGuaranteed is reported true for an aggregated handle (always a single split), so Trino
    // relies on the connector to apply the limit exactly.
    @Test
    void groupByWithLimitReturnsExactlyTheLimit() {
        assertThat(
                        computeActual(
                                        "SELECT city, count(*) FROM arango.agg.sales GROUP BY city"
                                                + " LIMIT 1")
                                .getMaterializedRows())
                .hasSize(1);
        assertThat(
                        computeActual("SELECT DISTINCT city FROM arango.agg.sales LIMIT 1")
                                .getMaterializedRows())
                .hasSize(1);
    }

    // min/max on DOUBLE is claimed but was never asserted end-to-end -- and it is exactly what the
    // S1 fix changed by dropping the `+ 0.0` promotion.
    @Test
    void minMaxOnDoubleAndBooleanColumnsMatchTheReference() {
        assertSameAsReference("SELECT min(price), max(price) FROM %s.sales");
        assertSameAsReference("SELECT count(vip) FROM %s.sales");
        assertSameAsReference("SELECT vip, count(*) FROM %s.sales GROUP BY vip");
        assertThat(query("SELECT min(price), max(price) FROM arango.agg.sales"))
                .isFullyPushedDown();
        assertThat(query("SELECT vip, count(*) FROM arango.agg.sales GROUP BY vip"))
                .isFullyPushedDown();
    }

    // Grouping on a DOUBLE column exercises the `+ 0.0` promotion that collapses -0.0 into 0.0.
    @Test
    void groupByOnADoubleColumnMatchesTheReference() {
        assertSameAsReference("SELECT price, count(*) FROM %s.sales GROUP BY price");
    }

    // isFullyPushedDown proves only that Trino REMOVED its AggregationNode -- that the connector
    // claimed the work. These assertions prove the reduction physically happened in ArangoDB: the
    // connector hands Trino one row per group instead of one row per document. A refactor that
    // returned documents and aggregated locally would keep every result correct and every plan
    // assertion green, and would fail here.
    @Test
    void pushedAggregatesReduceRowsBeforeTheyCrossTheWire() {
        assertQueryStats(
                getSession(),
                "SELECT count(*) FROM arango.agg.sales",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(1),
                results -> {});
        assertQueryStats(
                getSession(),
                "SELECT city, count(*) FROM arango.agg.sales GROUP BY city",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(2),
                results -> {});
        // The same queries without pushdown read every document -- the control that makes the
        // assertions above meaningful rather than incidental.
        assertQueryStats(
                getSession(),
                "SELECT count(*) FROM noagg.agg.sales",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(3),
                results -> {});
        // A declined aggregate must also read everything.
        assertQueryStats(
                getSession(),
                "SELECT min(city) FROM arango.agg.sales",
                stats -> assertThat(stats.getPhysicalInputPositions()).isEqualTo(3),
                results -> {});
    }

    // applyProjection turns `addr.city` into a synthetic addr$city handle and applyAggregation
    // groups by it -- two pushdowns composing. Unit- and wire-tested, but no SQL query exercised
    // the pair until now.
    // Guards the guards. Every dirty-data assertion compares pushed against unpushed, and BOTH
    // sides use the same inferred schema -- so if the fixture silently stopped being dirty, those
    // tests would keep passing while testing nothing. This pins the two properties they depend on.
    //
    // It exists because the fixture DID silently stop being dirty: `x` was seeded as 1.0/2.0, and
    // VelocyPack normalizes an integral double to an integer at insert, so `x` inferred as BIGINT
    // and `sum(x)`/`avg(x)` exercised the DECLINED bigint path instead of the guarded DOUBLE one.
    // Real fractions (1.5/2.5) keep it a DOUBLE column. The same normalization is why design
    // §4/12's original int-vs-double grouping probe was vacuous.
    @Test
    void dirtyFixtureIsActuallyDirty() {
        assertThat(computeActual("DESCRIBE arango.agg.dirty").getMaterializedRows())
                .as("column types the guard tests depend on")
                .anySatisfy(
                        r ->
                                assertThat(List.of(r.getField(0), r.getField(1)))
                                        .isEqualTo(List.of("n", "bigint")))
                .anySatisfy(
                        r ->
                                assertThat(List.of(r.getField(0), r.getField(1)))
                                        .isEqualTo(List.of("x", "double")))
                .anySatisfy(
                        r ->
                                assertThat(List.of(r.getField(0), r.getField(1)))
                                        .isEqualTo(List.of("g", "varchar")));

        // Six documents, but the guards must reject most values of n and x: n keeps only 10 and 20
        // (42.5 is fractional, 1e19 is outside int64, one absent, one stored null); x keeps the
        // three numerics and rejects "not-a-number", the absent field and the stored null.
        MaterializedResult counts =
                computeActual("SELECT count(*), count(n), count(x) FROM arango.agg.dirty");
        assertThat(counts.getMaterializedRows().get(0).getField(0)).isEqualTo(6L);
        assertThat(counts.getMaterializedRows().get(0).getField(1)).isEqualTo(2L);
        assertThat(counts.getMaterializedRows().get(0).getField(2)).isEqualTo(3L);
    }

    @Test
    void groupByOnANestedFieldMatchesTheReference() {
        assertSameAsReference("SELECT addr.city, count(*) FROM %s.people GROUP BY addr.city");
        assertSameAsReference(
                "SELECT addr.city, count(addr.zip) FROM %s.people GROUP BY addr.city");
        assertThat(
                        computeActual(
                                        "SELECT addr.city, count(*) FROM arango.agg.people"
                                                + " GROUP BY addr.city")
                                .getMaterializedRows())
                .hasSize(2);
    }

    @Test
    void disablingPushdownChangesNothingButThePlan() {
        assertSameAsReference("SELECT city, count(*), sum(price) FROM %s.sales GROUP BY city");
    }

    // Guards the guards, same reason dirtyFixtureIsActuallyDirty exists: every assertion below
    // rests on the override actually applying "amount"/"at" as DECIMAL/TIMESTAMP rather than the
    // VARCHAR the raw stored strings would otherwise infer as. If it silently stopped applying,
    // min(at) would pass for the wrong reason (VARCHAR min/max is declined too) while count(at)/
    // GROUP BY at would start pushing and sum(amount) would fail analysis -- so most, but not all,
    // of the ensemble below would still catch it. This pins the premise directly.
    @Test
    void declaredFixtureIsActuallyDeclared() {
        assertThat(computeActual("DESCRIBE arango.agg.declared").getMaterializedRows())
                .anySatisfy(
                        r ->
                                assertThat(List.of(r.getField(0), r.getField(1)))
                                        .isEqualTo(List.of("tag", "varchar")))
                .anySatisfy(
                        r ->
                                assertThat(List.of(r.getField(0), r.getField(1)))
                                        .isEqualTo(List.of("amount", "decimal(12,2)")))
                .anySatisfy(
                        r ->
                                assertThat(List.of(r.getField(0), r.getField(1)))
                                        .isEqualTo(List.of("at", "timestamp(3)")));
    }

    // Pins the four separate decline code paths a declared TIMESTAMP/DECIMAL column hits:
    // isMinMaxable's BIGINT/DOUBLE allowlist (min), specFor's sum gate (DoubleType-only),
    // ColumnGuard.predicate's guard allowlist for count(col), and the grouping-key
    // ColumnGuard.predicate check in AggregatePushdown.plan's groupingColumns loop.
    @Test
    void declaredTimestampAndDecimalColumnsDeclineAggregation() {
        assertThat(query("SELECT min(at) FROM arango.agg.declared"))
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT sum(amount) FROM arango.agg.declared"))
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT count(at) FROM arango.agg.declared"))
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT at, count(*) FROM arango.agg.declared GROUP BY at"))
                .isNotFullyPushedDown(AggregationNode.class);
    }

    // Each declined shape above must still return the value Trino computes locally -- proven both
    // against the "noagg" reference (same pattern as declinedShapesStillReturnCorrectResults) and,
    // since the fixture is a single known row, against an explicit expected value.
    @Test
    void declaredTimestampAndDecimalColumnsStillReturnCorrectResults() {
        assertSameAsReference("SELECT min(at) FROM %s.declared");
        assertSameAsReference("SELECT sum(amount) FROM %s.declared");
        assertSameAsReference("SELECT count(at) FROM %s.declared");
        assertSameAsReference("SELECT at, count(*) FROM %s.declared GROUP BY at");

        MaterializedResult minAt = computeActual("SELECT min(at) FROM arango.agg.declared");
        assertThat(minAt.getMaterializedRows().get(0).getField(0))
                .isEqualTo(LocalDateTime.parse("2026-01-02T03:04:05.678"));

        MaterializedResult sumAmount = computeActual("SELECT sum(amount) FROM arango.agg.declared");
        assertThat((BigDecimal) sumAmount.getMaterializedRows().get(0).getField(0))
                .isEqualByComparingTo(new BigDecimal("12.34"));

        MaterializedResult countAt = computeActual("SELECT count(at) FROM arango.agg.declared");
        assertThat(countAt.getMaterializedRows().get(0).getField(0)).isEqualTo(1L);

        MaterializedResult groupByAt =
                computeActual("SELECT at, count(*) FROM arango.agg.declared GROUP BY at");
        assertThat(groupByAt.getMaterializedRows()).hasSize(1);
        assertThat(groupByAt.getMaterializedRows().get(0).getField(0))
                .isEqualTo(LocalDateTime.parse("2026-01-02T03:04:05.678"));
        assertThat(groupByAt.getMaterializedRows().get(0).getField(1)).isEqualTo(1L);
    }

    // Positive controls: the override doc typing "amount"/"at" doesn't taint the same table's
    // other pushdown-eligible shapes -- a global count(*) and a GROUP BY on the plain VARCHAR
    // "tag" column both still push fully.
    @Test
    void declaredTableStillPushesOrdinaryAggregateShapes() {
        assertThat(query("SELECT count(*) FROM arango.agg.declared")).isFullyPushedDown();
        assertThat(query("SELECT tag, count(*) FROM arango.agg.declared GROUP BY tag"))
                .isFullyPushedDown();
    }
}
