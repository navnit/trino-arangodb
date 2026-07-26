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
import java.util.HashMap;
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
            seed.insertForTest("agg", "sales", Map.of("city", "nyc", "qty", 3L, "price", 10.5));
            seed.insertForTest("agg", "sales", Map.of("city", "nyc", "qty", 5L, "price", 2.5));
            seed.insertForTest("agg", "sales", Map.of("city", "sfo", "qty", 7L, "price", 4.0));

            // Dirty fixture: the first two documents type the columns (sample-size 2), so the
            // later mismatched / absent / out-of-range values are invisible to inference and
            // exercise the guards. This depends on sampleDocuments' unsorted LIMIT landing on
            // insertion order -- the same assumption ArangoConnectorPushdownTest documents and
            // relies on (empirically stable on a fresh collection, not an AQL guarantee).
            seed.createDocumentCollectionForTest("agg", "dirty");
            seed.insertForTest("agg", "dirty", Map.of("g", "a", "n", 10L, "x", 1.0));
            seed.insertForTest("agg", "dirty", Map.of("g", "b", "n", 20L, "x", 2.0));
            seed.insertForTest("agg", "dirty", Map.of("g", "a", "n", 42.5, "x", "not-a-number"));
            seed.insertForTest("agg", "dirty", Map.of("g", "b", "n", 1e19, "x", 4.0));
            seed.insertForTest("agg", "dirty", Map.of("g", "a"));
            Map<String, Object> allNullGroup = new HashMap<>();
            allNullGroup.put("g", "c");
            allNullGroup.put("n", null);
            allNullGroup.put("x", null);
            seed.insertForTest("agg", "dirty", allNullGroup);

            // Review finding B1: a BIGINT column holding 0, -0.0 and 0.0. All three read back as
            // BIGINT 0, so SQL sees exactly one group.
            seed.createDocumentCollectionForTest("agg", "zeros");
            seed.insertForTest("agg", "zeros", Map.of("z", 0L));
            seed.insertForTest("agg", "zeros", Map.of("z", -0.0d));
            seed.insertForTest("agg", "zeros", Map.of("z", 0.0d));
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

    @Test
    void disablingPushdownChangesNothingButThePlan() {
        assertSameAsReference("SELECT city, count(*), sum(price) FROM %s.sales GROUP BY city");
    }
}
