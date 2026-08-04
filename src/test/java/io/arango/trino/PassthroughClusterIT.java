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
        Optional<AqlReadOnlyGate.Rejection> v =
                verdict("FOR d IN users UPDATE d WITH {x: 1} IN users");
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
                                + "database => '"
                                + DB
                                + "', "
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
                                                + "database => '"
                                                + DB
                                                + "', "
                                                + "query => 'INSERT {x: 1} INTO users'))"))
                .hasMessageContaining("read-only");
        assertThat(userCount()).isEqualTo(before);
    }
}
