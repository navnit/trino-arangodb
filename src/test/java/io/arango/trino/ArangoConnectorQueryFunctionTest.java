package io.arango.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;
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
            seed.insertForTest("shop", "follows", Map.of("_from", "users/ada", "_to", "users/bob"));
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
                        "arangodb.hosts",
                        server.hostPort(),
                        "arangodb.user",
                        "root",
                        "arangodb.password",
                        server.rootPassword(),
                        "arangodb.query-function-enabled",
                        "false"));
        // small planning sample: lets a test place a pathological row BEYOND the derivation batch
        queryRunner.createCatalog(
                "arango_k2",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts",
                        server.hostPort(),
                        "arangodb.user",
                        "root",
                        "arangodb.password",
                        server.rootPassword(),
                        "arangodb.schema.sample-size",
                        "2"));
    }

    @AfterAll
    void teardown() {
        if (queryRunner != null) queryRunner.close();
        if (server != null) server.close();
    }

    private long userCount() {
        return (long) queryRunner.execute("SELECT count(*) FROM arango.shop.users").getOnlyValue();
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
        // Positive control: run the identical query text against BOTH catalogs (same server,
        // same database, differing only in arangodb.query-function-enabled) so the difference in
        // outcome is pinned to the flag, not merely to an error-message string that could pass for
        // an unrelated reason. Enabled must succeed, then disabled must fail with "not registered".
        String queryText = "FOR d IN users RETURN {name: d.name}";

        MaterializedResult enabled =
                queryRunner.execute(
                        "SELECT * FROM TABLE(arango.system.query(database => 'shop', query => '"
                                + queryText
                                + "'))");
        assertThat(enabled.getRowCount()).isEqualTo(2);

        assertThatThrownBy(
                        () ->
                                queryRunner.execute(
                                        "SELECT * FROM TABLE(arango_off.system.query(database"
                                                + " => 'shop', query => '"
                                                + queryText
                                                + "'))"))
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
