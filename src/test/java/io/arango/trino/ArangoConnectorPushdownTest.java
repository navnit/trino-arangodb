package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

class ArangoConnectorPushdownTest extends AbstractTestQueryFramework {
    private TestingArangoServer server;

    @Override
    protected QueryRunner createQueryRunner() throws Exception {
        server = new TestingArangoServer();
        try (ArangoClient seed = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword()))) {
            seed.createDatabaseForTest("shop");

            seed.createDocumentCollectionForTest("shop", "users");
            seed.insertForTest("shop", "users", Map.of("name", "ada", "age", 30L, "active", true));
            seed.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L, "active", false));
            seed.insertForTest("shop", "users", Map.of("name", "cleo", "age", 25L, "active", true));

            seed.createDocumentCollectionForTest("shop", "people");
            seed.insertForTest("shop", "people", Map.of(
                    "name", "dee", "address", Map.of("city", "nyc", "zip", "10001")));

            // First two docs in "skewed" are numeric-only, so a 2-row sample types the
            // "val" column as BIGINT; a later, non-numeric doc is invisible to that sample.
            seed.createDocumentCollectionForTest("shop", "skewed");
            seed.insertForTest("shop", "skewed", Map.of("val", 10L));
            seed.insertForTest("shop", "skewed", Map.of("val", 20L));
            seed.insertForTest("shop", "skewed", Map.of("val", "not-a-number"));
        }

        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("arango").setSchema("shop").build())
                .build();
        queryRunner.installPlugin(new ArangoPlugin());
        queryRunner.createCatalog("arango", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword()));
        queryRunner.createCatalog("arangoskew", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword(),
                "arangodb.schema.sample-size", "2"));
        return queryRunner;
    }

    @AfterAll
    void teardown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void equalityFilterIsFullyPushedDown() {
        assertThat(query("SELECT name FROM arango.shop.users WHERE age = 30")).isFullyPushedDown();
    }

    @Test
    void numericRangeFilterIsFullyPushedDownAndCorrect() {
        assertThat(query("SELECT name FROM arango.shop.users WHERE age > 28"))
                .matches("VALUES (VARCHAR 'ada'), (VARCHAR 'bob')")
                .isFullyPushedDown();
    }

    @Test
    void isNotNullFilterIsNotPushedDownButStillCorrect() {
        // IS NULL/IS NOT NULL are never pushed (see Global Constraints: AQL's `== null`/`!= null`
        // test the raw stored value, which can disagree with Trino's post-coercion null-ness for
        // any sample/reality type mismatch). Proven two ways: a residual FilterNode remains in
        // the plan, and the result is correct.
        assertThat(query("SELECT name FROM arango.shop.users WHERE age IS NOT NULL"))
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    void isNotNullFilterIsCorrectUnderSampleTypeSkew() {
        // arangoskew's "val" samples as BIGINT from its first 2 (sample-size=2) numeric docs; a
        // 3rd doc's "val" is a string, invisible to the sample. If IS NOT NULL were pushed as a
        // raw `!= null` in AQL, it would wrongly include this row (present-but-wrong-type is
        // "not null" to AQL) even though ArangoPageSource.appendValue's lenient coercion nulls it
        // out for Trino. Left residual, Trino applies the predicate itself post-coercion,
        // correctly excluding the row -- this is the exact regression Task 3's review caught.
        MaterializedResult result = getQueryRunner().execute(
                "SELECT val FROM arangoskew.shop.skewed WHERE val IS NOT NULL");
        assertThat(result.getOnlyColumnAsSet()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void limitIsFullyPushedDown() {
        assertThat(query("SELECT name FROM arango.shop.users LIMIT 2")).isFullyPushedDown();
    }

    @Test
    void nestedProjectionReturnsCorrectValueProvingPushdownEngaged() {
        // If applyProjection failed to push this FieldDereference, Trino would fall back to
        // materializing the whole "address" ROW itself -- which ArangoPageSourceProvider
        // .checkMaterializable refuses to do (TrinoException NOT_SUPPORTED). So this query
        // succeeding with the right value is itself proof the dereference was pushed.
        assertThat(query("SELECT address.city FROM arango.shop.people WHERE name = 'dee'"))
                .matches("VALUES VARCHAR 'nyc'");
    }

    @Test
    void mixedTypeGuardExcludesNonNumericValueUnderSampleSkew() {
        MaterializedResult result = getQueryRunner().execute(
                "SELECT val FROM arangoskew.shop.skewed WHERE val > 5");
        assertThat(result.getOnlyColumnAsSet()).containsExactlyInAnyOrder(10L, 20L);
    }
}
