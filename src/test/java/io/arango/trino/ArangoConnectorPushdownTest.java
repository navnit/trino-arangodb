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
            // This relies on ArangoClient.sampleDocuments's unsorted `LIMIT n` landing on
            // these two docs first -- empirically stable (RocksDB primary-index scan order
            // tracks insertion order on a fresh collection with no concurrent writes) but not
            // a documented AQL ordering guarantee. Revisit if sampleDocuments ever gains a
            // SORT clause or the storage engine changes.
            seed.createDocumentCollectionForTest("shop", "skewed");
            seed.insertForTest("shop", "skewed", Map.of("val", 10L));
            seed.insertForTest("shop", "skewed", Map.of("val", 20L));
            seed.insertForTest("shop", "skewed", Map.of("val", "not-a-number"));

            // "code" holds a numeric 42 in one doc and the string "42" in another, so it infers
            // as VARCHAR (TypeMapper.merge widens an incompatible String/number pair to VARCHAR).
            // Both docs are within the default sample size, so the inference is order-independent.
            // Used by the regression test proving the removed VARCHAR-equality pushdown no longer
            // silently drops the numeric-42 row.
            seed.createDocumentCollectionForTest("shop", "mixed");
            seed.insertForTest("shop", "mixed", Map.of("label", "num", "code", 42L));
            seed.insertForTest("shop", "mixed", Map.of("label", "str", "code", "42"));
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
    void bigintEqualityFilterIsResidualButStillCorrect() {
        // BIGINT equality is no longer pushed (only BOOLEAN equality/IN remains pushable -- see
        // ArangoMetadata.isPushable). A raw AQL `==` never coerces, but ArangoPageSource
        // .appendValue coerces any Number to long on read, so the two could disagree for an
        // out-of-sample stored value; left residual, Trino re-applies it post-coercion. Still
        // correct: age = 30 matches only 'ada'.
        assertThat(query("SELECT name FROM arango.shop.users WHERE age = 30"))
                .matches("VALUES VARCHAR 'ada'")
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    void bigintRangeFilterIsResidualButStillCorrect() {
        // BIGINT range is no longer pushed either (isPushable only pushes BOOLEAN discrete sets).
        // Left residual, Trino applies `age > 28` itself, correctly returning 'ada' and 'bob'.
        assertThat(query("SELECT name FROM arango.shop.users WHERE age > 28"))
                .matches("VALUES (VARCHAR 'ada'), (VARCHAR 'bob')")
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    void booleanEqualityFilterIsFullyPushedDown() {
        // BOOLEAN equality/IN is the one predicate still pushed down: a non-Boolean stored value
        // coerces to Trino NULL AND fails AQL's strict `== true`, so the AQL-side and Trino-side
        // answers can never diverge. Fully pushed and correct: active = true matches 'ada','cleo'.
        assertThat(query("SELECT name FROM arango.shop.users WHERE active = true"))
                .matches("VALUES (VARCHAR 'ada'), (VARCHAR 'cleo')")
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
    void residualFilterIsCorrectOnSampleTypeSkewedColumn() {
        // arangoskew's "val" samples as BIGINT from its first 2 (sample-size=2) numeric docs; a
        // 3rd doc's "val" is the string "not-a-number", invisible to the sample. BIGINT ranges are
        // no longer pushed at all (only BOOLEAN discrete sets are -- see ArangoMetadata.isPushable),
        // so there is no AQL-side guard doing the work here: the whole `val > 5` predicate is
        // residual, and Trino applies it post-coercion. ArangoPageSource.appendValue coerces the
        // non-numeric "val" to NULL, which fails `> 5`, so the row is correctly excluded. Same
        // correct result (10, 20) as before -- only the reason changed (residual-always, not an
        // AQL IS_NUMBER guard).
        MaterializedResult result = getQueryRunner().execute(
                "SELECT val FROM arangoskew.shop.skewed WHERE val > 5");
        assertThat(result.getOnlyColumnAsSet()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void varcharEqualityIsResidualSoMixedTypeRowIsNotSilentlyDropped() {
        // Regression proof for the M2 final-review Critical fix. shop.mixed.code holds a numeric 42
        // in one doc and the string "42" in another, so it infers as VARCHAR. Under the old
        // VARCHAR-equality pushdown, `code = '42'` rendered as a type-strict AQL `d["code"] == "42"`,
        // which the numeric-42 doc fails (number != string in AQL) -- that row silently vanished,
        // even though ArangoPageSource.appendValue stringifies 42 -> "42" for Trino, which should
        // match. Now VARCHAR equality is residual: Trino reads both rows, coerces both to "42", and
        // the predicate matches both. So the result correctly includes the previously-dropped row.
        assertThat(query("SELECT label FROM arango.shop.mixed WHERE code = '42'"))
                .matches("VALUES (VARCHAR 'num'), (VARCHAR 'str')")
                .isNotFullyPushedDown(FilterNode.class);
    }
}
