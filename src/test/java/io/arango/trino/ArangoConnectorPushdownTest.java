package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
            // "val" column as BIGINT; a later non-numeric doc and a later fractional-double doc
            // are both invisible to that sample. This relies on ArangoClient.sampleDocuments's
            // unsorted `LIMIT n` landing on these two docs first -- empirically stable (RocksDB
            // primary-index scan order tracks insertion order on a fresh collection with no
            // concurrent writes) but not a documented AQL ordering guarantee. Revisit if
            // sampleDocuments ever gains a SORT clause or the storage engine changes.
            seed.createDocumentCollectionForTest("shop", "skewed");
            seed.insertForTest("shop", "skewed", Map.of("val", 10L));
            seed.insertForTest("shop", "skewed", Map.of("val", 20L));
            seed.insertForTest("shop", "skewed", Map.of("val", "not-a-number"));
            seed.insertForTest("shop", "skewed", Map.of("val", 42.5)); // fractional outlier, out of sample

            // "code" holds a numeric 42 in one doc and the string "42" in another, so it infers
            // as VARCHAR (TypeMapper.merge widens an incompatible String/number pair to VARCHAR).
            // Both docs are within the default sample size, so the inference is order-independent.
            // Used by the regression test proving the re-added VARCHAR-equality pushdown does not
            // silently drop the numeric-42 row.
            seed.createDocumentCollectionForTest("shop", "mixed");
            seed.insertForTest("shop", "mixed", Map.of("label", "num", "code", 42L));
            seed.insertForTest("shop", "mixed", Map.of("label", "str", "code", "42"));

            seed.createDocumentCollectionForTest("shop", "prices");
            seed.insertForTest("shop", "prices", Map.of("amount", 10.5));
            seed.insertForTest("shop", "prices", Map.of("amount", 20.0));

            // "big" samples BIGINT from its first two (sample-size-2 arangoskew) docs; a 3rd,
            // out-of-sample doc holds 1e19 (>= 2^63), which the AQL guard admits but appendValue
            // reads back as NULL -- exercised by bigintRangeAgreesOnOutOfLongRangeOutlier. This
            // relies on ArangoClient.sampleDocuments's unsorted `LIMIT n` landing on the first two
            // docs first -- empirically stable (RocksDB primary-index scan order tracks insertion
            // order on a fresh collection with no concurrent writes) but not a documented AQL
            // ordering guarantee. Revisit if sampleDocuments ever gains a SORT clause or the
            // storage engine changes.
            seed.createDocumentCollectionForTest("shop", "bigskew");
            seed.insertForTest("shop", "bigskew", Map.of("big", 10L));
            seed.insertForTest("shop", "bigskew", Map.of("big", 20L));
            seed.insertForTest("shop", "bigskew", Map.of("big", 1e19)); // >= 2^63, out of sample

            // "bigdbl" is a DOUBLE column (mixed int + float sample) that also holds stored int64 values
            // beyond 2^53. appendValue reads those rounded to double, but a bare AQL comparison against the
            // raw stored int64 compares exactly -- diverging from the read path (review finding C1).
            // AqlBuilder's `+ 0.0` promotion closes that gap; exercised by
            // doublePushdownAgreesWithReadPathForStoredInt64BeyondPrecision.
            seed.createDocumentCollectionForTest("shop", "bigdbl");
            seed.insertForTest("shop", "bigdbl", Map.of("v", 1.5));                     // float -> forces DOUBLE inference
            seed.insertForTest("shop", "bigdbl", Map.of("v", 9_007_199_254_740_993L));  // 2^53+1, reads back as 2^53
            seed.insertForTest("shop", "bigdbl", Map.of("v", 18_014_398_509_481_983L)); // 2^54-1, reads back as 2^54

            // "bigint53" is an all-integer (BIGINT) column holding a stored int64 beyond 2^53 that is not
            // exactly double-representable. The read path reads it exactly (longValue()), so a range filter
            // must return it -- but a `== FLOOR(...)` guard would drop it server-side (FLOOR returns a
            // double). Exercised by bigintRangeReturnsInt64BeyondPrecisionThatFloorGuardWouldDrop (finding C3).
            seed.createDocumentCollectionForTest("shop", "bigint53");
            seed.insertForTest("shop", "bigint53", Map.of("age", 100L));
            seed.insertForTest("shop", "bigint53", Map.of("age", 9_007_199_254_740_993L)); // 2^53+1
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
        queryRunner.createCatalog("arangostrict", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword(),
                "arangodb.type-coercion", "STRICT"));
        return queryRunner;
    }

    @AfterAll
    void teardown() {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void bigintEqualityFilterIsFullyPushedDown() {
        // BIGINT equality is now pushed: AQL `==` is type-strict and the type-exact read path agrees,
        // so no guard is needed. age = 30 matches only 'ada'.
        assertThat(query("SELECT name FROM arango.shop.users WHERE age = 30"))
                .matches("VALUES VARCHAR 'ada'")
                .isFullyPushedDown();
    }

    @Test
    void bigintRangeFilterIsResidualButStillCorrect() {
        // BIGINT range is pushed to AQL as a prefilter and re-checked residually (isPrefilterOnly):
        // the guard admits integral values outside signed-64-bit range that appendValue reads as
        // NULL, so the same domain also stays in the residual. Trino re-applies `age > 28` itself,
        // correctly returning 'ada' and 'bob'.
        assertThat(query("SELECT name FROM arango.shop.users WHERE age > 28"))
                .matches("VALUES (VARCHAR 'ada'), (VARCHAR 'bob')")
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    void booleanEqualityFilterIsFullyPushedDown() {
        // Equality/IN is pushed for BOOLEAN/VARCHAR/BIGINT/DOUBLE: the type-exact read path
        // (ArangoPageSource.appendValue) agrees with AQL's type-strict `==`, so the AQL-side and
        // Trino-side answers can never diverge. Fully pushed and correct: active = true matches
        // 'ada','cleo'.
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
    void limitIsNotFullyPushedDownWhenShardParallelismEnabled() {
        // Master spec §6.2 (Task 7 fix): with shard-parallelism enabled (the default), a table
        // may fan out into multiple splits (ArangoSplitManager), each of which applies LIMIT n
        // independently, so the pushed AQL LIMIT is no longer an exact global cap.
        // ArangoMetadata.applyLimit now reports limitGuaranteed=false, so Trino must keep its own
        // Limit node on top of the pushed-down AQL LIMIT rather than eliding it (ArangoMetadataLimitTest
        // covers the boolean directly; this proves the planner actually honors it end to end).
        assertThat(query("SELECT name FROM arango.shop.users LIMIT 2"))
                .isNotFullyPushedDown(LimitNode.class);
    }

    @Test
    void nestedProjectionReturnsCorrectValueProvingPushdownEngaged() {
        // Since M4, ValueMaterializer can materialize a whole ROW, so a value-only assertion here
        // would no longer discriminate: a failed dereference push would fall back to materializing
        // the whole "address" ROW and evaluating ".city" over it in a residual ProjectNode, and
        // that path returns the same correct value. What actually proves the dereference was
        // pushed is the plan shape: `name = 'dee'` is a fully-pushed VARCHAR equality, so if the
        // dereference is *also* pushed (as an ArangoColumnHandle with path ["address","city"] --
        // see ArangoMetadataTest.applyProjectionPushesNestedFieldDereference for the direct unit
        // guard), the plan is just a TableScan with nothing left to elide; a failed push would
        // instead leave a ProjectNode computing .city over the materialized ROW.
        assertThat(query("SELECT address.city FROM arango.shop.people WHERE name = 'dee'"))
                .matches("VALUES VARCHAR 'nyc'")
                .isFullyPushedDown();
    }

    @Test
    void residualFilterIsCorrectOnSampleTypeSkewedColumn() {
        // arangoskew's "val" samples as BIGINT from its first 2 (sample-size=2) numeric docs; a
        // 3rd doc's "val" is the string "not-a-number" and a 4th holds fractional 42.5, both
        // invisible to the sample. `val > 5` is a BIGINT range predicate, pushed to AQL as a prefilter
        // (bare IS_NUMBER guard) AND kept in Trino's residual (isPrefilterOnly). The AQL guard admits a
        // SUPERSET: IS_NUMBER drops "not-a-number" server-side, but fractional 42.5 PASSES it
        // (IS_NUMBER(42.5) and 42.5 > 5), flows through ValueMaterializer, and reads back NULL -- so Trino's residual
        // re-check is what excludes it. The guard is intentionally bare: a `== FLOOR(...)` conjunct would
        // false-miss a stored int64 > 2^53 (review finding C3). Same correct result (10, 20), now
        // genuinely exercising the prefilter+residual backstop.
        MaterializedResult result = getQueryRunner().execute(
                "SELECT val FROM arangoskew.shop.skewed WHERE val > 5");
        assertThat(result.getOnlyColumnAsSet()).containsExactlyInAnyOrder(10L, 20L);
    }

    @Test
    void varcharEqualityIsFullyPushedDownAndMixedTypeRowIsNotSilentlyDropped() {
        // shop.mixed.code holds numeric 42 (label 'num') and string "42" (label 'str'), inferring
        // VARCHAR. With type-exact coercion, the numeric 42 in a VARCHAR column now reads as NULL
        // (not "42"), so `code = '42'` correctly matches only the genuine string row. VARCHAR
        // equality is now pushed: AQL `==` is type-strict and the type-exact read path agrees, so
        // no guard is needed -- fully pushed and still correct.
        assertThat(query("SELECT label FROM arango.shop.mixed WHERE code = '42'"))
                .matches("VALUES VARCHAR 'str'")
                .isFullyPushedDown();
    }

    @Test
    void strictModeRaisesOnTypeMismatch() {
        // arango.shop.mixed.code infers VARCHAR; the numeric-42 doc is a type mismatch. Under the
        // strict catalog, reading it raises rather than NULLing. Strict mode declines all
        // pushdown, so Trino reads every row itself, hitting the mismatch.
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT code FROM arangostrict.shop.mixed"))
                .hasMessageContaining("expected varchar");
    }

    @Test
    void strictModeDeclinesPushdownLeavingResidual() {
        // Strict mode declines all pushdown, so age = 30 stays residual (Trino applies it post-read).
        // users is clean-typed, so reading it under strict raises nothing. Correct: age = 30 -> 'ada'.
        assertThat(query("SELECT name FROM arangostrict.shop.users WHERE age = 30"))
                .matches("VALUES VARCHAR 'ada'")
                .isNotFullyPushedDown(FilterNode.class);
    }

    @Test
    void varcharInFilterIsFullyPushedDown() {
        assertThat(query("SELECT name FROM arango.shop.users WHERE name IN ('ada', 'bob')"))
                .matches("VALUES (VARCHAR 'ada'), (VARCHAR 'bob')")
                .isFullyPushedDown();
    }

    @Test
    void doubleEqualityFilterIsFullyPushedDown() {
        // DOUBLE equality is pushed alongside BOOLEAN/VARCHAR/BIGINT, but rendered with an IS_NUMBER
        // guard and a `+ 0.0` promotion so AQL compares in the same double space appendValue reads
        // (see doublePushdownAgreesWithReadPathForStoredInt64BeyondPrecision). amount = 20.0 matches
        // only the second "prices" doc.
        assertThat(query("SELECT amount FROM arango.shop.prices WHERE amount = 20.0"))
                .matches("VALUES DOUBLE '20.0'")
                .isFullyPushedDown();
    }

    @Test
    void doubleRangeFilterIsFullyPushedDown() {
        // DOUBLE range is fully enforced: the IS_NUMBER guard plus the `+ 0.0` promotion make the pushed
        // AQL admit exactly what the read path writes non-NULL, so nothing is left residual.
        assertThat(query("SELECT amount FROM arango.shop.prices WHERE amount > 15.0"))
                .matches("VALUES DOUBLE '20.0'")
                .isFullyPushedDown();
    }

    @Test
    void doublePushdownAgreesWithReadPathForStoredInt64BeyondPrecision() {
        // Regression for review finding C1. "bigdbl.v" is DOUBLE but holds stored int64 values > 2^53,
        // which appendValue reads back ROUNDED (2^53+1 -> 2^53, 2^54-1 -> 2^54). Before the `+ 0.0`
        // promotion the pushed AQL compared the raw int64 EXACTLY, diverging from that rounded read path
        // in both directions. Both are pinned here and must stay fully pushed down.
        //
        // False-INCLUDE direction: `v > 2^53`. The stored 2^53+1 reads as 2^53 (not > 2^53) so it must be
        // EXCLUDED; only the 2^54-1 doc (reads as 2^54) qualifies. A bare comparison would have wrongly
        // included the 2^53+1 row.
        assertThat(query("SELECT v FROM arango.shop.bigdbl WHERE v > DOUBLE '9.007199254740992E15'"))
                .matches("VALUES DOUBLE '1.8014398509481984E16'")
                .isFullyPushedDown();

        // False-MISS direction: `v = 2^54`. The stored 2^54-1 reads as 2^54, so it must be INCLUDED. A
        // bare comparison would have excluded it server-side (2^54-1 != 2^54), silently dropping the row.
        assertThat(query("SELECT v FROM arango.shop.bigdbl WHERE v = DOUBLE '1.8014398509481984E16'"))
                .matches("VALUES DOUBLE '1.8014398509481984E16'")
                .isFullyPushedDown();
    }

    @Test
    void bigintRangeReturnsInt64BeyondPrecisionThatFloorGuardWouldDrop() {
        // Regression for review finding C3. "bigint53.age" holds a stored int64 2^53+1 that is not exactly
        // double-representable. The read path reads it exactly (longValue()), so `age > 100` must return it.
        // The old `age == FLOOR(age)` guard dropped it server-side (FLOOR returns a double: 2^53+1 != 2^53.0),
        // a silent false-miss the C2 residual could not recover. With the bare IS_NUMBER guard it is returned.
        // BIGINT range is prefilter-only, so this stays not-fully-pushed-down (the residual re-check is kept).
        assertThat(query("SELECT age FROM arango.shop.bigint53 WHERE age > 100"))
                .matches("VALUES BIGINT '9007199254740993'")
                .isNotFullyPushedDown(FilterNode.class);
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
}
