package io.arango.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoConnectorQueryTest {
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
            seed.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
            seed.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
            // edge collection to prove _from/_to surface as visible columns
            seed.createEdgeCollectionForTest("shop", "knows");
            seed.insertForTest(
                    "shop",
                    "knows",
                    Map.of("_from", "users/ada", "_to", "users/bob", "since", 2020L));

            seed.createDocumentCollectionForTest("shop", "profiles");
            seed.insertForTest(
                    "shop",
                    "profiles",
                    Map.of(
                            "who",
                            "ada",
                            "tags",
                            List.of("pioneer", "math"),
                            "address",
                            Map.of("city", "london", "zip", 1815L),
                            "big",
                            new BigInteger("18446744073709551615"))); // uint64 -> DECIMAL(38,0)
            seed.insertForTest(
                    "shop",
                    "profiles",
                    Map.of(
                            "who",
                            "bob",
                            "tags",
                            List.of("ops", 5L), // 5L under merged VARCHAR element -> leaf NULL
                            "address",
                            Map.of("city", "berlin"), // absent zip -> NULL field
                            "big",
                            7L)); // plain long under the DECIMAL column reads back

            // -- M6-C: override-driven table (spec §8 e2e) --
            seed.createDocumentCollectionForTest("shop", "invoices");
            seed.insertForTest(
                    "shop",
                    "invoices",
                    Map.of(
                            "total", "12.34",
                            "placed_at", "2026-08-05T12:34:56.789+05:30",
                            "updated_at", "2026-08-05T12:34:56.789",
                            "internal_note", "secret"));
            // The override also declares "missplled" (sic), matching NO stored attribute --
            // pinning the spec §3 accepted limitation (all-NULL column, no error).
            seed.createDocumentCollectionForTest("shop", "trino_schema");
            seed.insertForTest(
                    "shop",
                    "trino_schema",
                    Map.of(
                            "table",
                            "invoices",
                            "fields",
                            List.of(
                                    Map.of("name", "total", "type", "decimal(12,2)"),
                                    Map.of(
                                            "name",
                                            "placed_at",
                                            "type",
                                            "timestamp(3) with time zone"),
                                    Map.of("name", "updated_at", "type", "timestamp(3)"),
                                    Map.of(
                                            "name",
                                            "internal_note",
                                            "type",
                                            "varchar",
                                            "hidden",
                                            true),
                                    Map.of("name", "missplled", "type", "varchar"))));
            // Malformed override, QUARANTINED in its own database: resolving 'broken' must fail
            // lazily without poisoning shop's schema-wide enumeration.
            seed.createDatabaseForTest("shop_broken");
            seed.createDocumentCollectionForTest("shop_broken", "broken");
            seed.insertForTest("shop_broken", "broken", Map.of("x", 1L));
            seed.createDocumentCollectionForTest("shop_broken", "trino_schema");
            seed.insertForTest(
                    "shop_broken",
                    "trino_schema",
                    Map.of(
                            "table",
                            "broken",
                            "fields",
                            List.of(Map.of("name", "x", "type", "not_a_type"))));
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
        queryRunner.createCatalog(
                "arango_strict",
                "arangodb",
                ImmutableMap.of(
                        "arangodb.hosts",
                        server.hostPort(),
                        "arangodb.user",
                        "root",
                        "arangodb.password",
                        server.rootPassword(),
                        "arangodb.type-coercion",
                        "STRICT"));
    }

    @AfterAll
    void teardown() {
        if (queryRunner != null) queryRunner.close();
        if (server != null) server.close();
    }

    @Test
    void showTablesListsCollection() {
        MaterializedResult r = queryRunner.execute("SHOW TABLES FROM arango.shop");
        assertThat(r.getOnlyColumnAsSet()).contains("users");
    }

    @Test
    void selectReturnsTypedRows() {
        // M6-C e2e (assertion 7): 'users' has no schema-override document. By the time this
        // (and every other pre-existing test in this class) runs, 'shop' also contains a
        // trino_schema collection (seeded in @BeforeAll for the override-driven 'invoices'
        // table below) -- this test continuing to pass proves a table with no matching
        // override resolves exactly as before, unaffected by trino_schema's mere existence.
        MaterializedResult r =
                queryRunner.execute("SELECT name, age FROM arango.shop.users ORDER BY age");
        assertThat(r.getRowCount()).isEqualTo(2);
        assertThat(r.getMaterializedRows().get(0).getField(0)).isEqualTo("ada");
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo(36L);
        assertThat(r.getMaterializedRows().get(1).getField(0)).isEqualTo("bob");
    }

    @Test
    void edgeCollectionExposesFromAndToColumns() {
        MaterializedResult r =
                queryRunner.execute("SELECT \"_from\", \"_to\", since FROM arango.shop.knows");
        assertThat(r.getRowCount()).isEqualTo(1);
        assertThat(r.getMaterializedRows().get(0).getField(0)).isEqualTo("users/ada");
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo("users/bob");
        assertThat(r.getMaterializedRows().get(0).getField(2)).isEqualTo(2020L);
    }

    @Test
    void arrayColumnMaterializesWithLeafNulls() {
        MaterializedResult r =
                queryRunner.execute("SELECT who, tags FROM arango.shop.profiles ORDER BY who");
        assertThat(r.getRowCount()).isEqualTo(2);
        assertThat(r.getMaterializedRows().get(0).getField(1))
                .isEqualTo(List.of("pioneer", "math"));
        // element 5L under the merged VARCHAR element type is a leaf mismatch -> NULL, not row loss
        assertThat(r.getMaterializedRows().get(1).getField(1))
                .isEqualTo(Arrays.asList("ops", null));
    }

    @Test
    void rowColumnAndScalarDereferenceBothMaterializeCorrectValues() {
        // whole-row select works post-M4 ...
        MaterializedResult whole =
                queryRunner.execute("SELECT address FROM arango.shop.profiles WHERE who = 'bob'");
        assertThat(whole.getRowCount()).isEqualTo(1);
        assertThat(whole.getMaterializedRows().get(0).getField(0)).isNotNull();
        // ... and the pre-existing scalar dereference query still returns the correct value.
        // Note: this class uses a raw QueryRunner (no plan-shape assertions available), so this
        // is a value-only check -- it does NOT prove the dereference was pushed down as opposed
        // to Trino falling back to materializing the whole ROW and evaluating ".city" itself
        // (since M4, ValueMaterializer supports whole-ROW materialization, so that fallback would
        // also return "london"/"berlin" here). The genuine pushdown guard is
        // ArangoMetadataTest.applyProjectionPushesNestedFieldDereference (asserts the pushed
        // column handle's path is ["address","city"]); a plan-shape variant of this check lives in
        // ArangoConnectorPushdownTest.nestedProjectionReturnsCorrectValueProvingPushdownEngaged.
        MaterializedResult city =
                queryRunner.execute("SELECT address.city FROM arango.shop.profiles ORDER BY who");
        assertThat(city.getMaterializedRows().get(0).getField(0)).isEqualTo("london");
        assertThat(city.getMaterializedRows().get(1).getField(0)).isEqualTo("berlin");
    }

    @Test
    void decimalColumnMaterializesUint64AndPlainLongs() {
        MaterializedResult r =
                queryRunner.execute("SELECT who, big FROM arango.shop.profiles ORDER BY who");
        assertThat(r.getMaterializedRows().get(0).getField(1))
                .isEqualTo(new BigDecimal("18446744073709551615"));
        assertThat(r.getMaterializedRows().get(1).getField(1)).isEqualTo(new BigDecimal("7"));
    }

    @Test
    void strictModeRaisesOnNestedMismatchThroughSql() {
        // bob's tags hold 5L under the VARCHAR element type -> nested mismatch under strict.
        // Assert only the connector's message text: the error-code NAME does not surface through
        // DistributedQueryRunner's failure wrapping (see the pre-existing
        // ArangoConnectorPushdownTest.strictModeRaisesOnTypeMismatch, which asserts message text
        // only); the code identity is already unit-covered in ValueMaterializerTest.
        assertThatThrownBy(
                        () -> queryRunner.execute("SELECT tags FROM arango_strict.shop.profiles"))
                .hasMessageContaining("value at tags[");
    }

    @Test
    void showStatsSurfacesRowCount() {
        // M6 exit criterion made executable: "row-count stats surfaced" (spec §5).
        MaterializedResult r = queryRunner.execute("SHOW STATS FOR (SELECT * FROM users)");
        var summary =
                r.getMaterializedRows().stream()
                        .filter(row -> row.getField(0) == null) // summary row: column_name IS NULL
                        .findFirst()
                        .orElseThrow();
        assertThat(((Number) summary.getField(4)).doubleValue()).isEqualTo(2.0);
    }

    @Test
    void showStatsForPassthroughIsUnknown() {
        // The ArangoQueryHandle decline observed end-to-end: unknown row_count is SQL NULL.
        MaterializedResult r =
                queryRunner.execute(
                        "SHOW STATS FOR (SELECT * FROM TABLE(arango.system.query("
                                + "database => 'shop',"
                                + " query => 'FOR d IN users RETURN d')))");
        var summary =
                r.getMaterializedRows().stream()
                        .filter(row -> row.getField(0) == null)
                        .findFirst()
                        .orElseThrow();
        assertThat(summary.getField(4)).isNull();
    }

    @Test
    void overrideDrivenTableMaterializesDeclaredTypes() {
        // M6-C e2e (assertion 1): decimal(12,2) and both timestamp(3) variants read through the
        // full connector stack from a declared override doc, not sampled inference.
        MaterializedResult r =
                queryRunner.execute(
                        "SELECT total, updated_at, placed_at FROM arango.shop.invoices");
        assertThat(r.getRowCount()).isEqualTo(1);
        var row = r.getMaterializedRows().get(0);
        assertThat(row.getField(0)).isEqualTo(new BigDecimal("12.34"));
        assertThat(row.getField(1)).isEqualTo(LocalDateTime.parse("2026-08-05T12:34:56.789"));
        ZonedDateTime placedAt = (ZonedDateTime) row.getField(2);
        assertThat(placedAt.toInstant())
                .isEqualTo(OffsetDateTime.parse("2026-08-05T12:34:56.789+05:30").toInstant());
        assertThat(placedAt.getOffset()).isEqualTo(ZoneOffset.ofHoursMinutes(5, 30));
    }

    @Test
    void overrideDrivenTableHidesInternalNoteButItRemainsSelectable() {
        // M6-C e2e (assertion 2): SELECT * exposes exactly the four non-hidden declared
        // columns, in declaration order, with the hidden 'internal_note' excluded; an explicit
        // SELECT of the hidden column still reads it. 'missplled' (matching no stored
        // attribute) surfaces here as an ordinary visible column -- its all-NULL behavior is
        // proven separately by misspelledOverrideFieldReadsAsNull below (spec §3).
        MaterializedResult star = queryRunner.execute("SELECT * FROM arango.shop.invoices");
        assertThat(star.getColumnNames())
                .containsExactly("total", "placed_at", "updated_at", "missplled");
        MaterializedResult note =
                queryRunner.execute("SELECT internal_note FROM arango.shop.invoices");
        assertThat(note.getMaterializedRows().get(0).getField(0)).isEqualTo("secret");
    }

    @Test
    void showTablesListsOverrideCollectionAsOrdinaryTable() {
        // M6-C e2e (assertion 3): the schema-override collection itself is an ordinary table,
        // not hidden from enumeration (spec §8 decision).
        MaterializedResult r = queryRunner.execute("SHOW TABLES FROM arango.shop");
        assertThat(r.getOnlyColumnAsSet()).contains("invoices", "trino_schema");
    }

    @Test
    void malformedOverrideFailsLazilyButTableEnumerationStillSucceeds() {
        // M6-C e2e (assertion 4): an invalid declared type is a lazy ARANGODB_SCHEMA_ERROR at
        // column-resolution time, not an eager failure at listTables time; the malformed doc is
        // quarantined to shop_broken (see @BeforeAll) so it can't poison 'shop' enumeration.
        assertThatThrownBy(() -> queryRunner.execute("SELECT * FROM arango.shop_broken.broken"))
                .hasMessageContaining("not_a_type");
        MaterializedResult tables = queryRunner.execute("SHOW TABLES FROM arango.shop_broken");
        assertThat(tables.getOnlyColumnAsSet()).contains("broken");
    }

    @Test
    void informationSchemaIsUnpoisonedByMalformedOverride() {
        // M6-C e2e (assertion 5): OBSERVED, then pinned (spec §8's "success" branch, not its
        // "accepted deviation from master-spec §4.2" branch): the malformed 'broken' override
        // doc present in shop_broken (see @BeforeAll) does NOT poison schema-wide column
        // enumeration for its sibling table 'trino_schema'.
        //
        // Two observations pin this precisely (this connector implements no bulk-metadata
        // listing -- ArangoMetadata declares no streamRelationColumns/streamTableColumns -- so
        // information_schema.columns falls back to the engine's per-table resolution path):
        //  1. Filtered only by table_schema (no table_name predicate), the query returns ZERO
        //     rows in shop_broken -- but the identical zero-row result against the wholly clean
        //     'shop' schema (asserted below) proves this is a pre-existing connector gap, not
        //     something the malformed doc caused: without an explicit table_name predicate, this
        //     engine path never resolves any table's columns, healthy schema or not.
        //  2. Adding a table_name equality predicate for the malformed doc's sibling
        //     ('trino_schema', which has no override of its own) resolves its columns
        //     correctly -- proving that predicate-scoped path never touches 'broken' at all.
        // SHOW COLUMNS on that same sibling succeeds too; SHOW COLUMNS on 'broken' itself still
        // fails with the same lazy ARANGODB_SCHEMA_ERROR asserted in
        // malformedOverrideFailsLazilyButTableEnumerationStillSucceeds above.
        MaterializedResult unfilteredBroken =
                queryRunner.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema ="
                                + " 'shop_broken'");
        assertThat(unfilteredBroken.getRowCount()).isEqualTo(0);

        MaterializedResult unfilteredCleanControl =
                queryRunner.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema ="
                                + " 'shop'");
        assertThat(unfilteredCleanControl.getRowCount()).isEqualTo(0);

        MaterializedResult filteredToSibling =
                queryRunner.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema ="
                                + " 'shop_broken' AND table_name = 'trino_schema'");
        assertThat(filteredToSibling.getMaterializedRows().stream().map(row -> row.getField(0)))
                .containsExactlyInAnyOrder("table", "fields");

        MaterializedResult shown =
                queryRunner.execute("SHOW COLUMNS FROM arango.shop_broken.trino_schema");
        assertThat(shown.getMaterializedRows().stream().map(row -> row.getField(0)))
                .containsExactlyInAnyOrder("table", "fields");
    }

    @Test
    void misspelledOverrideFieldReadsAsNull() {
        // M6-C e2e (assertion 6): a declared field name matching no stored attribute is an
        // accepted limitation (spec §3) -- the column exists and reads NULL, not an error.
        MaterializedResult r = queryRunner.execute("SELECT missplled FROM arango.shop.invoices");
        assertThat(r.getMaterializedRows().get(0).getField(0)).isNull();
    }
}
