package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoConnectorQueryTest {
    private TestingArangoServer server;
    private QueryRunner queryRunner;

    @BeforeAll
    void setup() throws Exception {
        server = new TestingArangoServer();
        try (ArangoClient seed = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword()))) {
            seed.createDatabaseForTest("shop");
            seed.createDocumentCollectionForTest("shop", "users");
            seed.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
            seed.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
            // edge collection to prove _from/_to surface as visible columns
            seed.createEdgeCollectionForTest("shop", "knows");
            seed.insertForTest("shop", "knows",
                    Map.of("_from", "users/ada", "_to", "users/bob", "since", 2020L));

            seed.createDocumentCollectionForTest("shop", "profiles");
            seed.insertForTest("shop", "profiles", Map.of(
                    "who", "ada",
                    "tags", List.of("pioneer", "math"),
                    "address", Map.of("city", "london", "zip", 1815L),
                    "big", new BigInteger("18446744073709551615"))); // uint64 -> DECIMAL(38,0)
            seed.insertForTest("shop", "profiles", Map.of(
                    "who", "bob",
                    "tags", List.of("ops", 5L),  // 5L under merged VARCHAR element -> leaf NULL
                    "address", Map.of("city", "berlin"), // absent zip -> NULL field
                    "big", 7L));                 // plain long under the DECIMAL column reads back
        }

        queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("arango").setSchema("shop").build())
                .build();
        queryRunner.installPlugin(new ArangoPlugin());
        queryRunner.createCatalog("arango", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword()));
        queryRunner.createCatalog("arango_strict", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword(),
                "arangodb.type-coercion", "STRICT"));
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
        MaterializedResult r = queryRunner.execute(
                "SELECT name, age FROM arango.shop.users ORDER BY age");
        assertThat(r.getRowCount()).isEqualTo(2);
        assertThat(r.getMaterializedRows().get(0).getField(0)).isEqualTo("ada");
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo(36L);
        assertThat(r.getMaterializedRows().get(1).getField(0)).isEqualTo("bob");
    }

    @Test
    void edgeCollectionExposesFromAndToColumns() {
        MaterializedResult r = queryRunner.execute(
                "SELECT \"_from\", \"_to\", since FROM arango.shop.knows");
        assertThat(r.getRowCount()).isEqualTo(1);
        assertThat(r.getMaterializedRows().get(0).getField(0)).isEqualTo("users/ada");
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo("users/bob");
        assertThat(r.getMaterializedRows().get(0).getField(2)).isEqualTo(2020L);
    }

    @Test
    void arrayColumnMaterializesWithLeafNulls() {
        MaterializedResult r = queryRunner.execute(
                "SELECT who, tags FROM arango.shop.profiles ORDER BY who");
        assertThat(r.getRowCount()).isEqualTo(2);
        assertThat(r.getMaterializedRows().get(0).getField(1)).isEqualTo(List.of("pioneer", "math"));
        // element 5L under the merged VARCHAR element type is a leaf mismatch -> NULL, not row loss
        assertThat(r.getMaterializedRows().get(1).getField(1)).isEqualTo(Arrays.asList("ops", null));
    }

    @Test
    void rowColumnAndScalarDereferenceBothMaterializeCorrectValues() {
        // whole-row select works post-M4 ...
        MaterializedResult whole = queryRunner.execute(
                "SELECT address FROM arango.shop.profiles WHERE who = 'bob'");
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
        MaterializedResult city = queryRunner.execute(
                "SELECT address.city FROM arango.shop.profiles ORDER BY who");
        assertThat(city.getMaterializedRows().get(0).getField(0)).isEqualTo("london");
        assertThat(city.getMaterializedRows().get(1).getField(0)).isEqualTo("berlin");
    }

    @Test
    void decimalColumnMaterializesUint64AndPlainLongs() {
        MaterializedResult r = queryRunner.execute(
                "SELECT who, big FROM arango.shop.profiles ORDER BY who");
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
        assertThatThrownBy(() -> queryRunner.execute(
                "SELECT tags FROM arango_strict.shop.profiles"))
                .hasMessageContaining("value at tags[");
    }
}
