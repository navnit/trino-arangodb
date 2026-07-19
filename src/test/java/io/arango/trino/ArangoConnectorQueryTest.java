package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.client.ArangoClient;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.*;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

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
        }

        queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder().setCatalog("arango").setSchema("shop").build())
                .build();
        queryRunner.installPlugin(new ArangoPlugin());
        queryRunner.createCatalog("arango", "arangodb", ImmutableMap.of(
                "arangodb.hosts", server.hostPort(),
                "arangodb.user", "root",
                "arangodb.password", server.rootPassword()));
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
}
