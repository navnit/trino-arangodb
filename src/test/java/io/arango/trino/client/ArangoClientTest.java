package io.arango.trino.client;

import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoClientTest {
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort())
                .setUser("root")
                .setPassword(server.rootPassword()));
        // seed: database "shop" with document collection "users"
        client.createDatabaseForTest("shop");
        client.createDocumentCollectionForTest("shop", "users");
        client.insertForTest("shop", "users", Map.of("name", "ada", "age", 36L));
        client.insertForTest("shop", "users", Map.of("name", "bob", "age", 41L));
    }

    @AfterAll
    void teardown() {
        client.close();
        server.close();
    }

    @Test
    void listDatabasesIncludesSeed() {
        assertThat(client.listDatabases()).contains("shop");
    }

    @Test
    void listCollectionsMarksTypeAndSystem() {
        List<CollectionInfo> cols = client.listCollections("shop");
        assertThat(cols).anySatisfy(c -> {
            assertThat(c.name()).isEqualTo("users");
            assertThat(c.isEdge()).isFalse();
            assertThat(c.isSystem()).isFalse();
        });
    }

    @Test
    void sampleDocumentsReturnsRows() {
        List<Map<String, Object>> docs = client.sampleDocuments("shop", "users", 10, false);
        assertThat(docs).hasSize(2);
        assertThat(docs).allSatisfy(d -> assertThat(d).containsKey("name"));
    }
}
