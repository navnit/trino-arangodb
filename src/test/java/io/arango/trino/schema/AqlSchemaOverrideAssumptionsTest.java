package io.arango.trino.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.arangodb.ArangoDBException;
import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.client.ArangoClient;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AqlSchemaOverrideAssumptionsTest {
    private TestingArangoServer server;
    private ArangoClient client;

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest("ovr");
        client.createDocumentCollectionForTest("ovr", "trino_schema");
        client.insertForTest(
                "ovr",
                "trino_schema",
                Map.of(
                        "table",
                        "orders",
                        "fields",
                        List.of(Map.of("name", "total", "type", "decimal(12,2)"))));
    }

    @AfterAll
    void teardown() {
        client.close();
        server.close();
    }

    @Test
    void missingCollectionViaBindParameterIs1203() {
        // Spec §4.5: the @@sc BIND-PARAMETER shape was never exercised by M6-B's
        // literal-reference queries; pin that it really is 1203, not some plan-time code.
        ArangoDBException e =
                Assertions.catchThrowableOfType(
                        ArangoDBException.class,
                        () ->
                                client.fetchSchemaOverrideDocs(
                                        "ovr", "no_such_collection", "orders"));
        assertThat(e.getErrorNum()).isEqualTo(1203);
    }

    @Test
    void fetchReturnsMatchingDocOnly() {
        assertThat(client.fetchSchemaOverrideDocs("ovr", "trino_schema", "orders")).hasSize(1);
        assertThat(client.fetchSchemaOverrideDocs("ovr", "trino_schema", "nope")).isEmpty();
    }

    @Test
    void collectionExistsProbe() {
        assertThat(client.collectionExists("ovr", "trino_schema")).isTrue();
        assertThat(client.collectionExists("ovr", "no_such_collection")).isFalse();
    }

    @Test
    void forbiddenCollectionErrorShape() {
        // Spec §4.5 mandates verifying the real errorNum for a collection-level
        // "grant: none" against a live server, so the reader can give a tailored message.
        client.createReadOnlyUserForTest("ovr", "limited", "pw");
        client.setCollectionAccessForTest("limited", "ovr", "trino_schema", "none");
        try (ArangoClient restricted =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("limited")
                                .setPassword("pw"))) {
            ArangoDBException e =
                    Assertions.catchThrowableOfType(
                            ArangoDBException.class,
                            () ->
                                    restricted.fetchSchemaOverrideDocs(
                                            "ovr", "trino_schema", "orders"));
            // Observed against the TestingArangoServer image: errorNum=11 ("forbidden"),
            // responseCode=403. Task 4's isForbidden reads the SAME constants, so the
            // observation travels by name, not by lore. Update ONLY on a new observation.
            assertThat(e.getErrorNum()).isEqualTo(ArangoClient.ERROR_NUM_FORBIDDEN);
            assertThat(e.getResponseCode()).isEqualTo(ArangoClient.HTTP_FORBIDDEN);
        }
    }
}
