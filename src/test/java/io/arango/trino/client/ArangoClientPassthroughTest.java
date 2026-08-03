package io.arango.trino.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.arangodb.ArangoDBException;
import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ArangoClientPassthroughTest {
    private static final String DB = "client_ptf";
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
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "docs");
        for (int i = 0; i < 10; i++) {
            client.insertForTest(DB, "docs", Map.of("_key", "k" + i, "v", (long) i));
        }
    }

    @AfterAll
    void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void explainPlanCarriesPerCollectionAccessType() {
        Map<String, Object> explain = client.explainPlan(DB, "FOR d IN docs RETURN d");
        Object plan = explain.get("plan");
        assertThat(plan).isInstanceOf(Map.class);
        Object collections = ((Map<?, ?>) plan).get("collections");
        assertThat(collections).isInstanceOf(List.class);
        Map<?, ?> entry = (Map<?, ?>) ((List<?>) collections).get(0);
        assertThat(entry.get("name")).isEqualTo("docs");
        assertThat(entry.get("type")).isEqualTo("read");
    }

    // Task 6's §9 error routing branches on getErrorNum() from exceptions raised by the raw
    // Request path — a different driver code path from the typed API ArangoMetadata already
    // relies on. Prove the error numbers survive it HERE, where the dependency is created,
    // so a mismatch fails one early test instead of four downstream ones.
    @Test
    void explainPlanSyntaxErrorCarriesAqlErrorNum() {
        assertThatThrownBy(() -> client.explainPlan(DB, "THIS IS NOT AQL"))
                .isInstanceOfSatisfying(
                        ArangoDBException.class,
                        e -> assertThat(e.getErrorNum()).isBetween(1500, 1599));
    }

    @Test
    void explainPlanUnknownDatabaseCarries1228() {
        assertThatThrownBy(() -> client.explainPlan("no_such_db", "FOR d IN docs RETURN d"))
                .isInstanceOfSatisfying(
                        ArangoDBException.class, e -> assertThat(e.getErrorNum()).isEqualTo(1228));
    }

    @Test
    void explainPlanUnknownCollectionCarries1203() {
        assertThatThrownBy(() -> client.explainPlan(DB, "FOR d IN nope RETURN d"))
                .isInstanceOfSatisfying(
                        ArangoDBException.class, e -> assertThat(e.getErrorNum()).isEqualTo(1203));
    }

    @Test
    void explainPlanDoesNotExecute() {
        // explain of an INSERT must not write (it only plans)
        long before = client.countWithShardIds(DB, "docs", List.of());
        client.explainPlan(DB, "INSERT {x: 1} INTO docs");
        assertThat(client.countWithShardIds(DB, "docs", List.of())).isEqualTo(before);
    }

    @Test
    void firstBatchIsBoundedByK() {
        List<Object> rows = client.firstBatch(DB, "FOR d IN docs RETURN d", 3);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)).isInstanceOf(Map.class);
    }

    @Test
    void firstBatchReturnsAllRowsWhenFewerThanK() {
        assertThat(client.firstBatch(DB, "FOR d IN docs RETURN d", 100)).hasSize(10);
    }

    @Test
    void firstBatchYieldsRawScalarsAndNulls() {
        // Object-typed on purpose: non-object rows must arrive inspectable, not as a driver
        // deserialization failure (§4.1 detection happens in connector code)
        List<Object> rows = client.firstBatch(DB, "FOR x IN [1, \"two\", null] RETURN x", 10);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(1)).isEqualTo("two");
        assertThat(rows.get(2)).isNull();
    }

    @Test
    void firstBatchOfEmptyResultIsEmpty() {
        assertThat(client.firstBatch(DB, "FOR d IN docs FILTER false RETURN d", 5)).isEmpty();
    }

    @Test
    void queryPassthroughStreamsObjectRows() {
        var cursor = client.queryPassthrough(DB, "FOR d IN docs LIMIT 2 RETURN d");
        try {
            assertThat(cursor.hasNext()).isTrue();
            assertThat(cursor.next()).isInstanceOf(Map.class);
        } finally {
            try {
                cursor.close();
            } catch (Exception ignored) {
            }
        }
    }

    // Not part of the brief's Step 2 test list, but Tasks 4/6/8/9 depend on these seeding
    // helpers working — prove them here, where the dependency is created, rather than letting
    // a broken helper surface as a confusing failure downstream.
    @Test
    void seedingHelpersForLaterTasksSucceed() {
        client.registerAqlFunctionForTest(
                DB, "PTF::NOOP", "function (params) { return params[0]; }");
        client.createDocumentCollectionForTest(DB, "ptf_vertices");
        client.createEdgeCollectionForTest(DB, "ptf_edges");
        client.createGraphForTest(DB, "ptf_graph", "ptf_edges", "ptf_vertices");
        client.createArangoSearchViewForTest(DB, "ptf_view", "docs");
    }
}
