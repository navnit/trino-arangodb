package io.arango.trino.client;

import com.arangodb.ArangoDBException;
import io.arango.trino.ArangoConfig;
import io.arango.trino.TestingArangoServer;
import io.arango.trino.split.ShardEligibility;
import io.arango.trino.split.ShardingInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoClientShardingTest {
    private static TestingArangoServer server;
    private static ArangoClient client;
    private static final String DB = "shard_client_test";
    private static final String COLL = "docs";

    @BeforeAll
    static void setup() {
        server = new TestingArangoServer();
        client = new ArangoClient(new ArangoConfig()
                .setHosts(server.hostPort()).setUser("root").setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, COLL);
        for (int i = 0; i < 5; i++) {
            client.insertForTest(DB, COLL, Map.of("_key", "k" + i, "v", i));
        }
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void serverVersionIsReported() {
        assertTrue(client.serverVersion().startsWith("3."), "expected a 3.x version, got " + client.serverVersion());
    }

    @Test
    void singleNodeCollectionIsIneligible() {
        ShardingInfo info = client.getShardingInfo(DB, COLL);
        // single-server: numberOfShards is 1 (or null) -> gate rejects, so no fan-out is attempted
        assertFalse(ShardEligibility.ineligibilityReason(info).isEmpty());
    }

    @Test
    void listShardIdsThrowsOnSingleServer() {
        // /_api/collection/{name}/shards is cluster-only; on single-server it errors -> fallback trigger
        assertThrows(ArangoDBException.class, () -> client.listShardIds(DB, COLL));
    }

    @Test
    void countWithEmptyShardIdsIsFullCount() {
        assertEquals(5L, client.countWithShardIds(DB, COLL, List.of()));
    }

    @Test
    void queryWithEmptyShardIdsMatchesLegacyPath() {
        String aql = "FOR d IN @@col RETURN {\"v\": d.v}";
        Map<String, Object> bind = Map.of("@col", COLL);
        long viaNew = count(client.query(DB, aql, bind, List.of()));
        long viaOld = count(client.query(DB, aql, bind));
        assertEquals(viaOld, viaNew);
        assertEquals(5L, viaNew);
    }

    private static long count(com.arangodb.ArangoCursor<?> cursor) {
        long n = 0;
        while (cursor.hasNext()) { cursor.next(); n++; }
        return n;
    }
}
