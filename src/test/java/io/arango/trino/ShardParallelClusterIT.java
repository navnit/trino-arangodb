package io.arango.trino;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.split.ShardEligibility;
import io.arango.trino.split.ShardingInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardParallelClusterIT {
    private static TestingArangoCluster cluster;
    private static ArangoClient client;
    static final String DB = "shard_it";
    static final String COLL = "docs";

    @BeforeAll
    static void setup() {
        cluster = new TestingArangoCluster();
        client = new ArangoClient(cluster.config());
        client.createDatabaseForTest(DB);
        client.createShardedCollectionForTest(DB, COLL, 3);
        for (int i = 0; i < 1000; i++) {
            client.insertForTest(DB, COLL, Map.of("_key", "k" + i, "v", i));
        }
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }

    @Test
    void clusterReportsThreeShards() {
        ShardingInfo info = client.getShardingInfo(DB, COLL);
        assertEquals(3, info.numberOfShards());
        assertTrue(ShardEligibility.ineligibilityReason(info).isEmpty(),
                "a 3-shard hash collection must be eligible for fan-out");
    }

    @Test
    void enumerationReturnsAllShardIds() {
        List<String> shards = client.listShardIds(DB, COLL);
        assertEquals(3, shards.size());
        assertEquals(3, shards.stream().distinct().count(), "shard IDs must be distinct");
    }
}
