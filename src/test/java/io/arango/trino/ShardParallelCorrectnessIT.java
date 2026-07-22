package io.arango.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.split.ShardFanoutCapability;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.TupleDomain;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

// @Tag("cluster"): excluded from the default failsafe run (too slow/flaky to boot a real
// ArangoDB cluster on a 2-vCPU CI runner within the wait window); run via `mvn verify
// -Pcluster-its`
// in a separate, non-blocking CI job. See pom.xml it.excludedGroups and .github/workflows/ci.yml.
@Tag("cluster")
@ExtendWith(SharedArangoClusterExtension.class)
class ShardParallelCorrectnessIT {
    private static TestingArangoCluster cluster;
    private static ArangoClient client;
    private static final String DB = "shard_correct_it";
    private static final String COLL = "docs";
    private static final int DOCS = 1000;

    @BeforeAll
    static void setup() {
        cluster = SharedArangoClusterExtension.cluster();
        client = new ArangoClient(cluster.config());
        client.createDatabaseForTest(DB);
        client.createShardedCollectionForTest(DB, COLL, 3);
        for (int i = 0; i < DOCS; i++) {
            client.insertForTest(DB, COLL, Map.of("_key", "k" + i, "v", i));
        }
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        // Do NOT close the shared cluster here: SharedArangoClusterExtension stops it once at
        // the end of the test plan. Closing it per-class would force the other cluster IT to
        // boot a second cluster, which the CI runner cannot stand up (the failure this fixes).
    }

    private static ArangoTableHandle handle() {
        // 5-arg record: (schema, table, edge, constraint, limit).
        return new ArangoTableHandle(DB, COLL, false, TupleDomain.all(), OptionalLong.empty());
    }

    private static List<ArangoSplit> splits(ArangoConfig config) {
        ArangoSplitManager mgr =
                new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        ConnectorSplitSource src =
                mgr.getSplits(null, null, handle(), Set.of(), Constraint.alwaysTrue());
        return src.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).getNow(null).stream()
                .map(ArangoSplit.class::cast)
                .toList();
    }

    @Test
    void perShardCountsSumToTotalWithNoGapsOrDupes() {
        List<String> shards = client.listShardIds(DB, COLL);
        // count-sum: Σ(per-shard counts) == full (shared function, same path as the runtime probe)
        assertTrue(
                ShardFanoutCapability.sumMatchesFull(
                        client, DB, COLL, shards.stream().map(List::of).toList()));
        // no-dupes: each _key appears in exactly one shard
        Set<String> all = new HashSet<>();
        int total = 0;
        for (String shard : shards) {
            List<String> keys = keysInShard(shard);
            total += keys.size();
            all.addAll(keys);
        }
        assertEquals(DOCS, total, "sum of per-shard key counts");
        assertEquals(DOCS, all.size(), "no key may appear in two shards");
    }

    @Test
    void threeShardsYieldThreeSplitsByDefault() {
        List<ArangoSplit> splits = splits(new ArangoConfig()); // S=1, M=32
        assertEquals(3, splits.size());
        assertEquals(3, splits.stream().flatMap(s -> s.shardIds().stream()).distinct().count());
    }

    @Test
    void maxSplitsCapGroupsShardsAndStillCovers() {
        List<ArangoSplit> splits =
                splits(new ArangoConfig().setMaxSplits(2)); // cap below shard count
        assertEquals(2, splits.size());
        List<List<String>> groups = splits.stream().map(ArangoSplit::shardIds).toList();
        assertTrue(
                ShardFanoutCapability.sumMatchesFull(client, DB, COLL, groups),
                "capped grouping must still cover all docs");
    }

    @Test
    void disabledFlagForcesSingleSplitOnCluster() {
        List<ArangoSplit> splits = splits(new ArangoConfig().setShardParallelismEnabled(false));
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }

    private static List<String> keysInShard(String shard) {
        var cursor =
                client.query(DB, "FOR d IN @@col RETURN d", Map.of("@col", COLL), List.of(shard));
        List<String> keys = new ArrayList<>();
        while (cursor.hasNext()) {
            keys.add(String.valueOf(cursor.next().get("_key")));
        }
        return keys;
    }
}
