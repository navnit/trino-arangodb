package io.arango.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.split.ShardFanoutCapability;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        return new ArangoTableHandle(
                DB, COLL, false, TupleDomain.all(), OptionalLong.empty(), Optional.empty());
    }

    private static List<ArangoSplit> splits(ArangoConfig config) {
        return splits(config, handle());
    }

    private static List<ArangoSplit> splits(ArangoConfig config, ArangoTableHandle table) {
        ArangoSplitManager mgr =
                new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        ConnectorSplitSource src =
                mgr.getSplits(null, null, table, Set.of(), Constraint.alwaysTrue());
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

    // The single-split rule for aggregated handles exists ENTIRELY for this deployment shape.
    // Trino treats connector aggregate output as final, so if a 3-shard collection fanned an
    // aggregated scan out into 3 splits, Trino would emit three "final" rows and a count(*) of
    // 1000 would come back as three rows of ~333. Everywhere else that rule is verified with a
    // client test double; this is the only place the hazard can actually occur.
    @Test
    void aggregatedHandleStaysOneSplitOnAThreeShardCollection() {
        ArangoTableHandle aggregated =
                handle().withAggregation(
                                new ArangoAggregation(
                                        List.of(),
                                        List.of(
                                                new AggregateSpec(
                                                        AggregateSpec.Kind.COUNT_STAR,
                                                        Optional.empty(),
                                                        "agg_0",
                                                        BigintType.BIGINT))));

        // The same collection fans out to 3 splits when not aggregated (asserted above), so this
        // is the aggregation rule taking effect, not a non-sharded collection.
        List<ArangoSplit> splits = splits(new ArangoConfig(), aggregated);
        assertEquals(1, splits.size(), "an aggregated handle must never fan out across shards");
        assertTrue(
                splits.get(0).shardIds().isEmpty(),
                "the single aggregated split must scan the whole collection, not one shard");
    }

    // End-to-end proof on real sharding: the pushed COLLECT must count every document exactly
    // once across all three shards. A per-shard fan-out would return three partial rows here.
    @Test
    void pushedCountOnAShardedCollectionReturnsOneExactRow() {
        ArangoTableHandle aggregated =
                handle().withAggregation(
                                new ArangoAggregation(
                                        List.of(),
                                        List.of(
                                                new AggregateSpec(
                                                        AggregateSpec.Kind.COUNT_STAR,
                                                        Optional.empty(),
                                                        "agg_0",
                                                        BigintType.BIGINT))));
        ArangoColumnHandle output =
                new ArangoColumnHandle("agg_0", BigintType.BIGINT, false, List.of());

        List<ArangoSplit> splits = splits(new ArangoConfig(), aggregated);
        long rows = 0;
        long total = 0;
        for (ArangoSplit split : splits) {
            try (ConnectorPageSource pageSource =
                    new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig())
                            .createPageSource(
                                    null,
                                    null,
                                    split,
                                    aggregated,
                                    Optional.empty(),
                                    List.of(output),
                                    null)) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        total += BigintType.BIGINT.getLong(page.getBlock(0), i);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        assertEquals(1, rows, "exactly one final row, not one per shard");
        assertEquals(DOCS, total, "the pushed COLLECT must see every document exactly once");
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
