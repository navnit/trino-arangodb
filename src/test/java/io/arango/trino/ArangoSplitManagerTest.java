package io.arango.trino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.split.ShardFanoutCapability;
import io.arango.trino.split.ShardingInfo;
import io.trino.spi.type.BigintType;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.Optional;

class ArangoSplitManagerTest {
    private static TestingArangoServer server;
    private static ArangoClient client;
    private static final String DB = "split_mgr_test";
    private static final String COLL = "docs";

    @BeforeAll
    static void setup() {
        server = new TestingArangoServer();
        client =
                new ArangoClient(
                        new ArangoConfig()
                                .setHosts(server.hostPort())
                                .setUser("root")
                                .setPassword(server.rootPassword()));
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, COLL);
        client.insertForTest(DB, COLL, Map.of("_key", "k0", "v", 0));
    }

    @AfterAll
    static void teardown() {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    private static ArangoTableHandle handle() {
        // ArangoTableHandle is a 5-arg record: (String schema, String table, boolean edge,
        // TupleDomain<ColumnHandle> constraint, OptionalLong limit). Only schema()/table() are read
        // here.
        return new ArangoTableHandle(
                DB,
                COLL,
                false,
                io.trino.spi.predicate.TupleDomain.all(),
                java.util.OptionalLong.empty(), Optional.empty());
    }

    private static List<ArangoSplit> collect(ArangoSplitManager mgr) {
        ConnectorSplitSource source =
                mgr.getSplits(null, null, handle(), Set.of(), Constraint.alwaysTrue());
        return source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).getNow(null).stream()
                .map(ArangoSplit.class::cast)
                .toList();
    }

    @Test
    void disabledFlagForcesSingleEmptySplit() {
        ArangoConfig config = new ArangoConfig().setShardParallelismEnabled(false);
        ArangoSplitManager mgr =
                new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        List<ArangoSplit> splits = collect(mgr);
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }

    @Test
    void singleNodeCollectionFallsBackToSingleSplit() {
        ArangoConfig config = new ArangoConfig(); // parallelism enabled by default
        ArangoSplitManager mgr =
                new ArangoSplitManager(client, config, new ShardFanoutCapability(client));
        List<ArangoSplit> splits = collect(mgr);
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }

    // Trino replaces the aggregation node and treats connector aggregate output as FINAL, so N
    // splits would emit N duplicate final rows. The check must also come FIRST: the shard
    // pipeline's capability probe costs a round trip per collection and is meaningless for a
    // query that cannot fan out. This double turns "the pipeline ran at all" into a failure.
    private static final class ShardingForbiddenClient extends ArangoClient {
        ShardingForbiddenClient() {
            super(new ArangoConfig());
        }

        @Override
        public ShardingInfo getShardingInfo(String database, String collection) {
            throw new AssertionError("shard discovery must not run for an aggregated handle");
        }
    }

    @Test
    void aggregatedHandleAlwaysProducesOneSplitWithoutShardDiscovery() {
        ArangoClient forbidden = new ShardingForbiddenClient();
        ArangoSplitManager mgr =
                new ArangoSplitManager(
                        forbidden, new ArangoConfig(), new ShardFanoutCapability(forbidden));
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
        ConnectorSplitSource source =
                mgr.getSplits(null, null, aggregated, Set.of(), Constraint.alwaysTrue());
        List<ArangoSplit> splits =
                source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY).getNow(null).stream()
                        .map(ArangoSplit.class::cast)
                        .toList();
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).shardIds().isEmpty());
    }
}
