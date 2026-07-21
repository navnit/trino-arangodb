package io.arango.trino;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.split.ShardEligibility;
import io.arango.trino.split.ShardFanoutCapability;
import io.arango.trino.split.ShardGrouping;
import io.arango.trino.split.ShardingInfo;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;

public class ArangoSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(ArangoSplitManager.class);

    private final ArangoClient client;
    private final ArangoConfig config;
    private final ShardFanoutCapability capability;

    @Inject
    public ArangoSplitManager(ArangoClient client, ArangoConfig config, ShardFanoutCapability capability) {
        this.client = client;
        this.config = config;
        this.capability = capability;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        return new FixedSplitSource(splitsFor((ArangoTableHandle) table));
    }

    private static final ArangoSplit SINGLE = new ArangoSplit(List.of());

    private List<ArangoSplit> splitsFor(ArangoTableHandle handle) {
        if (!config.isShardParallelismEnabled()) {
            return List.of(SINGLE);
        }
        String db = handle.schema();
        String coll = handle.table();
        try {
            ShardingInfo info = client.getShardingInfo(db, coll);
            Optional<String> reason = ShardEligibility.ineligibilityReason(info);
            if (reason.isPresent()) {
                boolean multiShard = info.numberOfShards() != null && info.numberOfShards() > 1;
                if (multiShard) {
                    log.warn("Collection %s.%s has %d shards but is scanned serially: %s",
                            db, coll, info.numberOfShards(), reason.get());
                }
                else {
                    log.debug("Collection %s.%s scanned serially: %s", db, coll, reason.get());
                }
                return List.of(SINGLE);
            }
            List<String> shardIds = client.listShardIds(db, coll);
            if (shardIds.size() <= 1) {
                return List.of(SINGLE);
            }
            List<List<String>> groups = ShardGrouping.partition(shardIds, config.getShardsPerSplit(), config.getMaxSplits());
            if (!capability.canFanOut(db, coll, groups)) { // probe the ACTUAL groups we would emit
                log.warn("Collection %s.%s scanned serially: shardIds capability not confirmed this cycle "
                        + "(server below minimum version, or the probe was inconclusive — e.g. concurrent "
                        + "writes or an empty collection); will re-probe on the next query", db, coll);
                return List.of(SINGLE);
            }
            return groups.stream().map(ArangoSplit::new).toList();
        }
        catch (RuntimeException e) {
            log.warn(e, "Collection %s.%s scanned serially: shard discovery failed", db, coll);
            return List.of(SINGLE);
        }
    }
}
