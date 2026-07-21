package io.arango.trino.split;

import com.arangodb.entity.ShardingStrategy;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

public final class ShardEligibility {
    private ShardEligibility() {}

    // Non-smart hash strategies: one shard per document, safe for per-shard enumeration.
    // Built from verified driver enum constants; the three *SMART* strategies are excluded
    // (SmartGraph edges live in multiple internal sub-shards -> double-count risk, master spec §5.1.5).
    private static final Set<String> ALLOWED_STRATEGIES = Stream.of(
                    ShardingStrategy.HASH,
                    ShardingStrategy.COMMUNITY_COMPAT,
                    ShardingStrategy.ENTERPRISE_COMPAT)
            .map(ShardingStrategy::getInternalName)
            .collect(toUnmodifiableSet());

    /** Empty ⇒ eligible for shard-parallel fan-out. Present ⇒ reason it is not (for the WARN log). */
    public static Optional<String> ineligibilityReason(ShardingInfo info) {
        Integer shards = info.numberOfShards();
        if (shards == null || shards <= 1) {
            return Optional.of("numberOfShards=" + shards + " (not a multi-shard collection)");
        }
        if (info.smartJoinAttribute() != null) {
            return Optional.of("SmartJoin collection (smartJoinAttribute=" + info.smartJoinAttribute() + ")");
        }
        String strategy = info.shardingStrategy();
        if (strategy == null || !ALLOWED_STRATEGIES.contains(strategy)) {
            return Optional.of("sharding strategy '" + strategy + "' is not a supported non-smart hash strategy " + ALLOWED_STRATEGIES);
        }
        return Optional.empty();
    }
}
