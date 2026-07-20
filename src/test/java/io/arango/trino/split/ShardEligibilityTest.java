package io.arango.trino.split;

import com.arangodb.entity.ShardingStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardEligibilityTest {
    private static String name(ShardingStrategy s) {
        return s.getInternalName();
    }

    @Test
    void eligibleForNonSmartHashMultiShard() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.HASH), null)).isEmpty());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(2, name(ShardingStrategy.COMMUNITY_COMPAT), null)).isEmpty());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(4, name(ShardingStrategy.ENTERPRISE_COMPAT), null)).isEmpty());
    }

    @Test
    void ineligibleWhenNotMultiShard() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(1, name(ShardingStrategy.HASH), null)).isPresent());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(null, name(ShardingStrategy.HASH), null)).isPresent());
    }

    @Test
    void ineligibleForSmartStrategies() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.ENTERPRISE_HASH_SMART_EDGE), null)).isPresent());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.ENTERPRISE_HEX_SMART_VERTEX), null)).isPresent());
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.ENTERPRISE_SMART_EDGE_COMPAT), null)).isPresent());
    }

    @Test
    void ineligibleForSmartJoin() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, name(ShardingStrategy.HASH), "region")).isPresent());
    }

    @Test
    void unknownStrategyFailsSafe() {
        assertTrue(ShardEligibility.ineligibilityReason(
                new ShardingInfo(3, "some-future-strategy", null)).isPresent());
    }
}
