package io.arango.trino;

import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Targets master spec §6.2: once shard-parallelism is enabled a table may fan out into multiple
 * splits (ArangoSplitManager), each of which applies LIMIT n independently, so a pushed LIMIT is
 * no longer an exact cap and must not be reported as guaranteed. Constructed with the same
 * null-deps test-double pattern ArangoMetadataTest uses elsewhere (e.g. getTableHandleRejects...)
 * since applyLimit's limitGuaranteed depends only on ArangoConfig, not the client/resolver.
 */
class ArangoMetadataLimitTest {
    private static ArangoTableHandle tableHandle() {
        return new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty());
    }

    @Test
    void limitNotGuaranteedWhenParallelismEnabled() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig()); // parallelism enabled by default
        Optional<LimitApplicationResult<ConnectorTableHandle>> r = metadata.applyLimit(null, tableHandle(), 10);
        assertTrue(r.isPresent());
        assertFalse(r.get().isLimitGuaranteed(), "a fan-out-capable table must not report an exact limit");
    }

    @Test
    void limitGuaranteedWhenParallelismDisabled() {
        ArangoMetadata metadata = new ArangoMetadata(null, null, new ArangoConfig().setShardParallelismEnabled(false));
        Optional<LimitApplicationResult<ConnectorTableHandle>> r = metadata.applyLimit(null, tableHandle(), 10);
        assertTrue(r.isPresent());
        assertTrue(r.get().isLimitGuaranteed(), "a single-split table reports an exact limit");
    }
}
