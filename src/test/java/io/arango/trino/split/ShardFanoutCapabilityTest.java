package io.arango.trino.split;

import com.arangodb.ArangoCursor;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardFanoutCapabilityTest {

    // Hand-written test double (no network; ArangoDB driver connects lazily so super(config) is safe).
    private static final class FakeClient extends ArangoClient {
        private final String version;
        long full;                    // mutable so a test can simulate an initially-empty collection
        Map<String, Long> perShard;
        final AtomicInteger versionCalls = new AtomicInteger();

        FakeClient(String version, long full, Map<String, Long> perShard) {
            super(new ArangoConfig().setHosts("localhost:8529"));
            this.version = version;
            this.full = full;
            this.perShard = perShard;
        }

        @Override
        public String serverVersion() {
            versionCalls.incrementAndGet();
            return version;
        }

        @Override
        public long countWithShardIds(String db, String coll, List<String> shardIds) {
            if (shardIds.isEmpty()) {
                return full;
            }
            return shardIds.stream().mapToLong(s -> perShard.getOrDefault(s, 0L)).sum();
        }

        @Override
        public ArangoCursor<Map> query(String db, String aql, Map<String, Object> bindVars, List<String> shardIds) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void versionComparison() {
        assertTrue(ShardFanoutCapability.isVersionAtLeastMinimum("3.12.1"));
        assertTrue(ShardFanoutCapability.isVersionAtLeastMinimum("3.11.0"));
        assertTrue(ShardFanoutCapability.isVersionAtLeastMinimum("3.12.0-devel"));
        assertFalse(ShardFanoutCapability.isVersionAtLeastMinimum("3.10.9"));
    }

    @Test
    void enabledWhenVersionAndProbePass() {
        FakeClient client = new FakeClient("3.12.0", 100, Map.of("s1", 60L, "s2", 40L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertTrue(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }

    @Test
    void disabledBelowVersionPin() {
        FakeClient client = new FakeClient("3.10.5", 100, Map.of("s1", 60L, "s2", 40L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertFalse(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }

    @Test
    void disabledWhenServerIgnoresShardIds() {
        // server ignores shardIds -> every shard-scoped count == full -> sum (200) != full (100)
        FakeClient client = new FakeClient("3.12.0", 100, Map.of("s1", 100L, "s2", 100L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertFalse(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }

    @Test
    void verdictIsCachedAfterFirstCall() {
        FakeClient client = new FakeClient("3.12.0", 100, Map.of("s1", 60L, "s2", 40L));
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2")));
        int afterFirst = client.versionCalls.get();
        cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2")));
        assertEquals(afterFirst, client.versionCalls.get(), "probe must not re-run after the verdict is cached");
    }

    @Test
    void inconclusiveFirstCallDoesNotLatch() {
        // Empty collection on first probe -> inconclusive; must NOT cache DISABLED permanently.
        FakeClient client = new FakeClient("3.12.0", 0, Map.of());
        ShardFanoutCapability cap = new ShardFanoutCapability(client);
        assertFalse(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
        // Collection later has data -> the retry must now succeed (proves no permanent latch).
        client.full = 100;
        client.perShard = Map.of("s1", 60L, "s2", 40L);
        assertTrue(cap.canFanOut("db", "c", List.of(List.of("s1"), List.of("s2"))));
    }
}
