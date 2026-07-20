package io.arango.trino;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class ArangoConfigTest {
    @Test
    void testDefaults() {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ArangoConfig.class)
                .setHosts("localhost:8529")
                .setUser("root")
                .setPassword("")
                .setSampleSize(1000)
                .setSampleRandom(false)
                .setMixedTypeStrategy(ArangoConfig.MixedTypeStrategy.VARCHAR)
                .setTypeCoercion(ArangoConfig.TypeCoercion.LENIENT)
                .setShardsPerSplit(1)
                .setMaxSplits(32)
                .setShardParallelismEnabled(true));
    }

    @Test
    void testExplicitPropertyMappings() {
        Map<String, String> props = ImmutableMap.<String, String>builder()
                .put("arangodb.hosts", "a:8529,b:8529")
                .put("arangodb.user", "reader")
                .put("arangodb.password", "secret")
                .put("arangodb.schema.sample-size", "50")
                .put("arangodb.schema.sample-random", "true")
                .put("arangodb.schema.mixed-type-strategy", "json")
                .put("arangodb.type-coercion", "STRICT")
                .put("arangodb.shards-per-split", "4")
                .put("arangodb.max-splits", "8")
                .put("arangodb.shard-parallelism-enabled", "false")
                .buildOrThrow();

        ArangoConfig expected = new ArangoConfig()
                .setHosts("a:8529,b:8529")
                .setUser("reader")
                .setPassword("secret")
                .setSampleSize(50)
                .setSampleRandom(true)
                .setMixedTypeStrategy(ArangoConfig.MixedTypeStrategy.JSON)
                .setTypeCoercion(ArangoConfig.TypeCoercion.STRICT)
                .setShardsPerSplit(4)
                .setMaxSplits(8)
                .setShardParallelismEnabled(false);

        ConfigAssertions.assertFullMapping(props, expected);
        // hosts parsed into list
        org.assertj.core.api.Assertions.assertThat(expected.getHostList())
                .isEqualTo(List.of("a:8529", "b:8529"));
    }
}
