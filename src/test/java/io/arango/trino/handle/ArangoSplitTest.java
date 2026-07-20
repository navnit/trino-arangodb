package io.arango.trino.handle;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ArangoSplitTest {
    @Test
    public void testJsonCodecRoundTrip() {
        JsonCodec<ArangoSplit> codec = JsonCodec.jsonCodec(ArangoSplit.class);

        // Test with empty shard list
        ArangoSplit original = new ArangoSplit(List.of());
        String json = codec.toJson(original);
        ArangoSplit decoded = codec.fromJson(json);
        assertEquals(original, decoded);
        assertEquals(List.of(), decoded.shardIds());

        // Test with multiple shards
        ArangoSplit multiShard = new ArangoSplit(List.of("shard1", "shard2", "shard3"));
        json = codec.toJson(multiShard);
        decoded = codec.fromJson(json);
        assertEquals(multiShard, decoded);
        assertEquals(List.of("shard1", "shard2", "shard3"), decoded.shardIds());
    }
}
