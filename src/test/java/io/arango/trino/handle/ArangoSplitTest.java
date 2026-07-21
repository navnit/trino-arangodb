package io.arango.trino.handle;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoSplitTest {
    private static final JsonCodec<ArangoSplit> CODEC = jsonCodec(ArangoSplit.class);

    @Test
    void roundTripsWithShardIds() {
        ArangoSplit split = new ArangoSplit(List.of("s100001", "s100002"));
        assertEquals(split, CODEC.fromJson(CODEC.toJson(split)));
    }

    @Test
    void roundTripsEmpty() {
        ArangoSplit split = new ArangoSplit(List.of());
        assertEquals(split, CODEC.fromJson(CODEC.toJson(split)));
        assertTrue(split.shardIds().isEmpty());
    }

    @Test
    void retainedSizeGrowsWithShards() {
        long empty = new ArangoSplit(List.of()).getRetainedSizeInBytes();
        long two = new ArangoSplit(List.of("s100001", "s100002")).getRetainedSizeInBytes();
        assertTrue(two > empty, "retained size must include the shard-id list");
    }
}
