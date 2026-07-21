package io.arango.trino;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArangoConfigShardingTest {
    @Test
    void defaults() {
        ArangoConfig c = new ArangoConfig();
        assertEquals(1, c.getShardsPerSplit());
        assertEquals(32, c.getMaxSplits());
        assertTrue(c.isShardParallelismEnabled());
    }

    @Test
    void setters() {
        ArangoConfig c = new ArangoConfig()
                .setShardsPerSplit(4)
                .setMaxSplits(8)
                .setShardParallelismEnabled(false);
        assertEquals(4, c.getShardsPerSplit());
        assertEquals(8, c.getMaxSplits());
        assertFalse(c.isShardParallelismEnabled());
    }
}
