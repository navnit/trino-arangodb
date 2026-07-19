package io.arango.trino.handle;

import io.trino.spi.connector.ConnectorSplit;

import static io.airlift.slice.SizeOf.instanceSize;

public record ArangoSplit() implements ConnectorSplit {
    // M1: a single whole-collection split carries no shard/range state.
    private static final int INSTANCE_SIZE = instanceSize(ArangoSplit.class);

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE;
    }
}
