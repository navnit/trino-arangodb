package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record ArangoSplit(List<String> shardIds) implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(ArangoSplit.class);

    @JsonCreator
    public ArangoSplit(@JsonProperty("shardIds") List<String> shardIds) {
        this.shardIds = List.copyOf(requireNonNull(shardIds, "shardIds is null"));
    }

    @JsonProperty
    @Override
    public List<String> shardIds() {
        return shardIds;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE + estimatedSizeOf(shardIds, SizeOf::estimatedSizeOf);
    }
}
