package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record ArangoColumnHandle(
        @JsonProperty("name") String name,
        @JsonProperty("type") Type type,
        @JsonProperty("hidden") boolean hidden,
        @JsonProperty("path") String path)
        implements ColumnHandle {

    @JsonCreator
    public ArangoColumnHandle {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(path, "path is null");
    }

    public ColumnMetadata toColumnMetadata() {
        return ColumnMetadata.builder().setName(name).setType(type).setHidden(hidden).build();
    }
}
