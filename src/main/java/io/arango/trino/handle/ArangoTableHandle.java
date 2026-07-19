package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record ArangoTableHandle(
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("edge") boolean edge)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoTableHandle {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
    }

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schema, table);
    }
}
