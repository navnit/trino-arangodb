package io.arango.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record ArangoTableHandle(
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("edge") boolean edge,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("limit") OptionalLong limit)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoTableHandle {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
    }

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schema, table);
    }

    public ArangoTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new ArangoTableHandle(schema, table, edge, newConstraint, limit);
    }

    public ArangoTableHandle withLimit(long newLimit) {
        return new ArangoTableHandle(schema, table, edge, constraint, OptionalLong.of(newLimit));
    }
}
