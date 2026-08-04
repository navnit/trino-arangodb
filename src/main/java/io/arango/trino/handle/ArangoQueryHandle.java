package io.arango.trino.handle;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;

/**
 * A passthrough query (spec §5): a SEPARATE handle type, not a field on ArangoTableHandle, so
 * "filter/limit/projection/aggregation pushed at opaque user AQL" is unrepresentable rather than a
 * state every hook must remember to decline. The query travels here, not on the split (§5.1).
 */
public record ArangoQueryHandle(
        @JsonProperty("database") String database,
        @JsonProperty("query") String query,
        @JsonProperty("columns") List<ArangoColumnHandle> columns)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoQueryHandle {
        requireNonNull(database, "database is null");
        requireNonNull(query, "query is null");
        columns = List.copyOf(requireNonNull(columns, "columns is null"));
    }

    /** A passthrough has no table identity; this synthesized name renders in EXPLAIN (§5.2). */
    public SchemaTableName schemaTableName() {
        return new SchemaTableName(database, "query");
    }
}
