package io.arango.trino.handle;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.arango.trino.aggregation.ArangoAggregation;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.Optional;
import java.util.OptionalLong;

public record ArangoTableHandle(
        @JsonProperty("schema") String schema,
        @JsonProperty("table") String table,
        @JsonProperty("edge") boolean edge,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("limit") OptionalLong limit,
        @JsonProperty("aggregation") Optional<ArangoAggregation> aggregation)
        implements ConnectorTableHandle {

    @JsonCreator
    public ArangoTableHandle {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(aggregation, "aggregation is null");
    }

    public SchemaTableName schemaTableName() {
        return new SchemaTableName(schema, table);
    }

    // The withers below must all carry `aggregation` through: dropping it would clear the flag
    // the split manager reads, letting an aggregated scan fan out into multiple splits and emit
    // one duplicate "final" row per split.
    public ArangoTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint) {
        return new ArangoTableHandle(schema, table, edge, newConstraint, limit, aggregation);
    }

    public ArangoTableHandle withLimit(long newLimit) {
        return new ArangoTableHandle(
                schema, table, edge, constraint, OptionalLong.of(newLimit), aggregation);
    }

    public ArangoTableHandle withAggregation(ArangoAggregation newAggregation) {
        return new ArangoTableHandle(
                schema,
                table,
                edge,
                constraint,
                limit,
                Optional.of(requireNonNull(newAggregation, "newAggregation is null")));
    }
}
