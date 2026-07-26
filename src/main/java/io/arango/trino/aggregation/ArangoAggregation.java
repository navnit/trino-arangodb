package io.arango.trino.aggregation;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.arango.trino.handle.ArangoColumnHandle;
import java.util.List;

/**
 * The aggregation pushed onto a table handle. Its presence on {@link
 * io.arango.trino.handle.ArangoTableHandle} <em>is</em> the "aggregated" flag -- there is no
 * separate boolean to keep in sync -- and it is what makes the split manager emit exactly one
 * split: Trino treats connector aggregation output as final, so N splits would emit N duplicate
 * final rows (master spec §6.4).
 *
 * <p>An empty {@code aggregates} list with a non-empty {@code groupingColumns} list is a pushed
 * {@code SELECT DISTINCT} / bare {@code GROUP BY}.
 */
public record ArangoAggregation(
        @JsonProperty("groupingColumns") List<ArangoColumnHandle> groupingColumns,
        @JsonProperty("aggregates") List<AggregateSpec> aggregates) {
    @JsonCreator
    public ArangoAggregation {
        requireNonNull(groupingColumns, "groupingColumns is null");
        requireNonNull(aggregates, "aggregates is null");
        groupingColumns = List.copyOf(groupingColumns);
        aggregates = List.copyOf(aggregates);
    }
}
