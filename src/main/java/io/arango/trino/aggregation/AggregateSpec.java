package io.arango.trino.aggregation;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.arango.trino.handle.ArangoColumnHandle;
import io.trino.spi.type.Type;
import java.util.Optional;

/**
 * One pushed aggregate: what to compute, over which column, under what output name and type. The
 * output type comes from Trino's {@code AggregateFunction.getOutputType()}, never from the inferred
 * column type -- {@code count} over a VARCHAR column outputs BIGINT.
 */
public record AggregateSpec(
        @JsonProperty("kind") Kind kind,
        @JsonProperty("input") Optional<ArangoColumnHandle> input,
        @JsonProperty("outputName") String outputName,
        @JsonProperty("outputType") Type outputType) {
    public enum Kind {
        /** {@code count(*)} -- no input column, so no coercion surface. */
        COUNT_STAR,
        /** {@code count(col)} -- counts values the read path materializes non-NULL. */
        COUNT_COLUMN,
        SUM,
        MIN,
        MAX,
        AVG
    }

    @JsonCreator
    public AggregateSpec {
        requireNonNull(kind, "kind is null");
        requireNonNull(input, "input is null");
        requireNonNull(outputName, "outputName is null");
        requireNonNull(outputType, "outputType is null");
        // AqlBuilder switches on the kind and dereferences input() for every kind but COUNT_STAR,
        // so the pairing is an invariant rather than a convention.
        boolean expectsInput = kind != Kind.COUNT_STAR;
        if (expectsInput != input.isPresent()) {
            throw new IllegalArgumentException(
                    "COUNT_STAR takes no input column; every other kind requires one (kind=%s, input present=%s)"
                            .formatted(kind, input.isPresent()));
        }
    }
}
