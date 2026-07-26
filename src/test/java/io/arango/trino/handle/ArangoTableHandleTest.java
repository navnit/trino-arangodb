package io.arango.trino.handle;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ArangoTableHandleTest {
    private static ArangoTableHandle base() {
        return new ArangoTableHandle(
                "shop",
                "users",
                false,
                TupleDomain.<ColumnHandle>all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    private static ArangoAggregation countStar() {
        return new ArangoAggregation(
                List.of(),
                List.of(
                        new AggregateSpec(
                                AggregateSpec.Kind.COUNT_STAR,
                                Optional.empty(),
                                "agg_0",
                                BigintType.BIGINT)));
    }

    @Test
    void aggregationDefaultsToAbsent() {
        assertThat(base().aggregation()).isEmpty();
    }

    @Test
    void withAggregationSetsItAndPreservesEverythingElse() {
        ArangoTableHandle aggregated = base().withLimit(10).withAggregation(countStar());
        assertThat(aggregated.aggregation()).contains(countStar());
        assertThat(aggregated.schema()).isEqualTo("shop");
        assertThat(aggregated.table()).isEqualTo("users");
        assertThat(aggregated.limit()).hasValue(10);
    }

    // withConstraint/withLimit predate the aggregation component; if either dropped it, an
    // aggregated handle could silently fan out into multiple splits and emit duplicate final rows.
    @Test
    void existingWithersPreserveTheAggregation() {
        ArangoTableHandle aggregated = base().withAggregation(countStar());
        assertThat(aggregated.withLimit(5).aggregation()).contains(countStar());
        assertThat(aggregated.withConstraint(TupleDomain.all()).aggregation())
                .contains(countStar());
    }
}
