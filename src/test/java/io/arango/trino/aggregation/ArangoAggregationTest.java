package io.arango.trino.aggregation;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.handle.ArangoColumnHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ArangoAggregationTest {
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BIGINT, false, List.of("age"));

    @Test
    void countStarCarriesNoInputColumn() {
        AggregateSpec spec =
                new AggregateSpec(AggregateSpec.Kind.COUNT_STAR, Optional.empty(), "agg_0", BIGINT);
        assertThat(spec.input()).isEmpty();
        assertThat(spec.outputType()).isEqualTo(BIGINT);
    }

    // The kind/input pairing is an invariant AqlBuilder relies on when it switches on the kind:
    // a COUNT_STAR with an input, or a MAX without one, would render nonsense AQL.
    @Test
    void kindAndInputMustAgree() {
        assertThatThrownBy(
                        () ->
                                new AggregateSpec(
                                        AggregateSpec.Kind.COUNT_STAR,
                                        Optional.of(AGE),
                                        "agg_0",
                                        BIGINT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new AggregateSpec(
                                        AggregateSpec.Kind.MAX, Optional.empty(), "agg_0", BIGINT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void componentsAreDefensivelyCopiedAndNonNull() {
        List<ArangoColumnHandle> mutable = new ArrayList<>();
        mutable.add(new ArangoColumnHandle("city", VARCHAR, false, List.of("city")));
        ArangoAggregation aggregation = new ArangoAggregation(mutable, List.of());
        mutable.clear();
        assertThat(aggregation.groupingColumns()).hasSize(1);

        assertThatThrownBy(() -> new ArangoAggregation(null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("groupingColumns");
    }
}
