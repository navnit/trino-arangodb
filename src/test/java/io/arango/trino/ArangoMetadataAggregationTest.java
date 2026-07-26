package io.arango.trino;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ArangoMetadataAggregationTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VARCHAR, false, List.of("city"));

    // No container needed: none of these paths touch the client. A hand-written subclass matches
    // the suite's convention of test doubles over a mocking framework (the ArangoClient
    // constructor builds a driver instance but opens no connection).
    private static final class ArangoClientStub extends ArangoClient {
        ArangoClientStub() {
            super(new ArangoConfig());
        }
    }

    private static ArangoMetadata metadata(ArangoConfig config) {
        return new ArangoMetadata(new ArangoClientStub(), null, config);
    }

    private static ArangoTableHandle handle() {
        return new ArangoTableHandle(
                "shop",
                "users",
                false,
                TupleDomain.<ColumnHandle>all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    private static ArangoAggregation countStarAggregation() {
        return new ArangoAggregation(
                List.of(),
                List.of(
                        new AggregateSpec(
                                AggregateSpec.Kind.COUNT_STAR, Optional.empty(), "agg_0", BIGINT)));
    }

    private static ArangoTableHandle aggregatedHandle() {
        return handle().withAggregation(countStarAggregation());
    }

    private static AggregateFunction countStar() {
        return new AggregateFunction(
                "count", BIGINT, List.of(), List.of(), false, Optional.empty());
    }

    @Test
    void applyAggregationProducesAnAggregatedHandleWithMatchingProjections() {
        AggregationApplicationResult<ConnectorTableHandle> result =
                metadata(new ArangoConfig())
                        .applyAggregation(
                                null,
                                handle(),
                                List.of(countStar()),
                                Map.of("city", CITY),
                                List.of(List.of(CITY)))
                        .orElseThrow();

        ArangoTableHandle newHandle = (ArangoTableHandle) result.getHandle();
        assertThat(newHandle.aggregation()).isPresent();
        assertThat(newHandle.aggregation().orElseThrow().groupingColumns()).containsExactly(CITY);
        assertThat(result.getProjections())
                .containsExactly((ConnectorExpression) new Variable("agg_0", BIGINT));
        assertThat(result.getAssignments())
                .singleElement()
                .satisfies(a -> assertThat(a.getVariable()).isEqualTo("agg_0"));
        // Grouping columns keep their own handles, so nothing needs remapping (as base-JDBC does).
        assertThat(result.getGroupingColumnMapping()).isEmpty();
        assertThat(result.isPrecalculateStatistics()).isFalse();
    }

    @Test
    void applyAggregationDeclinesWhenTheGateDeclines() {
        assertThat(
                        metadata(new ArangoConfig().setAggregationPushdownEnabled(false))
                                .applyAggregation(
                                        null,
                                        handle(),
                                        List.of(countStar()),
                                        Map.of(),
                                        List.of(List.of())))
                .isEmpty();
    }

    // A filter arriving after aggregation is a HAVING, but AqlBuilder renders pushed filters
    // BEFORE the COLLECT -- pushing it would silently turn HAVING into WHERE.
    @Test
    void applyFilterDeclinesOnAnAggregatedHandle() {
        Constraint constraint =
                new Constraint(
                        TupleDomain.withColumnDomains(
                                Map.<ColumnHandle, Domain>of(
                                        CITY, Domain.singleValue(VARCHAR, utf8Slice("nyc")))));
        assertThat(metadata(new ArangoConfig()).applyFilter(null, aggregatedHandle(), constraint))
                .isEmpty();
    }

    // Declines explicitly rather than by coincidence of the !progress exit, so a later widening
    // of the grouping-key matrix cannot turn it into a dereference pushed at a COLLECT variable.
    @Test
    void applyProjectionDeclinesOnAnAggregatedHandle() {
        assertThat(
                        metadata(new ArangoConfig())
                                .applyProjection(
                                        null,
                                        aggregatedHandle(),
                                        List.of(new Variable("agg_0", BIGINT)),
                                        Map.of("agg_0", CITY)))
                .isEmpty();
    }

    // An aggregated handle is always one split, so a LIMIT after COLLECT is the final limit.
    @Test
    void applyLimitIsGuaranteedOnAnAggregatedHandleEvenWithShardParallelismEnabled() {
        assertThat(
                        metadata(new ArangoConfig())
                                .applyLimit(null, aggregatedHandle(), 10)
                                .orElseThrow()
                                .isLimitGuaranteed())
                .isTrue();
        assertThat(
                        metadata(new ArangoConfig())
                                .applyLimit(null, handle(), 10)
                                .orElseThrow()
                                .isLimitGuaranteed())
                .isFalse();
    }
}
