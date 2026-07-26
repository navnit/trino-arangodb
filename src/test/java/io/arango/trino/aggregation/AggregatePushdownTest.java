package io.arango.trino.aggregation;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.ArangoConfig;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class AggregatePushdownTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VARCHAR, false, List.of("city"));
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
    private static final ArangoColumnHandle SCORE =
            new ArangoColumnHandle("score", DOUBLE, false, List.of("score"));
    private static final ArangoColumnHandle ACTIVE =
            new ArangoColumnHandle("active", BOOLEAN, false, List.of("active"));

    private static final Map<String, ColumnHandle> ASSIGNMENTS =
            Map.of("city", CITY, "age", AGE, "score", SCORE, "active", ACTIVE);

    private static ArangoTableHandle handle() {
        return new ArangoTableHandle(
                "shop",
                "users",
                false,
                TupleDomain.<ColumnHandle>all(),
                OptionalLong.empty(),
                Optional.empty());
    }

    private static AggregateFunction fn(String name, Type outputType, ArangoColumnHandle input) {
        List<ConnectorExpression> args =
                input == null ? List.of() : List.of(new Variable(input.name(), input.type()));
        return new AggregateFunction(name, outputType, args, List.of(), false, Optional.empty());
    }

    private static Optional<ArangoAggregation> plan(
            List<AggregateFunction> aggregates, List<List<ColumnHandle>> groupingSets) {
        return AggregatePushdown.plan(
                new ArangoConfig(), handle(), aggregates, ASSIGNMENTS, groupingSets);
    }

    private static Optional<ArangoAggregation> global(List<AggregateFunction> aggregates) {
        return plan(aggregates, List.of(List.of()));
    }

    // --- claimed shapes -------------------------------------------------------------------

    @Test
    void countStarPushesGlobally() {
        ArangoAggregation agg = global(List.of(fn("count", BIGINT, null))).orElseThrow();
        assertThat(agg.groupingColumns()).isEmpty();
        assertThat(agg.aggregates())
                .singleElement()
                .satisfies(
                        s -> {
                            assertThat(s.kind()).isEqualTo(AggregateSpec.Kind.COUNT_STAR);
                            assertThat(s.outputName()).isEqualTo("agg_0");
                            assertThat(s.outputType()).isEqualTo(BIGINT);
                        });
    }

    @Test
    void countOfEveryGuardableColumnTypePushes() {
        for (ArangoColumnHandle column : List.of(CITY, AGE, SCORE, ACTIVE)) {
            assertThat(global(List.of(fn("count", BIGINT, column))))
                    .as("count(%s)", column.name())
                    .isPresent();
        }
    }

    @Test
    void minMaxPushOnBigintAndDouble() {
        assertThat(global(List.of(fn("min", BIGINT, AGE)))).isPresent();
        assertThat(global(List.of(fn("max", DOUBLE, SCORE)))).isPresent();
    }

    @Test
    void sumAndAvgPushOnDouble() {
        assertThat(global(List.of(fn("sum", DOUBLE, SCORE)))).isPresent();
        assertThat(global(List.of(fn("avg", DOUBLE, SCORE)))).isPresent();
    }

    @Test
    void groupedAggregatePushesAndKeepsGroupingColumns() {
        ArangoAggregation agg =
                plan(List.of(fn("count", BIGINT, null)), List.of(List.of(CITY))).orElseThrow();
        assertThat(agg.groupingColumns()).containsExactly(CITY);
    }

    // SELECT DISTINCT city / bare GROUP BY city: zero aggregates, one grouping set (design §5).
    @Test
    void zeroAggregateGroupingPushes() {
        ArangoAggregation agg = plan(List.of(), List.of(List.of(CITY))).orElseThrow();
        assertThat(agg.aggregates()).isEmpty();
        assertThat(agg.groupingColumns()).containsExactly(CITY);
    }

    @Test
    void aggregateOutputNamesAreUniqueAndDoNotCollideWithGroupingColumns() {
        ArangoColumnHandle agg0 = new ArangoColumnHandle("agg_0", VARCHAR, false, List.of("agg_0"));
        Optional<ArangoAggregation> planned =
                AggregatePushdown.plan(
                        new ArangoConfig(),
                        handle(),
                        List.of(fn("count", BIGINT, null)),
                        Map.of("agg_0", agg0),
                        List.of(List.of(agg0)));
        assertThat(planned.orElseThrow().aggregates().get(0).outputName()).isNotEqualTo("agg_0");
    }

    // --- declines -------------------------------------------------------------------------

    @Test
    void declinesWhenDisabledByConfig() {
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig().setAggregationPushdownEnabled(false),
                                handle(),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // Strict mode must raise ARANGODB_TYPE_CONVERSION_ERROR on a mismatch; a pushed aggregate
    // would silently absorb it. Mirrors ArangoMetadata.isPushable's strict-mode decline.
    @Test
    void declinesUnderStrictCoercion() {
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig()
                                        .setTypeCoercion(ArangoConfig.TypeCoercion.STRICT),
                                handle(),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    @Test
    void declinesOnAnAlreadyAggregatedHandle() {
        ArangoTableHandle aggregated =
                handle().withAggregation(
                                new ArangoAggregation(
                                        List.of(),
                                        List.of(
                                                new AggregateSpec(
                                                        AggregateSpec.Kind.COUNT_STAR,
                                                        Optional.empty(),
                                                        "agg_0",
                                                        BIGINT))));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                aggregated,
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // LIMIT-then-GROUP BY is not GROUP BY-then-LIMIT, and the single-FOR AQL body expresses only
    // the latter.
    @Test
    void declinesWhenALimitIsAlreadyPushed() {
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle().withLimit(10),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // A prefilter-only domain (BIGINT range) is enforced jointly by AQL and Trino's residual
    // re-check; aggregating over the AQL side alone would include rows the residual would drop.
    @Test
    void declinesOverAPrefilterOnlyConstraint() {
        TupleDomain<ColumnHandle> bigintRange =
                TupleDomain.withColumnDomains(
                        Map.of(
                                AGE,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(BIGINT, 21L)), false)));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle().withConstraint(bigintRange),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isEmpty();
    }

    // A fully-enforced constraint (BIGINT equality, DOUBLE range) leaves no residual, so it does
    // not block the aggregate.
    @Test
    void allowsAggregationOverAFullyEnforcedConstraint() {
        TupleDomain<ColumnHandle> equality =
                TupleDomain.withColumnDomains(Map.of(AGE, Domain.singleValue(BIGINT, 21L)));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle().withConstraint(equality),
                                List.of(fn("count", BIGINT, null)),
                                ASSIGNMENTS,
                                List.of(List.of())))
                .isPresent();
    }

    @Test
    void declinesMultipleGroupingSets() {
        assertThat(plan(List.of(fn("count", BIGINT, null)), List.of(List.of(CITY), List.of())))
                .isEmpty();
    }

    @Test
    void declinesGlobalAggregationWithNoAggregateFunctions() {
        assertThat(plan(List.of(), List.of(List.of()))).isEmpty();
    }

    @Test
    void declinesDistinctFilteredAndOrderedAggregates() {
        AggregateFunction distinct =
                new AggregateFunction(
                        "count",
                        BIGINT,
                        List.of(new Variable("age", BIGINT)),
                        List.of(),
                        true,
                        Optional.empty());
        AggregateFunction filtered =
                new AggregateFunction(
                        "count",
                        BIGINT,
                        List.of(new Variable("age", BIGINT)),
                        List.of(),
                        false,
                        Optional.of(new Variable("active", BOOLEAN)));
        AggregateFunction ordered =
                new AggregateFunction(
                        "max",
                        BIGINT,
                        List.of(new Variable("age", BIGINT)),
                        List.of(new SortItem("age", SortOrder.ASC_NULLS_FIRST)),
                        false,
                        Optional.empty());
        assertThat(global(List.of(distinct))).isEmpty();
        assertThat(global(List.of(filtered))).isEmpty();
        assertThat(global(List.of(ordered))).isEmpty();
    }

    @Test
    void declinesUnknownFunctionNames() {
        assertThat(global(List.of(fn("approx_distinct", BIGINT, AGE)))).isEmpty();
        assertThat(global(List.of(fn("count_if", BIGINT, ACTIVE)))).isEmpty();
        assertThat(global(List.of(fn("arbitrary", BIGINT, AGE)))).isEmpty();
    }

    @Test
    void declinesNonVariableArguments() {
        AggregateFunction constantArg =
                new AggregateFunction(
                        "count",
                        BIGINT,
                        List.of(new Constant(1L, BIGINT)),
                        List.of(),
                        false,
                        Optional.empty());
        assertThat(global(List.of(constantArg))).isEmpty();
    }

    // ArangoDB orders strings by the server's collation; Trino orders by codepoint (design §5).
    @Test
    void declinesMinMaxOnVarchar() {
        assertThat(global(List.of(fn("min", VARCHAR, CITY)))).isEmpty();
        assertThat(global(List.of(fn("max", VARCHAR, CITY)))).isEmpty();
    }

    // AQL accumulates sums in double: precision is lost past 2^53 and Trino's loud
    // sum(bigint) overflow becomes silent (design §4/15).
    @Test
    void declinesSumAndAvgOnBigint() {
        assertThat(global(List.of(fn("sum", BIGINT, AGE)))).isEmpty();
        assertThat(global(List.of(fn("avg", DOUBLE, AGE)))).isEmpty();
    }

    @Test
    void declinesMinMaxOnBoolean() {
        assertThat(global(List.of(fn("min", BOOLEAN, ACTIVE)))).isEmpty();
    }

    @Test
    void declinesUnguardableGroupingKeyTypes() {
        ArangoColumnHandle tags =
                new ArangoColumnHandle("tags", new ArrayType(BIGINT), false, List.of("tags"));
        assertThat(
                        AggregatePushdown.plan(
                                new ArangoConfig(),
                                handle(),
                                List.of(fn("count", BIGINT, null)),
                                Map.of("tags", tags),
                                List.of(List.of(tags))))
                .isEmpty();
    }

    // All-or-nothing: one unsupported aggregate declines the whole call (base-JDBC's contract).
    @Test
    void oneUnsupportedAggregateDeclinesTheWholeCall() {
        assertThat(global(List.of(fn("count", BIGINT, null), fn("sum", BIGINT, AGE)))).isEmpty();
    }
}
