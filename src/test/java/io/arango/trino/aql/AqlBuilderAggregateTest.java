package io.arango.trino.aql;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.aggregation.AggregateSpec;
import io.arango.trino.aggregation.ArangoAggregation;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class AqlBuilderAggregateTest {
    private static final ArangoColumnHandle CITY =
            new ArangoColumnHandle("city", VARCHAR, false, List.of("city"));
    private static final ArangoColumnHandle SCORE =
            new ArangoColumnHandle("score", DOUBLE, false, List.of("score"));
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BIGINT, false, List.of("age"));

    private static ArangoColumnHandle output(String name) {
        return new ArangoColumnHandle(name, BIGINT, false, List.of(name));
    }

    private static ArangoTableHandle aggregated(ArangoAggregation aggregation) {
        return new ArangoTableHandle(
                        "shop",
                        "users",
                        false,
                        TupleDomain.<ColumnHandle>all(),
                        OptionalLong.empty(),
                        Optional.empty())
                .withAggregation(aggregation);
    }

    private static AggregateSpec spec(
            AggregateSpec.Kind kind, ArangoColumnHandle input, String name) {
        return new AggregateSpec(
                kind,
                Optional.ofNullable(input),
                name,
                kind == AggregateSpec.Kind.AVG || kind == AggregateSpec.Kind.SUM ? DOUBLE : BIGINT);
    }

    private static AqlQuery build(ArangoAggregation aggregation, List<ArangoColumnHandle> columns) {
        return new AqlBuilder().buildAggregate(aggregated(aggregation), columns);
    }

    @Test
    void globalCountStarUsesLengthAndReturnsItDirectly() {
        AqlQuery q =
                build(
                        new ArangoAggregation(
                                List.of(),
                                List.of(spec(AggregateSpec.Kind.COUNT_STAR, null, "agg_0"))),
                        List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = LENGTH(1) RETURN {\"agg_0\": a0}");
        assertThat(q.bindVars()).containsEntry("@col", "users");
    }

    // AQL COUNT is an alias of LENGTH and counts nulls, so count(col) must sum a guard predicate.
    // AQL SUM over zero rows is null, so an empty table would report NULL instead of 0 without
    // the wrap (design §4/1, §4/3).
    @Test
    void countOfColumnSumsTheGuardAndWrapsAgainstNull() {
        AqlQuery q =
                build(
                        new ArangoAggregation(
                                List.of(),
                                List.of(spec(AggregateSpec.Kind.COUNT_COLUMN, CITY, "agg_0"))),
                        List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = SUM((IS_STRING(d[\"city\"])) ? 1 :"
                                + " 0) RETURN {\"agg_0\": (a0 == null ? 0 : a0)}");
    }

    // AQL SUM of an all-null group is 0 where SQL says NULL, so a companion count converts it
    // (design §4/2). `null > 0` is false, which also covers the empty-table case (§4/23).
    @Test
    void sumCarriesACompanionCountToDistinguishZeroFromNull() {
        AqlQuery q =
                build(
                        new ArangoAggregation(
                                List.of(), List.of(spec(AggregateSpec.Kind.SUM, SCORE, "agg_0"))),
                        List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = SUM(((IS_NUMBER(d[\"score\"])) ?"
                                + " (d[\"score\"] + 0.0) : null)), a0n ="
                                + " SUM((IS_NUMBER(d[\"score\"])) ? 1 : 0) RETURN {\"agg_0\": (a0n"
                                + " > 0 ? a0 : null)}");
    }

    @Test
    void avgNeedsNoCompanionBecauseAqlAverageIsAlreadyNullOnEmpty() {
        AqlQuery q =
                build(
                        new ArangoAggregation(
                                List.of(), List.of(spec(AggregateSpec.Kind.AVG, SCORE, "agg_0"))),
                        List.of(output("agg_0")));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = AVERAGE(((IS_NUMBER(d[\"score\"])) ?"
                                + " (d[\"score\"] + 0.0) : null)) RETURN {\"agg_0\": a0}");
    }

    // Review finding S1: `+ 0.0` would turn a stored -0.0 into 0.0, and rounding's monotonicity
    // makes the promotion unnecessary for extrema.
    @Test
    void minMaxOnDoubleDoNotPromote() {
        AqlQuery q =
                build(
                        new ArangoAggregation(
                                List.of(), List.of(spec(AggregateSpec.Kind.MIN, SCORE, "agg_0"))),
                        List.of(output("agg_0")));
        assertThat(q.aql()).contains("MIN(((IS_NUMBER(d[\"score\"])) ? d[\"score\"] : null))");
        assertThat(q.aql()).doesNotContain("+ 0.0");
    }

    // Review finding B1: a bare BIGINT grouping key puts a stored -0.0 in its own hash-COLLECT
    // group, which then materializes to the same Trino key 0 -- duplicate output rows.
    @Test
    void bigintGroupingKeyNormalizesSignedZero() {
        AqlQuery q =
                build(
                        new ArangoAggregation(
                                List.of(AGE),
                                List.of(spec(AggregateSpec.Kind.COUNT_STAR, null, "agg_0"))),
                        List.of(AGE, output("agg_0")));
        assertThat(q.aql())
                .contains("d[\"age\"] == 0 ? 0 : d[\"age\"]")
                .endsWith("RETURN {\"age\": g0, \"agg_0\": a0}");
    }

    // Synthetic COLLECT variable names exist so a column name that is not a legal AQL identifier
    // (applyProjection's nested address$city) never has to be one.
    @Test
    void usesSyntheticVariableNamesButRealObjectKeys() {
        ArangoColumnHandle nested =
                new ArangoColumnHandle("address$city", VARCHAR, false, List.of("address", "city"));
        AqlQuery q = build(new ArangoAggregation(List.of(nested), List.of()), List.of(nested));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT g0 = ((IS_STRING(d[\"address\"][\"city\"])) ?"
                                + " d[\"address\"][\"city\"] : null) RETURN {\"address$city\":"
                                + " g0}");
    }

    @Test
    void clauseOrderIsFilterThenCollectThenLimit() {
        TupleDomain<ColumnHandle> constraint =
                TupleDomain.withColumnDomains(
                        Map.of(
                                SCORE,
                                Domain.create(
                                        ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false)));
        ArangoTableHandle handle =
                aggregated(
                                new ArangoAggregation(
                                        List.of(CITY),
                                        List.of(
                                                spec(
                                                        AggregateSpec.Kind.COUNT_STAR,
                                                        null,
                                                        "agg_0"))))
                        .withConstraint(constraint)
                        .withLimit(5);
        AqlQuery q = new AqlBuilder().buildAggregate(handle, List.of(CITY, output("agg_0")));
        assertThat(q.aql())
                .matches(
                        "FOR d IN @@col FILTER .*COLLECT g0 = .*AGGREGATE a0 = LENGTH\\(1\\) LIMIT"
                                + " 5 RETURN \\{.*\\}");
        assertThat(q.bindVars()).containsEntry("v0", 1.0);
    }

    // Trino may prune aggregate outputs it does not need, and the sum companion count is never a
    // requested column -- so RETURN is driven by the requested columns, not by the descriptor.
    @Test
    void returnsOnlyRequestedColumnsInRequestedOrder() {
        ArangoAggregation aggregation =
                new ArangoAggregation(
                        List.of(CITY),
                        List.of(
                                spec(AggregateSpec.Kind.COUNT_STAR, null, "agg_0"),
                                spec(AggregateSpec.Kind.MAX, AGE, "agg_1")));
        AqlQuery q = build(aggregation, List.of(output("agg_1"), CITY));
        assertThat(q.aql()).endsWith("RETURN {\"agg_1\": a1, \"city\": g0}");
        assertThat(q.aql()).contains("AGGREGATE a0 = LENGTH(1), a1 = MAX(");
    }

    @Test
    void zeroAggregateGroupingRendersABareCollect() {
        AqlQuery q = build(new ArangoAggregation(List.of(CITY), List.of()), List.of(CITY));
        assertThat(q.aql())
                .isEqualTo(
                        "FOR d IN @@col COLLECT g0 = ((IS_STRING(d[\"city\"])) ? d[\"city\"] :"
                                + " null) RETURN {\"city\": g0}");
    }
}
