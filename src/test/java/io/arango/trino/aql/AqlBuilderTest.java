package io.arango.trino.aql;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class AqlBuilderTest {
    private static ArangoTableHandle unconstrainedHandle() {
        return new ArangoTableHandle("shop", "users", false, TupleDomain.<ColumnHandle>all(), OptionalLong.empty());
    }

    @Test
    void buildsProjectedScanWithBoundCollection() {
        AqlQuery q = new AqlBuilder().buildScan(
                unconstrainedHandle(),
                List.of(new ArangoColumnHandle("name", VARCHAR, false, List.of("name"))));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN {\"name\": d[\"name\"]}");
        assertThat(q.bindVars()).containsEntry("@col", "users");
    }

    @Test
    void buildsEmptyReturnWhenNoColumnsRequested() {
        AqlQuery q = new AqlBuilder().buildScan(unconstrainedHandle(), List.of());
        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN {}");
    }

    @Test
    void treatsLiteralDotInColumnNameAsOneAttributeNotANestedPath() {
        ArangoColumnHandle dotted = new ArangoColumnHandle("a.b", VARCHAR, false, List.of("a.b"));
        AqlQuery q = new AqlBuilder().buildScan(unconstrainedHandle(), List.of(dotted));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN {\"a.b\": d[\"a.b\"]}");
    }

    private static ArangoTableHandle handleWithConstraint(Map<ColumnHandle, Domain> domains) {
        return new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(domains), OptionalLong.empty());
    }

    @Test
    void rendersEqualityFilter() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(age, Domain.singleValue(BIGINT, 30L))),
                List.of(age));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col FILTER (d[\"age\"] == @v0) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 30L);
    }

    @Test
    void rendersInFilter() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(age, Domain.multipleValues(BIGINT, List.of(30L, 40L)))),
                List.of(age));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col FILTER (d[\"age\"] IN @v0) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", List.of(30L, 40L));
    }

    @Test
    void bigintRangeRendersWithNumberGuardOnly() {
        // BIGINT range uses a bare IS_NUMBER guard -- NO `== FLOOR(...)` integrality conjunct. AQL FLOOR()
        // returns a double, so `d.f == FLOOR(d.f)` would false-miss a stored int64 > 2^53 (review finding
        // C3). It is unnecessary anyway: BIGINT range is prefilter-only, so admitting fractional/out-of-range
        // values (which read back NULL) is safe -- Trino's residual re-check drops them.
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(Map.of(age,
                        Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 30L)), false))),
                OptionalLong.empty());
        AqlQuery q = new AqlBuilder().buildScan(handle, List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"age\"]) AND d[\"age\"] > @v0) "
                        + "RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 30L);
    }

    @Test
    void doubleRangeRendersWithNumberGuardAndDoublePromotion() {
        // A DOUBLE range promotes the operand into double space with `+ 0.0` so AQL compares exactly
        // what appendValue's n.doubleValue() emits (a bare comparison diverges for a stored int64 > 2^53
        // -- review finding C1); the IS_NUMBER guard precedes it because `+ 0.0` coerces non-numbers.
        ArangoColumnHandle price = new ArangoColumnHandle("price", DOUBLE, false, List.of("price"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "items", false,
                TupleDomain.withColumnDomains(Map.of(price,
                        Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 9.99)), false))),
                OptionalLong.empty());
        AqlQuery q = new AqlBuilder().buildScan(handle, List.of(price));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"price\"]) AND (d[\"price\"] + 0.0) <= @v0) "
                        + "RETURN {\"price\": d[\"price\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 9.99);
    }

    @Test
    void doubleEqualityRendersWithNumberGuardAndDoublePromotion() {
        // DOUBLE equality/IN also promotes and guards (unlike BOOLEAN/VARCHAR/BIGINT equality, which is
        // type-exact in AQL and stays bare) -- otherwise a stored int64 > 2^53 would match a bind it
        // does not equal after rounding on read.
        ArangoColumnHandle price = new ArangoColumnHandle("price", DOUBLE, false, List.of("price"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "items", false,
                TupleDomain.withColumnDomains(Map.of(price,
                        Domain.create(ValueSet.of(DOUBLE, 20.0), false))),
                OptionalLong.empty());
        AqlQuery q = new AqlBuilder().buildScan(handle, List.of(price));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"price\"]) AND (d[\"price\"] + 0.0) == @v0) "
                        + "RETURN {\"price\": d[\"price\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 20.0);
    }

    @Test
    void bigintBoundedRangeRendersBothBoundsWithNumberGuard() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(Map.of(age,
                        Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 10L, true, 20L, false)), false))),
                OptionalLong.empty());
        AqlQuery q = new AqlBuilder().buildScan(handle, List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"age\"]) "
                        + "AND (d[\"age\"] >= @v0 AND d[\"age\"] < @v1)) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 10L).containsEntry("v1", 20L);
    }

    @Test
    void bigintMultiRangeRendersOrJoinedClausesInsideNumberGuard() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(Map.of(age,
                        Domain.create(ValueSet.ofRanges(
                                Range.lessThan(BIGINT, 10L), Range.greaterThan(BIGINT, 20L)), false))),
                OptionalLong.empty());
        AqlQuery q = new AqlBuilder().buildScan(handle, List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"age\"]) "
                        + "AND (d[\"age\"] < @v0 OR d[\"age\"] > @v1)) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 10L).containsEntry("v1", 20L);
    }

    @Test
    void rendersVarcharEqualityByConvertingSliceToString() {
        ArangoColumnHandle name = new ArangoColumnHandle("name", VARCHAR, false, List.of("name"));
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(name, Domain.singleValue(VARCHAR, utf8Slice("ada")))),
                List.of(name));
        assertThat(q.bindVars()).containsEntry("v0", "ada");
    }

    @Test
    void combinesTwoColumnFiltersWithAnd() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        ArangoColumnHandle active = new ArangoColumnHandle("active", BOOLEAN, false, List.of("active"));
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(
                        age, Domain.singleValue(BIGINT, 30L),
                        active, Domain.singleValue(BOOLEAN, true))),
                List.of(age, active));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (d[\"age\"] == @v0) AND (d[\"active\"] == @v1) RETURN {\"age\": d[\"age\"], \"active\": d[\"active\"]}");
    }

    @Test
    void rendersLimitAsLiteralAfterFilter() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, List.of("age"));
        ArangoTableHandle handle = new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(ImmutableMap.of(age, Domain.singleValue(BIGINT, 30L))),
                OptionalLong.of(5L));
        AqlQuery q = new AqlBuilder().buildScan(handle, List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (d[\"age\"] == @v0) LIMIT 5 RETURN {\"age\": d[\"age\"]}");
    }

    @Test
    void rendersLimitWithNoFilter() {
        AqlQuery q = new AqlBuilder().buildScan(
                new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.of(10L)),
                List.of());
        assertThat(q.aql()).isEqualTo("FOR d IN @@col LIMIT 10 RETURN {}");
    }

    @Test
    void rendersMultiSegmentPathAsChainedBracketAccess() {
        ArangoColumnHandle city = new ArangoColumnHandle("address$city", VARCHAR, false, List.of("address", "city"));
        AqlQuery q = new AqlBuilder().buildScan(unconstrainedHandle(), List.of(city));

        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN {\"address$city\": d[\"address\"][\"city\"]}");
    }
}
