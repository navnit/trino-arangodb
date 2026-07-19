package io.arango.trino.aql;

import com.google.common.collect.ImmutableMap;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
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
                List.of(new ArangoColumnHandle("name", VARCHAR, false, "name")));
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
        ArangoColumnHandle dotted = new ArangoColumnHandle("a.b", VARCHAR, false, "a.b");
        AqlQuery q = new AqlBuilder().buildScan(unconstrainedHandle(), List.of(dotted));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN {\"a.b\": d[\"a.b\"]}");
    }

    private static ArangoTableHandle handleWithConstraint(Map<ColumnHandle, Domain> domains) {
        return new ArangoTableHandle("shop", "users", false,
                TupleDomain.withColumnDomains(domains), OptionalLong.empty());
    }

    @Test
    void rendersEqualityFilter() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(age, Domain.singleValue(BIGINT, 30L))),
                List.of(age));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col FILTER (d[\"age\"] == @v0) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 30L);
    }

    @Test
    void rendersInFilter() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(age, Domain.multipleValues(BIGINT, List.of(30L, 40L)))),
                List.of(age));
        assertThat(q.aql()).isEqualTo("FOR d IN @@col FILTER (d[\"age\"] IN @v0) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", List.of(30L, 40L));
    }

    @Test
    void rendersGuardedRangeFilter() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        Domain range = Domain.create(
                io.trino.spi.predicate.ValueSet.ofRanges(io.trino.spi.predicate.Range.greaterThan(BIGINT, 30L)),
                false);
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(age, range)),
                List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"age\"]) AND d[\"age\"] > @v0) RETURN {\"age\": d[\"age\"]}");
        assertThat(q.bindVars()).containsEntry("v0", 30L);
    }

    @Test
    void rendersTwoSidedRangeFilterWithoutRedundantParens() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        Domain range = Domain.create(
                io.trino.spi.predicate.ValueSet.ofRanges(io.trino.spi.predicate.Range.range(BIGINT, 20L, true, 30L, false)),
                false);
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(age, range)),
                List.of(age));
        assertThat(q.aql()).isEqualTo(
                "FOR d IN @@col FILTER (IS_NUMBER(d[\"age\"]) AND (d[\"age\"] >= @v0 AND d[\"age\"] < @v1)) RETURN {\"age\": d[\"age\"]}");
    }

    // No rendersIsNullFilter/rendersIsNotNullFilter tests: IS NULL/IS NOT NULL are never pushed
    // (see ArangoMetadata.isPushable), so renderDomain never receives an only-null/not-null
    // domain -- there is no reachable input shape for this method to render that way.

    @Test
    void rendersVarcharEqualityByConvertingSliceToString() {
        ArangoColumnHandle name = new ArangoColumnHandle("name", VARCHAR, false, "name");
        AqlQuery q = new AqlBuilder().buildScan(
                handleWithConstraint(ImmutableMap.of(name, Domain.singleValue(VARCHAR, utf8Slice("ada")))),
                List.of(name));
        assertThat(q.bindVars()).containsEntry("v0", "ada");
    }

    @Test
    void combinesTwoColumnFiltersWithAnd() {
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
        ArangoColumnHandle active = new ArangoColumnHandle("active", BOOLEAN, false, "active");
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
        ArangoColumnHandle age = new ArangoColumnHandle("age", BIGINT, false, "age");
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
}
