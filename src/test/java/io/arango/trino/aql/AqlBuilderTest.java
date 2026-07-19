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

    // No range-filter tests: after the M2 final-review narrowing, ArangoMetadata.isPushable never
    // classifies a range domain (of any type) as pushable, so renderDomain's range-rendering code
    // was removed as dead. Likewise no rendersIsNullFilter/rendersIsNotNullFilter tests: IS NULL /
    // IS NOT NULL are never pushed either -- there is no reachable input shape for renderDomain to
    // render those ways. The equality/IN discrete-set rendering below stays reachable (BOOLEAN
    // equality/IN is the one predicate still pushed) and is exercised generically here.

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
