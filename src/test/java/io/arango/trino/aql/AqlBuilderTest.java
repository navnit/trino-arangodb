package io.arango.trino.aql;

import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

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
}
