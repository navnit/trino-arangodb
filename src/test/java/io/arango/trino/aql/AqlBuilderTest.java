package io.arango.trino.aql;

import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

class AqlBuilderTest {
    @Test
    void buildsFullScanWithBoundCollection() {
        AqlQuery q = new AqlBuilder().buildScan(
                new ArangoTableHandle("shop", "users", false, TupleDomain.all(), OptionalLong.empty()), List.of());
        assertThat(q.aql()).isEqualTo("FOR d IN @@col RETURN d");
        assertThat(q.bindVars()).containsEntry("@col", "users");
    }
}
