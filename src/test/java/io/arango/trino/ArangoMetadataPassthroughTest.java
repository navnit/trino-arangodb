package io.arango.trino;

import static org.assertj.core.api.Assertions.assertThat;

import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.ptf.ArangoQueryFunction.QueryFunctionHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * All four pushdown hooks decline a passthrough (spec §6) — each row here is the test for the
 * failure family that produced M5's TupleDomain.none() and N-splits findings. No container: the
 * dispatch must return before any client call.
 */
class ArangoMetadataPassthroughTest {
    private static final ArangoColumnHandle NAME =
            new ArangoColumnHandle("name", VarcharType.VARCHAR, false, List.of("name"));
    private static final ArangoColumnHandle AGE =
            new ArangoColumnHandle("age", BigintType.BIGINT, false, List.of("age"));

    private static ArangoQueryHandle queryHandle() {
        return new ArangoQueryHandle(
                "shop", "FOR d IN users RETURN {name: d.name, age: d.age}", List.of(NAME, AGE));
    }

    private static ArangoMetadata metadata() {
        // the builder does not connect; any call that reaches ArangoDB would throw
        ArangoClient client = new ArangoClient(new ArangoConfig());
        return new ArangoMetadata(
                client,
                new SchemaResolver(client, new TypeMapper(), new ArangoConfig()),
                new ArangoConfig());
    }

    @Test
    void applyFilterDeclines() {
        Constraint constraint =
                new Constraint(
                        TupleDomain.withColumnDomains(
                                Map.of(AGE, Domain.singleValue(BigintType.BIGINT, 36L))));
        assertThat(metadata().applyFilter(null, queryHandle(), constraint)).isEmpty();
    }

    @Test
    void applyLimitDeclines() {
        assertThat(metadata().applyLimit(null, queryHandle(), 5)).isEmpty();
    }

    @Test
    void applyProjectionDeclines() {
        assertThat(metadata().applyProjection(null, queryHandle(), List.of(), Map.of())).isEmpty();
    }

    @Test
    void applyAggregationDeclines() {
        assertThat(
                        metadata()
                                .applyAggregation(
                                        null,
                                        queryHandle(),
                                        List.of(),
                                        Map.of(),
                                        List.of(List.of())))
                .isEmpty();
    }

    @Test
    void applyTableFunctionUnwrapsTheHandleAndItsColumns() {
        Optional<TableFunctionApplicationResult<io.trino.spi.connector.ConnectorTableHandle>>
                result =
                        metadata().applyTableFunction(null, new QueryFunctionHandle(queryHandle()));
        assertThat(result).isPresent();
        assertThat(result.get().getTableHandle()).isEqualTo(queryHandle());
        assertThat(result.get().getColumnHandles()).containsExactly(NAME, AGE);
    }

    @Test
    void applyTableFunctionIgnoresForeignHandles() {
        assertThat(metadata().applyTableFunction(null, new ConnectorTableFunctionHandle() {}))
                .isEmpty();
    }

    @Test
    void getTableMetadataSynthesizesTheQueryTableName() {
        ConnectorTableMetadata tableMetadata = metadata().getTableMetadata(null, queryHandle());
        assertThat(tableMetadata.getTable()).isEqualTo(new SchemaTableName("shop", "query"));
        assertThat(tableMetadata.getColumns())
                .extracting(c -> c.getName())
                .containsExactly("name", "age");
    }

    @Test
    void getColumnHandlesServesTheDerivedColumns() {
        Map<String, ColumnHandle> handles = metadata().getColumnHandles(null, queryHandle());
        assertThat(handles).containsExactly(Map.entry("name", NAME), Map.entry("age", AGE));
    }
}
