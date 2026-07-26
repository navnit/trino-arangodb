package io.arango.trino;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

import com.arangodb.ArangoCursor;
import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Ties a pushed aggregation to the AQL that actually reaches the server.
 *
 * <p>This closes a real gap between the other two layers. {@code AqlBuilderAggregateTest} asserts
 * rendered AQL from a <em>hand-built</em> descriptor, and {@code ArangoConnectorAggregationTest}
 * asserts results and Trino plans — so a bug where {@code applyAggregation} builds a subtly wrong
 * descriptor would slip through both: the unit test never sees the real descriptor, and a
 * correct-but-different query still returns correct results. Here the descriptor comes from the
 * real {@link ArangoMetadata#applyAggregation} gate and the query is captured at the {@link
 * ArangoClient} boundary, on its way to a live server.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AggregationWireQueryTest {
    private static final String DB = "wirequery";
    private TestingArangoServer server;
    private RecordingArangoClient client;
    private ArangoMetadata metadata;

    /** Records every AQL query issued, then delegates to the real driver. */
    private static final class RecordingArangoClient extends ArangoClient {
        private final List<String> queries = new ArrayList<>();

        RecordingArangoClient(ArangoConfig config) {
            super(config);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public ArangoCursor<Map> query(
                String database, String aql, Map<String, Object> bindVars, List<String> shardIds) {
            queries.add(aql);
            return super.query(database, aql, bindVars, shardIds);
        }

        String lastQuery() {
            return queries.get(queries.size() - 1);
        }
    }

    @BeforeAll
    void setup() {
        server = new TestingArangoServer();
        ArangoConfig config =
                new ArangoConfig()
                        .setHosts(server.hostPort())
                        .setUser("root")
                        .setPassword(server.rootPassword());
        client = new RecordingArangoClient(config);
        client.createDatabaseForTest(DB);
        client.createDocumentCollectionForTest(DB, "sales");
        client.insertForTest(DB, "sales", Map.of("city", "nyc", "qty", 3L, "price", 10.5));
        client.insertForTest(DB, "sales", Map.of("city", "sfo", "qty", 7L, "price", 4.0));
        metadata =
                new ArangoMetadata(
                        client, new SchemaResolver(client, new TypeMapper(), config), config);
    }

    @AfterAll
    void teardown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    private static ArangoColumnHandle column(String name, Type type) {
        return new ArangoColumnHandle(name, type, false, List.of(name));
    }

    private static AggregateFunction fn(String name, Type outputType, ArangoColumnHandle input) {
        return new AggregateFunction(
                name,
                outputType,
                input == null ? List.of() : List.of(new Variable(input.name(), input.type())),
                List.of(),
                false,
                Optional.empty());
    }

    private static ArangoTableHandle scan() {
        return new ArangoTableHandle(
                DB, "sales", false, TupleDomain.all(), OptionalLong.empty(), Optional.empty());
    }

    /** Runs the aggregation through the real gate and the page source, returning the wire AQL. */
    private String wireQueryFor(
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets) {
        AggregationApplicationResult<ConnectorTableHandle> result =
                metadata.applyAggregation(null, scan(), aggregates, assignments, groupingSets)
                        .orElseThrow(() -> new AssertionError("gate declined the aggregation"));
        ArangoTableHandle handle = (ArangoTableHandle) result.getHandle();

        List<ColumnHandle> columns = new ArrayList<>();
        groupingSets.get(0).forEach(columns::add);
        result.getAssignments().forEach(a -> columns.add(a.getColumn()));

        try (var pageSource =
                new ArangoPageSourceProvider(client, new AqlBuilder(), new ArangoConfig())
                        .createPageSource(
                                null,
                                null,
                                new ArangoSplit(List.of()),
                                handle,
                                Optional.empty(),
                                columns,
                                null)) {
            pageSource.getNextSourcePage(); // force the cursor to execute
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return client.lastQuery();
    }

    @Test
    void globalCountStarReachesTheServerAsACollect() {
        String aql = wireQueryFor(List.of(fn("count", BIGINT, null)), Map.of(), List.of(List.of()));
        assertThat(aql)
                .isEqualTo(
                        "FOR d IN @@col COLLECT AGGREGATE a0 = LENGTH(1) RETURN {\"agg_0\": a0}");
    }

    @Test
    void groupedSumReachesTheServerGuardedAndWithItsCompanionCount() {
        ArangoColumnHandle city = column("city", VARCHAR);
        ArangoColumnHandle price = column("price", DOUBLE);
        String aql =
                wireQueryFor(
                        List.of(fn("sum", DOUBLE, price)),
                        Map.of("city", city, "price", price),
                        List.of(List.of(city)));
        assertThat(aql)
                .contains("COLLECT g0 = ((IS_STRING(d[\"city\"])) ? d[\"city\"] : null)")
                .contains(
                        "AGGREGATE a0 = SUM(((IS_NUMBER(d[\"price\"])) ? (d[\"price\"] + 0.0) : null))")
                .contains("a0n = SUM((IS_NUMBER(d[\"price\"])) ? 1 : 0)")
                .contains("RETURN {\"city\": g0, \"agg_0\": (a0n > 0 ? a0 : null)}");
    }

    // The BIGINT grouping key must carry the signed-zero normalization all the way to the wire --
    // this is review finding B1's guard at the layer where it actually matters.
    @Test
    void bigintGroupingKeyReachesTheServerNormalized() {
        ArangoColumnHandle qty = column("qty", BIGINT);
        String aql =
                wireQueryFor(
                        List.of(fn("count", BIGINT, null)),
                        Map.of("qty", qty),
                        List.of(List.of(qty)));
        assertThat(aql).contains("d[\"qty\"] == 0 ? 0 : d[\"qty\"]");
    }

    // min/max must NOT carry the `+ 0.0` promotion (review finding S1).
    @Test
    void minMaxOnDoubleReachesTheServerUnpromoted() {
        ArangoColumnHandle price = column("price", DOUBLE);
        String aql =
                wireQueryFor(
                        List.of(fn("max", DOUBLE, price)),
                        Map.of("price", price),
                        List.of(List.of()));
        assertThat(aql).contains("MAX(((IS_NUMBER(d[\"price\"])) ? d[\"price\"] : null))");
        assertThat(aql).doesNotContain("+ 0.0");
    }
}
