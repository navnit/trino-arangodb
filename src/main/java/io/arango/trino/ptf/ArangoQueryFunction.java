package io.arango.trino.ptf;

import static io.arango.trino.ArangoErrorCode.ARANGODB_QUERY_NOT_READ_ONLY;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

import com.arangodb.ArangoDBException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * arango.system.query(database, query): raw AQL passthrough (spec, whole document). analyze() order
 * is a correctness invariant (§3.1): explain -> AqlReadOnlyGate -> firstBatch. firstBatch executes
 * the query for real, so it must come strictly after a clean gate verdict.
 */
public class ArangoQueryFunction implements Provider<ConnectorTableFunction> {
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";
    public static final String NON_OBJECT_ROW_MESSAGE =
            "Query returned a non-object row; a table needs named columns. "
                    + "Return an object instead, e.g. RETURN {name: d.name}";

    // ArangoDB error numbers. 1228 = database not found; 1203 = collection or view not found;
    // 1500-1599 = AQL query errors (parse error 1501, missing bind parameter 1551, ...). The
    // 15xx range and 1203 are user errors in a user-supplied query string (§9).
    private static final int ERROR_DATABASE_NOT_FOUND = 1228;
    private static final int ERROR_DATA_SOURCE_NOT_FOUND = 1203;
    private static final int ERROR_QUERY_RANGE_START = 1500;
    private static final int ERROR_QUERY_RANGE_END = 1600;

    private final ArangoClient client;
    private final TypeMapper typeMapper;
    private final ArangoConfig config;

    @Inject
    public ArangoQueryFunction(ArangoClient client, TypeMapper typeMapper, ArangoConfig config) {
        this.client = requireNonNull(client, "client is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new QueryFunction(client, typeMapper, config);
    }

    public static class QueryFunction extends AbstractConnectorTableFunction {
        private final ArangoClient client;
        private final TypeMapper typeMapper;
        private final ArangoConfig config;

        QueryFunction(ArangoClient client, TypeMapper typeMapper, ArangoConfig config) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("DATABASE")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("QUERY")
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
            this.client = client;
            this.typeMapper = typeMapper;
            this.config = config;
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {
            String database = stringArgument(arguments, "DATABASE");
            String aql = stringArgument(arguments, "QUERY");

            // §3.1 ordering: explain -> gate -> firstBatch. Transposing gate and firstBatch
            // would execute a query the gate is about to reject.
            Map<String, Object> explain =
                    translate(database, () -> client.explainPlan(database, aql));
            AqlReadOnlyGate.check(explain)
                    .ifPresent(
                            rejection -> {
                                throw reject(rejection);
                            });
            List<Object> rows =
                    translate(
                            database,
                            () -> client.firstBatch(database, aql, config.getSampleSize()));
            List<ArangoColumnHandle> columns = deriveColumns(rows);

            Descriptor returnedType =
                    new Descriptor(
                            columns.stream()
                                    .map(c -> new Descriptor.Field(c.name(), Optional.of(c.type())))
                                    .collect(ImmutableList.toImmutableList()));
            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(new QueryFunctionHandle(new ArangoQueryHandle(database, aql, columns)))
                    .build();
        }

        // §4.1: field union across the batch, per-field types via TypeMapper.merge — identical
        // inference to SchemaResolver, so the same data types the same whether scanned or
        // passed through. No hidden columns: a passthrough result has no collection identity.
        private List<ArangoColumnHandle> deriveColumns(List<Object> rows) {
            if (rows.isEmpty()) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Query returned no rows at planning time, so no schema can be derived; "
                                + "a passthrough query must return at least one row");
            }
            LinkedHashMap<String, Type> fields = new LinkedHashMap<>();
            for (Object row : rows) {
                if (!(row instanceof Map<?, ?> document)) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, NON_OBJECT_ROW_MESSAGE);
                }
                for (Map.Entry<?, ?> entry : document.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    if (key.isEmpty()) {
                        // Descriptor.Field would throw an engine-internal IllegalArgumentException
                        // on an empty name; make it a user error instead (§4.1)
                        throw new TrinoException(
                                INVALID_FUNCTION_ARGUMENT,
                                "Query returned an object with an empty-string attribute key,"
                                        + " which cannot become a column name");
                    }
                    Type inferred = typeMapper.inferType(entry.getValue());
                    fields.merge(
                            key,
                            inferred,
                            (a, b) -> typeMapper.merge(a, b, config.getMixedTypeStrategy()));
                }
            }
            if (fields.isEmpty()) {
                throw new TrinoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Query returned only empty objects, so no columns can be derived");
            }
            return fields.entrySet().stream()
                    .map(
                            e ->
                                    new ArangoColumnHandle(
                                            e.getKey(),
                                            SchemaResolver.resolveUnknown(e.getValue()),
                                            false,
                                            List.of(e.getKey())))
                    .collect(ImmutableList.toImmutableList());
        }

        private static String stringArgument(Map<String, Argument> arguments, String name) {
            if (!(arguments.get(name) instanceof ScalarArgument scalar)
                    || scalar.getValue() == null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, name + " argument is required");
            }
            return ((Slice) scalar.getValue()).toStringUtf8();
        }

        private static TrinoException reject(AqlReadOnlyGate.Rejection rejection) {
            return switch (rejection.kind()) {
                case NOT_READ_ONLY ->
                        new TrinoException(
                                ARANGODB_QUERY_NOT_READ_ONLY,
                                "Only read-only AQL can be passed through: " + rejection.reason());
                case SYSTEM_COLLECTION ->
                        new TrinoException(
                                INVALID_FUNCTION_ARGUMENT,
                                "Query reads "
                                        + rejection.reason()
                                        + ", which this connector does not expose");
            };
        }

        // §9 classification. 1228 -> SchemaNotFoundException (the missing thing is a database;
        // TableNotFoundException would render as "Table 'db.query' does not exist", which is
        // misleading). 1203 and the 15xx AQL range -> user error carrying the server message.
        // Anything else -> GENERIC_INTERNAL_ERROR, matching ArangoMetadata's translation rule.
        private static <T> T translate(String database, Supplier<T> call) {
            try {
                return call.get();
            } catch (ArangoDBException e) {
                Integer errorNum = e.getErrorNum();
                if (errorNum != null && errorNum == ERROR_DATABASE_NOT_FOUND) {
                    throw new SchemaNotFoundException(database);
                }
                if (errorNum != null
                        && (errorNum == ERROR_DATA_SOURCE_NOT_FOUND
                                || (errorNum >= ERROR_QUERY_RANGE_START
                                        && errorNum < ERROR_QUERY_RANGE_END))) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e.getMessage(), e);
                }
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR, "ArangoDB request failed: " + e.getMessage(), e);
            }
        }
    }

    public record QueryFunctionHandle(@JsonProperty("tableHandle") ArangoQueryHandle tableHandle)
            implements ConnectorTableFunctionHandle {
        @JsonCreator
        public QueryFunctionHandle {
            requireNonNull(tableHandle, "tableHandle is null");
        }
    }
}
