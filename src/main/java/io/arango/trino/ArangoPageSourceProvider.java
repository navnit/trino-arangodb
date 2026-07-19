package io.arango.trino;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class ArangoPageSourceProvider implements ConnectorPageSourceProvider {
    private final ArangoClient client;
    private final AqlBuilder aqlBuilder;

    @com.google.inject.Inject
    public ArangoPageSourceProvider(ArangoClient client, AqlBuilder aqlBuilder) {
        this.client = client;
        this.aqlBuilder = aqlBuilder;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        List<ArangoColumnHandle> cols = columns.stream()
                .map(ArangoColumnHandle.class::cast).toList();
        cols.forEach(ArangoPageSourceProvider::checkMaterializable);
        AqlQuery q = aqlBuilder.buildScan(handle, cols);
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars()), cols);
    }

    // Fail fast and loud, before the query even runs, rather than silently emitting NULL for
    // every row of a structured column (ArangoPageSource.appendValue's lenient fallback).
    // ARRAY/ROW/DECIMAL value materialization is deferred to M2; the schema is still
    // correctly inferred for these types (they show up in SHOW COLUMNS), only reading their
    // values is unsupported so far. Only *requested* columns are checked here -- Trino's
    // actual projection -- so a query that doesn't touch an unsupported column elsewhere in
    // the table is unaffected.
    private static void checkMaterializable(ArangoColumnHandle column) {
        Type type = column.type();
        if (type instanceof ArrayType || type instanceof RowType || type instanceof DecimalType) {
            throw new TrinoException(NOT_SUPPORTED,
                    "Column '%s' has type %s; ARRAY/ROW/DECIMAL value materialization is not supported in this milestone"
                            .formatted(column.name(), type));
        }
    }
}
