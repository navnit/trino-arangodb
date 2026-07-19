package io.arango.trino;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.*;

import java.util.List;

public class ArangoPageSourceProvider implements ConnectorPageSourceProvider {
    private final ArangoClient client;
    private final AqlBuilder aqlBuilder;

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
        AqlQuery q = aqlBuilder.buildScan(handle, cols);
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars()), cols);
    }
}
