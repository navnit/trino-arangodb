package io.arango.trino;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.trino.spi.connector.*;
import java.util.List;
import java.util.Optional;

public class ArangoPageSourceProvider implements ConnectorPageSourceProvider {
    private final ArangoClient client;
    private final AqlBuilder aqlBuilder;
    private final ArangoConfig config;

    @com.google.inject.Inject
    public ArangoPageSourceProvider(
            ArangoClient client, AqlBuilder aqlBuilder, ArangoConfig config) {
        this.client = client;
        this.aqlBuilder = aqlBuilder;
        this.config = config;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ArangoSplit arangoSplit = (ArangoSplit) split;
        List<ArangoColumnHandle> cols =
                columns.stream().map(ArangoColumnHandle.class::cast).toList();
        AqlQuery q = aqlBuilder.buildScan(handle, cols);
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars(), arangoSplit.shardIds()),
                cols,
                config.getTypeCoercion());
    }
}
