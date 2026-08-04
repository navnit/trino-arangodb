package io.arango.trino;

import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.aql.AqlBuilder.AqlQuery;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoQueryHandle;
import io.arango.trino.handle.ArangoSplit;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.ptf.PassthroughCursor;
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
        if (table instanceof ArangoQueryHandle queryHandle) {
            // stored query verbatim — no AqlBuilder, no bind vars, no shard restriction (§5.1)
            List<ArangoColumnHandle> passthroughColumns =
                    columns.stream().map(ArangoColumnHandle.class::cast).toList();
            return new ArangoPageSource(
                    new PassthroughCursor(
                            client.queryPassthrough(queryHandle.database(), queryHandle.query())),
                    passthroughColumns,
                    config.getTypeCoercion());
        }
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ArangoSplit arangoSplit = (ArangoSplit) split;
        List<ArangoColumnHandle> cols =
                columns.stream().map(ArangoColumnHandle.class::cast).toList();
        // An aggregated handle renders COLLECT/AGGREGATE instead of a document projection; the
        // split it arrives on is always the single one ArangoSplitManager emits for it.
        AqlQuery q =
                handle.aggregation().isPresent()
                        ? aqlBuilder.buildAggregate(handle, cols)
                        : aqlBuilder.buildScan(handle, cols);
        return new ArangoPageSource(
                client.query(handle.schema(), q.aql(), q.bindVars(), arangoSplit.shardIds()),
                cols,
                config.getTypeCoercion());
    }
}
