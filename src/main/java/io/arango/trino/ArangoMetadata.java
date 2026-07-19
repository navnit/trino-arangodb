package io.arango.trino;

import com.arangodb.ArangoDBException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class ArangoMetadata implements ConnectorMetadata {
    private final ArangoClient client;
    private final SchemaResolver schemaResolver;
    // M1: unbounded per-connector memoization so one SELECT samples a collection once;
    // spec §4.3 TTL cache lands in a later milestone.
    private final Map<SchemaTableName, List<ArangoColumn>> columnCache = new ConcurrentHashMap<>();

    @Inject
    public ArangoMetadata(ArangoClient client, SchemaResolver schemaResolver) {
        this.client = client;
        this.schemaResolver = schemaResolver;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return client.listDatabases();
    }

    @Override
    public ArangoTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        try {
            return client.listCollections(tableName.getSchemaName()).stream()
                    .filter(c -> !c.isSystem() && c.name().equals(tableName.getTableName()))
                    .findFirst()
                    .map(c -> new ArangoTableHandle(tableName.getSchemaName(), c.name(), c.isEdge()))
                    .orElse(null); // null => table not found (Trino throws)
        }
        catch (ArangoDBException e) {
            return null; // schema (database) does not exist => table not found
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        List<ColumnMetadata> columns = resolve(handle).stream()
                .map(c -> new ArangoColumnHandle(c.name(), c.type(), c.hidden()).toColumnMetadata())
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ImmutableMap.Builder<String, ColumnHandle> out = ImmutableMap.builder();
        for (ArangoColumn c : resolve(handle)) {
            out.put(c.name(), new ArangoColumnHandle(c.name(), c.type(), c.hidden()));
        }
        return out.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle columnHandle) {
        return ((ArangoColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        ImmutableList.Builder<SchemaTableName> out = ImmutableList.builder();
        List<String> schemas = schemaName.map(List::of).orElseGet(() -> client.listDatabases());
        for (String schema : schemas) {
            for (CollectionInfo c : client.listCollections(schema)) {
                if (!c.isSystem()) {
                    out.add(new SchemaTableName(schema, c.name()));
                }
            }
        }
        return out.build();
    }

    private List<ArangoColumn> resolve(ArangoTableHandle handle) {
        return columnCache.computeIfAbsent(handle.schemaTableName(), key ->
                schemaResolver.resolveColumns(handle.schema(),
                        new CollectionInfo(handle.table(), handle.edge(), false)));
    }
}
