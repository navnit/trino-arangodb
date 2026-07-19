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
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class ArangoMetadata implements ConnectorMetadata {
    // ArangoDB's own documented error number for "database not found" (stable across the
    // driver). Only this specific error should be translated into Trino's "not found" signal;
    // any other ArangoDBException (auth failure, network partition, etc.) is a real error and
    // must propagate as such rather than being silently misreported as "table doesn't exist".
    private static final int ERROR_DATABASE_NOT_FOUND = 1228;

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
                    .map(c -> new ArangoTableHandle(tableName.getSchemaName(), c.name(), c.isEdge(),
                            TupleDomain.all(), OptionalLong.empty()))
                    .orElse(null); // null => table not found (Trino throws)
        }
        catch (ArangoDBException e) {
            if (isDatabaseNotFound(e)) {
                return null; // schema (database) does not exist => table not found
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to look up table '%s' in ArangoDB".formatted(tableName), e);
        }
    }

    private static boolean isDatabaseNotFound(ArangoDBException e) {
        return e.getErrorNum() != null && e.getErrorNum() == ERROR_DATABASE_NOT_FOUND;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        List<ColumnMetadata> columns = resolve(handle).stream()
                .map(c -> new ArangoColumnHandle(c.name(), c.type(), c.hidden(), c.name()).toColumnMetadata())
                .collect(ImmutableList.toImmutableList());
        return new ConnectorTableMetadata(handle.schemaTableName(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
        ArangoTableHandle handle = (ArangoTableHandle) table;
        ImmutableMap.Builder<String, ColumnHandle> out = ImmutableMap.builder();
        for (ArangoColumn c : resolve(handle)) {
            out.put(c.name(), new ArangoColumnHandle(c.name(), c.type(), c.hidden(), c.name()));
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
            List<CollectionInfo> collections;
            try {
                collections = client.listCollections(schema);
            }
            catch (ArangoDBException e) {
                if (isDatabaseNotFound(e)) {
                    continue; // schema (database) does not exist => zero tables in it
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "Failed to list tables in schema '%s' in ArangoDB".formatted(schema), e);
            }
            for (CollectionInfo c : collections) {
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
