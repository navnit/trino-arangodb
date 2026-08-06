package io.arango.trino.schema;

import static io.arango.trino.ArangoErrorCode.ARANGODB_SCHEMA_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.arangodb.ArangoDBException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.schema.SchemaResolver.ArangoColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Reads and validates user-curated schema-override docs (M6-C spec §3/§4). Consulted by
 * SchemaResolver before sampling; a present result IS the complete user-column set. Validation is
 * strict and fail-loud: the collection is user-authored, so a typo must never silently change a
 * schema — unknown keys anywhere are rejected.
 */
public class SchemaOverrideReader {
    private static final Logger log = Logger.get(SchemaOverrideReader.class);
    private static final Set<String> DOC_KEYS = Set.of("table", "fields", "_key", "_id", "_rev");
    private static final Set<String> FIELD_KEYS = Set.of("name", "type", "hidden");
    private static final int ERROR_COLLECTION_NOT_FOUND = 1203;

    private final ArangoClient client;
    private final TypeManager typeManager;
    private final ArangoConfig config;
    // The no-override deployment must not be an exception path per table (spec §4.1):
    // probe the collection's existence once per database, cached for the schema TTL.
    private final Cache<String, Boolean> existsCache;

    @com.google.inject.Inject
    public SchemaOverrideReader(ArangoClient client, TypeManager typeManager, ArangoConfig config) {
        this.client = client;
        this.typeManager = typeManager;
        this.config = config;
        this.existsCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(config.getSchemaCacheTtl().toMillis(), MILLISECONDS)
                        .build();
    }

    public Optional<List<ArangoColumn>> read(String database, String table) {
        String overrideCollection = config.getSchemaCollection();
        boolean exists;
        try {
            exists =
                    existsCache.get(
                            database, () -> client.collectionExists(database, overrideCollection));
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw translate(e.getCause(), overrideCollection);
        }
        if (!exists) {
            log.debug("No schema-override collection '%s' in %s", overrideCollection, database);
            return Optional.empty();
        }
        List<Map<String, Object>> docs;
        try {
            docs = client.fetchSchemaOverrideDocs(database, overrideCollection, table);
        } catch (ArangoDBException e) {
            if (e.getErrorNum() != null && e.getErrorNum() == ERROR_COLLECTION_NOT_FOUND) {
                return Optional.empty(); // dropped between probe and fetch
            }
            throw translate(e, overrideCollection);
        }
        if (docs.isEmpty()) {
            return Optional.empty();
        }
        if (docs.size() > 1) {
            throw error(table, "more than one document claims this table; keep exactly one");
        }
        return Optional.of(parse(table, docs.get(0)));
    }

    private List<ArangoColumn> parse(String table, Map<String, Object> doc) {
        // 'table' itself needs no presence/type validation here: the AQL FILTER
        // d.table == @t (string bind) already guarantees it equals the requested name;
        // re-checking would be dead code (recorded decision, spec §3).
        for (String key : doc.keySet()) {
            if (!DOC_KEYS.contains(key)) {
                throw error(
                        table,
                        "unrecognized key '"
                                + key
                                + "'"
                                + ("path".equals(key) ? " ('path' is not yet supported)" : ""));
            }
        }
        if (!(doc.get("fields") instanceof List<?> fields) || fields.isEmpty()) {
            throw error(table, "'fields' must be a non-empty array");
        }
        ImmutableList.Builder<ArangoColumn> out = ImmutableList.builder();
        Set<String> seen = new HashSet<>();
        for (Object raw : fields) {
            if (!(raw instanceof Map<?, ?> field)) {
                throw error(table, "every field must be an object");
            }
            for (Object key : field.keySet()) {
                if (!FIELD_KEYS.contains(String.valueOf(key))) {
                    throw error(
                            table,
                            "unrecognized field key '"
                                    + key
                                    + "'"
                                    + ("path".equals(key) ? " ('path' is not yet supported)" : ""));
                }
            }
            if (!(field.get("name") instanceof String name) || name.isEmpty()) {
                throw error(table, "every field needs a non-empty string 'name'");
            }
            if (name.startsWith("_")) {
                throw error(
                        table,
                        "field '"
                                + name
                                + "': the '_' namespace is reserved"
                                + " (system attributes are added automatically)");
            }
            if (!seen.add(name.toLowerCase(Locale.ENGLISH))) {
                // Trino resolves column identifiers case-insensitively; 'Total' and 'total'
                // would be indistinguishable at query time.
                throw error(
                        table,
                        "duplicate field name '"
                                + name
                                + "'"
                                + " (names are compared case-insensitively)");
            }
            if (!(field.get("type") instanceof String typeString)) {
                throw error(table, "field '" + name + "' needs a string 'type'");
            }
            Type type = DeclaredTypes.parse(typeManager, table, name, typeString);
            Object hidden = field.get("hidden");
            if (hidden != null && !(hidden instanceof Boolean)) {
                throw error(table, "field '" + name + "': 'hidden' must be a boolean");
            }
            out.add(new ArangoColumn(name, type, Boolean.TRUE.equals(hidden)));
        }
        return out.build();
    }

    private TrinoException translate(Throwable cause, String overrideCollection) {
        if (cause instanceof ArangoDBException e && isForbidden(e)) {
            return new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    "Cannot read schema-override collection '"
                            + overrideCollection
                            + "' (arangodb.schema-collection): grant read on it, or drop it: "
                            + e.getMessage(),
                    cause);
        }
        return new TrinoException(
                GENERIC_INTERNAL_ERROR,
                "Failed reading schema-override collection '"
                        + overrideCollection
                        + "': "
                        + cause.getMessage(),
                cause);
    }

    private static boolean isForbidden(ArangoDBException e) {
        // Constants live on ArangoClient, pinned against a real server by
        // AqlSchemaOverrideAssumptionsTest (Task 2) — never redeclare them here.
        return (e.getErrorNum() != null && e.getErrorNum() == ArangoClient.ERROR_NUM_FORBIDDEN)
                || (e.getResponseCode() != null
                        && e.getResponseCode() == ArangoClient.HTTP_FORBIDDEN);
    }

    private TrinoException error(String table, String reason) {
        return new TrinoException(
                ARANGODB_SCHEMA_ERROR,
                "Schema override for table '%s' in '%s': %s"
                        .formatted(table, config.getSchemaCollection(), reason));
    }
}
