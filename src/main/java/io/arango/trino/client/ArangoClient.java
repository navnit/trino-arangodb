package io.arango.trino.client;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.Protocol;
import com.arangodb.Request;
import com.arangodb.Response;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionPropertiesEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.entity.Permissions;
import com.arangodb.entity.arangosearch.CollectionLink;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.arangosearch.ArangoSearchCreateOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.arango.trino.ArangoConfig;
import io.arango.trino.split.ShardingInfo;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ArangoClient implements AutoCloseable {
    private static final Logger log = Logger.get(ArangoClient.class);

    /**
     * ArangoDB "forbidden" error shape for a collection the user lacks a grant on, observed and
     * pinned by AqlSchemaOverrideAssumptionsTest (M6-C spec §4.5). SchemaOverrideReader keys its
     * tailored diagnostic off these — update them ONLY with a new observation.
     */
    public static final int ERROR_NUM_FORBIDDEN = 11;

    public static final int HTTP_FORBIDDEN = 403;

    public record CollectionInfo(String name, boolean isEdge, boolean isSystem) {}

    private final ArangoDB arango;

    @Inject
    public ArangoClient(ArangoConfig config) {
        ArangoDB.Builder builder =
                new ArangoDB.Builder()
                        .protocol(Protocol.HTTP2_JSON)
                        .user(config.getUser())
                        .password(config.getPassword());
        for (String hostPort : config.getHostList()) {
            HostAndPort hp = HostAndPort.fromString(hostPort).withDefaultPort(8529);
            builder.host(hp.getHost(), hp.getPort());
        }
        this.arango = builder.build();
    }

    public List<String> listDatabases() {
        return ImmutableList.copyOf(arango.getAccessibleDatabases());
    }

    public List<CollectionInfo> listCollections(String database) {
        ImmutableList.Builder<CollectionInfo> out = ImmutableList.builder();
        for (CollectionEntity e : arango.db(database).getCollections()) {
            out.add(
                    new CollectionInfo(
                            e.getName(),
                            e.getType() == CollectionType.EDGES,
                            Boolean.TRUE.equals(e.getIsSystem())));
        }
        return out.build();
    }

    public List<Map<String, Object>> sampleDocuments(
            String database, String collection, int limit, boolean random) {
        String sort = random ? "SORT RAND() " : "";
        String aql = "FOR d IN @@col " + sort + "LIMIT @n RETURN d";
        @SuppressWarnings("unchecked")
        ArangoCursor<Map> cursor =
                arango.db(database).query(aql, Map.class, Map.of("@col", collection, "n", limit));
        ImmutableList.Builder<Map<String, Object>> out = ImmutableList.builder();
        cursor.forEach(m -> out.add((Map<String, Object>) m));
        return out.build();
    }

    /** Cheap collection-metadata existence probe (no AQL) for the override collection. */
    public boolean collectionExists(String database, String collection) {
        return arango.db(database).collection(collection).exists();
    }

    /**
     * Override docs for one table from the schema-override collection. LIMIT 2, not 1: a second row
     * is how SchemaOverrideReader detects duplicate claims (spec M6-C §4.1).
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> fetchSchemaOverrideDocs(
            String database, String schemaCollection, String table) {
        ArangoCursor<Map> cursor =
                arango.db(database)
                        .query(
                                "FOR d IN @@sc FILTER d.table == @t LIMIT 2 RETURN d",
                                Map.class,
                                Map.of("@sc", schemaCollection, "t", table));
        ImmutableList.Builder<Map<String, Object>> out = ImmutableList.builder();
        cursor.forEach(m -> out.add((Map<String, Object>) m));
        return out.build();
    }

    @SuppressWarnings("unchecked")
    public ArangoCursor<Map> query(String database, String aql, Map<String, Object> bindVars) {
        return arango.db(database).query(aql, Map.class, bindVars);
    }

    public ShardingInfo getShardingInfo(String database, String collection) {
        CollectionPropertiesEntity p = arango.db(database).collection(collection).getProperties();
        return new ShardingInfo(
                p.getNumberOfShards(), p.getShardingStrategy(), p.getSmartJoinAttribute());
    }

    @SuppressWarnings("unchecked")
    public List<String> listShardIds(String database, String collection) {
        Request<Void> req =
                new Request.Builder<Void>()
                        .db(database)
                        .method(Request.Method.GET)
                        .path("/_api/collection/" + collection + "/shards")
                        .build();
        Response<Map> resp = arango.execute(req, Map.class);
        Object shards = resp.getBody().get("shards");
        if (!(shards instanceof List<?> list)) {
            return List.of();
        }
        return list.stream().map(String::valueOf).toList();
    }

    public String serverVersion() {
        return arango.getVersion().getVersion();
    }

    /** Count through the same shardIds option path a real scan uses (empty list = full count). */
    public long countWithShardIds(String database, String collection, List<String> shardIds) {
        AqlQueryOptions options = new AqlQueryOptions();
        if (!shardIds.isEmpty()) {
            options.shardIds(shardIds.toArray(String[]::new));
        }
        ArangoCursor<Long> cursor =
                arango.db(database)
                        .query(
                                "FOR d IN @@col COLLECT WITH COUNT INTO n RETURN n",
                                Long.class,
                                Map.of("@col", collection),
                                options);
        return cursor.hasNext() ? cursor.next() : 0L;
    }

    /**
     * Metadata-level count (GET /_api/collection/{name}/count) for table statistics (spec M6-A §3).
     * Deliberately not countWithShardIds: that runs an AQL COLLECT WITH COUNT for probe fidelity,
     * and the optimizer is not guaranteed to collapse it to a metadata read. The driver returns a
     * nullable boxed Long; null or negative must not reach TableStatistics, whose constructor
     * throws on a negative row count — surface it as a failure the caller degrades to unknown stats
     * instead.
     */
    public long countDocuments(String database, String collection) {
        Long count = arango.db(database).collection(collection).count().getCount();
        if (count == null || count < 0) {
            throw new IllegalStateException(
                    "ArangoDB returned no usable count for %s.%s: %s"
                            .formatted(database, collection, count));
        }
        return count;
    }

    /**
     * Raw POST /_api/explain (spec §8.1): the gate needs plan.collections[].type, which the
     * driver's non-deprecated typed API does not expose. Full response body, uninterpreted —
     * AqlReadOnlyGate owns all shape validation so it can fail closed.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> explainPlan(String database, String aql) {
        Request<Map<String, Object>> req =
                new Request.Builder<Map<String, Object>>()
                        .db(database)
                        .method(Request.Method.POST)
                        .path("/_api/explain")
                        .body(Map.of("query", aql))
                        .build();
        return (Map<String, Object>) arango.execute(req, Map.class).getBody();
    }

    /**
     * First batch of a streaming execution, at most {@code k} rows, cursor always disposed.
     * stream(true) is load-bearing (spec §4): a non-stream cursor materializes the COMPLETE result
     * server-side before serving the first batch, which is the cost this method exists to avoid.
     * Object-typed so a non-object row arrives inspectable rather than as a driver deserialization
     * failure. The result may contain nulls.
     */
    public List<Object> firstBatch(String database, String aql, int k) {
        ArangoCursor<Object> cursor =
                arango.db(database)
                        .query(aql, Object.class, new AqlQueryOptions().batchSize(k).stream(true));
        try {
            List<Object> out = new ArrayList<>();
            while (out.size() < k && cursor.hasNext()) {
                out.add(cursor.next());
            }
            return Collections.unmodifiableList(out);
        } finally {
            // A stream cursor holds a server-side query snapshot open until disposed or TTL.
            try {
                cursor.close();
            } catch (Exception e) {
                // logged, not rethrown: disposal failure must not mask the rows already read
                // (referencing e also keeps SpotBugs DE_MIGHT_IGNORE satisfied — this file is
                // not grandfathered for it)
                log.debug(e, "Failed to dispose first-batch cursor");
            }
        }
    }

    /**
     * Execution-time passthrough cursor; Object-typed for the same reason as firstBatch.
     * Deliberately NOT stream(true): the execution path consumes the whole result anyway, and this
     * matches the existing scan path's cursor behavior (§4's streaming argument is about
     * planning-time cost only).
     */
    public ArangoCursor<Object> queryPassthrough(String database, String aql) {
        return arango.db(database).query(aql, Object.class);
    }

    @SuppressWarnings("unchecked")
    public ArangoCursor<Map> query(
            String database, String aql, Map<String, Object> bindVars, List<String> shardIds) {
        if (shardIds.isEmpty()) {
            return arango.db(database).query(aql, Map.class, bindVars);
        }
        AqlQueryOptions options = new AqlQueryOptions().shardIds(shardIds.toArray(String[]::new));
        return arango.db(database).query(aql, Map.class, bindVars, options);
    }

    // ---- test-only seeding helpers (public so cross-package tests in T9 can call them) ----
    public void createDatabaseForTest(String db) {
        if (!arango.db(db).exists()) arango.createDatabase(db);
    }

    public void createDocumentCollectionForTest(String db, String name) {
        if (!arango.db(db).collection(name).exists()) arango.db(db).createCollection(name);
    }

    public void createEdgeCollectionForTest(String db, String name) {
        if (!arango.db(db).collection(name).exists()) {
            arango.db(db)
                    .createCollection(
                            name, new CollectionCreateOptions().type(CollectionType.EDGES));
        }
    }

    public void createShardedCollectionForTest(String db, String name, int numberOfShards) {
        if (!arango.db(db).collection(name).exists()) {
            arango.db(db)
                    .createCollection(
                            name, new CollectionCreateOptions().numberOfShards(numberOfShards));
        }
    }

    public void insertForTest(String db, String name, Map<String, Object> doc) {
        arango.db(db).collection(name).insertDocument(doc);
    }

    /**
     * {@code name} must be {@code ::}-namespaced (e.g. {@code "test::fn"}) or the server rejects it
     * with error 1580. Registers server-side JavaScript for test seeding only — never reachable
     * from a user query path.
     */
    public void registerAqlFunctionForTest(String db, String name, String code) {
        Request<Map<String, Object>> req =
                new Request.Builder<Map<String, Object>>()
                        .db(db)
                        .method(Request.Method.POST)
                        .path("/_api/aqlfunction")
                        .body(Map.of("name", name, "code", code, "isDeterministic", false))
                        .build();
        arango.execute(req, Map.class);
    }

    /**
     * Test-only: creates (if absent) a server user and grants it read-only access to {@code db} —
     * mirrors the deployment guidance's read-only user, for measuring whether a UDF side effect is
     * bounded by grants rather than by the transaction-registration mechanism (spec §3.2).
     */
    public void createReadOnlyUserForTest(String db, String username, String password) {
        if (arango.getUsers().stream().noneMatch(u -> u.getUser().equals(username))) {
            arango.createUser(username, password);
        }
        arango.db(db).grantAccess(username, Permissions.RO);
    }

    /**
     * Test-only: grants (or revokes) a user's access to a single collection within {@code db},
     * mirroring {@link #createReadOnlyUserForTest} but at collection granularity — the driver's
     * typed API only exposes database-level grants.
     */
    public void setCollectionAccessForTest(
            String username, String db, String collection, String grant) {
        Request<Map<String, String>> req =
                new Request.Builder<Map<String, String>>()
                        .db("_system")
                        .method(Request.Method.PUT)
                        .path("/_api/user/" + username + "/database/" + db + "/" + collection)
                        .body(Map.of("grant", grant))
                        .build();
        arango.execute(req, Map.class); // Map.class like every other raw-Request site here
    }

    public void createGraphForTest(
            String db, String graph, String edgeCollection, String vertexCollection) {
        if (!arango.db(db).graph(graph).exists()) {
            arango.db(db)
                    .createGraph(
                            graph,
                            List.of(
                                    new EdgeDefinition()
                                            .collection(edgeCollection)
                                            .from(vertexCollection)
                                            .to(vertexCollection)));
        }
    }

    /**
     * Test-only: whether a server-level ArangoDB user exists (used to measure whether a UDF body
     * can reach {@code require("@arangodb/users")}, spec §3.2 widened probe).
     */
    public boolean userExistsForTest(String username) {
        try {
            arango.getUser(username);
            return true;
        } catch (ArangoDBException e) {
            return false;
        }
    }

    public void createArangoSearchViewForTest(String db, String view, String collection) {
        if (!arango.db(db).view(view).exists()) {
            arango.db(db)
                    .createArangoSearch(
                            view,
                            new ArangoSearchCreateOptions()
                                    .link(CollectionLink.on(collection).includeAllFields(true)));
        }
    }

    @PreDestroy
    @Override
    public void close() {
        arango.shutdown();
    }
}
