package io.arango.trino.client;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.Protocol;
import com.arangodb.Request;
import com.arangodb.Response;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionPropertiesEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.CollectionCreateOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.arango.trino.ArangoConfig;
import io.arango.trino.split.ShardingInfo;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Map;

public class ArangoClient implements AutoCloseable {
    public record CollectionInfo(String name, boolean isEdge, boolean isSystem) {}

    private final ArangoDB arango;

    @Inject
    public ArangoClient(ArangoConfig config) {
        ArangoDB.Builder builder = new ArangoDB.Builder()
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
            out.add(new CollectionInfo(
                    e.getName(),
                    e.getType() == CollectionType.EDGES,
                    Boolean.TRUE.equals(e.getIsSystem())));
        }
        return out.build();
    }

    public List<Map<String, Object>> sampleDocuments(String database, String collection, int limit, boolean random) {
        String sort = random ? "SORT RAND() " : "";
        String aql = "FOR d IN @@col " + sort + "LIMIT @n RETURN d";
        @SuppressWarnings("unchecked")
        ArangoCursor<Map> cursor = arango.db(database).query(
                aql, Map.class, Map.of("@col", collection, "n", limit));
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
        return new ShardingInfo(p.getNumberOfShards(), p.getShardingStrategy(), p.getSmartJoinAttribute());
    }

    @SuppressWarnings("unchecked")
    public List<String> listShardIds(String database, String collection) {
        Request<Void> req = new Request.Builder<Void>()
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
        ArangoCursor<Long> cursor = arango.db(database).query(
                "FOR d IN @@col COLLECT WITH COUNT INTO n RETURN n",
                Long.class,
                Map.of("@col", collection),
                options);
        return cursor.hasNext() ? cursor.next() : 0L;
    }

    @SuppressWarnings("unchecked")
    public ArangoCursor<Map> query(String database, String aql, Map<String, Object> bindVars, List<String> shardIds) {
        if (shardIds.isEmpty()) {
            return arango.db(database).query(aql, Map.class, bindVars);
        }
        AqlQueryOptions options = new AqlQueryOptions().shardIds(shardIds.toArray(String[]::new));
        return arango.db(database).query(aql, Map.class, bindVars, options);
    }

    // ---- test-only seeding helpers (public so cross-package tests in T9 can call them) ----
    public void createDatabaseForTest(String db) { if (!arango.db(db).exists()) arango.createDatabase(db); }
    public void createDocumentCollectionForTest(String db, String name) {
        if (!arango.db(db).collection(name).exists()) arango.db(db).createCollection(name);
    }
    public void createEdgeCollectionForTest(String db, String name) {
        if (!arango.db(db).collection(name).exists()) {
            arango.db(db).createCollection(name,
                    new CollectionCreateOptions().type(CollectionType.EDGES));
        }
    }
    public void createShardedCollectionForTest(String db, String name, int numberOfShards) {
        if (!arango.db(db).collection(name).exists()) {
            arango.db(db).createCollection(name,
                    new CollectionCreateOptions().numberOfShards(numberOfShards));
        }
    }
    public void insertForTest(String db, String name, Map<String, Object> doc) {
        arango.db(db).collection(name).insertDocument(doc);
    }

    @PreDestroy
    @Override
    public void close() { arango.shutdown(); }
}
