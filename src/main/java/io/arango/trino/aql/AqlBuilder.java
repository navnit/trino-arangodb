package io.arango.trino.aql;

import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.handle.ArangoTableHandle;

import java.util.List;
import java.util.Map;

public class AqlBuilder {
    public record AqlQuery(String aql, Map<String, Object> bindVars) {}

    public AqlQuery buildScan(ArangoTableHandle table, List<ArangoColumnHandle> columns) {
        // M1: full document scan. Projection/filter pushdown added in M2.
        return new AqlQuery("FOR d IN @@col RETURN d", Map.of("@col", table.table()));
    }
}
