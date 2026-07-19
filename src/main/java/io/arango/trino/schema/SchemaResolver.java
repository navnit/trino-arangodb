package io.arango.trino.schema;

import com.google.common.collect.ImmutableList;
import io.arango.trino.ArangoConfig;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.client.ArangoClient.CollectionInfo;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.arango.trino.type.UnknownType.UNKNOWN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class SchemaResolver {
    public record ArangoColumn(String name, Type type, boolean hidden) {}

    // List (not Set) for deterministic hidden-column order across JVM runs
    private static final List<String> SYSTEM_ATTRS = List.of("_key", "_id", "_rev");

    private final ArangoClient client;
    private final TypeMapper typeMapper;
    private final ArangoConfig config;

    public SchemaResolver(ArangoClient client, TypeMapper typeMapper, ArangoConfig config) {
        this.client = client;
        this.typeMapper = typeMapper;
        this.config = config;
    }

    public List<ArangoColumn> resolveColumns(String database, CollectionInfo collection) {
        List<Map<String, Object>> docs = client.sampleDocuments(
                database, collection.name(), config.getSampleSize(), config.isSampleRandom());

        // union of user fields, folding types via merge
        LinkedHashMap<String, Type> userFields = new LinkedHashMap<>();
        for (Map<String, Object> doc : docs) {
            for (Map.Entry<String, Object> e : doc.entrySet()) {
                String key = e.getKey();
                if (SYSTEM_ATTRS.contains(key) || key.equals("_from") || key.equals("_to")) {
                    continue; // handled explicitly below
                }
                Type inferred = typeMapper.inferType(e.getValue());
                userFields.merge(key, inferred,
                        (a, b) -> typeMapper.merge(a, b, config.getMixedTypeStrategy()));
            }
        }

        ImmutableList.Builder<ArangoColumn> out = ImmutableList.builder();
        // any field seen only as null across the whole sample stays UNKNOWN (possibly nested
        // inside a RowType/ArrayType) -> default to VARCHAR, recursively
        userFields.forEach((name, type) ->
                out.add(new ArangoColumn(name, resolveUnknown(type), false)));
        // system attributes: hidden varchar
        for (String sys : SYSTEM_ATTRS) {
            out.add(new ArangoColumn(sys, VARCHAR, true));
        }
        // edge attributes: visible varchar
        if (collection.isEdge()) {
            out.add(new ArangoColumn("_from", VARCHAR, false));
            out.add(new ArangoColumn("_to", VARCHAR, false));
        }
        return out.build();
    }

    // Resolves leftover UNKNOWN sentinels (fields/elements that were null in every sampled
    // document) to VARCHAR, recursing into RowType fields and ArrayType elements so UNKNOWN
    // never survives buried inside a nested structure.
    private static Type resolveUnknown(Type type) {
        if (type.equals(UNKNOWN)) {
            return VARCHAR;
        }
        if (type instanceof RowType rowType) {
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            for (RowType.Field f : rowType.getFields()) {
                fields.add(RowType.field(f.getName().orElseThrow(), resolveUnknown(f.getType())));
            }
            return RowType.from(fields.build());
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(resolveUnknown(arrayType.getElementType()));
        }
        return type;
    }
}
