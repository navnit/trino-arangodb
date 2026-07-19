package io.arango.trino.type;

import com.google.common.collect.ImmutableList;
import io.arango.trino.ArangoConfig.MixedTypeStrategy;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.arango.trino.type.UnknownType.UNKNOWN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TypeMapper {
    private static final Type BIG_DECIMAL = createDecimalType(38, 0);

    public Type inferType(Object value) {
        if (value == null) {
            return UNKNOWN; // bottom sentinel: merge(UNKNOWN, T) == T; SchemaResolver defaults leftover UNKNOWN to VARCHAR
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Integer || value instanceof Long) {
            return BIGINT;
        }
        if (value instanceof BigInteger bi) {
            return (bi.bitLength() > 63) ? BIG_DECIMAL : BIGINT;
        }
        if (value instanceof Number) {
            return DOUBLE;
        }
        if (value instanceof String) {
            return VARCHAR;
        }
        if (value instanceof List<?> list) {
            Type element = VARCHAR;
            boolean first = true;
            for (Object e : list) {
                Type et = inferType(e);
                element = first ? et : merge(element, et, MixedTypeStrategy.VARCHAR);
                first = false;
            }
            return new ArrayType(element);
        }
        if (value instanceof Map<?, ?> map) {
            ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                fields.add(RowType.field(String.valueOf(e.getKey()), inferType(e.getValue())));
            }
            return RowType.from(fields.build());
        }
        return VARCHAR;
    }

    public Type merge(Type a, Type b, MixedTypeStrategy strategy) {
        if (a.equals(UNKNOWN)) {
            return b; // bottom absorbs into the concrete type
        }
        if (b.equals(UNKNOWN)) {
            return a;
        }
        if (a.equals(b)) {
            return a;
        }
        // numeric widening: bigint + double -> double
        if (isNumeric(a) && isNumeric(b)) {
            if (a.equals(DOUBLE) || b.equals(DOUBLE)) {
                return DOUBLE;
            }
            return BIG_DECIMAL; // bigint + decimal
        }
        if (a instanceof ArrayType aa && b instanceof ArrayType bb) {
            return new ArrayType(merge(aa.getElementType(), bb.getElementType(), strategy));
        }
        if (a instanceof RowType ra && b instanceof RowType rb) {
            return mergeRows(ra, rb, strategy);
        }
        return strategy == MixedTypeStrategy.JSON ? VARCHAR : VARCHAR; // JSON type wired in M5
    }

    private Type mergeRows(RowType a, RowType b, MixedTypeStrategy strategy) {
        java.util.LinkedHashMap<String, Type> merged = new java.util.LinkedHashMap<>();
        a.getFields().forEach(f -> merged.put(f.getName().orElseThrow(), f.getType()));
        for (RowType.Field f : b.getFields()) {
            String name = f.getName().orElseThrow();
            merged.merge(name, f.getType(), (x, y) -> merge(x, y, strategy));
        }
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        merged.forEach((n, t) -> fields.add(RowType.field(n, t)));
        return RowType.from(fields.build());
    }

    private boolean isNumeric(Type t) {
        return t.equals(BIGINT) || t.equals(DOUBLE) || t.equals(BIG_DECIMAL);
    }
}
