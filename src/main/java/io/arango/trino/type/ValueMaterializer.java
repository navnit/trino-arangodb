package io.arango.trino.type;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.arango.trino.ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * TypeMapper's read-side dual: TypeMapper maps runtime values to Trino types at schema inference;
 * this class maps (inferred Trino type, runtime value) to block writes at read. Coercion is
 * type-exact and handled at the exact depth a mismatch occurs (M4 spec §3): LENIENT writes NULL for
 * the offending leaf only, STRICT raises naming the column and, for nested leaves, the path.
 * Not thread-safe: one instance per ArangoPageSource (single-threaded per split).
 */
public class ValueMaterializer {
    private final ArangoConfig.TypeCoercion coercion;
    // Segments of the in-flight recursion ("[2]", ".b"); rendered only when STRICT raises.
    private final Deque<String> path = new ArrayDeque<>();

    public ValueMaterializer(ArangoConfig.TypeCoercion coercion) {
        this.coercion = requireNonNull(coercion, "coercion is null");
    }

    public void writeValue(BlockBuilder out, Type type, Object value, String columnName) {
        path.clear(); // a prior strict throw can leave stale segments behind
        write(out, type, value, columnName);
    }

    private void write(BlockBuilder out, Type type, Object value, String columnName) {
        if (value == null) {
            out.appendNull();
            return;
        }
        if (type.equals(BooleanType.BOOLEAN) && value instanceof Boolean b) {
            BooleanType.BOOLEAN.writeBoolean(out, b);
            return;
        }
        if (type.equals(BigintType.BIGINT) && isIntegralInLongRange(value)) {
            BigintType.BIGINT.writeLong(out, ((Number) value).longValue());
            return;
        }
        if (type.equals(DoubleType.DOUBLE) && value instanceof Number n) {
            DoubleType.DOUBLE.writeDouble(out, n.doubleValue());
            return;
        }
        if (type instanceof VarcharType && value instanceof String s) {
            type.writeSlice(out, utf8Slice(s));
            return;
        }
        if (type instanceof ArrayType arrayType && value instanceof List<?> list) {
            ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
                int i = 0;
                for (Object element : list) {
                    path.addLast("[" + i + "]");
                    write(elementBuilder, arrayType.getElementType(), element, columnName);
                    path.removeLast();
                    i++;
                }
            });
            return;
        }
        mismatch(out, type, value, columnName);
    }

    private void mismatch(BlockBuilder out, Type type, Object value, String columnName) {
        if (coercion == ArangoConfig.TypeCoercion.STRICT) {
            throw new TrinoException(ARANGODB_TYPE_CONVERSION_ERROR, path.isEmpty()
                    ? "Column '%s' expected %s but a document held %s of type %s"
                            .formatted(columnName, type, truncateForError(value), value.getClass().getSimpleName())
                    : "Column '%s': value at %s%s expected %s but a document held %s of type %s"
                            .formatted(columnName, columnName, String.join("", path), type,
                                    truncateForError(value), value.getClass().getSimpleName()));
        }
        out.appendNull();
    }

    // Cap an offending value's rendering so a multi-megabyte stored string doesn't land verbatim in the error.
    private static String truncateForError(Object value) {
        String s = String.valueOf(value);
        return s.length() <= 100 ? s : s.substring(0, 100) + "... (" + s.length() + " chars)";
    }

    // A BIGINT column accepts an integer-valued number within signed 64-bit range. 42.0 is accepted
    // (reads as 42); 42.5 is a mismatch -- truncating it would disagree with a pushed FILTER.
    private static boolean isIntegralInLongRange(Object value) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return true;
        }
        if (value instanceof Double d) {
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof Float f) {
            double d = f;
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof java.math.BigInteger bi) {
            return bi.bitLength() < 64;
        }
        return false;
    }
}
