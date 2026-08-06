package io.arango.trino.type;

import static io.airlift.slice.Slices.utf8Slice;
import static io.arango.trino.ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR;
import static java.util.Objects.requireNonNull;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * TypeMapper's read-side dual: TypeMapper maps runtime values to Trino types at schema inference;
 * this class maps (inferred Trino type, runtime value) to block writes at read. Coercion is
 * type-exact and handled at the exact depth a mismatch occurs (M4 spec §3): LENIENT writes NULL for
 * the offending leaf only, STRICT raises naming the column and, for nested leaves, the path. Not
 * thread-safe: one instance per ArangoPageSource (single-threaded per split).
 */
public class ValueMaterializer {
    private final ArangoConfig.TypeCoercion coercion;
    private final boolean strict;
    // Segments of the in-flight recursion ("[2]", ".b"); rendered only when STRICT raises. Only
    // maintained under STRICT -- LENIENT never reads it, so the push/pop and segment string are
    // skipped entirely on the common path (LENIENT is the default).
    private final Deque<String> path = new ArrayDeque<>();

    public ValueMaterializer(ArangoConfig.TypeCoercion coercion) {
        this.coercion = requireNonNull(coercion, "coercion is null");
        this.strict = coercion == ArangoConfig.TypeCoercion.STRICT;
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
            ((ArrayBlockBuilder) out)
                    .buildEntry(
                            elementBuilder -> {
                                int i = 0;
                                for (Object element : list) {
                                    if (strict) path.addLast("[" + i + "]");
                                    write(
                                            elementBuilder,
                                            arrayType.getElementType(),
                                            element,
                                            columnName);
                                    if (strict) path.removeLast();
                                    i++;
                                }
                            });
            return;
        }
        if (type instanceof RowType rowType && value instanceof Map<?, ?> map) {
            ((RowBlockBuilder) out)
                    .buildEntry(
                            fieldBuilders -> {
                                List<RowType.Field> fields = rowType.getFields();
                                for (int i = 0; i < fields.size(); i++) {
                                    RowType.Field field = fields.get(i);
                                    // Inference always names fields (RowType.field(name, type)) --
                                    // same
                                    // assumption TypeMapper.mergeRows makes. An absent key reads as
                                    // null in
                                    // both modes: the union schema makes absence routine (spec
                                    // decision 4).
                                    String name = field.getName().orElseThrow();
                                    if (strict) path.addLast("." + name);
                                    write(
                                            fieldBuilders.get(i),
                                            field.getType(),
                                            map.get(name),
                                            columnName);
                                    if (strict) path.removeLast();
                                }
                            });
            return;
        }
        if (type instanceof DecimalType decimalType) {
            // Declared overrides emit arbitrary DECIMAL(p,s), not just DECIMAL(38,0) -- exact
            // conversion to scale s, with a short/long write dispatch on precision (M6-C).
            BigInteger unscaled = unscaledExact(value, decimalType.getScale());
            // Overflow gate BEFORE writing (M4 review B1): an oversized value must be a
            // mismatch (lenient NULL), not an ArithmeticException/Int128 throw.
            if (unscaled != null && !Decimals.overflows(unscaled, decimalType.getPrecision())) {
                if (decimalType.isShort()) {
                    // p <= 18 => |unscaled| < 10^18 < 2^63: longValueExact cannot throw here.
                    decimalType.writeLong(out, unscaled.longValueExact());
                } else {
                    decimalType.writeObject(out, Int128.valueOf(unscaled));
                }
                return;
            }
        }
        mismatch(out, type, value, columnName);
    }

    private void mismatch(BlockBuilder out, Type type, Object value, String columnName) {
        if (coercion == ArangoConfig.TypeCoercion.STRICT) {
            throw new TrinoException(
                    ARANGODB_TYPE_CONVERSION_ERROR,
                    path.isEmpty()
                            ? "Column '%s' expected %s but a document held %s of type %s"
                                    .formatted(
                                            columnName,
                                            type,
                                            truncateForError(value),
                                            value.getClass().getSimpleName())
                            : "Column '%s': value at %s%s expected %s but a document held %s of type %s"
                                    .formatted(
                                            columnName,
                                            columnName,
                                            String.join("", path),
                                            type,
                                            truncateForError(value),
                                            value.getClass().getSimpleName()));
        }
        out.appendNull();
    }

    // Cap an offending value's rendering so a multi-megabyte stored string doesn't land verbatim in
    // the error.
    private static String truncateForError(Object value) {
        String s = String.valueOf(value);
        return s.length() <= 100 ? s : s.substring(0, 100) + "... (" + s.length() + " chars)";
    }

    // A BIGINT column accepts an integer-valued number within signed 64-bit range. 42.0 is accepted
    // (reads as 42); 42.5 is a mismatch -- truncating it would disagree with a pushed FILTER.
    private static boolean isIntegralInLongRange(Object value) {
        if (value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte) {
            return true;
        }
        if (value instanceof Double d) {
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof Float f) {
            double d = f;
            return Double.isFinite(d) && d == Math.rint(d) && d >= -0x1p63 && d < 0x1p63;
        }
        if (value instanceof BigInteger bi) {
            return bi.bitLength() < 64;
        }
        return false;
    }

    /**
     * Exact conversion to the column's scale, or null on any mismatch (M6-C spec §5.1). Doubles
     * convert via new BigDecimal(d) -- the double's EXACT binary value, never BigDecimal.valueOf:
     * valueOf's shortest round-trip repr would silently diverge from the "read exactly what's
     * stored" invariant for integral doubles >= 2^53 (M4). Consequence: a stored double matches
     * only when its exact binary value fits scale s (0.25 does at s=2; 12.34 does not) -- decimal
     * STRINGS are the intended encoding (ArangoDB has no native decimal type). setScale with no
     * rounding is both the fractional gate (s=0 keeps DECIMAL(38,0)'s integral-only rule) and the
     * exact-fit gate. This is the leaf support {@code io.arango.trino.schema.DeclaredTypes}
     * (declared-override decimal(p,s) columns, not just TypeMapper-inferred DECIMAL(38,0)) relies
     * on materializing through.
     */
    private static BigInteger unscaledExact(Object value, int scale) {
        BigDecimal dec;
        if (value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte) {
            dec = BigDecimal.valueOf(((Number) value).longValue());
        } else if (value instanceof BigInteger bi) {
            dec = new BigDecimal(bi);
        } else if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (!Double.isFinite(d)) {
                return null;
            }
            dec = new BigDecimal(d);
        } else if (value instanceof String s) {
            try {
                dec = new BigDecimal(s); // accepts 1E+2 and +x; rejects whitespace
            } catch (NumberFormatException e) {
                return null;
            }
        } else {
            return null;
        }
        try {
            return dec.setScale(scale).unscaledValue(); // no rounding: inexact fit => mismatch
        } catch (ArithmeticException e) {
            return null;
        }
    }
}
