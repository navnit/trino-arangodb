package io.arango.trino;

import com.arangodb.ArangoCursor;
import io.arango.trino.handle.ArangoColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.*;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.arango.trino.ArangoErrorCode.ARANGODB_TYPE_CONVERSION_ERROR;
import static java.util.Objects.requireNonNull;

public class ArangoPageSource implements ConnectorPageSource {
    private static final int ROWS_PER_PAGE = 1024;

    private final List<ArangoColumnHandle> columns;
    private final List<Type> types;
    private final ArangoCursor<Map> cursor;
    private final PageBuilder pageBuilder;
    private final ArangoConfig.TypeCoercion coercion;
    private boolean finished;
    private long completedBytes;

    public ArangoPageSource(ArangoCursor<Map> cursor, List<ArangoColumnHandle> columns, ArangoConfig.TypeCoercion coercion) {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.columns = List.copyOf(columns);
        this.types = columns.stream().map(ArangoColumnHandle::type).toList();
        this.pageBuilder = new PageBuilder(types);
        this.coercion = requireNonNull(coercion, "coercion is null");
    }

    @Override
    public SourcePage getNextSourcePage() {
        int rows = 0;
        while (rows < ROWS_PER_PAGE && cursor.hasNext()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> row = cursor.next();
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                ArangoColumnHandle col = columns.get(i);
                BlockBuilder out = pageBuilder.getBlockBuilder(i);
                appendValue(out, col, types.get(i), row.get(col.name()));
            }
            rows++;
        }
        if (!cursor.hasNext()) {
            finished = true;
        }
        if (rows == 0) {
            return null;
        }
        Page page = pageBuilder.build();
        completedBytes += page.getSizeInBytes();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    // Type-exact coercion (spec §4.2 / core invariant): a stored value whose runtime type does not
    // match the column's inferred Trino type is a *mismatch*, handled per this.coercion -- LENIENT
    // writes NULL, STRICT raises. No String.valueOf, no longValue() truncation: exactness is what
    // lets ArangoMetadata push equality/range filters safely, because the pushed AQL comparison and
    // this read path then admit exactly the same values.
    private void appendValue(BlockBuilder out, ArangoColumnHandle column, Type type, Object value) {
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
        // Mismatch (or an unanticipated structured type -- ArangoPageSourceProvider.checkMaterializable
        // already rejects ARRAY/ROW/DECIMAL columns before the query runs).
        if (coercion == ArangoConfig.TypeCoercion.STRICT) {
            throw new TrinoException(ARANGODB_TYPE_CONVERSION_ERROR,
                    "Column '%s' expected %s but a document held %s of type %s"
                            .formatted(column.name(), type, truncateForError(value), value.getClass().getSimpleName()));
        }
        out.appendNull();
    }

    // Cap an offending value's rendering so a multi-megabyte stored string doesn't land verbatim in the error.
    private static String truncateForError(Object value) {
        String s = String.valueOf(value);
        return s.length() <= 100 ? s : s.substring(0, 100) + "... (" + s.length() + " chars)";
    }

    // A BIGINT column accepts an integer-valued number within signed 64-bit range. 42.0 is accepted
    // (reads as 42); 42.5 is a mismatch -- truncating it (the old longValue() behavior) would
    // disagree with a pushed FILTER, the exact bug this milestone closes. Non-numbers are mismatches.
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

    @Override public long getCompletedBytes() { return completedBytes; }
    @Override public long getReadTimeNanos() { return 0; }
    @Override public boolean isFinished() { return finished; }
    @Override public long getMemoryUsage() { return pageBuilder.getRetainedSizeInBytes(); }
    @Override public void close() { try { cursor.close(); } catch (Exception ignored) {} }
}
