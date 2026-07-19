package io.arango.trino;

import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import io.arango.trino.handle.ArangoColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.*;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class ArangoPageSource implements ConnectorPageSource {
    private static final int ROWS_PER_PAGE = 1024;

    private final List<ArangoColumnHandle> columns;
    private final List<Type> types;
    private final ArangoCursor<BaseDocument> cursor;
    private final PageBuilder pageBuilder;
    private boolean finished;
    private long completedBytes;

    public ArangoPageSource(ArangoCursor<BaseDocument> cursor, List<ArangoColumnHandle> columns) {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.columns = List.copyOf(columns);
        this.types = columns.stream().map(ArangoColumnHandle::type).toList();
        this.pageBuilder = new PageBuilder(types);
    }

    @Override
    public SourcePage getNextSourcePage() {
        int rows = 0;
        while (rows < ROWS_PER_PAGE && cursor.hasNext()) {
            BaseDocument doc = cursor.next();
            Map<String, Object> props = doc.getProperties(); // user fields
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                ArangoColumnHandle col = columns.get(i);
                BlockBuilder out = pageBuilder.getBlockBuilder(i);
                Object value = valueFor(doc, col.name(), props);
                appendValue(out, types.get(i), value);
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

    private static Object valueFor(BaseDocument doc, String name, Map<String, Object> props) {
        return switch (name) {
            case "_key" -> doc.getKey();
            case "_id" -> doc.getId();
            case "_rev" -> doc.getRevision();
            default -> props.get(name); // includes _from/_to for edges (driver exposes via attributes)
        };
    }

    // lenient coercion: mismatched/absent value -> NULL
    private static void appendValue(BlockBuilder out, Type type, Object value) {
        if (value == null) {
            out.appendNull();
            return;
        }
        try {
            if (type.equals(BooleanType.BOOLEAN) && value instanceof Boolean b) {
                BooleanType.BOOLEAN.writeBoolean(out, b);
            }
            else if (type.equals(BigintType.BIGINT) && value instanceof Number n) {
                BigintType.BIGINT.writeLong(out, n.longValue());
            }
            else if (type.equals(DoubleType.DOUBLE) && value instanceof Number n) {
                DoubleType.DOUBLE.writeDouble(out, n.doubleValue());
            }
            else if (type instanceof VarcharType) {
                type.writeSlice(out, utf8Slice(String.valueOf(value)));
            }
            else {
                // ROW/ARRAY/DECIMAL structured writing lands in M2 hardening; lenient NULL for now
                out.appendNull();
            }
        }
        catch (RuntimeException e) {
            out.appendNull(); // lenient
        }
    }

    @Override public long getCompletedBytes() { return completedBytes; }
    @Override public long getReadTimeNanos() { return 0; }
    @Override public boolean isFinished() { return finished; }
    @Override public long getMemoryUsage() { return pageBuilder.getRetainedSizeInBytes(); }
    @Override public void close() { try { cursor.close(); } catch (Exception ignored) {} }
}
