package io.arango.trino;

import com.arangodb.ArangoCursor;
import io.arango.trino.handle.ArangoColumnHandle;
import io.arango.trino.type.ValueMaterializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.*;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ArangoPageSource implements ConnectorPageSource {
    private static final int ROWS_PER_PAGE = 1024;

    private final List<ArangoColumnHandle> columns;
    private final List<Type> types;
    private final ArangoCursor<Map> cursor;
    private final PageBuilder pageBuilder;
    private final ValueMaterializer materializer;
    private boolean finished;
    private long completedBytes;

    public ArangoPageSource(ArangoCursor<Map> cursor, List<ArangoColumnHandle> columns, ArangoConfig.TypeCoercion coercion) {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.columns = List.copyOf(columns);
        this.types = columns.stream().map(ArangoColumnHandle::type).toList();
        this.pageBuilder = new PageBuilder(types);
        this.materializer = new ValueMaterializer(requireNonNull(coercion, "coercion is null"));
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
                materializer.writeValue(out, types.get(i), row.get(col.name()), col.name());
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

    @Override public long getCompletedBytes() { return completedBytes; }
    @Override public long getReadTimeNanos() { return 0; }
    @Override public boolean isFinished() { return finished; }
    @Override public long getMemoryUsage() { return pageBuilder.getRetainedSizeInBytes(); }
    @Override public void close() { try { cursor.close(); } catch (Exception ignored) {} }
}
