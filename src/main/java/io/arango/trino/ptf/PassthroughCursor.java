package io.arango.trino.ptf;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoIterator;
import com.arangodb.entity.CursorStats;
import com.arangodb.entity.CursorWarning;
import io.trino.spi.TrinoException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Adapts the Object-typed passthrough cursor to the Map-typed one ArangoPageSource consumes (which
 * stays untouched, spec §8). A row that is not a JSON object surfaces as the same
 * INVALID_FUNCTION_ARGUMENT guidance analyze() gives (§4.1 last row / §9) — deterministically,
 * instead of as a driver deserialization failure whose shape we would have to sniff.
 */
@SuppressWarnings("rawtypes")
public final class PassthroughCursor implements ArangoCursor<Map> {
    private final ArangoCursor<Object> delegate;

    public PassthroughCursor(ArangoCursor<Object> delegate) {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Map next() {
        if (!delegate.hasNext()) {
            throw new NoSuchElementException();
        }
        Object row = delegate.next();
        if (!(row instanceof Map<?, ?> document)) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT, ArangoQueryFunction.NON_OBJECT_ROW_MESSAGE);
        }
        return document;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public ArangoIterator<Map> iterator() {
        return this;
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public Class<Map> getType() {
        return Map.class;
    }

    @Override
    public Integer getCount() {
        return delegate.getCount();
    }

    @Override
    public CursorStats getStats() {
        return delegate.getStats();
    }

    @Override
    public Collection<CursorWarning> getWarnings() {
        return delegate.getWarnings();
    }

    @Override
    public boolean isCached() {
        return delegate.isCached();
    }

    @Override
    public boolean isPotentialDirtyRead() {
        return delegate.isPotentialDirtyRead();
    }

    @Override
    public String getNextBatchId() {
        return delegate.getNextBatchId();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
