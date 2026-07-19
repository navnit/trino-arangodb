package io.arango.trino;

import io.arango.trino.handle.ArangoSplit;
import io.trino.spi.connector.*;

import java.util.List;

public class ArangoSplitManager implements ConnectorSplitManager {
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        // M1: exactly one split per table (single-server / no shard fan-out).
        return new FixedSplitSource(List.of(new ArangoSplit()));
    }
}
