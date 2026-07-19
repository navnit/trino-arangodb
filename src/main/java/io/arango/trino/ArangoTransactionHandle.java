package io.arango.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum ArangoTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE
}
