package io.arango.trino;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

public class ArangoConnector implements Connector {
    private final LifeCycleManager lifeCycleManager;
    private final ArangoMetadata metadata;
    private final ArangoSplitManager splitManager;
    private final ArangoPageSourceProvider pageSourceProvider;

    @Inject
    public ArangoConnector(LifeCycleManager lifeCycleManager, ArangoMetadata metadata,
            ArangoSplitManager splitManager, ArangoPageSourceProvider pageSourceProvider) {
        this.lifeCycleManager = lifeCycleManager;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
            boolean readOnly, boolean autoCommit) {
        return ArangoTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() { return splitManager; }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() { return pageSourceProvider; }

    @Override
    public void shutdown() {
        // stops Airlift-managed singletons, invoking @PreDestroy on ArangoClient (closes driver threads)
        lifeCycleManager.stop();
    }
}
