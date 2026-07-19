package io.arango.trino;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

public class ArangoConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() { return "arangodb"; }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        Bootstrap app = new Bootstrap(new ArangoModule());
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(ArangoConnector.class);
    }
}
