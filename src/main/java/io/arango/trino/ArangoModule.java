package io.arango.trino;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.ptf.ArangoQueryFunction;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;
import io.trino.spi.function.table.ConnectorTableFunction;

public class ArangoModule extends AbstractConfigurationAwareModule {
    @Override
    protected void setup(Binder binder) {
        configBinder(binder).bindConfig(ArangoConfig.class);
        binder.bind(ArangoClient.class).in(Scopes.SINGLETON);
        binder.bind(TypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(SchemaResolver.class).in(Scopes.SINGLETON);
        binder.bind(AqlBuilder.class).in(Scopes.SINGLETON);
        binder.bind(ArangoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(io.arango.trino.split.ShardFanoutCapability.class).in(Scopes.SINGLETON);
        binder.bind(ArangoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArangoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ArangoConnector.class).in(Scopes.SINGLETON);
        // The set must exist even when the flag is off: ArangoConnector injects it
        // unconditionally, and disabled-means-unregistered (spec §7) is an EMPTY set, not a
        // missing binding.
        newSetBinder(binder, ConnectorTableFunction.class);
        install(
                conditionalModule(
                        ArangoConfig.class,
                        ArangoConfig::isQueryFunctionEnabled,
                        inner ->
                                newSetBinder(inner, ConnectorTableFunction.class)
                                        .addBinding()
                                        .toProvider(ArangoQueryFunction.class)
                                        .in(Scopes.SINGLETON)));
    }
}
