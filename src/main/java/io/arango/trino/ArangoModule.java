package io.arango.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.arango.trino.aql.AqlBuilder;
import io.arango.trino.client.ArangoClient;
import io.arango.trino.schema.SchemaResolver;
import io.arango.trino.type.TypeMapper;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ArangoModule implements Module {
    @Override
    public void configure(Binder binder) {
        configBinder(binder).bindConfig(ArangoConfig.class);
        binder.bind(ArangoClient.class).in(Scopes.SINGLETON);
        binder.bind(TypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(SchemaResolver.class).in(Scopes.SINGLETON);
        binder.bind(AqlBuilder.class).in(Scopes.SINGLETON);
        binder.bind(ArangoMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArangoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ArangoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ArangoConnector.class).in(Scopes.SINGLETON);
    }
}
