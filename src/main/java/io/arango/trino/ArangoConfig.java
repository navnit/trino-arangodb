package io.arango.trino;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class ArangoConfig {
    public enum MixedTypeStrategy { VARCHAR, JSON }

    private String hosts = "localhost:8529";
    private String user = "root";
    private String password = "";
    private int sampleSize = 1000;
    private boolean sampleRandom = false;
    private MixedTypeStrategy mixedTypeStrategy = MixedTypeStrategy.VARCHAR;

    @NotNull
    public String getHosts() { return hosts; }

    @Config("arangodb.hosts")
    @ConfigDescription("Comma-separated host:port coordinators")
    public ArangoConfig setHosts(String hosts) { this.hosts = hosts; return this; }

    public List<String> getHostList() {
        return ImmutableList.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(hosts));
    }

    @NotNull
    public String getUser() { return user; }

    @Config("arangodb.user")
    public ArangoConfig setUser(String user) { this.user = user; return this; }

    public String getPassword() { return password; }

    @Config("arangodb.password")
    @ConfigSecuritySensitive
    public ArangoConfig setPassword(String password) { this.password = password; return this; }

    @Min(1)
    public int getSampleSize() { return sampleSize; }

    @Config("arangodb.schema.sample-size")
    public ArangoConfig setSampleSize(int sampleSize) { this.sampleSize = sampleSize; return this; }

    public boolean isSampleRandom() { return sampleRandom; }

    @Config("arangodb.schema.sample-random")
    public ArangoConfig setSampleRandom(boolean sampleRandom) { this.sampleRandom = sampleRandom; return this; }

    @NotNull
    public MixedTypeStrategy getMixedTypeStrategy() { return mixedTypeStrategy; }

    @Config("arangodb.schema.mixed-type-strategy")
    public ArangoConfig setMixedTypeStrategy(MixedTypeStrategy mixedTypeStrategy) {
        this.mixedTypeStrategy = mixedTypeStrategy; return this;
    }
}
