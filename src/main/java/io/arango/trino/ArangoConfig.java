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
    public enum TypeCoercion { LENIENT, STRICT }

    private String hosts = "localhost:8529";
    private String user = "root";
    private String password = "";
    private int sampleSize = 1000;
    private boolean sampleRandom = false;
    private MixedTypeStrategy mixedTypeStrategy = MixedTypeStrategy.VARCHAR;
    private TypeCoercion typeCoercion = TypeCoercion.LENIENT;
    private int shardsPerSplit = 1;
    private int maxSplits = 32;
    private boolean shardParallelismEnabled = true;

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

    @NotNull
    public TypeCoercion getTypeCoercion() { return typeCoercion; }

    @Config("arangodb.type-coercion")
    @ConfigDescription("Per-cell type-mismatch policy: LENIENT reads a mismatched value as NULL, STRICT raises an error")
    public ArangoConfig setTypeCoercion(TypeCoercion typeCoercion) {
        this.typeCoercion = typeCoercion; return this;
    }

    @Min(1)
    public int getShardsPerSplit() {
        return shardsPerSplit;
    }

    @Config("arangodb.shards-per-split")
    @ConfigDescription("Target number of shards grouped into each split on cluster fan-out")
    public ArangoConfig setShardsPerSplit(int shardsPerSplit) {
        this.shardsPerSplit = shardsPerSplit;
        return this;
    }

    @Min(1)
    public int getMaxSplits() {
        return maxSplits;
    }

    @Config("arangodb.max-splits")
    @ConfigDescription("Hard cap on the number of splits per collection scan")
    public ArangoConfig setMaxSplits(int maxSplits) {
        this.maxSplits = maxSplits;
        return this;
    }

    public boolean isShardParallelismEnabled() {
        return shardParallelismEnabled;
    }

    @Config("arangodb.shard-parallelism-enabled")
    @ConfigDescription("Enable per-shard parallel splits on clusters; false forces a single split and never uses the internal shardIds API")
    public ArangoConfig setShardParallelismEnabled(boolean shardParallelismEnabled) {
        this.shardParallelismEnabled = shardParallelismEnabled;
        return this;
    }
}
