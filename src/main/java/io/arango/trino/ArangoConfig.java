package io.arango.trino;

public class ArangoConfig {
    private final String connectionString;
    private final String databaseName;

    public ArangoConfig(String connectionString, String databaseName) {
        this.connectionString = connectionString;
        this.databaseName = databaseName;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
