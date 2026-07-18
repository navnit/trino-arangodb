package io.arango.trino;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ArangoConfigTest {
    @Test
    void testConfigCreation() {
        ArangoConfig config = new ArangoConfig("localhost:8529", "test_db");
        assertNotNull(config);
        assertEquals("localhost:8529", config.getConnectionString());
        assertEquals("test_db", config.getDatabaseName());
    }
}
