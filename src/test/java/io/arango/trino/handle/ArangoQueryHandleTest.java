package io.arango.trino.handle;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.TypeDeserializer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArangoQueryHandleTest {
    // Serializes a Type the way the engine does on the wire: by TypeId. The matching
    // deserializer is trino-main's TypeDeserializer over TESTING_TYPE_MANAGER.
    private static final class TestTypeSerializer extends JsonSerializer<Type> {
        @Override
        public void serialize(Type value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeString(value.getTypeId().getId());
        }
    }

    static JsonCodecFactory codecFactory() {
        // JsonMapperProvider, not ObjectMapperProvider: airlift-439's JsonCodecFactory
        // constructors take JsonMapper / Provider<JsonMapper> only (verified via javap)
        JsonMapperProvider provider = new JsonMapperProvider();
        provider.setJsonSerializers(Map.of(Type.class, new TestTypeSerializer()));
        provider.setJsonDeserializers(
                Map.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        return new JsonCodecFactory(provider);
    }

    private static final JsonCodec<ArangoQueryHandle> CODEC =
            codecFactory().jsonCodec(ArangoQueryHandle.class);

    static ArangoQueryHandle sample() {
        return new ArangoQueryHandle(
                "shop",
                "WITH users FOR v IN 1..1 OUTBOUND \"users/a\" follows RETURN {name: v.name}",
                List.of(
                        new ArangoColumnHandle("name", VarcharType.VARCHAR, false, List.of("name")),
                        new ArangoColumnHandle("age", BigintType.BIGINT, false, List.of("age")),
                        new ArangoColumnHandle(
                                "address",
                                RowType.rowType(
                                        RowType.field("tags", new ArrayType(VarcharType.VARCHAR))),
                                false,
                                List.of("address"))));
    }

    @Test
    void roundTripsThroughJson() {
        ArangoQueryHandle handle = sample();
        assertThat(CODEC.fromJson(CODEC.toJson(handle))).isEqualTo(handle);
    }

    @Test
    void schemaTableNameSynthesizesQueryAsTableName() {
        assertThat(sample().schemaTableName()).isEqualTo(new SchemaTableName("shop", "query"));
    }

    @Test
    void columnsAreDefensivelyCopied() {
        assertThat(sample().columns()).isUnmodifiable();
    }
}
