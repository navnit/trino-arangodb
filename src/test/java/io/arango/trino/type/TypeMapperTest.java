package io.arango.trino.type;

import io.arango.trino.ArangoConfig.MixedTypeStrategy;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TypeMapperTest {
    private final TypeMapper mapper = new TypeMapper();

    @Test
    void inferScalars() {
        assertThat(mapper.inferType(true)).isEqualTo(BOOLEAN);
        assertThat(mapper.inferType(42L)).isEqualTo(BIGINT);
        assertThat(mapper.inferType(3.14)).isEqualTo(DOUBLE);
        assertThat(mapper.inferType("hi")).isEqualTo(VARCHAR);
    }

    @Test
    void mergeIntAndFloatWidensToDouble() {
        assertThat(mapper.merge(BIGINT, DOUBLE, MixedTypeStrategy.VARCHAR)).isEqualTo(DOUBLE);
    }

    @Test
    void mergeIncompatibleFallsBackToVarchar() {
        assertThat(mapper.merge(BIGINT, VARCHAR, MixedTypeStrategy.VARCHAR)).isEqualTo(VARCHAR);
    }

    @Test
    void nullThenNumericResolvesToNumericNotVarchar() {
        // regression: a field null in early docs and numeric later must NOT degrade to VARCHAR
        Type unknown = mapper.inferType(null); // bottom sentinel (UNKNOWN)
        assertThat(mapper.merge(unknown, DOUBLE, MixedTypeStrategy.VARCHAR)).isEqualTo(DOUBLE);
        assertThat(mapper.merge(BIGINT, unknown, MixedTypeStrategy.VARCHAR)).isEqualTo(BIGINT);
    }

    @Test
    void inferArrayOfLongs() {
        assertThat(mapper.inferType(List.of(1L, 2L)).getDisplayName()).isEqualTo("array(bigint)");
    }

    @Test
    void inferNestedObjectAsRow() {
        Type t = mapper.inferType(Map.of("city", "NYC", "zip", 10001L));
        assertThat(t.getDisplayName()).contains("row(");
        assertThat(t.getDisplayName()).contains("varchar");
        assertThat(t.getDisplayName()).contains("bigint");
    }

    @Test
    void mergeArraysMergesElementTypes() {
        Type a = mapper.inferType(List.of(1L));
        Type b = mapper.inferType(List.of(1.5));
        assertThat(mapper.merge(a, b, MixedTypeStrategy.VARCHAR).getDisplayName()).isEqualTo("array(double)");
    }
}
