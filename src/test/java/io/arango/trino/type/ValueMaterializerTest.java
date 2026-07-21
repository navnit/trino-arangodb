package io.arango.trino.type;

import io.arango.trino.ArangoConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static io.arango.trino.ArangoConfig.TypeCoercion.LENIENT;
import static io.arango.trino.ArangoConfig.TypeCoercion.STRICT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValueMaterializerTest {

    // Writes one value through ValueMaterializer and returns the built single-position block.
    static Block materialize(Type type, Object value, ArangoConfig.TypeCoercion coercion) {
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        new ValueMaterializer(coercion).writeValue(builder, type, value, "col");
        return builder.build();
    }

    @Test
    void scalarParityWithM1AppendValue() {
        assertThat(BOOLEAN.getBoolean(materialize(BOOLEAN, true, LENIENT), 0)).isTrue();
        assertThat(BIGINT.getLong(materialize(BIGINT, 42L, LENIENT), 0)).isEqualTo(42L);
        assertThat(BIGINT.getLong(materialize(BIGINT, 42.0, LENIENT), 0)).isEqualTo(42L); // fraction-free double accepted
        assertThat(BIGINT.getLong(materialize(BIGINT, BigInteger.valueOf(7), LENIENT), 0)).isEqualTo(7L);
        assertThat(DOUBLE.getDouble(materialize(DOUBLE, 2.5, LENIENT), 0)).isEqualTo(2.5);
        assertThat(DOUBLE.getDouble(materialize(DOUBLE, 3L, LENIENT), 0)).isEqualTo(3.0); // any Number under DOUBLE
        assertThat(VARCHAR.getSlice(materialize(VARCHAR, "hi", LENIENT), 0).toStringUtf8()).isEqualTo("hi");
    }

    @Test
    void nullAppendsNullInBothModes() {
        assertThat(materialize(BIGINT, null, LENIENT).isNull(0)).isTrue();
        assertThat(materialize(BIGINT, null, STRICT).isNull(0)).isTrue();
    }

    @Test
    void scalarMismatchIsNullUnderLenient() {
        assertThat(materialize(BIGINT, 42.5, LENIENT).isNull(0)).isTrue();      // genuine fraction
        assertThat(materialize(VARCHAR, 42L, LENIENT).isNull(0)).isTrue();      // number under VARCHAR
        assertThat(materialize(BOOLEAN, "true", LENIENT).isNull(0)).isTrue();   // string under BOOLEAN
    }

    @Test
    void scalarMismatchRaisesUnderStrictWithM1MessageShape() {
        assertThatThrownBy(() -> materialize(VARCHAR, 42L, STRICT))
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    assertThat(e.getErrorCode().getName()).isEqualTo("ARANGODB_TYPE_CONVERSION_ERROR");
                    // top-level mismatch keeps today's message shape: no path suffix
                    assertThat(e.getMessage()).startsWith("Column 'col' expected");
                });
    }
}
