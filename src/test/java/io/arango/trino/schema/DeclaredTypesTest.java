package io.arango.trino.schema;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.arango.trino.ArangoErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

class DeclaredTypesTest {
    private static Type parse(String s) {
        return DeclaredTypes.parse(TESTING_TYPE_MANAGER, "orders", "f", s);
    }

    private static void assertRejected(String s, String messagePart) {
        assertThatThrownBy(() -> parse(s))
                .isInstanceOf(TrinoException.class)
                .hasFieldOrPropertyWithValue(
                        "errorCode", ArangoErrorCode.ARANGODB_SCHEMA_ERROR.toErrorCode())
                .hasMessageContaining("orders")
                .hasMessageContaining("f")
                .hasMessageContaining(messagePart);
    }

    @Test
    void scalarVocabulary() {
        assertThat(parse("boolean")).isEqualTo(BooleanType.BOOLEAN);
        assertThat(parse("bigint")).isEqualTo(BigintType.BIGINT);
        assertThat(parse("double")).isEqualTo(DoubleType.DOUBLE);
        assertThat(parse("varchar")).isEqualTo(VarcharType.VARCHAR);
        assertThat(parse("decimal(12,2)")).isEqualTo(DecimalType.createDecimalType(12, 2));
        assertThat(parse("timestamp(3)")).isEqualTo(TimestampType.TIMESTAMP_MILLIS);
        assertThat(parse("timestamp(3) with time zone"))
                .isEqualTo(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
    }

    @Test
    void aliasSpellingsResolveToAdmittedTypes() {
        // Admission is by resolved Type object (spec §5): bare spellings pass.
        assertThat(parse("timestamp")).isEqualTo(TimestampType.TIMESTAMP_MILLIS);
        assertThat(parse("decimal")).isEqualTo(DecimalType.createDecimalType(38, 0));
        assertThat(parse("timestamp(3) without time zone"))
                .isEqualTo(TimestampType.TIMESTAMP_MILLIS);
    }

    @Test
    void nestedForms() {
        assertThat(parse("array(timestamp(3))"))
                .isEqualTo(new ArrayType(TimestampType.TIMESTAMP_MILLIS));
        assertThat(parse("row(\"amount\" decimal(10,2))")).isInstanceOf(RowType.class);
    }

    @Test
    void rowFieldCaseCanonicalizationPinned() {
        // Spec §3: unquoted identifiers are LOWERCASED by the parser; quoted preserve case.
        // This test pins the behavior the README documents.
        RowType unquoted = (RowType) parse("row(myField varchar)");
        assertThat(unquoted.getFields().get(0).getName().orElseThrow()).isEqualTo("myfield");
        RowType quoted = (RowType) parse("row(\"myField\" varchar)");
        assertThat(quoted.getFields().get(0).getName().orElseThrow()).isEqualTo("myField");
    }

    @Test
    void rejectedFamilies() {
        assertRejected("timestamp(6)", "timestamp");
        assertRejected("timestamp(6) with time zone", "timestamp");
        assertRejected("varchar(10)", "varchar"); // bounded: isUnbounded() gate
        assertRejected("char(3)", "char(3)");
        assertRejected("date", "date");
        assertRejected("time", "time");
        assertRejected("real", "real");
        assertRejected("integer", "integer");
        assertRejected("smallint", "smallint");
        assertRejected("tinyint", "tinyint");
        assertRejected("uuid", "uuid");
        assertRejected("varbinary", "varbinary");
        assertRejected("json", "json");
        assertRejected("array(date)", "date"); // recursion into element
        assertRejected("row(\"a\" time)", "time"); // recursion into field
    }

    @Test
    void parseFailures() {
        assertRejected("decimal(", "decimal("); // syntactically invalid
        assertRejected("not_a_type", "not_a_type"); // valid syntax, unknown type
    }

    @Test
    void rowStructuralRules() {
        assertRejected("row(varchar)", "named"); // anonymous field
        assertRejected("row(a varchar, a bigint)", "duplicate"); // duplicate
        assertRejected("row(a varchar, \"A\" bigint)", "duplicate"); // case-insensitive dup
    }
}
