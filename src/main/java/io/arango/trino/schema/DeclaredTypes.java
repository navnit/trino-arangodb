package io.arango.trino.schema;

import static io.arango.trino.ArangoErrorCode.ARANGODB_SCHEMA_ERROR;

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
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Parses and admits declared type strings from schema-override docs (M6-C spec §5). Parsing is
 * TypeManager.fromSqlType (never a second hand-rolled grammar); admission is a recursive allowlist
 * of exactly what the M6-C spec §5 defines. ValueMaterializer's corresponding leaf-materialization
 * support for decimal(p,s) and timestamp variants lands as part of the same M6-C milestone. Alias
 * spellings that resolve to an admitted Type (bare "timestamp"/"decimal", "without time zone") are
 * admitted by design.
 */
final class DeclaredTypes {
    static final String SUPPORTED =
            "boolean, bigint, double, varchar, decimal(p,s), timestamp(3), "
                    + "timestamp(3) with time zone, array(...), row(...)";

    private DeclaredTypes() {}

    static Type parse(TypeManager typeManager, String table, String field, String typeString) {
        Type type;
        try {
            type = typeManager.fromSqlType(typeString);
        } catch (RuntimeException e) {
            // fromSqlType failures arrive as the ENGINE's Guava UncheckedExecutionException
            // wrapping trino-parser's ParsingException or SPI TypeNotFoundException; neither
            // the wrapper (engine classloader) nor ParsingException (plugin-invisible package)
            // can be caught by name from plugin code (spec §5), hence RuntimeException.
            throw error(table, field, "cannot parse type '" + typeString + "'");
        }
        validate(type, table, field);
        return type;
    }

    private static void validate(Type type, String table, String field) {
        if (type.equals(BooleanType.BOOLEAN)
                || type.equals(BigintType.BIGINT)
                || type.equals(DoubleType.DOUBLE)
                || type instanceof DecimalType) {
            return;
        }
        if (type instanceof VarcharType varchar) {
            if (varchar.isUnbounded()) {
                return; // varchar(n) must NOT slip through: the write path is unchecked
            }
            throw error(table, field, "bounded varchar is not supported, use varchar");
        }
        if (type instanceof TimestampType ts && ts.getPrecision() == 3) {
            return;
        }
        if (type instanceof TimestampWithTimeZoneType tz && tz.getPrecision() == 3) {
            return;
        }
        if (type instanceof ArrayType array) {
            validate(array.getElementType(), table, field);
            return;
        }
        if (type instanceof RowType row) {
            Set<String> seen = new HashSet<>();
            for (RowType.Field f : row.getFields()) {
                Optional<String> name = f.getName();
                if (name.isEmpty() || name.get().isEmpty()) {
                    throw error(table, field, "every row field must be named");
                }
                if (!seen.add(name.get().toLowerCase(Locale.ENGLISH))) {
                    throw error(table, field, "duplicate row field '" + name.get() + "'");
                }
                validate(f.getType(), table, field);
            }
            return;
        }
        throw error(table, field, "type '" + type.getDisplayName() + "' is not supported");
    }

    private static TrinoException error(String table, String field, String reason) {
        return new TrinoException(
                ARANGODB_SCHEMA_ERROR,
                "Schema override for table '%s', field '%s': %s. Supported types: %s"
                        .formatted(table, field, reason, SUPPORTED));
    }
}
