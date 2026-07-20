package io.arango.trino;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.USER_ERROR;

public enum ArangoErrorCode implements ErrorCodeSupplier {
    // 0x0100_0000 is this connector's private error-code base -- it only needs to be stable and
    // clear of Trino's StandardErrorCode range. Extend this enum as more error paths are built.
    ARANGODB_TYPE_CONVERSION_ERROR(0, USER_ERROR);

    private final ErrorCode errorCode;

    ArangoErrorCode(int code, ErrorType type) {
        this.errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
