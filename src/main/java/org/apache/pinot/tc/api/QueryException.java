package org.apache.pinot.tc.api;

import com.google.common.base.MoreObjects;

public class QueryException {

    private String message;
    private Integer errorCode;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(Integer errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("message", message)
                .add("errorCode", errorCode)
                .toString();
    }
}
