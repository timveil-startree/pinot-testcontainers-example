package org.apache.pinot.tc.api;

public class SqlQuery {

    private final String sql;

    public SqlQuery(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }
}
