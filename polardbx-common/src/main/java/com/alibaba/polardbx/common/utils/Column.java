package com.alibaba.polardbx.common.utils;

import com.google.common.base.Objects;

public class Column {

    private Pair<Pair<String, String>, String> column;

    public Column(Pair<Pair<String, String>, String> column) {
        this.column = column;
    }

    public static Column of(String schema, String table, String column) {
        return new Column(Pair.of(Pair.of(schema, table), column));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column1 = (Column) o;
        return Objects.equal(column, column1.column);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(column);
    }

    public Table getTable() {
        return new Table(column.getKey());
    }

    public String getColumnName() {
        return column.getValue();
    }

}
