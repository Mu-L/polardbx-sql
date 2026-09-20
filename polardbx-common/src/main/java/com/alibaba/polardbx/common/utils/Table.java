package com.alibaba.polardbx.common.utils;

import com.google.common.base.Objects;

public class Table {

    private Pair<String, String> table;

    public Table(Pair<String, String> table) {
        this.table = table;
    }

    public static Table of(String schema, String table) {
        return new Table(Pair.of(schema, table));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table1 = (Table) o;
        return Objects.equal(table, table1.table);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table);
    }
}
