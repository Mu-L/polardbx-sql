package com.alibaba.polardbx.executor.mpp.planner;

import java.util.Objects;

// OrderByItem class
public class OrderByItem {
    String columnName;
    boolean asc; // true if asc, false if desc

    public OrderByItem(String columnName, boolean asc) {
        this.columnName = columnName;
        this.asc = asc;
    }

    // Override equals method
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof OrderByItem)) {
            return false;
        }
        OrderByItem other = (OrderByItem) obj;
        return Objects.equals(this.columnName, other.columnName) && this.asc == other.asc;
    }

    // Override hashCode method
    @Override
    public int hashCode() {
        return Objects.hash(columnName, asc);
    }

    @Override
    public String toString() {
        return columnName + (asc ? " ASC" : " DESC");
    }
}
