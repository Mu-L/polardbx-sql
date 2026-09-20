package com.alibaba.polardbx.transaction.mock;

import java.util.ArrayList;
import java.util.List;

/**
 * Data holder for mock result set configuration.
 * Contains column names and row data for a specific SQL query result.
 */
public class MockResultSetData {
    private final String[] columnNames;
    private final List<Object[]> rows;

    /**
     * Create a new MockResultSetData with column names and rows.
     *
     * @param columnNames Array of column names
     * @param rows List of rows, where each row is an array of column values
     */
    public MockResultSetData(String[] columnNames, List<Object[]> rows) {
        this.columnNames = columnNames.clone();
        this.rows = new ArrayList<>(rows);
    }

    /**
     * Get a copy of the column names.
     *
     * @return Array of column names
     */
    public String[] getColumnNames() {
        return columnNames.clone();
    }

    /**
     * Get a copy of the rows data.
     *
     * @return List of rows
     */
    public List<Object[]> getRows() {
        return new ArrayList<>(rows);
    }
}
