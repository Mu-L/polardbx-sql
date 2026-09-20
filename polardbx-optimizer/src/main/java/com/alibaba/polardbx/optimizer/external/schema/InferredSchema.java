package com.alibaba.polardbx.optimizer.external.schema;

import java.util.List;

public class InferredSchema {
    private final List<ColumnDef> columns;

    public InferredSchema(List<ColumnDef> columns) {
        this.columns = columns;
    }

    public List<ColumnDef> getColumns() {
        return columns;
    }

    public int getColumnCount() {
        return columns.size();
    }
}
