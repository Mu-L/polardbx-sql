package com.alibaba.polardbx.optimizer.external.schema;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class ColumnDef {
    private final String name;
    private final DataType dataType;

    public ColumnDef(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }
}
