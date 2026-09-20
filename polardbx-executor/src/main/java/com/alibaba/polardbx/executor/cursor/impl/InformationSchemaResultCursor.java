package com.alibaba.polardbx.executor.cursor.impl;

// for information_schema alias
public class InformationSchemaResultCursor extends ArrayResultCursor {
    public InformationSchemaResultCursor(String tableName) {
        super(tableName);
    }
}
