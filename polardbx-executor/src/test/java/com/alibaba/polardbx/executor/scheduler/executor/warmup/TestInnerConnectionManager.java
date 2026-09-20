package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;

import java.sql.SQLException;
import java.util.Map;

public class TestInnerConnectionManager implements IInnerConnectionManager {

    private Map<String, TestInnerConnection> connectionMap;
    private String schemaName;

    public TestInnerConnectionManager(String schemaName, Map<String, TestInnerConnection> connectionMap) {
        this.connectionMap = connectionMap;
        this.schemaName = schemaName;
    }

    @Override
    public IInnerConnection getConnection() throws SQLException {
        return connectionMap.get(schemaName);
    }

    @Override
    public IInnerConnection getConnection(String schema) throws SQLException {
        return connectionMap.get(schema);
    }

}
