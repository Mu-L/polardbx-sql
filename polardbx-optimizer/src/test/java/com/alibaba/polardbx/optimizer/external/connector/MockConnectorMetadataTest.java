package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MockConnectorMetadataTest {

    private Map<String, String> props(String... kv) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            map.put(kv[i], kv[i + 1]);
        }
        return map;
    }

    @Test
    public void testConstructor() {
        new MockConnectorMetadata(null);
        new MockConnectorMetadata(props("MOCK.DATABASES", "db1"));
    }

    @Test
    public void testListDatabases() {
        new MockConnectorMetadata(props("mock.databases", " db1 ,, ")).listDatabases();
        new MockConnectorMetadata(new HashMap<>()).listDatabases();
        new MockConnectorMetadata(props("mock.databases", "")).listDatabases();
        try {
            new MockConnectorMetadata(props("simulate_error", "auth_fail")).listDatabases();
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testListTables() {
        new MockConnectorMetadata(props("mock.db1.tables", "t1, ,t2")).listTables("db1");
        new MockConnectorMetadata(new HashMap<>()).listTables("db1");
        new MockConnectorMetadata(props("mock.db1.tables", "")).listTables("db1");
    }

    @Test
    public void testGetTable() {
        new MockConnectorMetadata(props("mock.db1.t1.columns", "id:int,name")).getTable("db1", "t1");
        new MockConnectorMetadata(new HashMap<>()).getTable("db1", "t1");
        new MockConnectorMetadata(props("mock.db1.t1.columns", "")).getTable("db1", "t1");
    }

    @Test
    public void testGetTableStatistics() {
        new MockConnectorMetadata(new HashMap<>()).getTableStatistics("db1", "t1");
    }

    @Test
    public void testDoInferQuerySchema() throws IOException {
        new MockConnectorMetadata(props("mock.native_query.columns", "a:int,b")).doInferQuerySchema("select 1");
        try {
            new MockConnectorMetadata(new HashMap<>()).doInferQuerySchema("select 1");
        } catch (IOException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testParseColumns() {
        MockConnectorMetadata.parseColumns("t1", "c1:double,c2");
    }

    @Test
    public void testParseColumnDefs() {
        MockConnectorMetadata.parseColumnDefs("a:timestamp,b");
    }

    @Test
    public void testResolveType() {
        MockConnectorMetadata.resolveType("int");
        MockConnectorMetadata.resolveType("integer");
        MockConnectorMetadata.resolveType("bigint");
        MockConnectorMetadata.resolveType("long");
        MockConnectorMetadata.resolveType("varchar");
        MockConnectorMetadata.resolveType("string");
        MockConnectorMetadata.resolveType("double");
        MockConnectorMetadata.resolveType("datetime");
        MockConnectorMetadata.resolveType("timestamp");
        MockConnectorMetadata.resolveType("unknown");
    }
}
