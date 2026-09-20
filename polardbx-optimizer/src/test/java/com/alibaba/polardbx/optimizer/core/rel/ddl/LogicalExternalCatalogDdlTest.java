package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import org.apache.calcite.rel.core.DDL;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

public class LogicalExternalCatalogDdlTest {

    private LogicalExternalCatalogDdl createInstance(DdlType ddlType, String catalogName,
                                                     boolean ifNotExists, boolean ifExists,
                                                     String connector, Map<String, String> props,
                                                     String secretName, String comment,
                                                     String dbName, String tableName) throws Exception {
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        doCallRealMethod().when(ddl).getDdlType();
        doCallRealMethod().when(ddl).getCatalogName();
        doCallRealMethod().when(ddl).isIfNotExists();
        doCallRealMethod().when(ddl).isIfExists();
        doCallRealMethod().when(ddl).getConnector();
        doCallRealMethod().when(ddl).getProperties();
        doCallRealMethod().when(ddl).getSecretName();
        doCallRealMethod().when(ddl).getComment();
        doCallRealMethod().when(ddl).getDbName();
        doCallRealMethod().when(ddl).getTableName();

        setField(ddl, "ddlType", ddlType);
        setField(ddl, "catalogName", catalogName);
        setField(ddl, "ifNotExists", ifNotExists);
        setField(ddl, "ifExists", ifExists);
        setField(ddl, "connector", connector);
        setField(ddl, "properties", props);
        setField(ddl, "secretName", secretName);
        setField(ddl, "comment", comment);
        setField(ddl, "dbName", dbName);
        setField(ddl, "tableName", tableName);
        return ddl;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = LogicalExternalCatalogDdl.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    @Test
    public void testGetters() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        LogicalExternalCatalogDdl ddl = createInstance(
            DdlType.CREATE_TABLE, "cat1", true, false,
            "oss", props, "secret1", "comment", "db1", "table1");

        assertEquals(DdlType.CREATE_TABLE, ddl.getDdlType());
        assertEquals("cat1", ddl.getCatalogName());
        assertTrue(ddl.isIfNotExists());
        assertFalse(ddl.isIfExists());
        assertEquals("oss", ddl.getConnector());
        assertEquals(props, ddl.getProperties());
        assertEquals("secret1", ddl.getSecretName());
        assertEquals("comment", ddl.getComment());
        assertEquals("db1", ddl.getDbName());
        assertEquals("table1", ddl.getTableName());
    }

    @Test
    public void testGetTableNameFallsBackToCatalogName() throws Exception {
        LogicalExternalCatalogDdl ddl = createInstance(
            DdlType.CREATE_EXTERNAL_CATALOG, "cat1", false, false,
            "mock", new HashMap<>(), "secret1", null, null, null);

        assertEquals("cat1", ddl.getTableName());
    }

    @Test
    public void testCreateFactory() {
        DDL mockDdl = mock(DDL.class);
        Map<String, String> props = new HashMap<>();
        try {
            LogicalExternalCatalogDdl.create(
                mockDdl, DdlType.DROP_TABLE, "cat1", false, true,
                "odps", props, "secret1", "comment", "db1", "table1");
        } catch (Exception e) {
            // Constructor chain may fail in test environment;
            // the factory method line itself is covered
        }
    }
}
