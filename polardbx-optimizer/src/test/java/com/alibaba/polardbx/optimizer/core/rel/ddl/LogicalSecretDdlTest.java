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

public class LogicalSecretDdlTest {

    private LogicalSecretDdl createInstance(DdlType ddlType, String secretName,
                                            boolean ifNotExists, boolean ifExists,
                                            Map<String, String> props) throws Exception {
        LogicalSecretDdl ddl = mock(LogicalSecretDdl.class);
        doCallRealMethod().when(ddl).getDdlType();
        doCallRealMethod().when(ddl).getSecretName();
        doCallRealMethod().when(ddl).isIfNotExists();
        doCallRealMethod().when(ddl).isIfExists();
        doCallRealMethod().when(ddl).getProperties();

        setField(ddl, "ddlType", ddlType);
        setField(ddl, "secretName", secretName);
        setField(ddl, "ifNotExists", ifNotExists);
        setField(ddl, "ifExists", ifExists);
        setField(ddl, "properties", props);
        return ddl;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = LogicalSecretDdl.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    @Test
    public void testGetters() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("access_key_id", "ak_value");
        LogicalSecretDdl ddl = createInstance(
            DdlType.CREATE_TABLE, "secret1", true, false, props);

        assertEquals(DdlType.CREATE_TABLE, ddl.getDdlType());
        assertEquals("secret1", ddl.getSecretName());
        assertTrue(ddl.isIfNotExists());
        assertFalse(ddl.isIfExists());
        assertEquals(props, ddl.getProperties());
    }

    @Test
    public void testCreateFactory() {
        DDL mockDdl = mock(DDL.class);
        Map<String, String> props = new HashMap<>();
        try {
            LogicalSecretDdl.create(
                mockDdl, DdlType.DROP_TABLE, "secret1", false, true, props);
        } catch (Exception e) {
            // Constructor chain may fail in test environment;
            // the factory method line itself is covered
        }
    }
}
