package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DdlEventLogJsonTest {

    private static final String DDL_STMT = "create table t1(a int)";

    private DdlContext mockDdlContext() {
        DdlContext ddlContext = Mockito.mock(DdlContext.class);
        Mockito.when(ddlContext.getJobId()).thenReturn(12345L);
        Mockito.when(ddlContext.getSchemaName()).thenReturn("test_schema");
        Mockito.when(ddlContext.getObjectName()).thenReturn("t1");
        Mockito.when(ddlContext.getState()).thenReturn(DdlState.COMPLETED);
        Mockito.when(ddlContext.getDdlType()).thenReturn(DdlType.CREATE_TABLE);
        Mockito.when(ddlContext.getDdlJobFactoryName()).thenReturn("CreateTableJobFactory");
        Mockito.when(ddlContext.getDdlStmt()).thenReturn(DDL_STMT);
        return ddlContext;
    }

    @Test
    public void testCreateCarriesStmtLengthAndDigest() {
        DdlEventLogJson json = DdlEventLogJson.create(mockDdlContext());

        Assert.assertEquals(12345L, json.jobId);
        Assert.assertEquals("test_schema", json.schemaName);
        Assert.assertEquals("t1", json.objectName);
        Assert.assertEquals(DdlState.COMPLETED.name(), json.state);
        Assert.assertEquals(DdlType.CREATE_TABLE.name(), json.type);
        Assert.assertEquals("CreateTableJobFactory", json.jobFactoryName);
        Assert.assertEquals(DDL_STMT.length(), json.getDdlStmtLength());
        Assert.assertEquals("c7d9dd581a7800ffad42d6d1a7d8a32d", json.getDdlStmtDigest());
    }

    @Test
    public void testCreateWithTaskAndError() {
        DdlEventLogJson json = DdlEventLogJson.create(mockDdlContext(), "CreateTableTask", "some error");

        Assert.assertEquals("CreateTableTask", json.taskName);
        Assert.assertEquals("some error", json.errorMessage);
        Assert.assertEquals(DDL_STMT.length(), json.getDdlStmtLength());
        Assert.assertEquals("c7d9dd581a7800ffad42d6d1a7d8a32d", json.getDdlStmtDigest());
    }

    @Test
    public void testJsonRoundTripKeepsLengthAndDigest() {
        DdlEventLogJson original = DdlEventLogJson.create(mockDdlContext());
        String serialized = DdlEventLogJson.toJson(original);
        DdlEventLogJson parsed = DdlEventLogJson.fromJson(serialized);

        Assert.assertEquals(original.getDdlStmtLength(), parsed.getDdlStmtLength());
        Assert.assertEquals(original.getDdlStmtDigest(), parsed.getDdlStmtDigest());
        Assert.assertEquals(original.jobId, parsed.jobId);
    }

    @Test
    public void testToJsonWithNull() {
        Assert.assertEquals("", DdlEventLogJson.toJson(null));
    }
}
