package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class DdlEngineRecordTest {

    @Test
    public void testBuildUpdateFullParamsKeepsExpectedOrder() {
        DdlEngineRecord record = buildRecord("result");

        Map<Integer, ParameterContext> params = record.buildUpdateFullParams();

        Assert.assertEquals(18, params.size());
        assertParam(params, 1, ParameterMethod.setString, "CREATE_TABLE");
        assertParam(params, 2, ParameterMethod.setString, "test_schema");
        assertParam(params, 3, ParameterMethod.setString, "test_table");
        assertParam(params, 4, ParameterMethod.setString, "response_node");
        assertParam(params, 5, ParameterMethod.setString, "execution_node");
        assertParam(params, 6, ParameterMethod.setString, "RUNNING");
        assertParam(params, 7, ParameterMethod.setString, "test_schema.test_table");
        assertParam(params, 8, ParameterMethod.setInt, 30);
        assertParam(params, 9, ParameterMethod.setString, "trace_id");
        assertParam(params, 10, ParameterMethod.setString, "context");
        assertParam(params, 11, ParameterMethod.setString, "task_graph");
        assertParam(params, 12, ParameterMethod.setString, "result");
        assertParam(params, 13, ParameterMethod.setString, "create table test_table(id int)");
        assertParam(params, 14, ParameterMethod.setLong, 200L);
        assertParam(params, 15, ParameterMethod.setInt, 8);
        assertParam(params, 16, ParameterMethod.setInt, 3);
        assertParam(params, 17, ParameterMethod.setString, "RUNNING");
        assertParam(params, 18, ParameterMethod.setString, "ROLLBACK_RUNNING");
    }

    @Test
    public void testBuildUpdateFullParamsAllowsNullableResult() {
        DdlEngineRecord record = buildRecord(null);

        Map<Integer, ParameterContext> params = record.buildUpdateFullParams();

        Assert.assertEquals(18, params.size());
        assertParam(params, 12, ParameterMethod.setString, null);
        assertParam(params, 13, ParameterMethod.setString, "create table test_table(id int)");
    }

    private DdlEngineRecord buildRecord(String result) {
        DdlEngineRecord record = new DdlEngineRecord();
        record.ddlType = "CREATE_TABLE";
        record.schemaName = "test_schema";
        record.objectName = "test_table";
        record.responseNode = "response_node";
        record.executionNode = "execution_node";
        record.state = "RUNNING";
        record.resources = "test_schema.test_table";
        record.progress = 30;
        record.traceId = "trace_id";
        record.context = "context";
        record.taskGraph = "task_graph";
        record.result = result;
        record.ddlStmt = "create table test_table(id int)";
        record.gmtModified = 200L;
        record.maxParallelism = 8;
        record.supportedCommands = 3;
        record.pausedPolicy = "RUNNING";
        record.rollbackPausedPolicy = "ROLLBACK_RUNNING";
        return record;
    }

    private void assertParam(Map<Integer, ParameterContext> params, int index, ParameterMethod method,
                             Object value) {
        ParameterContext parameterContext = params.get(index);
        Assert.assertEquals(method, parameterContext.getParameterMethod());
        Assert.assertEquals(index, parameterContext.getArgs()[0]);
        Assert.assertEquals(value, parameterContext.getValue());
    }
}
