package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Map;

public class BaselineInfoRecordTest {

    @Test
    public void testBuildInsertParamsForPlanStats_WithNormalValues() {
        // Arrange
        BaselineInfoRecord record = new BaselineInfoRecord();
        record.setLastExecuteTime(1609459200L); // 2021-01-01 00:00:00 in seconds
        record.setChooseCount(10);
        record.setTablesHashCode(12345);
        record.setInstId("inst_001");
        record.setSchemaName("test_schema");
        record.setId(1);
        record.setPlanId(100);

        // Act
        Map<Integer, ParameterContext> params = record.buildInsertParamsForPlanStats();

        // Assert
        Assert.assertNotNull(params);
        Assert.assertEquals(7, params.size());

        // Verify parameter 1: lastExecuteTime (as Timestamp)
        ParameterContext param1 = params.get(1);
        Assert.assertNotNull(param1);
        Assert.assertEquals(ParameterMethod.setTimestamp1, param1.getParameterMethod());
        Assert.assertEquals(new Timestamp(1609459200L * 1000), param1.getValue());

        // Verify parameter 2: chooseCount
        ParameterContext param2 = params.get(2);
        Assert.assertNotNull(param2);
        Assert.assertEquals(ParameterMethod.setInt, param2.getParameterMethod());
        Assert.assertEquals(10, param2.getValue());

        // Verify parameter 3: tablesHashCode
        ParameterContext param3 = params.get(3);
        Assert.assertNotNull(param3);
        Assert.assertEquals(ParameterMethod.setInt, param3.getParameterMethod());
        Assert.assertEquals(12345, param3.getValue());

        // Verify parameter 4: instId
        ParameterContext param4 = params.get(4);
        Assert.assertNotNull(param4);
        Assert.assertEquals(ParameterMethod.setString, param4.getParameterMethod());
        Assert.assertEquals("inst_001", param4.getValue());

        // Verify parameter 5: schemaName
        ParameterContext param5 = params.get(5);
        Assert.assertNotNull(param5);
        Assert.assertEquals(ParameterMethod.setString, param5.getParameterMethod());
        Assert.assertEquals("test_schema", param5.getValue());

        // Verify parameter 6: id
        ParameterContext param6 = params.get(6);
        Assert.assertNotNull(param6);
        Assert.assertEquals(ParameterMethod.setInt, param6.getParameterMethod());
        Assert.assertEquals(1, param6.getValue());

        // Verify parameter 7: planId
        ParameterContext param7 = params.get(7);
        Assert.assertNotNull(param7);
        Assert.assertEquals(ParameterMethod.setInt, param7.getParameterMethod());
        Assert.assertEquals(100, param7.getValue());
    }

    @Test
    public void testBuildInsertParamsForPlanStats_WithLastExecuteTimeAsMinusOne() {
        // Arrange
        BaselineInfoRecord record = new BaselineInfoRecord();
        record.setLastExecuteTime(-1L); // Special case: should result in null timestamp
        record.setChooseCount(5);
        record.setTablesHashCode(54321);
        record.setInstId("inst_002");
        record.setSchemaName("test_schema_2");
        record.setId(2);
        record.setPlanId(200);

        // Act
        Map<Integer, ParameterContext> params = record.buildInsertParamsForPlanStats();

        // Assert
        Assert.assertNotNull(params);
        Assert.assertEquals(7, params.size());

        // Verify parameter 1: lastExecuteTime should be null when -1
        ParameterContext param1 = params.get(1);
        Assert.assertNotNull(param1);
        Assert.assertEquals(ParameterMethod.setTimestamp1, param1.getParameterMethod());
        Assert.assertNull(param1.getValue());

        // Verify parameter 2: chooseCount
        ParameterContext param2 = params.get(2);
        Assert.assertNotNull(param2);
        Assert.assertEquals(ParameterMethod.setInt, param2.getParameterMethod());
        Assert.assertEquals(5, param2.getValue());

        // Verify parameter 3: tablesHashCode
        ParameterContext param3 = params.get(3);
        Assert.assertNotNull(param3);
        Assert.assertEquals(ParameterMethod.setInt, param3.getParameterMethod());
        Assert.assertEquals(54321, param3.getValue());

        // Verify parameter 4: instId
        ParameterContext param4 = params.get(4);
        Assert.assertNotNull(param4);
        Assert.assertEquals(ParameterMethod.setString, param4.getParameterMethod());
        Assert.assertEquals("inst_002", param4.getValue());

        // Verify parameter 5: schemaName
        ParameterContext param5 = params.get(5);
        Assert.assertNotNull(param5);
        Assert.assertEquals(ParameterMethod.setString, param5.getParameterMethod());
        Assert.assertEquals("test_schema_2", param5.getValue());

        // Verify parameter 6: id
        ParameterContext param6 = params.get(6);
        Assert.assertNotNull(param6);
        Assert.assertEquals(ParameterMethod.setInt, param6.getParameterMethod());
        Assert.assertEquals(2, param6.getValue());

        // Verify parameter 7: planId
        ParameterContext param7 = params.get(7);
        Assert.assertNotNull(param7);
        Assert.assertEquals(ParameterMethod.setInt, param7.getParameterMethod());
        Assert.assertEquals(200, param7.getValue());
    }

    @Test
    public void testBuildInsertParamsForPlanStats_ParameterIndexing() {
        // Arrange
        BaselineInfoRecord record = new BaselineInfoRecord();
        record.setLastExecuteTime(0L);
        record.setChooseCount(0);
        record.setTablesHashCode(0);
        record.setInstId("");
        record.setSchemaName("");
        record.setId(0);
        record.setPlanId(0);

        // Act
        Map<Integer, ParameterContext> params = record.buildInsertParamsForPlanStats();

        // Assert - Verify all parameters have correct indexes (1-7)
        Assert.assertNotNull(params);
        Assert.assertEquals(7, params.size());
        for (int i = 1; i <= 7; i++) {
            Assert.assertTrue("Parameter index " + i + " should exist", params.containsKey(i));
        }
    }
}
