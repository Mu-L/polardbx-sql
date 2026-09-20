package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class WarmupTaskTest {
    @Test
    public void test() {
        long taskId = 12345;
        String schemaName = "test";
        String sqlDef = "{select * from test.t where 1=1} {select * from test.t1 where 1=1}";
        String instId = "pxc-xxx";
        String cronExpr = "*/2 * * * *";
        String nextExecutionTime = "2024-12-30T17:26+08:00[Asia/Shanghai]";

        WarmupTask warmupTask = WarmupTask.createInstance(
            taskId, schemaName, sqlDef, instId, cronExpr, nextExecutionTime
        );

        long executionTimeInMillis = 32;
        // map: def -> result
        Map<String, ArrayResultSet> sqlAndResultSet = new HashMap<>();
        addResult(sqlAndResultSet, sqlDef);

        // inner connection: delay, def + result
        ExecutorContext executorContext =
            buildExecutorContext(executionTimeInMillis, sqlAndResultSet, schemaName);
        ExecutorContext.setContext(DEFAULT_DB_NAME, executorContext);

        warmupTask.run();

        Assert.assertTrue(warmupTask.getPacketInfo().size() == 2);
        for (Object[] info : warmupTask.getPacketInfo()) {
            System.out.println(Arrays.toString(info));
        }

        Assert.assertEquals(taskId, warmupTask.getTaskId());
        Assert.assertEquals(sqlDef, warmupTask.getSqlDef());
        Assert.assertEquals(schemaName, warmupTask.getSchemaName());
    }

    private static void addResult(Map<String, ArrayResultSet> sqlAndResultSet, String sqlDef) {
        ArrayResultSet arrayResultSet = new ArrayResultSet();
        arrayResultSet.getColumnName().add("START_TIME"); // datetime
        arrayResultSet.getColumnName().add("FINISH_TIME"); // datetime
        arrayResultSet.getColumnName().add("TIME_COST"); // bigint
        arrayResultSet.getColumnName().add("IO_MESSAGE"); // varchar

        // result row 1
        arrayResultSet.getRows().add(new Object[] {
            new OriginalTimestamp(StringTimeParser.parseDatetime("2024-12-30 17:25:41".getBytes())),
            new OriginalTimestamp(StringTimeParser.parseDatetime("2024-12-30 17:26:15".getBytes())),
            34497, "OSSTableScan: 56880438 bytes.  Total: 56880438 bytes."
        });

        // result row 2
        arrayResultSet.getRows().add(new Object[] {
            new OriginalTimestamp(StringTimeParser.parseDatetime("2024-12-30 17:26:16".getBytes())),
            new OriginalTimestamp(StringTimeParser.parseDatetime("2024-12-30 17:26:16".getBytes())),
            430, "OSSTableScan: 322 bytes.  Total: 322 bytes."
        });

        sqlAndResultSet.put("warmup " + sqlDef, arrayResultSet);
    }

    private static ExecutorContext buildExecutorContext(long executionTimeInMillis,
                                                        Map<String, ArrayResultSet> sqlAndResultSet,
                                                        String schemaName) {
        TestInnerConnection innerConnection = new TestInnerConnection(
            executionTimeInMillis, sqlAndResultSet
        );

        // map: schema name -> inner connection
        Map<String, TestInnerConnection> connectionMap = new HashMap<>();
        connectionMap.put(schemaName, innerConnection);

        // connectionManager: schemaName + connectionMap
        TestInnerConnectionManager connectionManager = new TestInnerConnectionManager(
            schemaName, connectionMap
        );

        ExecutorContext executorContext = new ExecutorContext();
        executorContext.setInnerConnectionManager(connectionManager);
        return executorContext;
    }
}
