package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.ddl.newengine.meta.TableMetaSerializer;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 测试ALTER TABLE在dry_run模式下的行为
 * 主要验证
 * 在dry_run模式下不设置DDL任务的暂停策略为PAUSED
 *
 * @author auto-generated
 */
@NotThreadSafe
@RunWith(Parameterized.class)
public class AlterTableDryRunTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(AlterTableDryRunTest.class);

    private String tableName = "";

    public AlterTableDryRunTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}
        });
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("dry_run_test", 4);
    }

    /**
     * 测试干跑模式下ALTER TABLE不会设置任务暂停策略
     * 这个测试验证了LogicalAlterTableHandler.java:884行的修改
     */
    @Test
    public void testDryRunModeAlterTableNotSetPaused() throws Exception {
        String testTable = tableName + "_dry_run";
        dropTableIfExists(testTable);

        // 创建测试表
        String createSql = String.format(
            "CREATE TABLE %s (id INT NOT NULL PRIMARY KEY, name VARCHAR(50), age INT) PARTITION BY HASH(id)",
            testTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // 插入测试数据
        String insertSql = String.format("INSERT INTO %s (id, name, age) VALUES (1, 'test', 25)", testTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);

        try {
            String dryRunAlterDropColumnSql = String.format(
                "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META=true)*/ ALTER TABLE %s DROP COLUMN name ",
                testTable);
            log.info("执行dry_run模式的ALTER TABLE（带故障注入延时）: " + dryRunAlterDropColumnSql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, dryRunAlterDropColumnSql);
            // 功能1: 测试dry_run模式下不设置pausePolicy为PAUSED
            // 使用FP_RANDOM_SUSPEND故障注入点进行延时，确保我们能在任务调度前检查到初始状态
            String dryRunAlterAddColumnSql = String.format(
                "/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,FP_RANDOM_SUSPEND='100,5000',ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META=true)*/ ALTER TABLE %s ADD COLUMN name VARCHAR(50)",
                testTable);

            log.info("执行dry_run模式的ALTER TABLE（带故障注入延时）: " + dryRunAlterAddColumnSql);

            // 异步执行DDL，立即检查pausePolicy
            Thread ddlThread = new Thread(() -> {
                try {
                    JdbcUtil.executeUpdateSuccess(tddlConnection, dryRunAlterAddColumnSql);
                } catch (Exception e) {
                    log.error("DDL执行出错", e);
                }
            });
            ddlThread.start();

            // 等待DDL任务写入ddl_engine表
            Thread.sleep(1000);

            // 立即检查pausePolicy字段，应该不是PAUSED
            String pausePolicy = checkDdlTaskPausePolicy(testTable);
            log.info("Dry run模式下检测到的pausePolicy: " + pausePolicy);

            // 在dry_run模式下，pausePolicy应该为null或者不是PAUSED
            assertTrue("Dry run模式下pausePolicy不应该是PAUSED",
                pausePolicy == null || !"PAUSED".equals(pausePolicy));

            // 等待DDL线程完成
            ddlThread.join();
        } finally {
            dropTableIfExists(testTable);
        }
    }

    /**
     * 检查DDL任务的pausePolicy字段
     *
     * @param tableName 表名
     * @return pausePolicy的值，如果没有找到任务则返回null
     */
    private String checkDdlTaskPausePolicy(String tableName) {
        Connection tddlConnection = getTddlConnection2();
        try {
            // 查询最新的DDL任务的pausePolicy
            String sql = "SELECT paused_policy FROM metadb.ddl_engine " +
                "WHERE object_name = '" + getSimpleTableName(tableName) + "' " +
                "ORDER BY gmt_created DESC LIMIT 1";

            try (Statement stmt = tddlConnection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    String pausePolicy = rs.getString("paused_policy");
                    log.info("找到DDL任务，表: " + tableName + ", pausePolicy: " + pausePolicy);
                    return pausePolicy;
                }
            }
        } catch (SQLException e) {
            log.warn("检查DDL任务pausePolicy时出错: " + e.getMessage());
        }
        return null;
    }

    @Test
    public void testDumpTableMeta() {
        TableMetaSerializer tableMetaSerializer = new TableMetaSerializer();
        String result = tableMetaSerializer.serialize(new ArrayList<>());
        Assert.assertEqual(result, "null");
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}