package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@NotThreadSafe
public class DdlEngineSchedulerWaitSyncTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(DdlEngineSchedulerWaitSyncTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public int smallDelay = 10;

    public DdlEngineSchedulerWaitSyncTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void enableInject() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @FP_FAILED_TABLE_SYNC = 'true'");
    }

    @After
    public void disableInject() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @FP_FAILED_TABLE_SYNC = false");
        cleanDataBase();
    }

    @Test
    @CdcIgnore(ignoreReason = "Pause Ddl Waiting")
    public void testAlterTablePauseDdlWaiting() throws SQLException, InterruptedException {
        String mytable = schemaPrefix + randomTableName("alter_table_pause_ddl_for_wait", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "/*+TDDL:cmd_extra(%s=%s)*/create table " + createOption
            + " %s(a int,b char, d int, primary key(d)) partition by hash(a) partitions 16";
        String sql =
            String.format(createTableStmt, ConnectionProperties.ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META, "true",
                mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        logger.info(" create table success!");

        sql = String.format(
            "/*+TDDL:CMD_EXTRA(%s=%d,%s=%s)*/ALTER TABLE %s ADD COLUMN c int",
            ConnectionProperties.DDL_TASK_ERROR_RETRY_WAIT_TIME,
            smallDelay,
            ConnectionProperties.FP_FAILED_TABLE_SYNC,
            "true",
            mytable);
        Long currentTimeMillis = System.currentTimeMillis();
        JdbcUtil.executeIgnoreErrors(tddlConnection, sql);
        double duration = (System.currentTimeMillis() - currentTimeMillis) / 1000.0;
        logger.info(" sleep for " + duration + " s");
        if (duration <= smallDelay * 6) {
            throw new RuntimeException(
                "sleep for " + duration + " s" + ", but expected at lease " + smallDelay * 6 + " s");
        }
    }

    @Test
    @CdcIgnore(ignoreReason = "Pause Ddl Waiting")
    public void testAlterTablePauseDdlWaitingDefault() throws SQLException, InterruptedException {
        String mytable = schemaPrefix + randomTableName("alter_table_pause_ddl_for_wait_default", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption
            + " %s(a int,b char, d int, primary key(d)) partition by hash(a) partitions 8";
        String sql = String.format(createTableStmt, mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        logger.info(" create table success!");

        sql = String.format(
            "/*+TDDL:CMD_EXTRA(%s=%d,%s=%s)*/ALTER TABLE %s ADD COLUMN c int",
            ConnectionProperties.DDL_TASK_ERROR_RETRY_WAIT_TIME,
            0,
            ConnectionProperties.FP_FAILED_TABLE_SYNC,
            "true",
            mytable);
        Long currentTimeMillis = System.currentTimeMillis();
        JdbcUtil.executeIgnoreErrors(tddlConnection, sql);
        double duration = (System.currentTimeMillis() - currentTimeMillis) / 1000.0;
        logger.info(" sleep for " + duration + " s");
        if (duration > smallDelay * 2) {
            throw new RuntimeException(
                "sleep for " + duration + " s" + ", but expected at most " + smallDelay * 2 + " s");
        }
    }
}
