package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.scheduler.executor.OptimizerAlertScheduledJob;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

@FileStoreIgnore
public class OptimizerAlertScheduleJobTest extends ReadBaseTestCase {
    private static final Log log = LogFactory.getLog(JdbcUtil.class);

    protected static final String FIND_SCHEDULE =
        String.format("select schedule_id from metadb.SCHEDULED_JOBS where executor_type='%s'",
            ScheduledJobExecutorType.OPTIMIZER_ALERT.name());

    protected static final String FIND_STATISTIC_SCHEDULES =
        String.format("select schedule_id from metadb.SCHEDULED_JOBS where executor_type in ('%s','%s','%s')",
            ScheduledJobExecutorType.STATISTIC_SAMPLE_SKETCH.name(),
            ScheduledJobExecutorType.STATISTIC_HLL_SKETCH.name(),
            ScheduledJobExecutorType.STATISTIC_INFO_SCHEMA_TABLES.name());

    protected static final String ACTIVE_SCHEDULE_JOBS_COUNT =
        "select count(1) from metadb." + GmsSystemTables.FIRED_SCHEDULED_JOBS
            + " where schedule_id=%s and fire_time <= UNIX_TIMESTAMP() and state in ('QUEUED','RUNNING')";

    protected static final String OPTIMIZER_ALERT =
        "select COMPUTE_NODE, ALERT_COUNT from information_schema.optimizer_alert where ALERT_TYPE='%s'";

    protected static final String SCHEDULE_REMARK =
        String.format("select LASTJOB_REMARK from information_schema.schedule_jobs where JOB_TYPE='%s'",
            ScheduledJobExecutorType.OPTIMIZER_ALERT.name());
    protected static final String FIRE_SCHEDULE = "fire schedule %s";

    protected static final String JOBS_COUNT =
        "select count(1) from metadb." + GmsSystemTables.FIRED_SCHEDULED_JOBS +
            " WHERE fire_time <= UNIX_TIMESTAMP() AND schedule_id = %s";
    protected static ErrorCode ERR_FROM_SCHEDULE = ErrorCode.ERR_VIEW;

    protected static final String JOB_STATE_CHECK =
        "select state from metadb.fired_scheduled_jobs where schedule_id=%s and from_unixtime(fire_time)>='%s' order by fire_time limit 1";

    protected static final String ENABLE_DEFAULT_CHECK = "set global " +
        ConnectionProperties.ENABLE_ALERT_TEST_DEFAULT + "= true";

    protected static final String DISABLE_DEFAULT_CHECK = "set global " +
        ConnectionProperties.ENABLE_ALERT_TEST_DEFAULT + "= false";

    protected static final String ENABLE_BKA_ALERT = "set global " +
        ConnectionProperties.ENABLE_OPTIMIZER_ALERT_BKA + "= true";

    protected static final String DISABLE_BKA_ALERT = "set global " +
        ConnectionProperties.ENABLE_OPTIMIZER_ALERT_BKA + "= false";

    protected static final String DISABLE_TP_SLOW_ALERT_THRESHOLD = "set global ENABLE_TP_SLOW_ALERT_THRESHOLD = 0";

    protected static final String ENABLE_TP_SLOW_ALERT_THRESHOLD = "set global ENABLE_TP_SLOW_ALERT_THRESHOLD = 10";

    public OptimizerAlertScheduleJobTest() {
    }

    @After
    public void clearFailPoint() throws Exception {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s=true", FailPoint.FP_CLEAR));
            statement.execute("set global alert_statistic_interrupt=false");
            statement.execute("set global alert_statistic_inconsistent=false");
        }
    }

    @Test
    public void testGSITooMuch() throws SQLException {
        String tableName = "gsi_too_much_alter";
        String gsiName = "gsi_a";
        try {
            try (Connection conn = getPolardbxConnection()) {
                JdbcUtil.dropTable(conn, tableName);
                JdbcUtil.executeSuccess(conn,
                    String.format(
                        "create table %s(pk int not null, a int not null,b int not null,primary key(pk),global index %s(a) dbpartition by hash(a)) dbpartition by hash(pk)",
                        tableName, gsiName));
                JdbcUtil.executeSuccess(conn,
                    String.format("insert into %s(pk,a,b) values(1,2,3)", tableName));
            }
            testShouldAlert(OptimizerAlertType.GSI_TOO_MUCH, () -> {
                try (Connection conn = getPolardbxConnection()) {
                    String sql =
                        String.format(
                            "/*+TDDL:cmd_extra(ENABLE_ALERT_TEST=true)*/ select * from %s force index(%s) where a = 2"
                            , tableName, gsiName);
                    JdbcUtil.executeSuccess(conn, sql);
                    return true;
                }
            });
        } finally {
            try (Connection conn = getPolardbxConnection()) {
                JdbcUtil.dropTable(conn, tableName);
            }
        }
    }

    @Test
    public void testBKATooMuch() {
        testShouldAlert(OptimizerAlertType.BKA_TOO_MUCH, () -> {
            try (Connection conn = getPolardbxConnection()) {
                String table = "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
                String sql =
                    String.format("/*+TDDL:cmd_extra(ENABLE_ALERT_TEST=true) BKA_JOIN(%s, %s)*/ "
                            + "select * from %s A, %S B where A.integer_test = B.integer_test"
                        , table, table, table, table);
                JdbcUtil.executeSuccess(conn, sql);
                return true;
            }
        });
    }

    @Test
    public void testTpSlow() {
        testShouldAlert(OptimizerAlertType.TP_SLOW, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                String sql =
                    String.format(
                        "/*+TDDL:cmd_extra(ENABLE_ALERT_TEST=true enable_mpp=false WORKLOAD_TYPE=TP)*/"
                            + "select * from select_base_four_%s where integer_test =10",
                        ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
                statement.execute(sql);
                return true;
            }
        });
    }

    @Test
    public void testTpSlowNotAlert() {
        testShouldNotAlert(OptimizerAlertType.TP_SLOW, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                // test direct plan
                String sql = "set ENABLE_ALERT_TEST=true";
                statement.execute(sql);
                sql = "set WORKLOAD_TYPE='TP'";
                statement.execute(sql);
                sql = String.format("select * from select_base_four_%s where pk = 1;",
                    ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
                statement.execute(sql);
                sql = String.format("select * from select_base_four_%s where pk = 1;",
                    ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
                // test node hint
                statement.execute(sql);
                sql = String.format("/*+TDDL:node(0)*/ select * from select_base_four_%s;",
                    ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX);
                statement.execute(sql);
                return true;
            }
        });
    }

    @Test
    public void testStatisticJobInterrupt() throws SQLException, InterruptedException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("set global alert_statistic_interrupt=true");
            statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
            statement.execute("analyze table select_base_four_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX);
        }
        // wait 5 sec
        Thread.sleep(5 * 1000L);

        testShouldAlert(OptimizerAlertType.STATISTIC_JOB_INTERRUPT, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

        testShouldAlert(OptimizerAlertType.STATISTIC_JOB_INTERRUPT, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {

                statement.execute("set global alert_statistic_interrupt=true");
                statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
                // wait 5 sec
                Thread.sleep(5 * 1000L);
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_hll_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("set global alert_statistic_interrupt=false");
        }
        Thread.sleep(2000);
    }

    @Test
    public void testScheduleJobFailedAlert() throws Exception {
        List<Long> statisticScheduleIds = findStatisticScheduleIds(false);
        assertWithMessage("all statistic schedules should exist").that(statisticScheduleIds).hasSize(3);
        List<Long> enabledStatisticScheduleIds = findStatisticScheduleIds(true);
        try {
            // alert_statistic_interrupt is global. Pause automatic triggers so that only the explicit FIRE SCHEDULE
            // statements below can generate statistic alerts. Manual firing of these statistic jobs still works while
            // their schedules are disabled.
            changeStatisticScheduleState(enabledStatisticScheduleIds, "pause");
            waitStatisticScheduleJobsIdle(statisticScheduleIds);

            try (Connection connection = getPolardbxConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("set global alert_statistic_interrupt=true");
                statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
            }
            // wait 5 sec
            Thread.sleep(5 * 1000L);

            testShouldAlert(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_SAMPLE_FAIL, () -> {
                try (Connection conn = getPolardbxConnection();
                    Statement statement = conn.createStatement()) {
                    ResultSet rs = statement.executeQuery(
                        "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                    rs.next();
                    String scheduleId = rs.getString("schedule_id");
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    statement.execute("fire schedule " + scheduleId);
                    return true;
                }
            });

            testShouldAlert(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_HLL_FAIL, () -> {
                try (Connection conn = getPolardbxConnection();
                    Statement statement = conn.createStatement()) {

                    statement.execute("set global alert_statistic_interrupt=true");
                    statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
                    // wait 5 sec
                    Thread.sleep(5 * 1000L);
                    ResultSet rs = statement.executeQuery(
                        "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_hll_sketch'");
                    rs.next();
                    String scheduleId = rs.getString("schedule_id");
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    statement.execute("fire schedule " + scheduleId);
                    return true;
                }
            });

            testShouldAlert(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_INFORMATION_TABLES_FAIL, () -> {
                try (Connection conn = getPolardbxConnection();
                    Statement statement = conn.createStatement()) {

                    statement.execute("set global alert_statistic_interrupt=true");
                    statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='true'");
                    // wait 5 sec
                    Thread.sleep(5 * 1000L);
                    ResultSet rs = statement.executeQuery(
                        "select schedule_id from metadb.scheduled_jobs "
                            + "where executor_type='STATISTIC_INFO_SCHEMA_TABLES'");
                    rs.next();
                    String scheduleId = rs.getString("schedule_id");
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    statement.execute("fire schedule " + scheduleId);
                    return true;
                }
            });
        } finally {
            try {
                try (Connection connection = getPolardbxConnection()) {
                    connection.createStatement().execute("set global alert_statistic_interrupt=false");
                }
            } finally {
                changeStatisticScheduleState(enabledStatisticScheduleIds, "continue");
            }
        }
        Thread.sleep(2000);
    }

    private List<Long> findStatisticScheduleIds(boolean enabledOnly) throws SQLException {
        List<Long> scheduleIds = new ArrayList<>();
        String sql = FIND_STATISTIC_SCHEDULES + (enabledOnly ? " and status='ENABLED'" : "");
        try (Connection connection = getPolardbxConnection();
            ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            while (rs.next()) {
                scheduleIds.add(rs.getLong("schedule_id"));
            }
        }
        return scheduleIds;
    }

    private void changeStatisticScheduleState(List<Long> scheduleIds, String operation) throws SQLException {
        try (Connection connection = getPolardbxConnection();
            Statement statement = connection.createStatement()) {
            for (Long scheduleId : scheduleIds) {
                statement.execute(operation + " schedule " + scheduleId);
            }
        }
    }

    private void waitStatisticScheduleJobsIdle(List<Long> scheduleIds) throws SQLException, InterruptedException {
        // PAUSE prevents new automatic triggers, but a job that is already queued or running must finish before the
        // global interrupt injection is enabled. Ignore future QUEUED trigger records that are materialized in advance.
        try (Connection connection = getPolardbxConnection()) {
            for (Long scheduleId : scheduleIds) {
                for (int waitCount = 0; waitCount < 20; waitCount++) {
                    try (ResultSet rs = JdbcUtil.executeQuery(
                        String.format(ACTIVE_SCHEDULE_JOBS_COUNT, scheduleId), connection)) {
                        Assert.assertTrue(rs.next());
                        if (rs.getLong(1) == 0) {
                            break;
                        }
                    }
                    if (waitCount == 19) {
                        throw new RuntimeException("statistic schedule job is still active: " + scheduleId);
                    }
                    Thread.sleep(5 * 1000L);
                }
            }
        }
    }

    @Test
    public void testHllSubProcessStatisticAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_HLL_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_HLL_TASK_EXCEPTION));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_hll_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });
    }

    @Test
    public void testRowCountSubProcessStatisticAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_COLLECT_ROWCOUNT_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(
                    String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_ROWCOUNT_TASK_EXCEPTION));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

    }

    @Test
    public void testSampleSubProcessStatisticAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_SAMPLE_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_SAMPLE_TASK_EXCEPTION));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

    }

    @Test
    public void testPersistSubProcessStatisticAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_PERSIST_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(
                    String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

    }

    @Test
    public void testSyncSubProcessStatisticAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_SYNC_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_SYNC_TASK_EXCEPTION));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });

    }

    @Test
    public void testPersistTableStatisticFailedAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_PERSIST_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(
                    String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_PERSIST_TABLE_STATISTIC));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });
    }

    @Test
    public void testPersistColumnStatisticFailedAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_PERSIST_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(
                    String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_PERSIST_COLUMN_STATISTIC));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });
    }

    @Test
    public void testPersistNDVStatisticFailedAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_HLL_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_PERSIST_NDV_STATISTIC));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_hll_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });
    }

    /**
     * 列存HLL收集失败之后，会进行行存hll收集
     * 通过STATISTIC_HLL_FAIL检验列存hll超时
     */
    @Test
    public void testHllOnColumnarTimeout() throws Exception {
        String tddlDatabase1 = PropertiesUtil.polardbXDBName1(true);
        String tableName = "test_hll_on_columnar_timeout_tb";
        String TABLE_DEFINITION_FORMAT =
            "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS='WaitColumnarTableCreationTask')*/CREATE TABLE `%s` (\n" +
                "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "\t`a` int(32) UNSIGNED DEFAULT NULL,\n"
                + "\t`b` int(32) UNSIGNED DEFAULT NULL,\n"
                + "\t`c` int(32) UNSIGNED DEFAULT NULL,\n"
                + "\tprimary key(id),\n"
                + "\tkey idx_a(a),\n"
                + "\tkey idx_b(b),\n"
                + "\tkey idx_c(c),\n"
                + "\tclustered columnar index `cci_a`(`a`) partition by hash(`a`) partitions 4\n"
                + ") partition by key(id) partitions 8";
        try (Connection connection = getPolardbxConnection(tddlDatabase1)) {
            JdbcUtil.dropTable(connection, tableName);
            JdbcUtil.executeSuccess(connection, String.format(TABLE_DEFINITION_FORMAT, tableName));
            JdbcUtil.executeSuccess(connection,
                String.format("insert into %s(a,b,c) values(1,2,3),(4,5,6)", tableName));
        }

        try {
            JdbcUtil.executeSuccess(tddlConnection,
                String.format("set @%s='true'", FailPointKey.FP_INJECT_STATISTIC_HLL_ON_COLUMNAR_EXCEPTION));

            OptimizerAlertScheduleJobTest.testShouldAlert(OptimizerAlertType.STATISTIC_HLL_FAIL, () -> {
                try (Connection conn = getPolardbxConnection(tddlDatabase1)) {
                    JdbcUtil.executeSuccess(conn,
                        "set global " + ConnectionProperties.STATISTIC_NDV_SKETCH_EXPIRE_TIME + "=1");
                    Thread.sleep(3000L);
                    JdbcUtil.executeSuccess(conn,
                        String.format("/*+TDDL:enable_collect_hll=true*/collect statistic %s.%s", tddlDatabase1,
                            tableName));
                }
                return true;
            });
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, String.format("set @%s=true", FailPoint.FP_CLEAR));
            JdbcUtil.executeSuccess(tddlConnection, "set global "
                + ConnectionProperties.STATISTIC_NDV_SKETCH_EXPIRE_TIME + "="
                + ConnectionParams.STATISTIC_NDV_SKETCH_EXPIRE_TIME.getDefault());
        }
    }

    @Test
    public void testStatisticCollectFromDnFailedAlert() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL));
            statement.execute(
                String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB));
        }

        testShouldAlert(OptimizerAlertType.STATISTIC_COLLECT_CARDINALITY_FROM_DN_FAIL, () -> {
            try (Connection conn = getPolardbxConnection();
                Statement statement = conn.createStatement()) {
                statement.execute(
                    String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_COLLECT_FROM_DN_EXCEPTION));
                ResultSet rs = statement.executeQuery(
                    "select schedule_id from metadb.scheduled_jobs where executor_type='statistic_sample_sketch'");
                rs.next();
                String scheduleId = rs.getString("schedule_id");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                statement.execute("fire schedule " + scheduleId);
                return true;
            }
        });
    }

    public static void testShouldAlert(OptimizerAlertType targetAlert, Callable<?> func) {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(conn, DISABLE_DEFAULT_CHECK);
            JdbcUtil.executeSuccess(conn, DISABLE_TP_SLOW_ALERT_THRESHOLD);
            JdbcUtil.executeSuccess(conn, ENABLE_BKA_ALERT);
            Thread.sleep(5000L);
            for (int cnt = 3; cnt >= 0; cnt--) {
                try {
                    // test single may throw ERR_FROM_SCHEDULE when FIRE_SCHEDULE clashes with auto schedule,
                    // retry this three times to avoid the case
                    testSingle(targetAlert, func, true);
                    return;
                } catch (TddlRuntimeException e) {
                    if (e.getErrorCodeType() != ERR_FROM_SCHEDULE || cnt == 0) {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try (Connection conn = getPolardbxConnection0()) {
                JdbcUtil.executeSuccess(conn, ENABLE_DEFAULT_CHECK);
                JdbcUtil.executeSuccess(conn, ENABLE_TP_SLOW_ALERT_THRESHOLD);
                JdbcUtil.executeSuccess(conn, DISABLE_BKA_ALERT);
                Thread.sleep(2000L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void testShouldNotAlert(OptimizerAlertType targetAlert, Callable<?> func) {
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, DISABLE_DEFAULT_CHECK);
            JdbcUtil.executeSuccess(conn, DISABLE_TP_SLOW_ALERT_THRESHOLD);
            JdbcUtil.executeSuccess(conn, ENABLE_BKA_ALERT);
            // wait for 5 seconds to make it works
            Thread.sleep(5000L);
            testSingle(targetAlert, func, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try (Connection conn = getPolardbxConnection()) {
                JdbcUtil.executeSuccess(conn, ENABLE_DEFAULT_CHECK);
                JdbcUtil.executeSuccess(conn, ENABLE_TP_SLOW_ALERT_THRESHOLD);
                JdbcUtil.executeSuccess(conn, DISABLE_BKA_ALERT);
                Thread.sleep(2000L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    protected static void testSingle(OptimizerAlertType targetAlert, Callable<?> func, boolean shouldAlert) {
        // find schedule id
        long schedule_id;
        try (Connection conn = getPolardbxConnection0();
            ResultSet rs = JdbcUtil.executeQuery(FIND_SCHEDULE, conn)) {
            if (rs.next()) {
                schedule_id = rs.getLong("schedule_id");
            } else {
                throw new RuntimeException("OPTIMIZER_ALERT schedule job not found!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // clear schedule count
        String result;
        fireSchedule(schedule_id);
        long beforeCount = alertCount(targetAlert);
        // add alert
        try {
            func.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.error(String.format("start to check alert, before count = %d", beforeCount));
        if (shouldAlert) {
            // schedule job did alert
            result = fireSchedule(schedule_id);
            long afterCount = alertCount(targetAlert);
            if (OptimizerAlertType.isStatisticAlertType(targetAlert)) {
                assertThat(result).contains(OptimizerAlertScheduledJob.HAS_STATISTIC_ALERT);
            } else {
                assertThat(result).contains(OptimizerAlertScheduledJob.HAS_OPTIMIZER_ALERT);
            }
            assertThat(result).contains(targetAlert.name());
            assertWithMessage(String.format("more alerts should be recorded")).that(afterCount)
                .isGreaterThan(beforeCount);
            // no alert happened
            result = fireSchedule(schedule_id);
            long finalCount = alertCount(targetAlert);
            assertThat(result).contains(OptimizerAlertScheduledJob.NO_ALERT);
            assertWithMessage(String.format("no more alert should be recorded")).that(finalCount).isEqualTo(afterCount);
        } else {
            // schedule job didn't alert
            result = fireSchedule(schedule_id);
            if (result.contains(OptimizerAlertScheduledJob.HAS_OPTIMIZER_ALERT)
                || result.contains(OptimizerAlertScheduledJob.HAS_STATISTIC_ALERT)) {
                assertThat(result).doesNotContain(targetAlert.name());
            }

            // no alert happened
            result = fireSchedule(schedule_id);
            long finalCount = alertCount(targetAlert);
            assertThat(result).contains(OptimizerAlertScheduledJob.NO_ALERT);
            assertWithMessage(String.format("no more alert should be recorded")).that(finalCount)
                .isEqualTo(beforeCount);
        }
    }

    private static String fireSchedule(long id) throws TddlRuntimeException {
        try (Connection conn = getPolardbxConnection0()) {
            long targetCnt = countFiredScheduleJobs(conn, id) + 1;
            JdbcUtil.executeQuery(String.format(FIRE_SCHEDULE, id), conn);
            Thread.sleep(1000L);

            // wait until the job is finished
            int waitCnt = 0;
            while (!checkScheduleResultState(conn, id, targetCnt,
                new String[] {
                    FiredScheduledJobState.SUCCESS.name(), FiredScheduledJobState.INTERRUPTED.name()})) {
                waitCnt++;
                assertWithMessage("optimizer alert job is unfinished, schedule_id is: " + id)
                    .that(waitCnt).isLessThan(20);
                Thread.sleep(5 * 1000L);
            }
            ResultSet rs = JdbcUtil.executeQuery(SCHEDULE_REMARK, conn);
            Assert.assertTrue(rs.next());
            return rs.getString(1);
        } catch (InterruptedException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static long alertCount(OptimizerAlertType targetAlert) {
        long count = 0L;
        try (Connection conn = getPolardbxConnection0();
            ResultSet rs = JdbcUtil.executeQuery(String.format(OPTIMIZER_ALERT, targetAlert.name()), conn)) {
            while (rs.next()) {
                System.out.println(rs.getString(1) + " " + rs.getLong(2));
                count += rs.getLong(2);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    private static long countFiredScheduleJobs(Connection conn, long id) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(JOBS_COUNT, id), conn)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0L;
    }

    private static boolean checkScheduleResultState(Connection conn, long id, long targetCnt, String[] states)
        throws SQLException {
        if (countFiredScheduleJobs(conn, id) != targetCnt) {
            throw new TddlRuntimeException(ERR_FROM_SCHEDULE, "");
        }
        try (ResultSet resultSet = JdbcUtil.executeQuery("show schedule result " + id, conn)) {
            Assert.assertTrue(resultSet.next());
            for (String state : states) {
                if (state.equalsIgnoreCase(resultSet.getString(6))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean checkScheduleResultState(Connection conn, long id, String[] states, String now)
        throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuery(
            String.format(JOB_STATE_CHECK, id, now), conn)) {
            Assert.assertTrue(resultSet.next());
            String curState = resultSet.getString(1);
            System.out.println(curState);
            for (String state : states) {
                if (state.equalsIgnoreCase(curState)) {
                    return true;
                }
            }
        }
        return false;
    }

    @AfterClass
    public static void clean() throws Exception {
        try (Connection conn = getPolardbxConnection0();
            Statement statement = conn.createStatement()) {
            statement.execute("set global alert_statistic_interrupt=false");
            statement.execute("set @fp_inject_ignore_interrupted_to_statistic_schedule_job='false'");
            statement.execute("set global alert_statistic_inconsistent=false");
        }
        Thread.sleep(2000);
    }
}
