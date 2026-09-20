package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CheckPhyTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.PhysicalBackfillTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class AlterTableGroupBackfillRestartTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME1 = "TABLE_NAME1_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME2 = "TABLE_NAME2_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME3 = "TABLE_NAME3_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME4 = "TABLE_NAME4_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME5 = "TABLE_NAME5_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME6 = "TABLE_NAME6_" + RandomUtils.getStringBetween(4, 6);
    private static final String DB_NAME_ = "AlterTableGroupBackfillRestartTest";
    static String DB_NAME = DB_NAME_;
    static boolean disableRebalanceMpp = false;
    final static AtomicBoolean forceStop = new AtomicBoolean(false);
    final static String KEY_KEY_PART =
        "partition by key(id) partitions 3 subpartition by key(bigint_col) subpartitions 3;";
    final static String KEY_PART =
        "partition by key(id) partitions 9;";
    final static String RANGE_KEY_PART =
        "partition by range(id) subpartition by key(bigint_col) subpartitions 3 (partition p1 values less than (300), partition p2 values less than (600), partition p3 values less than (maxvalue));";
    private ExecutorService executor;

    // 构造函数接收两个参数
    public AlterTableGroupBackfillRestartTest() {
    }

    @Before
    public void setUp() throws SQLException {
        executor = Executors.newFixedThreadPool(4); // Thread pool for concurrent operations
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            DB_NAME = DB_NAME_ + RandomUtils.getStringBetween(4, 6);
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            ResultSet rs = JdbcUtil.executeQuery(
                META_DB_HINT + "select param_val from inst_config where param_key='DISABLE_REBALANCE_MPP'", conn);
            if (rs.next()) {
                disableRebalanceMpp = rs.getString(1).equalsIgnoreCase("true");
            }
            if (!disableRebalanceMpp) {
                JdbcUtil.executeUpdate(conn, "set global DISABLE_REBALANCE_MPP=true");
            }
        }
    }

    @After
    public void tearDown() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            if (!disableRebalanceMpp) {
                JdbcUtil.executeUpdate(conn, "set global DISABLE_REBALANCE_MPP=false");
            }
        }
    }

    @Test
    public void testPhysicalBackfillTaskRestart() throws InterruptedException, ExecutionException, SQLException {
        createTable();
        insertRandomData(200000);
        physicalBackfillTaskRestart();

    }

    private void createTable() throws SQLException {
        String sql = "CREATE TABLE `%s` (\n"
            + "\t`id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,\n"
            + "\t`c1` char(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'AAAAAAAAAAAAAA',\n"
            + "\t`c2` char(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'AAAAAAAAAAAAAA',\n"
            + "\t`c3` char(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'AAAAAAAAAAAAAA',\n"
            + "\t`c4` char(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'AAAAAAAAAAAAAA',\n"
            + "\t`c5` char(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',\n"
            + "\t`d1` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ")\n"
            + "PARTITION BY KEY(`id`)\n"
            + "PARTITIONS 2\n"
            + "(PARTITION `p1` VALUES LESS THAN (1) ENGINE = InnoDB,\n"
            + " PARTITION `p2` VALUES LESS THAN (9223372036854775807) ENGINE = InnoDB)";

        String sql1 = String.format(sql, TABLE_NAME1);
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME2, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME3, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME4, TABLE_NAME1));
        }
    }

    private void physicalBackfillTaskRestart() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);

        StringBuilder hintSb = new StringBuilder();
        hintSb.append(
            "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=true,TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=1,PHYSICAL_BACKFILL_TASK_INJECT_FAIL_TIME=2,PHYSICAL_BACKFILL_MIN_SUCCESS_BATCH_UPDATE=500)*/");
        Set<String> allJobIds = new HashSet<>();
        for (String tableName : ImmutableList.of(TABLE_NAME1)) {
            String partStr = "partitions";
            Map<String, List<String>> storageAndPartitions = showTopologyByStorage(tddlConnection, tableName);
            if (storageAndPartitions.size() <= 1) {
                return;
            }
            List<String> storages = new ArrayList<>();
            List<List<String>> partitions = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : storageAndPartitions.entrySet()) {
                String storage = entry.getKey();
                storages.add(storage);
                partitions.add(entry.getValue());
            }
            try (Statement stmt = tddlConnection.createStatement()) {
                for (int i = 0; i < storages.size(); i++) {
                    int j = ((i == storages.size() - 1) ? 0 : i + 1);
                    String movePgSql =
                        String.format(
                            "%s alter tablegroup by table " + tableName + " move " + partStr
                                + " %s to '%s' async=true",
                            hintSb.toString(), String.join(",", partitions.get(i)), storages.get(j));
                    System.out.println(movePgSql);
                    stmt.executeUpdate(movePgSql);
                    List<Map<String, String>> curDDL = showFullDDL();
                    for (Map<String, String> map : GeneralUtil.emptyIfNull(curDDL)) {
                        allJobIds.add(map.get("JOB_ID"));
                    }
                }
            } catch (SQLException e) {
                Assert.fail(e.getMessage());
            }
        }
        long startTime = System.currentTimeMillis();
        Set<String> jobIds = new HashSet<>();
        List<Map<String, String>> fullDDL;
        do {
            jobIds.clear();
            fullDDL = showFullDDL();
            if (fullDDL.size() == 0) {
                break;
            } else {
                for (Map<String, String> map : fullDDL) {
                    jobIds.add(map.get("JOB_ID"));
                    allJobIds.add(map.get("JOB_ID"));
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {

                }
            }
        } while (System.currentTimeMillis() - startTime < 600 * 1000);
        if (fullDDL.size() > 0) {
            Assert.fail("DDL job is not finished after 600 seconds");
        }
        try (Connection metadbConn = getMetaConnection()) {
            for (String jobId : allJobIds) {
                DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
                DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
                ddlEngineAccessor.setConnection(metadbConn);
                ddlEngineTaskAccessor.setConnection(metadbConn);
                DdlEngineRecord ddlJobRec = ddlEngineAccessor.queryArchive(Long.valueOf(jobId));
                if (ddlJobRec != null) {
                    Assert.assertTrue(
                        "COMPLETED".equalsIgnoreCase(ddlJobRec.state) || "ROLLBACK_COMPLETED".equalsIgnoreCase(
                            ddlJobRec.state), ddlJobRec.state);
                }
                List<DdlEngineTaskRecord> records =
                    ddlEngineTaskAccessor.queryAllTaskRecord(Long.valueOf(jobId), PhysicalBackfillTask.TASK_NAME);
                for (int i = 0; i < records.size(); i++) {
                    DdlEngineTaskRecord record = records.get(i);
                    PhysicalBackfillTask physicalBackfillTask =
                        (PhysicalBackfillTask) TaskHelper.deSerializeTask(PhysicalBackfillTask.TASK_NAME,
                            record.value);
                    if ("COMPLETED".equalsIgnoreCase(ddlJobRec.state)) {
                        if (physicalBackfillTask.getPhysicalTableName().toLowerCase()
                            .startsWith(TABLE_NAME1.toLowerCase())) {
                            Assert.assertTrue(physicalBackfillTask.getExecTime() > 1, record.value);
                            Assert.assertTrue(physicalBackfillTask.getInjectFailTime() > 1, record.value);
                        } else {
                            Assert.assertTrue(physicalBackfillTask.getExecTime() > 1, record.value);
                            Assert.assertTrue(physicalBackfillTask.getInjectFailTime() == 1, record.value);
                        }
                    }
                }

            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    private void insertRandomData(int count) {
        String sql =
            "INSERT INTO " + TABLE_NAME1 + "(C1,C2,C3,C4,C5) select "
                + "'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD',"
                + "'HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH',"
                + "'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ',"
                + "'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',"
                + "'GGGGGGGGGG' ";
        int time = 63 - Long.numberOfLeadingZeros(count);
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            JdbcUtil.executeUpdate(connection, sql);
            do {
                JdbcUtil.executeUpdate(connection,
                    "insert into " + TABLE_NAME1 + "(C1,C2,C3,C4,C5) select C1,C2,C3,C4,C5 from " + TABLE_NAME1);
            } while (time-- > 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
