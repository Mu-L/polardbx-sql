package com.alibaba.polardbx.qatest.ddl.auto.tablegroup.concurrent;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.misc.Sleep;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

public class AlterTableGroupMovePgTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME1 = "TABLE_NAME1_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME2 = "TABLE_NAME2_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME3 = "TABLE_NAME3_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME4 = "TABLE_NAME4_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME5 = "TABLE_NAME5_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME6 = "TABLE_NAME6_" + RandomUtils.getStringBetween(4, 6);
    private static final String DB_NAME_ = "AlterTableGroupMovePgTest";
    static String DB_NAME = DB_NAME_;
    final static AtomicBoolean forceStop = new AtomicBoolean(false);
    final static String KEY_KEY_PART =
        "partition by key(id) partitions 3 subpartition by key(bigint_col) subpartitions 3;";
    final static String KEY_PART =
        "partition by key(id) partitions 9;";
    final static String RANGE_KEY_PART =
        "partition by range(id) subpartition by key(bigint_col) subpartitions 3 (partition p1 values less than (300), partition p2 values less than (600), partition p3 values less than (maxvalue));";
    private ExecutorService executor;

    private final boolean enableBarrier;
    private final boolean enablePhysicalBackfill;

    // 构造函数接收两个参数
    public AlterTableGroupMovePgTest(boolean enableBarrier, boolean enablePhysicalBackfill) {
        this.enableBarrier = enableBarrier;
        this.enablePhysicalBackfill = enablePhysicalBackfill;
    }

    @Parameterized.Parameters(name = "{index}: enableBarrier={0}, enablePhysicalBackfill={1}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(
            new Object[][] {
                {false, false},
                {false, true},
                {true, false},
                {true, true}
            }
        );
    }

    @Before
    public void setUp() throws SQLException {
        executor = Executors.newFixedThreadPool(4); // Thread pool for concurrent operations
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            DB_NAME = DB_NAME_ + RandomUtils.getStringBetween(4, 6);
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
        }
    }

    @After
    public void tearDown() throws SQLException {
        if (executor != null) {
            executor.shutdownNow(); // Stop all threads immediately
        }
    }

    @Test
    @Ignore
    public void testConcurrentCRUDWithDDL() throws InterruptedException, ExecutionException, SQLException {
        createTable(true);
        insertRandomData(1000, false);

        List<Future> futureList = new ArrayList<>();
        // Submit DDL operation to a separate thread
        Future<?> ddlFuture = executor.submit(() -> movePartitionGroupConcurrently());
        futureList.add(ddlFuture);
        // Perform concurrent CRUD operations
        futureList.add(executor.submit(() -> insertRandomData(1000000, true)));
        futureList.add(executor.submit(() -> updateRandomData(500000)));
        futureList.add(executor.submit(() -> deleteRandomData(500000)));

        // Wait for all operations to complete
        for (Future future : futureList) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            if (!forceStop.get()) {
                forceStop.set(true);
            }
        }
    }

    @Test
    @Ignore
    public void checkDmlPlan() throws SQLException {
        createTable(false);
        insertRandomData(1000, false);
        Set<String> retureJobIds = movePartitionGroupConcurrentlyWithPause();
        String sqlToCheckP1 = "delete from " + TABLE_NAME1 + " where id=50";
        JdbcUtil.executeUpdate(tddlConnection, sqlToCheckP1);
        List<List<String>> trace = insertOneRandomRow(50);
        int insertCount = getDMLCountFromTrace("insert into", trace);
        Assert.assertTrue(insertCount == 1, trace.toString());

        sqlToCheckP1 = "trace delete from " + TABLE_NAME1 + " where id=50";
        JdbcUtil.executeQuery(sqlToCheckP1, tddlConnection);
        trace = getTrace(tddlConnection);
        int deleteCount = getDMLCountFromTrace("delete ", trace);
        Assert.assertTrue(deleteCount == 2, trace.toString());

        String sqlToCheckP2 = "delete from " + TABLE_NAME1 + " where id=500";
        JdbcUtil.executeUpdate(tddlConnection, sqlToCheckP2);
        trace = insertOneRandomRow(500);
        insertCount = getDMLCountFromTrace("insert into", trace);
        Assert.assertTrue(insertCount == 2, trace.toString());

        sqlToCheckP2 = "trace delete from " + TABLE_NAME1 + " where id=500";
        JdbcUtil.executeQuery(sqlToCheckP2, tddlConnection);
        trace = getTrace(tddlConnection);
        deleteCount = getDMLCountFromTrace("delete ", trace);
        Assert.assertTrue(deleteCount == 2, trace.toString());
        for (String jobId : retureJobIds) {
            try {
                JdbcUtil.executeQuery("rollback ddl " + jobId, tddlConnection);
            } catch (Exception ex) {
            }
        }
    }

    int getDMLCountFromTrace(String keyWord, List<List<String>> trace) {
        int count = 0;
        for (List<String> item : trace) {
            if (item.get(11).toLowerCase().indexOf(keyWord.toLowerCase()) != -1) {
                count++;
            }
        }
        return count;
    }

    private void createTable(boolean isKeyPart) throws SQLException {
        String sql = "CREATE TABLE %s (" +
            "id INT AUTO_INCREMENT PRIMARY KEY," +
            "tinyint_col TINYINT," +
            "smallint_col SMALLINT," +
            "mediumint_col MEDIUMINT," +
            "int_col INT," +
            "bigint_col BIGINT," +
            "decimal_col DECIMAL(10, 2)," +
            "float_col FLOAT," +
            "double_col DOUBLE," +
            "bit_col BIT(8)," +
            "char_col CHAR(10)," +
            "varchar_col VARCHAR(255)," +
            "binary_col BINARY(10)," +
            "varbinary_col VARBINARY(255)," +
            "tinyblob_col TINYBLOB," +
            "blob_col BLOB," +
            "mediumblob_col MEDIUMBLOB," +
            "longblob_col LONGBLOB," +
            "date_col DATE," +
            "datetime_col DATETIME," +
            "timestamp_col TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
            "time_col TIME," +
            "year_col YEAR," +
            "enum_col ENUM('A', 'B', 'C')," +
            "set_col SET('X', 'Y', 'Z')) ";

        String sql1 = isKeyPart ? String.format(sql, TABLE_NAME1) + KEY_KEY_PART :
            String.format(sql, TABLE_NAME1) + RANGE_KEY_PART;
        String sql2 = String.format(sql, TABLE_NAME5) + KEY_PART;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME2, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME3, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME4, TABLE_NAME1));
            stmt.executeUpdate(sql2);
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME6, TABLE_NAME5));
        }
    }

    private void movePartitionGroupConcurrently() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);

        StringBuilder hintSb = new StringBuilder();
        hintSb.append("/*+TDDL:CMD_EXTRA(");
        if (enableBarrier) {
            hintSb.append("ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP=true,");
        }
        if (enablePhysicalBackfill) {
            hintSb.append(
                "PHYSICAL_BACKFILL_ENABLE=true,TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=1,FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true");
        } else {
            hintSb.append("PHYSICAL_BACKFILL_ENABLE=false,FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true");
        }
        hintSb.append(")*/");
        for (String tableName : ImmutableList.of(TABLE_NAME1, TABLE_NAME5)) {
            String partStr = tableName.equalsIgnoreCase(TABLE_NAME1) ? "subpartitions" : "partitions";
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
                            "%s alter tablegroup by table " + tableName + " move " + partStr + " %s to '%s' async=true",
                            hintSb.toString(), String.join(",", partitions.get(i)), storages.get(j));
                    System.out.println(movePgSql);
                    stmt.executeUpdate(movePgSql);
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
            for (String jobId : jobIds) {
                DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
                ddlEngineAccessor.setConnection(metadbConn);
                DdlEngineRecord ddlJobRec = ddlEngineAccessor.queryArchive(Long.valueOf(jobId));
                if (ddlJobRec != null) {
                    Assert.assertTrue("SUCCESS".equalsIgnoreCase(ddlJobRec.state), ddlJobRec.state);
                }
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    private Set<String> movePartitionGroupConcurrentlyWithPause() {
        Set<String> retureJobIds = new HashSet<>();
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        Map<String, List<String>> storageAndPartitions = showTopologyByStorage(tddlConnection, TABLE_NAME1);
        if (storageAndPartitions.size() <= 1) {
            return retureJobIds;
        }
        StringBuilder hintSb = new StringBuilder();
        StringBuilder hintSb1 = new StringBuilder();
        hintSb1.append(",");
        hintSb.append("/*+TDDL:CMD_EXTRA(");
        if (enableBarrier) {
            hintSb.append("ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP=true,");
            hintSb1.append("ADD_BARRIER_TASK_FOR_MOVE_TABLEGROUP=true,");
        }
        if (enablePhysicalBackfill) {
            hintSb.append(
                "PHYSICAL_BACKFILL_ENABLE=true,TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=1,FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true");
            hintSb1.append(
                "PHYSICAL_BACKFILL_ENABLE=true,TABLE_SIZE_THRESHOLD_TO_ENABLE_PHYSICAL_BACKFILL=1,FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true");
        } else {
            hintSb.append("PHYSICAL_BACKFILL_ENABLE=false,FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true");
            hintSb1.append("PHYSICAL_BACKFILL_ENABLE=false,FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true");
        }
        hintSb.append(")*/");
        List<String> storages = new ArrayList<>();
        List<List<String>> partitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : storageAndPartitions.entrySet()) {
            String storage = entry.getKey();
            storages.add(storage);
            partitions.add(entry.getValue());
        }
        try (Statement stmt = tddlConnection.createStatement()) {
            String movePgSql1 = String.format(
                "%s alter tablegroup by table " + TABLE_NAME1 + " move partitions (p1) to '%s',(p2) to '%s'",
                hintSb, storages.get(0), storages.get(1));
            System.out.println(movePgSql1);
            stmt.executeUpdate(movePgSql1);
            String movePgSql2 =
                String.format(
                    "/*+TDDL:CMD_EXTRA(FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='DELETE_ONLY' %s)*/alter tablegroup by table "
                        + TABLE_NAME1 + " move partitions p1 to '%s' async=true", hintSb1, storages.get(1));
            System.out.println(movePgSql2);
            stmt.executeUpdate(movePgSql2);

            String movePgSql3 =
                String.format(
                    "/*+TDDL:CMD_EXTRA(FORCE_DOWNGRADE_RW_LOCK_FOR_TABLEGROUP=true, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='WRITE_ONLY' %s)*/alter tablegroup by table "
                        + TABLE_NAME1 + " move partitions p2 to '%s' async=true", hintSb1, storages.get(0));
            System.out.println(movePgSql3);
            stmt.executeUpdate(movePgSql3);

        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        long startTime = System.currentTimeMillis();
        Set<String> jobIds = new HashSet<>();
        List<Map<String, String>> fullDDL;
        do {
            jobIds.clear();
            retureJobIds.clear();
            fullDDL = showFullDDL();
            if (fullDDL.size() == 0) {
                break;
            } else {
                int i = 0;
                for (Map<String, String> map : fullDDL) {
                    if (!"PAUSED".equalsIgnoreCase(map.get("STATE"))) {
                        jobIds.add(map.get("JOB_ID"));
                        i++;
                    } else {
                        retureJobIds.add(map.get("JOB_ID"));
                    }
                }
                if (i == 0) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {

                }
            }
        } while (System.currentTimeMillis() - startTime < 600 * 1000);
        if (jobIds.size() > 0) {
            Assert.fail("DDL job is not finished after 600 seconds");
        }
        return retureJobIds;
    }

    private List<List<String>> insertOneRandomRow(int pkId) {
        List<List<String>> trace = new ArrayList<>();
        String sql = "trace INSERT INTO " + TABLE_NAME1
            + "(id, tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col) VALUES ("
            + pkId + ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setByte(1, (byte) (Math.random() * 256));
            pstmt.setShort(2, (short) (Math.random() * Short.MAX_VALUE));
            pstmt.setInt(3, ((int) (Math.random() * Integer.MAX_VALUE)) & 0x007FFFFF);
            pstmt.setInt(4, (int) (Math.random() * Integer.MAX_VALUE));
            pstmt.setLong(5, Math.abs(new Random().nextLong()));
            pstmt.setBigDecimal(6, BigDecimal.valueOf(RandomUtils.getIntegerBetween(1, 199) * 1000, 2));
            pstmt.setFloat(7, (float) (Math.random() * 1000));
            pstmt.setDouble(8, Math.random() * 1000);
            //pstmt.setByte(9, (byte) ((int) (Math.random() * 256) & 0x7F));
            //pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(48,57)));
            pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(58, 127)));
            pstmt.setString(10, "char" + (int) (Math.random() * 10));
            pstmt.setString(11, "varchar" + (int) (Math.random() * 100));
            pstmt.setBytes(12, ("binary" + (int) (Math.random() * 10)).getBytes());
            pstmt.setBytes(13, ("varbinary" + (int) (Math.random() * 100)).getBytes());
            pstmt.setDate(14, Date.valueOf(LocalDate.now()));
            pstmt.setTimestamp(15, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setTimestamp(16, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setTime(17, Time.valueOf(LocalTime.now()));
            pstmt.setInt(18, Year.now().getValue());
            pstmt.setString(19, "A");
            pstmt.setString(20, "X,Y");

            pstmt.executeUpdate();
            trace = getTrace(connection);
        } catch (Exception exception) {
            Assert.fail(exception.getMessage());
            exception.printStackTrace();
        }
        return trace;
    }

    private void insertRandomData(int count, boolean sleep) {
        String sql = "INSERT INTO " + TABLE_NAME1
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            while (count > 0 && !forceStop.get()) {
                try {
                    JdbcUtil.useDb(connection, DB_NAME);
                    PreparedStatement pstmt = connection.prepareStatement(sql);
                    pstmt.setByte(1, (byte) (Math.random() * 256));
                    pstmt.setShort(2, (short) (Math.random() * Short.MAX_VALUE));
                    pstmt.setInt(3, ((int) (Math.random() * Integer.MAX_VALUE)) & 0x007FFFFF);
                    pstmt.setInt(4, (int) (Math.random() * Integer.MAX_VALUE));
                    pstmt.setLong(5, Math.abs(new Random().nextLong()));
                    pstmt.setBigDecimal(6, BigDecimal.valueOf(RandomUtils.getIntegerBetween(1, 199) * 1000, 2));
                    pstmt.setFloat(7, (float) (Math.random() * 1000));
                    pstmt.setDouble(8, Math.random() * 1000);
                    //pstmt.setByte(9, (byte) ((int) (Math.random() * 256) & 0x7F));
                    //pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(48,57)));
                    pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(0, 127)));
                    pstmt.setString(10, "char" + (int) (Math.random() * 10));
                    pstmt.setString(11, "varchar" + (int) (Math.random() * 100));
                    pstmt.setBytes(12, ("binary" + (int) (Math.random() * 10)).getBytes());
                    pstmt.setBytes(13, ("varbinary" + (int) (Math.random() * 100)).getBytes());
                    pstmt.setDate(14, Date.valueOf(LocalDate.now()));
                    pstmt.setTimestamp(15, Timestamp.valueOf(LocalDateTime.now()));
                    pstmt.setTimestamp(16, Timestamp.valueOf(LocalDateTime.now()));
                    pstmt.setTime(17, Time.valueOf(LocalTime.now()));
                    pstmt.setInt(18, Year.now().getValue());
                    pstmt.setString(19, "A");
                    pstmt.setString(20, "X,Y");

                    pstmt.executeUpdate();
                    count--;
                    if (sleep) {
                        try {
                            Thread.sleep(100);
                        } catch (Exception e) {

                        }
                    }
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateRandomData(int count) {
        String sql = "UPDATE " + TABLE_NAME1 + " SET tinyint_col = ? WHERE id = ?";

        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            while (count > 0 && !forceStop.get()) {
                JdbcUtil.useDb(connection, DB_NAME);
                PreparedStatement pstmt = connection.prepareStatement(sql);
                pstmt.setByte(1, (byte) (Math.random() * 256));
                pstmt.setInt(2, getRandomId());

                pstmt.executeUpdate();
                try {
                    Thread.sleep(100);
                } catch (Exception e) {

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void deleteRandomData(int count) {
        String sql = "DELETE FROM " + TABLE_NAME1 + " WHERE id = ?";

        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            while (count > 0 && !forceStop.get()) {
                JdbcUtil.useDb(connection, DB_NAME);
                PreparedStatement pstmt = connection.prepareStatement(sql);
                pstmt.setInt(1, getRandomId());

                pstmt.executeUpdate();
                try {
                    Thread.sleep(100);
                } catch (Exception e) {

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private int getRandomId() throws SQLException {
        String sql = "SELECT id FROM " + TABLE_NAME1 + " ORDER BY RAND() LIMIT 1";
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getInt("id");
                }
            }
        }
        return 0;
    }
}
