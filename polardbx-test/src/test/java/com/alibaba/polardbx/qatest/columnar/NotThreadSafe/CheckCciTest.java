package com.alibaba.polardbx.qatest.columnar.NotThreadSafe;

import com.alibaba.polardbx.qatest.columnar.dql.ColumnarReadBaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.transfer.Runner;
import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.plugin.BasePlugin;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.TomlWriter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CheckCciTest extends ColumnarReadBaseTestCase {
    private final String TABLE_NAME = "test_columnar_check_cci_xxx";
    private final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
        + " id int primary key auto_increment, "
        + " name varchar(255), "
        + " CLUSTERED COLUMNAR INDEX `cci_check_test_cci` (`id`) partition by key(`id`) "
        + ") partition by key (id)";
    private final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS " + TABLE_NAME;
    private final String fastCheckerOff = "/*+TDDL:enable_cci_fast_checker=false*/";
    private final int dataSize = 100;
    private AtomicBoolean stop = new AtomicBoolean(false);
    String insertSql = "INSERT INTO " + TABLE_NAME + "(name) VALUES ('%s')";
    private final Runnable writeOnly = () -> {
        try {
            SecureRandom random = new SecureRandom();
            try (Connection conn = getPolardbxConnection(DB_NAME);
                Statement stmt = conn.createStatement()) {
                stmt.execute("set transaction_policy = tso");
                while (!stop.get()) {
                    stmt.execute("begin");
                    insert(random.nextInt(4), conn);
                    update(conn);
                    delete(conn);
                    stmt.execute("commit");
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    };

    private void delete(Connection conn) throws SQLException {
        ResultSet rs;
        long id;

        // delete
        rs = JdbcUtil.executeQuerySuccess(conn, "SELECT MIN(id), MAX(id) FROM " + TABLE_NAME);
        int min, max;
        if (rs.next()) {
            min = rs.getInt(1);
            max = rs.getInt(2);
        } else {
            throw new RuntimeException("Can not find min/max id from " + TABLE_NAME);
        }
        rs = JdbcUtil.executeQuerySuccess(conn, "SELECT id, name FROM " + TABLE_NAME +
            " WHERE id BETWEEN " + min + " AND " + max + " ORDER BY RAND() LIMIT 1");
        id = 0;
        if (rs.next()) {
            id = rs.getLong(1);
        }
        JdbcUtil.executeUpdateSuccess(conn, "delete from " + TABLE_NAME + " where id = " + id);
    }

    private void update(Connection conn) throws SQLException {
        // get an existed row
        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "SELECT MIN(id), MAX(id) FROM " + TABLE_NAME);
        int min, max;
        if (rs.next()) {
            min = rs.getInt(1);
            max = rs.getInt(2);
        } else {
            throw new RuntimeException("Can not find min/max id from " + TABLE_NAME);
        }
        rs = JdbcUtil.executeQuerySuccess(conn, "SELECT id, name FROM " + TABLE_NAME +
            " WHERE id BETWEEN " + min + " AND " + max + " ORDER BY RAND() LIMIT 1");
        long id = 0;
        String name = "";
        if (rs.next()) {
            id = rs.getLong(1);
            name = rs.getString(2);
        }

        // update
        String newName;
        if ("a".equals(name)) {
            newName = "b";
        } else if ("b".equals(name)) {
            newName = "c";
        } else if ("c".equals(name)) {
            newName = "d";
        } else {
            newName = "a";
        }
        JdbcUtil.executeUpdateSuccess(conn,
            "update " + TABLE_NAME + " set name = '" + newName + "' where id = " + id);
    }

    private void insert(int random, Connection conn) {
        // insert
        switch (random) {
        case 0:
            JdbcUtil.executeSuccess(conn, String.format(insertSql, "a"));
            break;
        case 1:
            JdbcUtil.executeSuccess(conn, String.format(insertSql, "b"));
            break;
        case 2:
            JdbcUtil.executeSuccess(conn, String.format(insertSql, "c"));
            break;
        case 3:
            JdbcUtil.executeSuccess(conn, String.format(insertSql, "d"));
            break;
        default:
        }
    }

    private void createCciThenInsert(Connection connection) throws SQLException {
        JdbcUtil.executeSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeSuccess(connection, CREATE_TABLE_SQL);
        try (Statement statement = connection.createStatement()) {
            statement.execute("set transaction_policy = tso");
            statement.execute("begin");
            for (int i = 0; i < dataSize; i++) {
                insert(i % 4, connection);
            }
            statement.execute("commit");
        }
    }

    private void insertThenCreateCci(Connection connection) throws SQLException {
        JdbcUtil.executeSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeSuccess(connection, CREATE_TABLE_SQL);
        try (Statement statement = connection.createStatement()) {
            statement.execute("set transaction_policy = tso");
            statement.execute("begin");
            for (int i = 0; i < dataSize; i++) {
                insert(i % 4, connection);
            }
            statement.execute("commit");
        }
    }

    @Test
    public void testSimple() throws SQLException, InterruptedException {
        createCciThenInsert(tddlConnection);
        testCheckCci();
        insertThenCreateCci(tddlConnection);
        testCheckCci();
    }

    public void testCheckCci() throws SQLException, InterruptedException {
        stop.set(false);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 4; i++) {
            executor.submit(writeOnly);
        }
        executor.shutdown();

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            fastCheckerOff + "check columnar index cci_check_test_cci");
        checkOk(rs);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.columnar_flush()");
        Assert.assertTrue(rs.next());
        long tso = rs.getLong(1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            fastCheckerOff + "check columnar index cci_check_test_cci snapshot " + tso + " " + tso);
        checkOk(rs);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.columnar_flush()");
        Assert.assertTrue(rs.next());
        tso = rs.getLong(1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            fastCheckerOff + "check columnar index cci_check_test_cci increment " + tso + " " + tso + " " + tso);
        checkOk(rs);

        stop.set(true);
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    private void checkOk(ResultSet rs) throws SQLException {
        String details = "";
        while (rs.next()) {
            details += rs.getString("DETAILS") + " ";
        }
        System.out.println(details);
        Assert.assertTrue("Details: " + details, details.contains("OK"));
    }

    @Test
    public void testFailure() throws SQLException {
        insertThenCreateCci(tddlConnection);
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            delete(conn);
            update(conn);
            insert(0, conn);
        }
        try {
            JdbcUtil.executeQuerySuccess(tddlConnection, "set @fp_clear=true");
            JdbcUtil.executeQuerySuccess(tddlConnection, "SET @FP_FAIL_DURING_CAL_PRIMARY_HASH = 'true'");
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                fastCheckerOff + "check columnar index cci_check_test_cci");
            checkError(rs, "FP_FAIL_DURING_CAL_PRIMARY_HASH");

            JdbcUtil.executeQuerySuccess(tddlConnection, "set @fp_clear=true");
            JdbcUtil.executeQuerySuccess(tddlConnection, "SET @FP_FAIL_DURING_CAL_COLUMNAR_HASH = 'true'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                fastCheckerOff + "check columnar index cci_check_test_cci");
            checkError(rs, "FP_FAIL_DURING_CAL_COLUMNAR_HASH");
        } finally {
            JdbcUtil.executeQuerySuccess(tddlConnection, "set @fp_clear=true");
        }
    }

    @Test(timeout = 600000)
    public void testInterrupt() throws SQLException, InterruptedException {
        String dbName = "test_columnar_check_cci_interrupt";
        String dropDB = "drop database if exists " + dbName;
        String createDB = "create database " + dbName + " mode=auto";
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropDB);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createDB);
            try (Connection conn = getPolardbxConnection(dbName)) {
                insertThenCreateCci(conn);
                delete(conn);
                update(conn);
                insert(0, conn);
                JdbcUtil.executeQuerySuccess(conn, "set @fp_clear=true");
                Thread thread = new Thread(() -> {
                    try (Connection connection = getPolardbxConnection(dbName)) {
                        JdbcUtil.executeQuerySuccess(connection, "SET @FP_SLEEP_DURING_CHECK_CCI = 'true'");
                        JdbcUtil.executeFailed(connection,
                            fastCheckerOff + "check columnar index cci_check_test_cci",
                            "DDL job has been cancelled or interrupted");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                thread.start();
                long jobId = getJobId(conn);
                JdbcUtil.executeUpdateSuccess(conn, "pause ddl " + jobId);
                thread.join();
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, dropDB);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createDB);
            try (Connection conn = getPolardbxConnection(dbName)) {
                insertThenCreateCci(conn);
                delete(conn);
                update(conn);
                insert(0, conn);
                JdbcUtil.executeQuerySuccess(conn, "set @fp_clear=true");
                Thread thread = new Thread(() -> {
                    try (Connection connection = getPolardbxConnection(dbName)) {
                        JdbcUtil.executeQuerySuccess(connection, "SET @FP_SLEEP_DURING_CHECK_CCI = 'true'");
                        JdbcUtil.executeFailed(connection,
                            fastCheckerOff + "check columnar index cci_check_test_cci",
                            "DDL job has been cancelled or interrupted");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                thread.start();
                thread.interrupt();
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, dropDB);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createDB);
            try (Connection conn = getPolardbxConnection(dbName)) {
                insertThenCreateCci(conn);
                delete(conn);
                update(conn);
                JdbcUtil.executeQuerySuccess(conn, "set @fp_clear=true");
                Thread thread = new Thread(() -> {
                    try (Connection connection = getPolardbxConnection(dbName)) {
                        JdbcUtil.executeQuerySuccess(connection, "SET @FP_SLEEP_DURING_CHECK_CCI = 'true'");
                        JdbcUtil.executeFailed(connection,
                            fastCheckerOff + "check columnar index cci_check_test_cci",
                            "DDL job has been cancelled or interrupted");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                thread.start();
                long id = getId(dbName);
                JdbcUtil.executeUpdateSuccess(conn, "kill " + id);
                thread.join();
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropDB);
            JdbcUtil.executeQuerySuccess(tddlConnection, "set @fp_clear=true");
        }
    }

    private long getId(String dnName) throws SQLException, InterruptedException {
        int retry = 10;
        while (retry-- > 0) {
            String sql = "select id from information_schema.processlist where db = '" + dnName + "' "
                + "and info like '%check columnar%' and info not like '%information_schema.processlist%'";
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            if (rs.next()) {
                return rs.getLong(1);
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("fail to get job id");
    }

    private long getJobId(Connection conn) throws SQLException, InterruptedException {
        int retry = 10;
        while (retry-- > 0) {
            String sql = "show ddl";
            ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
            while (rs.next()) {
                long jobId = rs.getLong("JOB_ID");
                String objectName = rs.getString("OBJECT_NAME");
                String ddlType = rs.getString("DDL_TYPE");
                if (objectName.contains("cci_check_test_cci") && ddlType.equalsIgnoreCase("CHECK_COLUMNAR_INDEX")) {
                    System.out.println(jobId);
                    return jobId;
                }
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("fail to get job id");
    }

    private void checkError(ResultSet rs, String errorMsg) throws SQLException {
        String details = "";
        while (rs.next()) {
            details += rs.getString("DETAILS") + " ";
        }
        System.out.println(details);
        Assert.assertTrue("Details: " + details, details.contains("Error"));
        Assert.assertTrue("Details: " + details, details.contains(errorMsg));
    }

    @Test
    public void testColumnarFlush() throws SQLException, InterruptedException {
        JdbcUtil.executeSuccess(tddlConnection, "SET GLOBAL COLUMNAR_FLUSH_USING_SYNC_POINT = false");
        JdbcUtil.waitUntilVariableChanged(tddlConnection, "COLUMNAR_FLUSH_USING_SYNC_POINT", "false", 10);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.columnar_flush()");
        Assert.assertTrue(rs.next());
        long tso = rs.getLong(1);
        rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, "select * from metadb.cdc_sync_point_meta where tso = " + tso);
        Assert.assertFalse(rs.next());
    }

    private final String path = ConfigConstant.RESOURCE_PATH + "all_types_test.toml";

    @Ignore
    public void allTypesTest() throws Exception {
        prepareConfig();
        TomlConfig.getInstance().init(path);
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists all_types_test");
            JdbcUtil.executeUpdate(connection, "create database all_types_test mode=auto");
        }
        // Prepare all types test table.
        Utils.prepare();

        // Run all types test.
        Runner.runAllPlugins();

        if (BasePlugin.success()) {
            System.out.println("All types test success.");
        } else {
            throw new RuntimeException("All types test failed.");
        }
    }

    private void prepareConfig() throws IOException {
        // Delete old file if exists.
        try {
            Files.delete(Paths.get(path));
        } catch (NoSuchFileException ex) {
            // ignore.
        }
        // Create new config file.
        AllTypesConfig config = new AllTypesConfig();
        config.dsn =
            PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_USER)
                + ":"
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD)
                + "@tcp("
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS)
                + ":"
                + PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PORT)
                + ")/all_types_test";
        config.timeout = PropertiesUtil.transferTestTime * 60;
        config.threads = PropertiesUtil.allTypesTestPrepareThreads;
        config.big_column = PropertiesUtil.allTypesTestBigColumn;
        config.row_count = PropertiesUtil.transferRowCount;
        config.conn_properties = PropertiesUtil.getConnectionProperties();
        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

    private static class AllTypesConfig {
        String dsn;
        String conn_properties;
        String test_type = "all-types-test";
        String runmode = "local";
        long row_count = 10000;
        String create_table_suffix = "PARTITION BY KEY(id) PARTITIONS 16";
        long report_interval = 5;
        long timeout;
        long threads = 16;
        boolean big_column = false;
        WriteOnly write_only = new WriteOnly();
        CheckColumnar check_columnar = new CheckColumnar();
    }

    private static class WriteOnly {
        boolean enabled = true;
        boolean skip_trx = true;
        long threads = 5;
    }

    private static class CheckColumnar {
        boolean enabled = true;
        long threads = 1;
    }
}
