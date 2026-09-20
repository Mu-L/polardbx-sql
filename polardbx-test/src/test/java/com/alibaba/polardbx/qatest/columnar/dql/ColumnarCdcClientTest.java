package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.transfer.Runner;
import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.plugin.BasePlugin;
import com.alibaba.polardbx.transfer.utils.AllTypesTestUtils;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.TomlWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

public class ColumnarCdcClientTest extends DDLBaseNewDBTestCase {
    private final String PATH = ConfigConstant.RESOURCE_PATH + "all_types_test_columnar_cdc.toml";
    private final String DB_NAME = "all_types_test_columnar_cdc";

    @Before
    public void setUp() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists " + DB_NAME);
            JdbcUtil.executeUpdate(connection, "create database " + DB_NAME + " mode=auto");
        }
    }

    @After
    public void tearDown() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void allTypesTest() throws Exception {
        prepareConfig();
        TomlConfig.getInstance().init(PATH);
        // Prepare transfer table.
        Utils.prepare();

        try (Connection connection = ConnectionManager.newPolarDBXConnection0();
            Statement stmt = connection.createStatement()) {
            stmt.execute("use " + DB_NAME);
            // Create columnar index.
            String createSql = AllTypesTestUtils.FULL_TYPE_TABLE_COLUMNAR_INDEX;
            stmt.execute(createSql);
        }

        // flush logs to make new binlog file
        AtomicBoolean stop = new AtomicBoolean(false);
        new Thread(() -> {
            try (Connection connection = ConnectionManager.newPolarDBXConnection0();
                Statement stmt = connection.createStatement()) {
                while (!stop.get()) {
                    stmt.execute("flush logs");
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                // ignore
            }
        }).start();

        // Run transfer test.
        Runner.runAllPlugins();

        stop.set(true);

        if (BasePlugin.success()) {
            System.out.println("All types test success.");
        } else {
            throw new RuntimeException("All types test failed.");
        }
    }

    private void prepareConfig() throws IOException {
        // Delete old file if exists.
        try {
            Files.delete(Paths.get(PATH));
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
                + ")/" + DB_NAME;
        config.timeout = PropertiesUtil.transferTestTime * 60;
        config.threads = PropertiesUtil.allTypesTestPrepareThreads;
        config.big_column = PropertiesUtil.allTypesTestBigColumn;
        config.row_count = PropertiesUtil.transferRowCount;
        config.conn_properties = PropertiesUtil.getConnectionProperties();
        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(PATH));
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
    }

    private static class WriteOnly {
        boolean enabled = true;
        long threads = 8;
    }

}
