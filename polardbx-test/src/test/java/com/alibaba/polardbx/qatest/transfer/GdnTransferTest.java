package com.alibaba.polardbx.qatest.transfer;

import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.transfer.Runner;
import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.plugin.BasePlugin;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.TomlWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.Connection;

public class GdnTransferTest {
    private final String path = ConfigConstant.RESOURCE_PATH + "transfer_test.toml";

    @Test
    public void transferTest() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists transfer_test");
            JdbcUtil.executeUpdate(connection, "create database transfer_test mode=auto");
        }
        prepareConfig();
        TomlConfig.getInstance().init(path);

        // Prepare transfer table.
        Utils.prepare();

        // Run transfer test.
        Runner.runAllPlugins();

        if (BasePlugin.success()) {
            System.out.println("Transfer test success.");
        } else {
            throw new RuntimeException("Transfer test failed.");
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
        String cdcDsn = PropertiesUtil.configProp.getProperty(ConfigConstant.GDN_SLAVE_USER)
            + ":"
            + PropertiesUtil.configProp.getProperty(ConfigConstant.GDN_SLAVE_PASSWORD)
            + "@tcp("
            + PropertiesUtil.configProp.getProperty(ConfigConstant.GDN_SLAVE_ADDRESS)
            + ":"
            + PropertiesUtil.configProp.getProperty(ConfigConstant.GDN_SLAVE_PORT)
            + ")/transfer_test";

        TransferTestBase.TransferConfig config = TransferTestBase.newPolarDBXTransferConfigBuilder()
            .setEnableTransferSimple(true)
            .setEnableCheckBalance(true)
            .setEnableCheckCdc(true)
            .setCdcDsn(cdcDsn)
            .setCdcBeforeCheckStmt("set transaction_policy = xa")
            .build();

        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

}
