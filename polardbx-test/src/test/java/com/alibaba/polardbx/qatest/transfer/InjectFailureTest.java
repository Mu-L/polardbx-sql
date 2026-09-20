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
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.Connection;

public class InjectFailureTest {
    private final String path = ConfigConstant.RESOURCE_PATH + "transfer_test.toml";

    @Ignore("Local test only")
    public void transferTestTso() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists transfer_test");
            JdbcUtil.executeUpdate(connection, "create database transfer_test mode=auto");
        }
        prepareTsoConfig();
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

    @Ignore("Local test only")
    public void transferTestTsoOpt() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists transfer_test");
            JdbcUtil.executeUpdate(connection, "create database transfer_test mode=auto");
        }
        prepareTsoOptConfig();
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

    @Ignore("Local test only")
    public void transferTestAsyncCommit() throws Exception {
        try (Connection connection = ConnectionManager.newPolarDBXConnection0()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists transfer_test");
            JdbcUtil.executeUpdate(connection, "create database transfer_test mode=auto");
        }
        prepareAsyncCommitConfig();
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

    private void prepareTsoConfig() throws IOException {
        // Delete old file if exists.
        try {
            Files.delete(Paths.get(path));
        } catch (NoSuchFileException ex) {
            // ignore.
        }
        // Create new config file.
        TransferTestBase.TransferConfig config = TransferTestBase.newPolarDBXTransferConfigBuilder()
            .setEnableTransferSimple(true)
            .setEnableCheckBalance(true)
            .build();

        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

    private void prepareTsoOptConfig() throws IOException {
        // Delete old file if exists.
        try {
            Files.delete(Paths.get(path));
        } catch (NoSuchFileException ex) {
            // ignore.
        }
        // Create new config file.
        TransferTestBase.TransferConfig config = TransferTestBase.newPolarDBXTransferConfigBuilder()
            .setEnableTransferSimple(true)
            .setEnableCheckBalance(true)
            .setEnableTransferTsoOpt(true)
            .build();

        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

    private void prepareAsyncCommitConfig() throws IOException {
        // Delete old file if exists.
        try {
            Files.delete(Paths.get(path));
        } catch (NoSuchFileException ex) {
            // ignore.
        }
        // Create new config file.
        TransferTestBase.TransferConfig config = TransferTestBase.newPolarDBXTransferConfigBuilder()
            .setEnableTransferSimple(true)
            .setEnableCheckBalance(true)
            .setEnableTransferAsyncCommit(true)
            .build();

        TomlWriter writer = new TomlWriter();
        writer.write(config, new File(path));
    }

}
