package com.alibaba.polardbx.qatest.NotThreadSafe.externalized;

import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Serial coverage for externalized-column information schema under global configuration changes.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnInfoSchemaTest extends ExternalizedColumnTestBase {

    private static final String LOWER_CASE_VARIABLE = "ENABLE_LOWER_CASE_TABLE_NAMES";
    private static final String INSTANCE_ID = PropertiesUtil.configProp.getProperty("instanceId");
    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_info_schema_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_info_schema_drds_" + DB_SUFFIX;

    public ExternalizedColumnInfoSchemaTest(DatabaseMode databaseMode) {
        super(databaseMode);
    }

    @Parameterized.Parameters(name = "{index}:mode={0}")
    public static List<Object[]> parameters() {
        return autoAndDrdsModes();
    }

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(AUTO_DB, DatabaseMode.AUTO);
        createIsolatedDatabase(DRDS_DB, DatabaseMode.DRDS);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(AUTO_DB);
        dropIsolatedDatabase(DRDS_DB);
    }

    @Test
    public void testLogicalColumnsForBothLowerCaseNameModes() throws Exception {
        String tableName = "ext_InfoSchema_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase())) {
            JdbcUtil.executeSuccess(connection, "SET SESSION WORKLOAD_TYPE=TP");
            JdbcUtil.executeUpdateSuccess(connection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "content LONGTEXT EXTERNALIZE,"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                tableName, tableDistribution("id", 4)));
            try {
                String originalTableName = queryInfoSchemaTableName(connection, tableName);
                Assert.assertTrue("Unexpected INFORMATION_SCHEMA table name: " + originalTableName,
                    tableName.equalsIgnoreCase(originalTableName));
                boolean originalLowerCaseBehavior = !tableName.equals(originalTableName);
                GlobalBooleanSnapshot original =
                    snapshotGlobalBoolean(LOWER_CASE_VARIABLE, originalLowerCaseBehavior);
                try {
                    setGlobalBoolean(connection, LOWER_CASE_VARIABLE, false);
                    waitForInfoSchemaTableName(connection, tableName, tableName);
                    assertLogicalColumnView(connection, tableName);

                    setGlobalBoolean(connection, LOWER_CASE_VARIABLE, true);
                    waitForInfoSchemaTableName(connection, tableName, tableName.toLowerCase(Locale.ROOT));
                    assertLogicalColumnView(connection, tableName);
                } finally {
                    restoreGlobalBoolean(connection, original);
                    waitForInfoSchemaTableName(connection, tableName, originalTableName);
                }
            } finally {
                dropTestTable(connection, tableName);
            }
        }
    }

    private void assertLogicalColumnView(Connection connection, String tableName) throws SQLException {
        boolean foundLogical = false;
        boolean foundPhysical = false;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
            String.format("SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, "
                    + "CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, COLUMN_TYPE "
                    + "FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = DATABASE() AND LOWER(TABLE_NAME) = LOWER('%s') "
                    + "ORDER BY ORDINAL_POSITION",
                tableName))) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                if ("content".equals(columnName)) {
                    foundLogical = true;
                    Assert.assertNull(rs.getString("COLUMN_DEFAULT"));
                    Assert.assertEquals("YES", rs.getString("IS_NULLABLE"));
                    Assert.assertEquals("longtext", rs.getString("DATA_TYPE"));
                    Assert.assertEquals(4294967295L, rs.getLong("CHARACTER_MAXIMUM_LENGTH"));
                    Assert.assertEquals(4294967295L, rs.getLong("CHARACTER_OCTET_LENGTH"));
                    Assert.assertEquals("longtext", rs.getString("COLUMN_TYPE"));
                }
                if (columnName.endsWith("_addr_")) {
                    foundPhysical = true;
                }
            }
        }
        Assert.assertTrue("INFORMATION_SCHEMA.COLUMNS should show logical column name", foundLogical);
        Assert.assertFalse("INFORMATION_SCHEMA.COLUMNS must NOT expose physical addr column", foundPhysical);
    }

    private String queryInfoSchemaTableName(Connection connection, String tableName) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
            String.format("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    + "WHERE TABLE_SCHEMA = DATABASE() AND LOWER(TABLE_NAME) = LOWER('%s') LIMIT 1",
                tableName))) {
            Assert.assertTrue("INFORMATION_SCHEMA.COLUMNS should contain table " + tableName, rs.next());
            return rs.getString(1);
        }
    }

    private void waitForInfoSchemaTableName(Connection connection, String tableName, String expected)
        throws SQLException, InterruptedException {
        String actual = null;
        for (int attempt = 0; attempt < 50; attempt++) {
            actual = queryInfoSchemaTableName(connection, tableName);
            if (expected.equals(actual)) {
                return;
            }
            Thread.sleep(200L);
        }
        Assert.fail("Timed out waiting for INFORMATION_SCHEMA table name " + expected + ", actual " + actual);
    }

    private void setGlobalBoolean(Connection connection, String variableName, boolean value) {
        JdbcUtil.executeUpdateSuccess(connection, "SET GLOBAL " + variableName + " = " + value);
    }

    private GlobalBooleanSnapshot snapshotGlobalBoolean(String variableName, boolean defaultValue) throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            String currentInstanceId = currentInstanceId(metaConnection);
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT param_val FROM inst_config WHERE inst_id = ? AND param_key = ? ORDER BY id DESC LIMIT 1")) {
                statement.setString(1, currentInstanceId);
                statement.setString(2, variableName);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        String value = rs.getString(1);
                        boolean effectiveValue = "1".equals(value) || Boolean.parseBoolean(value);
                        return new GlobalBooleanSnapshot(currentInstanceId, variableName, true, effectiveValue);
                    }
                    return new GlobalBooleanSnapshot(currentInstanceId, variableName, false, defaultValue);
                }
            }
        }
    }

    private void restoreGlobalBoolean(Connection connection, GlobalBooleanSnapshot snapshot)
        throws SQLException {
        if (snapshot.persisted) {
            setGlobalBoolean(connection, snapshot.variableName, snapshot.effectiveValue);
        } else {
            removePersistedGlobalValue(snapshot.instanceId, snapshot.variableName);
        }
    }

    private String currentInstanceId(Connection metaConnection) throws SQLException {
        if (INSTANCE_ID != null && !INSTANCE_ID.trim().isEmpty()) {
            try (PreparedStatement statement = metaConnection.prepareStatement(
                "SELECT 1 FROM server_info WHERE inst_id = ? AND status != 2 AND inst_type = 0 LIMIT 1")) {
                statement.setString(1, INSTANCE_ID);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return INSTANCE_ID;
                    }
                }
            }
        }
        String currentInstanceId = null;
        try (PreparedStatement statement = metaConnection.prepareStatement(
            "SELECT DISTINCT inst_id FROM server_info WHERE status != 2 AND inst_type = 0")) {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Assert.assertNull("Multiple active master instanceIds found in server_info", currentInstanceId);
                    currentInstanceId = rs.getString(1);
                }
            }
        }
        Assert.assertNotNull("No active master instanceId found in server_info", currentInstanceId);
        return currentInstanceId;
    }

    private void removePersistedGlobalValue(String instanceId, String variableName) throws SQLException {
        try (Connection metaConnection = getMetaConnection()) {
            boolean oldAutoCommit = metaConnection.getAutoCommit();
            try {
                metaConnection.setAutoCommit(false);
                try (PreparedStatement delete = metaConnection.prepareStatement(
                    "DELETE FROM inst_config WHERE inst_id = ? AND param_key = ?")) {
                    delete.setString(1, instanceId);
                    delete.setString(2, variableName);
                    delete.executeUpdate();
                }
                try (PreparedStatement notify = metaConnection.prepareStatement(
                    "UPDATE config_listener SET op_version = op_version + 1 WHERE data_id = ?")) {
                    notify.setString(1, "polardbx.inst.config." + instanceId);
                    notify.executeUpdate();
                }
                metaConnection.commit();
            } catch (SQLException e) {
                metaConnection.rollback();
                throw e;
            } finally {
                metaConnection.setAutoCommit(oldAutoCommit);
            }
        }
    }

    private static class GlobalBooleanSnapshot {
        private final String instanceId;
        private final String variableName;
        private final boolean persisted;
        private final boolean effectiveValue;

        private GlobalBooleanSnapshot(String instanceId, String variableName, boolean persisted,
                                      boolean effectiveValue) {
            this.instanceId = instanceId;
            this.variableName = variableName;
            this.persisted = persisted;
            this.effectiveValue = effectiveValue;
        }
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }
}
