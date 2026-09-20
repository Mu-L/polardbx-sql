package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

public class StatisticCollectFromDnTest extends AutoReadBaseTestCase {

    @After
    public void clearFailPoint() throws Exception {
        try (Connection connection = getPolardbxConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(String.format("set @%s=true", FailPoint.FP_CLEAR));
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set global %s = false", ConnectionProperties.ENABLE_HLL));
        }
        Thread.sleep(2000);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection,
                String.format("set global %s = true", ConnectionProperties.ENABLE_HLL));
        }
        Thread.sleep(2000);
    }

    @Test
    public void testShardColumnNdv() throws SQLException {
        String schema = PropertiesUtil.polardbXDBName1(usingNewPartDb());
        Connection conn = getPolardbxConnection(schema);
        JdbcUtil.executeUpdateSuccess(conn,
            String.format("set @%s='true'", FailPointKey.FP_INJECT_IGNORE_STATISTIC_SAMPLE_SKETCH));

        String tableName = "t_statistic_collect_from_dn_test";
        JdbcUtil.dropTable(conn, tableName);

        String createTableSql = String.format("CREATE TABLE `%s` (\n" +
            "\t`id` int(11) NOT NULL auto_increment,\n" +
            "\t`col0` int(11) DEFAULT NULL,\n" +
            "\t`col1` int(11) DEFAULT NULL,\n" +
            "\t`col2` int(11) DEFAULT NULL,\n" +
            "\tPRIMARY KEY (`id`),\n" +
            "\tLOCAL KEY `idx_col1`(`col1`),\n" +
            "\tLOCAL KEY `idx_col2`(`col2`),\n" +
            "\tLOCAL KEY `idx_col1_col2`(`col1`,`col2`),\n" +
            "\tLOCAL KEY `idx_col2_col1`(`col2`,`col1`),\n" +
            "\tGLOBAL INDEX `gsi_col1`(`col1`) partition by hash(`col1`) partitions 8,\n" +
            "\tGLOBAL INDEX `gsi_col2`(`col2`) partition by hash(`col2`) partitions 8\n" +
            ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
            "PARTITION BY KEY(`col0`)\n" +
            "PARTITIONS 8;", tableName);

        JdbcUtil.executeUpdateSuccess(conn, createTableSql);
        JdbcUtil.executeUpdateSuccess(conn, "insert into " + tableName + " values (null, 1, 1, 1)");
        int size = 1;
        while (size < 1000) {
            String insertSql = String.format(
                "insert into %s select null, FLOOR(RAND() * 1000000), %s, FLOOR(RAND() * 1000000) from %s", tableName,
                size, tableName);
            JdbcUtil.executeUpdateSuccess(conn, insertSql);
            size *= 2;
        }

        long now = System.currentTimeMillis();
        String collectStatisticSql = String.format("collect statistic %s.%s", schema, tableName);
        JdbcUtil.executeUpdateSuccess(conn, collectStatisticSql);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String selectModuleLogSql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "' and LOG_PATTERN = 'PROCESS_END'";
        System.out.println(selectModuleLogSql);
        ResultSet moduleRs = JdbcUtil.executeQuery(selectModuleLogSql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");
        boolean hashPrimaryTb = false;
        boolean hasGsi1 = false;
        boolean hashGsi2 = false;
        JSONObject columnNdv = new JSONObject();
        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (event.contains("collect cardinality from dn succeed ended")) {
                System.out.println(event);
                if (event.contains(tableName)) {
                    hashPrimaryTb = true;
                } else if (event.contains("gsi_col1")) {
                    hasGsi1 = true;
                } else if (event.contains("gsi_col2")) {
                    hashGsi2 = true;
                }
                JSONObject obj = JSON.parseObject(event.substring(event.lastIndexOf("{")));
                for (String columnName : obj.keySet()) {
                    columnNdv.put(columnName,
                        Math.max(obj.getLong(columnName), (Long) columnNdv.getOrDefault(columnName, 0L)));
                }
            }
            if (hashPrimaryTb && hasGsi1 && hashGsi2) {
                break;
            }
        }

        System.out.println(columnNdv.toJSONString());

        String selectNdvFromVirtualStatisticSql = String.format(
            "select COLUMN_NAME, CARDINALITY, NDV_SOURCE from information_schema.virtual_statistic where schema_name = '%s' and table_name = '%s' "
                +
                "and COLUMN_NAME in ('col0','col1','col2')", schema, tableName);
        ResultSet ndvRs = JdbcUtil.executeQuery(selectNdvFromVirtualStatisticSql, this.getPolardbxConnection());
        while (ndvRs.next()) {
            System.out.println(
                ndvRs.getString("COLUMN_NAME") + ":" + ndvRs.getLong("CARDINALITY") + ":" + ndvRs.getString(
                    "NDV_SOURCE"));
            String columnName = ndvRs.getString("COLUMN_NAME");
            Assert.assertTrue((long) columnNdv.getLong(columnName) == (long) ndvRs.getLong("CARDINALITY"));
            Assert.assertTrue(ndvRs.getString("NDV_SOURCE").equalsIgnoreCase("DN_STAT"));
        }

        String selectNdvFromStatisticDataSql = String.format(
            "select COLUMN_NAME, NDV, NDV_SOURCE from information_schema.statistics_data where schema_name = '%s' and table_name = '%s' "
                +
                "and COLUMN_NAME in ('col0','col1','col2')", schema, tableName);
        ndvRs = JdbcUtil.executeQuery(selectNdvFromStatisticDataSql, this.getPolardbxConnection());
        while (ndvRs.next()) {
            System.out.println(
                ndvRs.getString("COLUMN_NAME") + ":" + ndvRs.getLong("NDV") + ":" + ndvRs.getString("NDV_SOURCE"));
            String columnName = ndvRs.getString("COLUMN_NAME");
            Assert.assertTrue((long) columnNdv.getLong(columnName) == (long) ndvRs.getLong("NDV"));
            Assert.assertTrue(ndvRs.getString("NDV_SOURCE").equalsIgnoreCase("DN_STAT"));
        }

        JdbcUtil.dropTable(conn, tableName);
        conn.close();
    }

}
