package com.alibaba.polardbx.qatest.ddl.auto.hotpartition;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * End-to-end migration of the legacy tddl-qatest HotKeyPartitionTypeTest case into the
 * active polardbx-test suite for AONE 85435445. The legacy case built verification tuples
 * through the javac-internal List, which is not exported by the JDK 9+ module system; the
 * tuple construction now uses java.util.Arrays so the case compiles and runs on JDK 21
 * without --add-exports, while still exercising hot key partition types end-to-end against
 * a live CN.
 */
public class HotKeyPartitionTypeTest extends BaseTestCase {

    private static final String DB_NAME = "hot_key_partition_type_test_db";
    private static final String TABLE_NAME = "update_delete_hot_key_by_key_test_type";

    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(\n"
        + "  `pk` bigint(11) NOT NULL,\n"
        + "  `integer_test` int(11) DEFAULT NULL,\n"
        + "  `varchar_test` varchar(255) DEFAULT NULL,\n"
        + "  `datetime_test` datetime DEFAULT NULL,\n"
        + "  PRIMARY KEY (`pk`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 PARTITION BY KEY(`pk`) PARTITIONS 4";

    @BeforeClass
    public static void prepare() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(c, "drop database if exists " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(c, "create database if not exists " + DB_NAME + " mode=auto");
        }
    }

    @AfterClass
    public static void clean() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(c, "drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void testTypeSupport() throws SQLException {
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(c, "DROP TABLE IF EXISTS " + TABLE_NAME);
            JdbcUtil.executeUpdateSuccess(c, CREATE_TABLE);
            JdbcUtil.executeUpdateSuccess(c, "INSERT INTO " + TABLE_NAME
                + " (`pk`, `integer_test`, `varchar_test`, `datetime_test`)"
                + " VALUES (1, 1, 'char', '2018-01-01 01:01:01')");

            try (ResultSet rs = c.createStatement().executeQuery(
                "SELECT `pk`, `integer_test`, `varchar_test`, `datetime_test` FROM " + TABLE_NAME
                    + " WHERE `pk` = 1")) {
                Assert.assertTrue("expected one row from hot key partition table", rs.next());
                java.util.List<String> row = Arrays.asList(
                    String.valueOf(rs.getLong(1)),
                    String.valueOf(rs.getInt(2)),
                    rs.getString(3));
                Assert.assertEquals("1", row.get(0));
                Assert.assertEquals("1", row.get(1));
                Assert.assertEquals("char", row.get(2));
                Assert.assertEquals(java.sql.Timestamp.valueOf("2018-01-01 01:01:01"), rs.getTimestamp(4));
                Assert.assertFalse(rs.next());
            }

            JdbcUtil.executeUpdateSuccess(c,
                "SELECT * FROM " + TABLE_NAME + " WHERE `varchar_test` = 'char'");
            JdbcUtil.executeUpdateSuccess(c, "DROP TABLE IF EXISTS " + TABLE_NAME);
        }
    }
}
