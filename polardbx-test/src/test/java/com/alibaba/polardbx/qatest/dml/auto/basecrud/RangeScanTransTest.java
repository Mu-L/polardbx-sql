package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class RangeScanTransTest extends ReadBaseTestCase {
    String tableName = "order1";
    String tableDef = "CREATE TABLE " + tableName + " (`id` bigint NOT NULL AUTO_INCREMENT,\n"
        + "        `red_serial_number` varchar(32) COLLATE utf8mb4_general_ci DEFAULT NULL ,\n"
        + "        `operate_time` datetime(3) NOT NULL,\n"
        + "        `customer_phone` varchar(20) DEFAULT NULL,\n"
        + "        PRIMARY KEY (`id`),\n"
        + "        KEY `operate_time` (`operate_time`),\n"
        + "        KEY `auto_shard_key_customer_phone` USING BTREE (`customer_phone`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci \n"
        + "PARTITION BY KEY(`customer_phone`)\n"
        + "PARTITIONS 2\n"
        + "SUBPARTITION BY RANGE COLUMNS(`operate_time`)\n"
        + "(SUBPARTITION `sp20250705` VALUES LESS THAN ('2025-07-06 00:00:00.000'),\n"
        + " SUBPARTITION `sp20250706` VALUES LESS THAN ('2025-07-07 00:00:00.000'),\n"
        + " SUBPARTITION `sp20250707` VALUES LESS THAN ('2025-07-08 00:00:00.000'),\n"
        + " SUBPARTITION `sp20250708` VALUES LESS THAN ('2025-07-09 00:00:00.000'),\n"
        + " SUBPARTITION `sp20250709` VALUES LESS THAN ('2025-07-10 00:00:00.000'))";

    String sql = "select * from order1 where customer_phone =1 order by operate_time desc";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void prepare() {
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, tableDef);
    }

    @After
    public void clear() {
        JdbcUtil.dropTable(tddlConnection, tableName);
    }

    @Test
    public void testRangeScanRc() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                connection.setAutoCommit(false);
                List<List<Object>> result =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery("explain physical " + sql, connection));
                boolean hasRangeScan = false;
                for (List<Object> row : result) {
                    for (Object obj : row) {
                        if (obj != null && obj.toString().toLowerCase().contains("rangescan")) {
                            hasRangeScan = true;
                        }
                    }
                }
                Truth.assertThat(hasRangeScan).isFalse();
                JdbcUtil.executeQuery(sql, connection);
            } finally {
                connection.rollback();
                connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            }
        }
    }

    @Test
    public void testRangeScanRR() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            try {
                connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                connection.setAutoCommit(false);
                List<List<Object>> result =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery("explain physical " + sql, connection));
                boolean hasRangeScan = false;
                for (List<Object> row : result) {
                    for (Object obj : row) {
                        if (obj != null && obj.toString().toLowerCase().contains("rangescan")) {
                            hasRangeScan = true;
                        }
                    }
                }
                Truth.assertThat(hasRangeScan).isTrue();
                JdbcUtil.executeQuery(sql, connection);
            } finally {
                connection.rollback();
                connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            }
        }
    }
}
