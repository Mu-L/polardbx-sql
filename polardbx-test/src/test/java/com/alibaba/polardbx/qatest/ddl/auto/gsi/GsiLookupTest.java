package com.alibaba.polardbx.qatest.ddl.auto.gsi;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;

public class GsiLookupTest extends BaseTestCase {

    @Test
    public void testGsiTest1() throws SQLException {
        createDBAndTbForGsiTest1();

        try (Connection c = getPolardbxConnection("gsi_test_1")) {
            // 写入模拟数据
            for (int i = 0; i < 2000; i++) {
                c.createStatement()
                    .execute("insert into t_order(order_id, buyer_id, seller_id, order_snapshot, order_detail, status)"
                        + " values('" + i + "', 'b1', 's1', 'mock_order_1', 'mock_order_detail_1', '1')");
            }

            // 测试lookup优化因主表没有索引覆盖导致拒绝
            String sql = "trace select * from t_order force index(g_i_seller) where seller_id = 's1'";
            c.createStatement().execute(sql);
            ResultSet rs = c.createStatement().executeQuery("show trace");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isGreaterThan(17);

            // 测试lookup优化主表有索引覆盖而触发
            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id = 'b1'";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(17);

            // 测试lookup优化因 limit 被禁止
            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id = 'b1' limit 2000";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isGreaterThan(17);

            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id = 'b1' limit 10, 2000";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isGreaterThan(17);

            // 测试lookup优化因 order 被禁止
            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id = 'b1' order by id";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isGreaterThan(17);
        }
    }

    @Test
    public void testGsiTest2MultiShardingColumns() throws SQLException {
        createDBAndTbForGsiTest2();

        try (Connection c = getPolardbxConnection("gsi_test_2")) {
            // 写入模拟数据
            for (int i = 0; i < 2000; i++) {
                c.createStatement()
                    .execute("insert into t_order(order_id, buyer_id, seller_id, order_snapshot, order_detail, status)"
                        + " values('" + i + "', 'b1', 's1', 'mock_order_1', 'mock_order_detail_1', '1')");
            }

            // 测试lookup优化因主表没有索引覆盖全导致拒绝
            String sql = "trace select * from t_order force index(g_i_seller) where seller_id = 's1'";
            c.createStatement().execute(sql);
            ResultSet rs = c.createStatement().executeQuery("show trace");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isGreaterThan(17);

            // 测试lookup优化虽然条件没有覆盖所有索引列,但依然可以触发优化
            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id = 'b1'";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(17);

            // 测试其它条件的触发情况
            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id != 'b2'";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(32);

            sql = "trace select * from t_order force index(g_i_buyer) where buyer_id in('b1', 'b3')";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(18);

            // test 函数对 gsi 的特殊影响
            sql = "trace select *, length(status) from t_order force index(g_i_buyer) where buyer_id in('b1', 'b3')";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(18);

            sql =
                "trace select buyer_id,seller_id, count(status) from t_order force index(g_i_buyer) where buyer_id in('b1', 'b3') group by buyer_id,seller_id;";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(18);

            // 测试多列条件的影响
            sql = "trace select buyer_id,seller_id, count(status) from t_order force index(g_i_buyer) "
                + "where status=1 and buyer_id in('b1', 'b3') group by buyer_id,seller_id;";
            c.createStatement().execute(sql);
            rs = c.createStatement().executeQuery("show trace");
            count = 0;
            while (rs.next()) {
                count++;
            }
            rs.close();
            System.out.println(count);
            assertThat(count).isEqualTo(18);
        }
    }

    private void createDBAndTbForGsiTest1() throws SQLException {
        // create database and table
        String createDB = "create database if not exists gsi_test_1 mode=auto";
        String createTb = "CREATE TABLE `t_order` (\n"
            + "\t`id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(32) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(32) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(32) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\t`status` varchar(10) DEFAULT '',\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX `g_i_buyer` (`buyer_id`) COVERING (`order_id`, `order_snapshot`)\n"
            + "\t\tPARTITION BY KEY(`buyer_id`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tGLOBAL INDEX `g_i_seller` (`seller_id`) COVERING (`order_id`, `order_snapshot`)\n"
            + "\t\tPARTITION BY KEY(`seller_id`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tKEY `l_i_order` (`order_id`),\n"
            + "\tKEY `idx_buyer_id` (`buyer_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 607705 DEFAULT CHARSET = utf8mb3\n"
            + "PARTITION BY KEY(`order_id`)\n"
            + "PARTITIONS 16";
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists gsi_test_1");
            c.createStatement().execute(createDB);
            c.createStatement().execute("use gsi_test_1");
            c.createStatement().execute(createTb);
        }
    }

    private void createDBAndTbForGsiTest2() throws SQLException {
        // create database and table
        String createDB = "create database if not exists gsi_test_2 mode=auto";
        String createTb = "CREATE TABLE `t_order` (\n"
            + "\t`id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(32) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(32) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(32) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\t`order_detail` longtext,\n"
            + "\t`status` varchar(10) DEFAULT '',\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tGLOBAL INDEX `g_i_buyer` (`buyer_id`, `status`) COVERING (`order_id`, `order_snapshot`)\n"
            + "\t\tPARTITION BY KEY(`buyer_id`, `status`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tGLOBAL INDEX `g_i_seller` (`seller_id`, `status`) COVERING (`order_id`, `order_snapshot`)\n"
            + "\t\tPARTITION BY KEY(`seller_id`, `status`)\n"
            + "\t\tPARTITIONS 16,\n"
            + "\tKEY `l_i_order` (`order_id`),\n"
            + "\tKEY `idx_buyer_id_status` (`buyer_id`, `status`),\n"
            + "\tKEY `idx_seller_id` (`seller_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 607705 DEFAULT CHARSET = utf8mb3\n"
            + "PARTITION BY KEY(`order_id`)\n"
            + "PARTITIONS 16";
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists gsi_test_2");
            c.createStatement().execute(createDB);
            c.createStatement().execute("use gsi_test_2");
            c.createStatement().execute(createTb);
        }
    }
}
