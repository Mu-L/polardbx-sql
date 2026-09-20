package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StatisticSpecialColumnTypeTest extends BaseTestCase {
    static final String DB = "statistic_bit_test";

    @BeforeClass
    public static void prepare() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists statistic_bit_test");
            c.createStatement().execute("create database if not exists statistic_bit_test");
        }
    }

    @Test
    public void testBitType() {
        String createTable = "CREATE TABLE IF NOT EXISTS `bit_test` (\n"
            + "\t`id` int NOT NULL,\n"
            + "\t`name` varchar(30) DEFAULT NULL,\n"
            + "\t`int_col2` int DEFAULT NULL,\n"
            + "\t`bit1` bit(1) DEFAULT NULL,\n"
            + "\t`bit2` bit(2) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb3";

        try (Connection c = getPolardbxConnection0(DB)) {
            c.createStatement().execute(createTable);
            c.createStatement()
                .execute("insert into bit_test(id, name, int_col2, bit1, bit2) values (1, 'name1', 1, 1, 0)");
            c.createStatement().execute("analyze table bit_test");
            ResultSet rs = c.createStatement().executeQuery(
                "select * from information_schema.statistics_data where schema_name = 'statistic_bit_test' and column_name in ('bit1', 'bit2')");
            int count = 0;
            while (rs.next()) {
                String histogram = rs.getString("HISTOGRAM");
                System.out.println(histogram);
                Assert.assertTrue(!StringUtils.isEmpty(histogram));
                count++;
            }
            System.out.println(count);
            assert count >= 2;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
