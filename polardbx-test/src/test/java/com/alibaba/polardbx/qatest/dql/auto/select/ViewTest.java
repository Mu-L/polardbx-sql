package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * @author fangwu
 */
public class ViewTest extends BaseTestCase {
    private static final String DB_NAME = randomTableName("view_test_db", 12);
    private static final String TB_NAME = randomTableName("view_test_tb", 12);

    private static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s (\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  KEY `l_i_order` (`order_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 partition by hash(`order_id`) partitions 2";

    @BeforeClass
    public static void prepare() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
            c.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            c.createStatement().execute("use " + DB_NAME);
            c.createStatement().execute(String.format(CREATE_TABLE, TB_NAME));
            c.createStatement().execute("insert into " + TB_NAME + " values(1, '1', '1')");
            c.createStatement().execute("set global ENABLE_USE_VIEW=true");
            c.createStatement().execute("set global ENABLE_CREATE_VIEW=true");
        }
    }

    @AfterClass
    public static void clean() throws Exception {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + DB_NAME);
        }
    }

    @Test
    public void testViewAlter() throws Exception {
        final String viewName = randomTableName("view_test_alter", 12);
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String sql = "create view " + viewName + " as select id, order_id from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            sql = "select * from " + viewName;
            ResultSet rs = c.createStatement().executeQuery(sql);
            Assert.assertTrue(rs.getMetaData().getColumnCount() == 2);

            c.createStatement().execute("alter view " + viewName + " as select id from " + TB_NAME + " limit 1");
            rs = c.createStatement().executeQuery(sql);
            Assert.assertTrue(rs.getMetaData().getColumnCount() == 1);
        }
    }

    @Test
    public void testViewFix() throws Exception {
        final String viewName = randomTableName("view_test_fix", 12);
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("drop view if exists " + viewName);
            String sql = "create view " + viewName + " as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            sql = " select * from " + viewName;
            c.createStatement().executeQuery("baseline fix sql /*TDDL:a()*/" + sql);

            ResultSet rs = c.createStatement().executeQuery("explain " + sql);
            StringBuilder sb = new StringBuilder();
            String source = "";
            String baselineId1 = "";
            String planId1 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId1 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId1 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX");

            c.createStatement().execute("alter view " + viewName + " as select 1 from " + TB_NAME + " limit 1");
            rs = c.createStatement().executeQuery("explain " + sql);
            sb.setLength(0);
            String baselineId2 = "";
            String planId2 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId2 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId2 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE");
            assert baselineId1.equals(baselineId2);
            assert !planId2.equals(planId1);
        }
    }

    @Test
    public void testViewFixWhenTableChange() throws Exception {
        final String viewName = randomTableName("view_test_table_change", 12);
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String sql = "create view " + viewName + " as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            sql = " select * from " + viewName;
            c.createStatement().executeQuery("baseline fix sql /*TDDL:a()*/" + sql);

            ResultSet rs = c.createStatement().executeQuery("explain " + sql);
            StringBuilder sb = new StringBuilder();
            String source = "";
            String baselineId1 = "";
            String planId1 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId1 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId1 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX");

            c.createStatement().execute("alter table " + TB_NAME + " add column create_time3 date");
            rs = c.createStatement().executeQuery("explain " + sql);
            sb.setLength(0);
            String baselineId2 = "";
            String planId2 = "";
            while (rs.next()) {
                String line = rs.getString(1);
                if (line.startsWith("Source:")) {
                    source = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("BaselineInfo Id:")) {
                    baselineId2 = line.substring(line.indexOf(":") + 1);
                } else if (line.startsWith("PlanInfo Id:")) {
                    planId2 = line.substring(line.indexOf(":") + 1);
                }
            }
            assert source.equals("SPM_FIX_DDL_HASHCODE_UPDATE");
            assert baselineId1.equals(baselineId2);
            assert !planId2.equals(planId1);
        }
    }

    @Test
    public void testViewCreateWithPartition() throws Exception {
        final String viewName = randomTableName("view_test_partition", 12);
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            String sql = "create view " + viewName + " as select * from %s partition(p2) limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));

            sql = "select * from " + viewName;

            c.createStatement().executeQuery(sql);

            sql = "explain select * from `" + viewName + "`";
            ResultSet rs = c.createStatement().executeQuery(sql);
            while (rs.next()) {
                String line = rs.getString(1).toLowerCase();
                if (line.contains("logicalview")) {
                    Assert.assertTrue(line.contains(TB_NAME.toLowerCase() + "[p2]"));
                    return;
                }
            }
            Assert.fail("not found logicalview");
        }
    }

    @Test
    public void testViewCreateEnable() throws Exception {
        final String viewName = randomTableName("view_test_enable", 12);
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("set global ENABLE_CREATE_VIEW=false");
            String sql = "create view " + viewName + " as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));
            Assert.fail("not found logicalview");
        } catch (Exception e) {
            e.printStackTrace();
            if (!e.getMessage().contains("CREATE VIEW is not ENABLED")) {
                Assert.fail("ENABLE_CREATE_VIEW is false");
            }
        } finally {
            try (Connection c = getPolardbxConnection(DB_NAME)) {
                c.createStatement().execute("set global ENABLE_CREATE_VIEW=true");
            }
        }
    }

    @Test
    public void testViewUseNotEnabled() throws Exception {
        final String viewName = randomTableName("view_test_use_enable", 12);
        try (Connection c = getPolardbxConnection(DB_NAME)) {
            c.createStatement().execute("set global ENABLE_CREATE_VIEW=true");
            String sql = "create view if not exists " + viewName + " as select * from %s limit 1";
            c.createStatement().execute(String.format(sql, TB_NAME));

            c.createStatement().execute("set global ENABLE_USE_VIEW=true");
            c.createStatement().execute("select * from " + viewName);

            try {
                c.createStatement().execute("set global ENABLE_USE_VIEW=false");
                c.createStatement().execute("/*TDDL:a()*/select * from " + viewName);
                Assert.fail("show report error");
            } catch (Exception e) {
                e.printStackTrace();
                if (!e.getMessage().contains("View error: view is not enabled")) {
                    Assert.fail("ENABLE_USE_VIEW test error msg is not expected");
                }
            }

            try {
                c.createStatement().execute("select * from " + viewName);
                Assert.fail("show report error");
            } catch (Exception e) {
                e.printStackTrace();
                if (!e.getMessage().contains("View error: view is not enabled")) {
                    Assert.fail("ENABLE_USE_VIEW test error msg is not expected");
                }
            }
        } finally {
            try (Connection c = getPolardbxConnection(DB_NAME)) {
                c.createStatement().execute("set global ENABLE_USE_VIEW=true");
            }
        }
    }
}
