package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@NotThreadSafe
public class ExprRoutTest extends DDLBaseNewDBTestCase {

    private static final String dataBaseName = "xuandi_route_test";

    private static final String CREATE_TABLE_TMPL_1 = "CREATE TABLE test_route (\n"
        + "    id INT AUTO_INCREMENT PRIMARY KEY,\n"
        + "    username VARCHAR(255) NOT NULL,\n"
        + "    age INT  NOT NULL\n"
        + ") ENGINE=InnoDB DEFAULT \n"
        + "dbpartition by hash(`username`)  tbpartition by hash(`age`) tbpartitions 4;";

    private static final String CREATE_TABLE_TMPL_2 = "CREATE TABLE test_route1 (\n"
        + "    id INT AUTO_INCREMENT PRIMARY KEY,\n"
        + "    username VARCHAR(255) NOT NULL,\n"
        + "    age INT  NOT NULL\n"
        + ") ENGINE=InnoDB DEFAULT \n"
        + "dbpartition by hash(`username`)  tbpartition by hash(`age`) tbpartitions 4;";

    private static final String SQL1 = "trace select * from test_route where username='Abc' and age=17;";
    private static final String SQL2 = "trace select * from test_route where username=0x416263 and age=17;";
    private static final String SQL3 = "trace select * from test_route where username='Abc' and age=16+1;";
    private static final String SQL4 = "trace select * from test_route where username='Abc' and age+1=17;";
    private static final String SQL5 = "trace select * from test_route where username='Abc' and age in (17);";
    private static final String SQL6 =
        "trace select * from test_route where username='Abc' and age = cast('1' as unsigned) + 16;";
    private static final String SQL7 = "trace select * from test_route where username='Abc' or age in (17);";
    private static final String SQL8 = "trace select * from test_route;";
    private static final String SQL9 = "trace delete from test_route where username=0x416263 and age=16+1;";
    private static final String SQL10 = "trace select * from test_route where username='Abc'";
    private static final String SQL11 =
        "trace select * from test_route where username=convert('0x416263' using utf8) and age = cast('1' as unsigned) + 16;";
    private static final String SQL12 =
        "trace select * from test_route inner join test_route1 on test_route.username=test_route1.username and test_route.age=test_route1.age where test_route.username=0x416263 and test_route.age=16+1; ";
    private static final String SQL13 =
        "trace select * from test_route where age=17;";

    private int mode;

    @Before
    public void before() {
        doReCreateDatabase();
    }

    @After
    public void after() {
        doClearDatabase();
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"4\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'drds'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + dataBaseName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        JdbcUtil.executeUpdate(tddlConnection, CREATE_TABLE_TMPL_1);
        JdbcUtil.executeUpdate(tddlConnection, CREATE_TABLE_TMPL_2);

        switch (mode) {
        case 1:
            JdbcUtil.executeUpdate(tddlConnection,
                "set session ENABLE_DRDS_REX_ROUTE=false;set session ENABLE_DRDS_OPTIMIZE_REX_ROUTE=false;set session merge_union=false;");
            break;
        case 2:
            JdbcUtil.executeUpdate(tddlConnection,
                "set session ENABLE_DRDS_REX_ROUTE=true;set session ENABLE_DRDS_OPTIMIZE_REX_ROUTE=false;set session merge_union=false;");
            break;
        case 3:
            JdbcUtil.executeUpdate(tddlConnection,
                "set session ENABLE_DRDS_REX_ROUTE=true;set session ENABLE_DRDS_OPTIMIZE_REX_ROUTE=true;set session merge_union=false;");
            break;
        case 4:
            JdbcUtil.executeUpdate(tddlConnection,
                "set session ENABLE_DRDS_REX_ROUTE=false;set session ENABLE_DRDS_OPTIMIZE_REX_ROUTE=true;set session merge_union=false;");
            break;
        default:
            break;
        }

    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + dataBaseName;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    public ExprRoutTest(int mode) {
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{index}:mode={0}")
    public static List<Integer[]> prepare() {
        List<Integer[]> result = new ArrayList<>();
        result.add(new Integer[] {1});
        result.add(new Integer[] {2});
        result.add(new Integer[] {3});
        result.add(new Integer[] {4});
        return result;
    }

    @Test
    public void testSimple() throws Exception {
        switch (mode) {
        case 1: {
            int fullScanCnt = calculateTraceCount(SQL8);
            int groupScanCnt = calculateTraceCount(SQL10);
            int traceCount = calculateTraceCount(SQL1);
            int tableScanCnt = calculateTraceCount(SQL13);
            Assert.assertEquals(traceCount, 1);

            traceCount = calculateTraceCount(SQL2);
            Assert.assertTrue(traceCount == tableScanCnt);

            traceCount = calculateTraceCount(SQL3);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL4);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL5);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL6);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL7);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL9);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL11);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL12);
            Assert.assertTrue(traceCount == fullScanCnt);

            break;
        }
        case 2: {
            int fullScanCnt = calculateTraceCount(SQL8);
            int groupScanCnt = calculateTraceCount(SQL10);
            int traceCount = calculateTraceCount(SQL1);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL2);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL3);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL4);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL5);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL6);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL7);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL9);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL11);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL12);
            Assert.assertTrue(traceCount == 1);
            break;
        }
        case 3: {
            int fullScanCnt = calculateTraceCount(SQL8);
            int groupScanCnt = calculateTraceCount(SQL10);
            int traceCount = calculateTraceCount(SQL1);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL2);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL3);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL4);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL5);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL6);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL7);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL9);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL11);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL12);
            Assert.assertTrue(traceCount == 1);
            break;
        }
        case 4: {
            int fullScanCnt = calculateTraceCount(SQL8);
            int groupScanCnt = calculateTraceCount(SQL10);
            int traceCount = calculateTraceCount(SQL1);
            int tableScanCnt = calculateTraceCount(SQL13);
            Assert.assertEquals(traceCount, 1);

            traceCount = calculateTraceCount(SQL2);
            Assert.assertTrue(traceCount == tableScanCnt);

            traceCount = calculateTraceCount(SQL3);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL4);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL5);
            Assert.assertTrue(traceCount == 1);

            traceCount = calculateTraceCount(SQL6);
            Assert.assertTrue(traceCount == groupScanCnt);

            traceCount = calculateTraceCount(SQL7);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL9);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL11);
            Assert.assertTrue(traceCount == fullScanCnt);

            traceCount = calculateTraceCount(SQL12);
            Assert.assertTrue(traceCount == fullScanCnt);

            break;
        }
        default:
            break;
        }

    }

    private int calculateTraceCount(String sql) throws Exception {
        Statement statement1 = tddlConnection.createStatement();
        statement1.executeQuery(sql);
        statement1.close();
        Statement statement2 = tddlConnection.createStatement();
        ResultSet rs2 = statement2.executeQuery("show trace");
        int traceCount = 0;
        while (rs2.next()) {
            traceCount++;
        }
        statement2.close();
        return traceCount;
    }

}
