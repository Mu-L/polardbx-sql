package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public class DeepPageTest extends AutoReadBaseTestCase {

    private static final String DB_NAME = PropertiesUtil.polardbXDBName1(true);

    private static final String TABLE1 = "deep_page_tb1";

    private static final String TABLE2 = "deep_page_tb2";

    private static final String TABLE3 = "deep_page_tb3";

    private final static int ROW_COUNT = 1000;

    private final static int MAX_EXECUTE_TIME = 20 * 1000;

    private final static int MAX_EXECUTE_COUNT = 20;

    private final static int FETCH_SIZE = 20;

    private final static int INIT_OFFSET = 200;

    private final static boolean LOCAL_DEBUG = false;

    private static boolean useJDBC = false;

    @BeforeClass
    public static void beforeClass() throws Exception {

        Connection connection = getPolardbxConnection0(DB_NAME);
        if (useJDBCProtocol(connection)) {
            useJDBC = true;
        }

        if (LOCAL_DEBUG) {
            return;
        }

        String createTableSql1 = String.format(
            "create table if not exists %s(col1 int primary key, col2 int, col3 int, col4 int, col5 int, " +
                "global unique index ugsi_col4(col4) partition by hash(col4), unique key(col5)) partition by hash(col1);",
            TABLE1);
        JdbcUtil.dropTable(connection, TABLE1);
        JdbcUtil.executeSuccess(connection, createTableSql1);
        insertRandomData(connection, ROW_COUNT, TABLE1, Arrays.asList("col1", "col2", "col3", "col4", "col5"));

        String createTableSql2 = String.format(
            "create table if not exists %s(col1 int, col2 int, col3 int, col4 int, col5 int, " +
                "primary key(col1, col2), global unique index ugsi_col4_5(col3, col4) partition by hash(col4), unique key(col5)) partition by hash(col1, col2);",
            TABLE2);
        JdbcUtil.dropTable(connection, TABLE2);
        JdbcUtil.executeSuccess(connection, createTableSql2);
        insertRandomData(connection, ROW_COUNT, TABLE2, Arrays.asList("col1", "col2", "col3", "col4", "col5"));

        String createTableSql3 = String.format("create table if not exists %s (\n" +
            "    id INT AUTO_INCREMENT PRIMARY KEY,\n" +
            "    int_column INT,\n" +
            "    float_column FLOAT,\n" +
            "    double_column DOUBLE,\n" +
            "    decimal_column DECIMAL(10, 5),\n" +
            "    char_column CHAR(10),\n" +
            "    varchar_column VARCHAR(50),\n" +
            "    text_column TEXT,\n" +
            "    date_column DATE,\n" +
            "    time_column TIME,\n" +
            "    datetime_column DATETIME,\n" +
            "    timestamp_column TIMESTAMP,\n" +
            "    year_column YEAR,\n" +
            "        LOCAL KEY `idx_int` (`int_column`),\n" +
            "        LOCAL KEY `idx_float` (`float_column`),\n" +
            "        LOCAL KEY `idx_double` (`double_column`),\n" +
            "        LOCAL KEY `idx_decimal` (`decimal_column`),\n" +
            "        LOCAL KEY `idx_char` (`char_column`),\n" +
            "        LOCAL KEY `idx_varchar` (`varchar_column`),\n" +
            "        LOCAL KEY `idx_date` (`date_column`),\n" +
            "        LOCAL KEY `idx_time` (`time_column`),\n" +
            "        LOCAL KEY `idx_datetime` (`datetime_column`),\n" +
            "        LOCAL KEY `idx_timestamp` (`timestamp_column`),\n" +
            "        LOCAL KEY `idx_year` (`year_column`)" +
            ") partition by hash(id);", TABLE3);
        JdbcUtil.dropTable(connection, TABLE3);
        JdbcUtil.executeSuccess(connection, createTableSql3);
        for (int i = 0; i < ROW_COUNT; i++) {
            String insertTable3Sql = "INSERT INTO " + TABLE3 + " (\n" +
                "            int_column,\n" +
                "            float_column,\n" +
                "            double_column,\n" +
                "            decimal_column,\n" +
                "            char_column,\n" +
                "            varchar_column,\n" +
                "            text_column,\n" +
                "            date_column,\n" +
                "            time_column,\n" +
                "            datetime_column,\n" +
                "            timestamp_column,\n" +
                "            year_column\n" +
                "        ) VALUES (\n" +
                "            FLOOR(RAND() * 1000),                                \n" +
                "            RAND() * 1000,                                       \n" +
                "            RAND(),                                       \n" +
                "            ROUND(RAND() * 1000, 5),                             \n" +
                "            CONCAT('Str', FLOOR(RAND() * 100)),                  \n" +
                "            CONCAT('VarStr', FLOOR(RAND() * 100)),               \n" +
                "            CONCAT('This is random text number: ', FLOOR(RAND() * 1000)), \n" +
                "            CURDATE() + INTERVAL FLOOR(RAND() * 365) DAY,       \n" +
                "            SEC_TO_TIME(FLOOR(RAND() * 86400)),                 \n" +
                "            NOW() + INTERVAL FLOOR(RAND() * 365) DAY,            \n" +
                "            NOW(),                                               \n" +
                "            2000 + FLOOR(RAND() * 20)                           \n" +
                "        );";
            JdbcUtil.executeUpdateSuccess(connection, insertTable3Sql);
        }
    }

    public static void insertRandomData(Connection connection, int maxRowCount, String tableName,
                                        List<String> colNameList) {
        String colNames = StringUtils.join(colNameList, ",");
        StringJoiner sj = new StringJoiner(",");
        for (int i = 0; i < colNameList.size(); i++) {
            sj.add("FLOOR(RAND() * 100000000)");
        }
        String values = sj.toString();
        JdbcUtil.executeSuccess(connection,
            "insert into " + tableName + " (" + colNames + ") " + " values(" + values + ")");
        int rowCount = 1;
        String randomInsertSql = String.format("" +
            "INSERT ignore INTO %s (" + colNames + ")\n" +
            "SELECT " + values +
            "FROM \n" +
            "    (select * from %s) AS e", tableName, tableName);
        while (rowCount < maxRowCount) {
            JdbcUtil.executeSuccess(connection, randomInsertSql);
            rowCount += rowCount;
        }
    }

    public static boolean useJDBCProtocol(Connection connection) throws Exception {
        String showDataSourceSql = "show datasources";
        ResultSet rs = JdbcUtil.executeQuery(showDataSourceSql, connection);
        while (rs.next()) {
            String url = rs.getString("URL");
            if (url.trim().startsWith("jdbc")) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testJoinDeePageQuery() throws Exception {
        testDeepPageWithSameResult(createJoinDeepPageSqlList(TABLE1, TABLE3, "*", null,
            TABLE1 + ".col1," + TABLE3 + ".id", TABLE1 + ".col1 = " + TABLE3 + ".int_column"));
        testDeepPageWithSameResult(createJoinDeepPageSqlList(TABLE1, TABLE3, "*", null,
            TABLE1 + ".col1," + TABLE3 + ".id", TABLE1 + ".col2 = " + TABLE3 + ".int_column"));
        testDeepPageWithSameResult(createJoinDeepPageSqlList(TABLE1, TABLE3, "*", null,
            TABLE1 + ".col1," + TABLE3 + ".id", TABLE1 + ".col2 = " + TABLE3 + ".id"));

        testDeepPageWithSameResult(createJoinDeepPageSqlList(TABLE1, TABLE3, "*", null,
            TABLE1 + ".col1 desc," + TABLE3 + ".id desc", TABLE1 + ".col1 = " + TABLE3 + ".int_column"));
        testDeepPageWithSameResult(createJoinDeepPageSqlList(TABLE1, TABLE3, "*", null,
            TABLE1 + ".col1 desc," + TABLE3 + ".id desc", TABLE1 + ".col2 = " + TABLE3 + ".int_column"));
        testDeepPageWithSameResult(createJoinDeepPageSqlList(TABLE1, TABLE3, "*", null,
            TABLE1 + ".col1 desc," + TABLE3 + ".id desc", TABLE1 + ".col2 = " + TABLE3 + ".id"));

    }

    @Test
    public void testAggDeePageQuery() throws Exception {
        testDeepPageWithSameResult(createAggDeepPageSqlList(TABLE1, "col1, max(col2), min(col3), avg(col4)", null,
            "col1", "col1"));
        testDeepPageWithSameResult(createAggDeepPageSqlList(TABLE1, "col1,col2, min(col3), avg(col4)", null,
            "col1,col2", "col1,col2"));
        testDeepPageWithSameResult(createAggDeepPageSqlList(TABLE1, "col3, max(col1), min(col2), avg(col4)", null,
            "col3", "col3"));

        testDeepPageWithSameResult(createAggDeepPageSqlList(TABLE1, "col1, max(col2), min(col3), avg(col4)", null,
            "col1 desc", "col1"));
        testDeepPageWithSameResult(createAggDeepPageSqlList(TABLE1, "col1,col2, min(col3), avg(col4)", null,
            "col1 desc,col2 desc", "col1,col2"));
        testDeepPageWithSameResult(createAggDeepPageSqlList(TABLE1, "col3, max(col1), min(col2), avg(col4)", null,
            "col3 desc", "col3"));
    }

    /**
     * 测试目标：当数据表没有修改时，是否开启deepPageOptimizer，翻页sql结果集应该完全相同
     * 1. 全部升序
     * 2. 全部降序
     */
    @Test
    public void testDeePageQuery() throws Exception {
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col1"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col2, col1"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col1, col2"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col4"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col4, col2"));

        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col1 desc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col2 desc, col1 desc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col1 desc, col2 desc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col4 desc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col4 desc, col2 desc"));

        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col1 asc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col2 asc, col1 asc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col1 asc, col2 asc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col4 asc"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE1, "*", "col4 asc, col2 asc"));
    }

    @Test
    public void testDeePageQueryOrderByDiffType() throws Exception {
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "int_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "decimal_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "char_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "varchar_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "text_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "date_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "time_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "datetime_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "timestamp_column, id"));
        testDeepPageWithSameResult(createDeepPageSqlList(TABLE3, "*", "year_column, id"));
    }

    @Test
    public void testDeePageQueryWithFilter() throws Exception {
        testDeepPageWithSameResult(createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000", "col1"));
        testDeepPageWithSameResult(createDeepPageSqlListWithFilter(TABLE1, "*", "col2 = 100000", "col2, col1"));
        testDeepPageWithSameResult(createDeepPageSqlListWithFilter(TABLE1, "*", "col3 != 10000", "col1, col2"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col4 in (10000, 1234344,412131)", "col4"));
        //testDeepPageWithSameResult(
            //createDeepPageSqlListWithFilter(TABLE1, "*", "col5 not in (10000, 1234344,412131)", "col4, col2"));

        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 and col2 > 10000", "col1 desc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 and col3 > 10000", "col2 desc, col1 desc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 and col4 < 1000000", "col1 desc, col2 desc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 or col5 < 1000000", "col4 desc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 or col4 > 1000000", "col4 desc, col2 desc"));

        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col2 > 10000 and col3 > 10000", "col1 asc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col3 > 10000 or col4 > 10000 ", "col2 asc, col1 asc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 and col3 < 10000000", "col1 asc, col2 asc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col1 > 10000 or (col2 > 10000 and col3 > 10000)",
                "col4 asc"));
        testDeepPageWithSameResult(
            createDeepPageSqlListWithFilter(TABLE1, "*", "col2 > 10000 and (col3 > 10000 or col5 < 10000000)",
                "col4 asc, col2 asc"));
    }

    @Test
    public void testDeePageQueryFinish() throws Exception {
        long count = Long.parseLong(
            JdbcUtil.executeQueryAndGetFirstStringResult(String.format("select count(*) from %s ", TABLE1),
                tddlConnection));

        String sql1 = String.format("select * from %s order by %s limit %s, %s", TABLE1, "col1", count - 5, 10);
        String sql2 = String.format("select * from %s order by %s limit %s, %s", TABLE1, "col1", count, 10);
        String sql3 = String.format("select * from %s order by %s limit %s, %s", TABLE1, "col1", count + 5, 10);

        Connection deepPageConnection = createDeepPageConnection(DB_NAME);
        JdbcUtil.executeSuccess(deepPageConnection, sql1);
        Assert.assertTrue(JdbcUtil.getExplainResult(deepPageConnection, sql2)
            .contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));
        Assert.assertFalse(JdbcUtil.getExplainResult(deepPageConnection, sql3)
            .contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));

        deepPageConnection.close();
    }

    @Test
    public void testDeePageQueryHint() throws Exception {
        String sql1 =
            String.format("/*+TDDL:enable_deep_page_optimizer=true*/select * from %s order by %s limit %s, %s", TABLE1,
                "col1", 0, 10);
        String sql2 =
            String.format("/*+TDDL:enable_deep_page_optimizer=true*/select * from %s order by %s limit %s, %s", TABLE1,
                "col1", 10, 10);

        Connection deepPageConnection = getPolardbxConnection(DB_NAME);
        JdbcUtil.executeSuccess(deepPageConnection, sql1);
        Assert.assertTrue(JdbcUtil.getExplainResult(deepPageConnection, sql2)
            .contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));

        deepPageConnection.close();
    }

    @Test
    public void testDeePageQueryPrepareStmt() throws Exception {
        String sql = "select * from " + TABLE1 + " where col1 > %s order by col1 limit %s,%s";

        try (Connection deepPageConnection = createDeepPageConnection(DB_NAME, true)) {
            PreparedStatement ps1 = deepPageConnection.prepareStatement(
                "select * from " + TABLE1 + " where col1 > ? order by col1 limit ?,?");
            PreparedStatement ps2 =
                tddlConnection.prepareStatement("select * from " + TABLE1 + " where col1 > ? order by col1 limit ?,?");
            for (int i = 0; i < 10; i++) {
                ps1.setInt(1, 10000);
                ps1.setInt(2, i * 10);
                ps1.setInt(3, 10);

                ps2.setInt(1, 10000);
                ps2.setInt(2, i * 10);
                ps2.setInt(3, 10);

                if (i != 0) {
                    Assert.assertTrue(JdbcUtil.getExplainResult(deepPageConnection,
                            String.format(sql, 10000, i * 10, 10))
                        .contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));
                }
                DataValidator.resultSetContentSameAssert(ps1.executeQuery(), ps2.executeQuery(), true);
            }
        }
    }

    @Test
    public void testDeePageQuerySingleTable() {
        //JDBC + single table会触发DeepPageLastRowTrigger报错x
        if (useJDBC) {
            return;
        }

        String tableName = "testDeePageQuerySingleTable_tb";
        String createTableSql1 = String.format(
            "create table if not exists %s(col1 int primary key, col2 int, col3 int, col4 int, col5 int) single;",
            tableName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql1);
        insertRandomData(tddlConnection, 100, tableName, Arrays.asList("col1", "col2", "col3", "col4", "col5"));

        Pair<String, String> sqlPair = createTwoSuccessiveDeepPageSql(tableName, "*", "col1");
        Connection deepPageConnection = createDeepPageConnection(DB_NAME);

        ResultSet rs = JdbcUtil.executeQuery(sqlPair.getKey(), deepPageConnection);
        String deepPagePlan = JdbcUtil.getExplainResult(deepPageConnection, sqlPair.getValue());
        Assert.assertTrue(deepPagePlan.contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));
        DataValidator.resultSetContentSameAssert(
            JdbcUtil.executeQuery(sqlPair.getValue(), tddlConnection),
            JdbcUtil.executeQuery(sqlPair.getValue(), deepPageConnection), false);
        JdbcUtil.dropTable(tddlConnection, tableName);
    }

    @Test
    public void testDeePageQueryNullbleColumn() {
        String tableName = "testDeePageQueryNullbleColumn_tb";
        String createTableSql1 = String.format(
            "create table if not exists %s(col1 int primary key auto_increment, col2 int) partition by hash(col1);",
            tableName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql1);
        for (int i = 0; i < 100; i++) {
            JdbcUtil.executeSuccess(tddlConnection, String.format("insert into %s(col2) values(null)", tableName));
        }

        Connection deepPageConnection = createDeepPageConnection(DB_NAME);
        String sql1 = String.format("select * from %s order by %s limit %s,%s", tableName, "col2", 0, 10);
        String sql2 = String.format("select * from %s order by %s limit %s,%s", tableName, "col2", 10, 10);

        ResultSet rs = JdbcUtil.executeQuery(sql1, deepPageConnection);

        String plan = JdbcUtil.getExplainResult(deepPageConnection, sql2);
        Assert.assertFalse(plan.contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));

        JdbcUtil.dropTable(tddlConnection, tableName);
    }

    /**
     * 测试目标：当order by的列不具有唯一性时，需要将主键列加入order by中
     * 1. order by 主键列
     * 2. order by 普通列
     * 3. order by 唯一键列
     * 4. order by 多列
     */
    @Test
    public void orderByColumnTest() throws Exception {
        testOrderByColumn(TABLE1, Arrays.asList("col1"), Arrays.asList("col1"));
        testOrderByColumn(TABLE1, Arrays.asList("col2", "col1"), Arrays.asList("col2", "col1"));
        testOrderByColumn(TABLE1, Arrays.asList("col1", "col2"), Arrays.asList("col1", "col2"));
        testOrderByColumn(TABLE1, Arrays.asList("col4"), Arrays.asList("col4"));
        testOrderByColumn(TABLE1, Arrays.asList("col4", "col2"), Arrays.asList("col4", "col2"));
        testOrderByColumn(TABLE1, Arrays.asList("col3"), Arrays.asList("col3", "col1"));
        testOrderByColumn(TABLE1, Arrays.asList("col5"), Arrays.asList("col5", "col1"));
        testOrderByColumn(TABLE1, Arrays.asList("col5", "col3"), Arrays.asList("col5", "col3", "col1"));

        testOrderByColumn(TABLE2, Arrays.asList("col2"), Arrays.asList("col2", "col1", "col2"));
        testOrderByColumn(TABLE2, Arrays.asList("col3"), Arrays.asList("col3", "col1", "col2"));
        testOrderByColumn(TABLE2, Arrays.asList("col3", "col4"), Arrays.asList("col3", "col4"));
        testOrderByColumn(TABLE2, Arrays.asList("col4"), Arrays.asList("col4", "col1", "col2"));
        testOrderByColumn(TABLE2, Arrays.asList("col5"), Arrays.asList("col5", "col1", "col2"));
    }

    @Test
    public void testDeepPageExplainExecute() throws Exception {
        String sql1 = String.format("select * from %s order by %s limit %s,%s", TABLE3, "id", 0, 10);
        String sql2 = String.format("select * from %s order by %s limit %s,%s", TABLE3, "id", 10, 10);
        String sql3 =
            String.format("explain execute select * from %s order by %s limit %s,%s", TABLE3, "id", 10, 10);

        Connection deepPageConnection = createDeepPageConnection(DB_NAME);
        JdbcUtil.executeSuccess(deepPageConnection, sql1);
        Assert.assertTrue(JdbcUtil.getExplainResult(deepPageConnection, sql2)
            .contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));

        ResultSet rs = JdbcUtil.executeQuery(sql3, deepPageConnection);
        while (rs.next()) {
            if (rs.getString("table").toLowerCase().equals(TABLE3)) {
                Assert.assertEquals("range", rs.getString("type"));
            }
        }

    }

    private void testOrderByColumn(String tableName, List<String> originOrderByColumns,
                                   List<String> deepPageOrderByColumns) throws Exception {
        Pair<String, String> sqlPair =
            createTwoSuccessiveDeepPageSql(tableName, "*", mergeOrderColumns(originOrderByColumns));
        Connection deepPageConnection = createDeepPageConnection(DB_NAME);
        String plan = JdbcUtil.getExplainResult(deepPageConnection, sqlPair.getKey());
        Assert.assertFalse(plan.contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));
        Assert.assertTrue(plan.contains("ORDER BY " + mergeOrderColumns(originOrderByColumns)));

        JdbcUtil.executeSuccess(deepPageConnection, sqlPair.getKey());
        String deepPagePlan = JdbcUtil.getExplainResult(deepPageConnection, sqlPair.getValue());
        Assert.assertTrue(deepPagePlan.contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER));
        Assert.assertTrue(deepPagePlan.contains("ORDER BY " + mergeOrderColumns(deepPageOrderByColumns)));

        deepPageConnection.close();
    }

    private String mergeOrderColumns(List<String> originOrderByColumns) {
        String orderByColumn = SqlIdentifier.surroundWithBacktick(originOrderByColumns.get(0));
        for (int i = 1; i < originOrderByColumns.size(); i++) {
            orderByColumn += ", " + SqlIdentifier.surroundWithBacktick(originOrderByColumns.get(i));
        }
        return orderByColumn;
    }

    private Pair<String, String> createTwoSuccessiveDeepPageSql(String tableName, String selectColumns,
                                                                String orderByColumns) {
        String sql1 =
            String.format("select %s from %s order by %s limit %s,%s", selectColumns, tableName, orderByColumns, 0, 10);
        String sql2 =
            String.format("select %s from %s order by %s limit %s,%s", selectColumns, tableName, orderByColumns, 10,
                10);
        return Pair.of(sql1, sql2);
    }

    public List<String> createDeepPageSqlList(String tableName, String selectColumns, String orderByColumns) {
        return createDeepPageSqlListWithFilter(tableName, selectColumns, null, orderByColumns);
    }

    public List<String> createDeepPageSqlListWithFilter(String tableName, String selectColumns, String filterCondition,
                                                        String orderByColumns) {
        int offset = ThreadLocalRandom.current().nextInt(INIT_OFFSET);
        List<String> deepPageList = new ArrayList<>();

        for (int i = 0; i < MAX_EXECUTE_COUNT; i++) {
            int fetchSize = ThreadLocalRandom.current().nextInt(1, FETCH_SIZE);

            String sql = "select " + selectColumns + " from " + tableName;
            if (filterCondition != null) {
                sql += " where " + filterCondition;
            }
            sql += " order by " + orderByColumns + " limit " + offset + "," + fetchSize;
            deepPageList.add(sql);
            offset += fetchSize;
        }
        return deepPageList;
    }

    public List<String> createJoinDeepPageSqlList(String tableName1, String tableName2, String selectColumns,
                                                  String filterCondition, String orderByColumns, String joinCondition) {
        int offset = ThreadLocalRandom.current().nextInt(INIT_OFFSET);
        List<String> deepPageList = new ArrayList<>();

        for (int i = 0; i < MAX_EXECUTE_COUNT; i++) {
            int fetchSize = ThreadLocalRandom.current().nextInt(1, FETCH_SIZE);

            String sql =
                "select " + selectColumns + " from " + tableName1 + " join " + tableName2 + " on " + joinCondition;
            if (filterCondition != null) {
                sql += " where " + filterCondition;
            }
            sql += " order by " + orderByColumns + " limit " + offset + "," + fetchSize;
            deepPageList.add(sql);
            offset += fetchSize;
        }
        return deepPageList;
    }

    public List<String> createAggDeepPageSqlList(String tableName, String selectColumns, String filterCondition,
                                                 String orderByColumns, String groupByColumns) {
        int offset = ThreadLocalRandom.current().nextInt(INIT_OFFSET);
        List<String> deepPageList = new ArrayList<>();

        for (int i = 0; i < MAX_EXECUTE_COUNT; i++) {
            int fetchSize = ThreadLocalRandom.current().nextInt(1, FETCH_SIZE);

            String sql = "select " + selectColumns + " from " + tableName;
            if (filterCondition != null) {
                sql += " where " + filterCondition;
            }
            sql += " group by " + groupByColumns;
            sql += " order by " + orderByColumns + " limit " + offset + "," + fetchSize;
            deepPageList.add(sql);
            offset += fetchSize;
        }
        return deepPageList;
    }

    /**
     * 执行100次或者20s
     */
    public void testDeepPageWithSameResult(List<String> deepPageSqlList) throws Exception {

        long start = System.currentTimeMillis();

        Connection deepPageConnection = createDeepPageConnection(DB_NAME);

        Connection normalConnection = getPolardbxConnection(DB_NAME);

        String firstSql = deepPageSqlList.get(0);
        DataValidator.selectContentSameAssert(firstSql, null, normalConnection, deepPageConnection, true);

        for (int i = 1; i < deepPageSqlList.size(); i++) {
            String sql = deepPageSqlList.get(i);
            String deepPagePlan = JdbcUtil.getExplainResult(deepPageConnection, sql);
            if (!deepPagePlan.contains("Source:" + PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER)) {
                //没走deepPage优化，说明上一次查询返回的rows和sql中的fetch值不一样，导致cn上deepPageCache中的offset没有和下一条接上
                List<List<Object>> res1 = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, normalConnection));
                Assert.assertEquals(sql, 0, res1.size());
            } else {
                DataValidator.selectContentSameAssert(sql, null, normalConnection, deepPageConnection, true);
            }

            if (i > MAX_EXECUTE_COUNT) {
                return;
            }
        }

        deepPageConnection.close();
        normalConnection.close();
    }

    private Connection createDeepPageConnection(String db) {
        return createDeepPageConnection(db, false);
    }

    private Connection createDeepPageConnection(String db, boolean prepare) {
        try {
            Connection deepPageConnection = null;
            if (prepare) {
                Properties props = new Properties();
                props.setProperty("user", ConnectionManager.getInstance().getPolardbxUser());
                props.setProperty("password", PropertiesUtil.configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD));
                String dbUrl = String.format("jdbc:mysql://%s:%s/%s?%s",
                    ConnectionManager.getInstance().getPolardbxAddress(),
                    ConnectionManager.getInstance().getPolardbxPort(),
                    db, "useServerPrepStmts=true&allowMultiQueries=true&rewriteBatchedStatements=true");
                deepPageConnection = java.sql.DriverManager.getConnection(dbUrl, props);
            } else {
                deepPageConnection = getPolardbxConnection(db);
            }

            JdbcUtil.executeSuccess(deepPageConnection,
                String.format("set %s=true", ConnectionProperties.ENABLE_DEEP_PAGE_OPTIMIZER));
            return deepPageConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
