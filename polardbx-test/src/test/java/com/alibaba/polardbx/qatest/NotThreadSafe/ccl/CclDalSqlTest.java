package com.alibaba.polardbx.qatest.NotThreadSafe.ccl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author busu
 * date: 2020/11/6 5:17 下午
 */
public class CclDalSqlTest extends ReadBaseTestCase {

    static String keyword = "dingfengdingfengdingfengdingfengdingfeng";

    static String testNoMatchCacheDbName = "ccl_test_sdfsalda";

    static String userName = "polardbx_root";

    final static String CCL_TEST_TABLE_NAME = "ccl_test_tb";

    @BeforeClass
    public static void beforeClass() throws Exception {
        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("clear ccl_blockers");
            stmt.execute("create database if not exists " + testNoMatchCacheDbName + " mode = 'auto'");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("create table if not exists " + CCL_TEST_TABLE_NAME
                + "(id int not null, myname char(50) not null, primary key(id)) partition by hash(id)");

            stmt.execute("clear ccl_rules");
        }
        connection.close();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("clear ccl_rules");
            stmt.execute("use " + PropertiesUtil.polardbXShardingDBName1());
            stmt.execute("clear ccl_blockers");
            stmt.execute("drop database if exists " + testNoMatchCacheDbName);
        }
        connection.close();
    }

    @Before
    public void before() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_blockers");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("clear ccl_rules");
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%' "
                + "             FOR SELECT " + "             FILTER BY KEYWORD('" + keyword + "') "
                + "             WITH MAX_CONCURRENCY=0");
        }
    }

    @After
    public void after() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_blockers");
            stmt.execute("clear ccl_rules");
            stmt.execute(" /*TDDL:FORBID_EXECUTE_DML_ALL=FALSE*/delete from  " + CCL_TEST_TABLE_NAME);
        }

    }

    @Test
    public void test1a() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            String createSql =
                "CREATE CCL_RULE  if not exists testassignments ON `*`.`*` TO 'polardbx_root'@'%' " + "FOR SELECT "
                    + "FILTER BY KEYWORD('" + keyword + "') "
                    + "WITH MAX_CONCURRENCY=1,WAIT_TIMEOUT=11,WAIT_QUEUE_SIZE=11,FAST_MATCH=1";
            try {
                stmt.execute(createSql);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try (ResultSet rs = stmt.executeQuery("show ccl_rule testassignments")) {
                rs.next();
                Assert.assertTrue(rs.getInt("WAIT_QUEUE_SIZE_PER_NODE") == 11);
                Assert.assertTrue(rs.getInt("FAST_MATCH") == 1);
                Assert.assertTrue(rs.getInt("MAX_CONCURRENCY_PER_NODE") == 1);
                Assert.assertTrue(rs.getInt("WAIT_TIMEOUT") == 11);
            }

        }

    }

    @Test
    public void test1() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("show ccl_rule busu1118")) {
                rs.next();
                Assert.assertEquals("busu1118", rs.getString("RULE_NAME"));
                Assert.assertEquals("SELECT", rs.getString("SQL_TYPE"));
                Assert.assertEquals(userName + "@%", rs.getString("USER"));
                Assert.assertEquals("*.*", rs.getString("TABLE"));
                Assert.assertTrue(rs.getInt("MAX_CONCURRENCY_PER_NODE") == 0);
                Assert.assertEquals("[\"" + keyword + "\"]", rs.getString("KEYWORDS"));
                Assert.assertEquals(null, rs.getString("TEMPLATE_ID"));
                Assert.assertTrue(rs.getInt("WAIT_QUEUE_SIZE_PER_NODE") == 0);
                Assert.assertTrue(rs.getInt("RUNNING") == 0);
                Assert.assertTrue(rs.getInt("WAITING") == 0);
                Assert.assertTrue(rs.getInt("KILLED") == 0);
                Assert.assertTrue(rs.getInt("ACTIVE_NODE_COUNT") > 0);
            }

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                rs.next();
                Assert.assertEquals("busu1118", rs.getString("RULE_NAME"));
            }

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                rs.next();
                Assert.assertEquals("busu1118", rs.getString("RULE_NAME"));
            }

            SQLException exception = null;
            try (ResultSet rs = stmt.executeQuery(String.format("select 1 as %s", keyword))) {

            } catch (SQLException e) {
                exception = e;
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("busu1118"));

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getInt("KILLED") == 1);
            }

        }
    }

    @Test
    public void testCreateCclRuleWithLongKeywords() throws SQLException {
        String[] letters =
            {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"};
        List<String> longKeywords = Lists.newArrayList();
        StringBuilder keywordList = new StringBuilder();
        for (String letter : letters) {
            String kw = "kw_" + StringUtils.repeat(letter, 25);
            longKeywords.add(kw);
            if (keywordList.length() > 0) {
                keywordList.append(",");
            }
            keywordList.append("'").append(kw).append("'");
        }
        String expectedKeywords = JSON.toJSONString(longKeywords);
        Assert.assertTrue(expectedKeywords.length() > 512);

        String ruleName = "long_kw_rule";
        String createSql = "CREATE CCL_RULE IF NOT EXISTS " + ruleName + " ON `*`.`*` TO '" + userName + "'@'%'\n"
            + "FOR SELECT\n"
            + "FILTER BY KEYWORD(" + keywordList + ")\n"
            + "WITH MAX_CONCURRENCY=5";
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute(createSql);

                String actualKeywords;
                try (ResultSet rs = stmt.executeQuery("show ccl_rule " + ruleName)) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(ruleName, rs.getString("RULE_NAME"));
                    actualKeywords = rs.getString("KEYWORDS");
                }
                Assert.assertEquals(expectedKeywords, actualKeywords);

                for (String kw : longKeywords) {
                    Assert.assertTrue("keyword " + kw + " must be kept in keywords json, actual: " + actualKeywords,
                        actualKeywords != null && actualKeywords.contains(kw));
                }
            } finally {
                try {
                    stmt.execute("drop ccl_rule " + ruleName);
                } catch (SQLException ignore) {
                    // rule may not exist if create failed
                }
            }
        }
    }

    @Test
    public void test2() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("drop ccl_rule busu1118");

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                Assert.assertFalse(rs.next());
            }

        }

    }

    @Test
    public void test3() throws SQLException {
        String[] cclRules = {
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=1"};

//        String[] cclRules = {
//            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
//                + " FILTER BY KEYWORD('sleep')\n"
//                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=0",
//            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
//                + " FILTER BY KEYWORD('sleep')\n"
//                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=1"};
        for (String cclRule : cclRules) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("clear ccl_rules;");
                stmt.execute(cclRule);
                final Connection anotherConnection = getPolardbxDirectConnection();
                Thread thread = new Thread() {
                    @SneakyThrows
                    public void run() {

                        try (Connection connection = anotherConnection; Statement stmt = connection.createStatement()) {
                            stmt.execute("select sleep(6)");
                        } catch (Exception e) {
                            System.out.println(ExceptionUtils.getFullStackTrace(e));
                        }
                    }
                };
                thread.start();
                Thread.sleep(1000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                        if (hasWait) {
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") > 0) {
                            hasWait = true;
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                anotherConnection.abort(Executors.newCachedThreadPool());
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCclWaitTimeout() throws SQLException {
        String[] cclRules = {
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=0,WAIT_TIMEOUT=6",
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=1,WAIT_TIMEOUT=6"};
        for (String cclRule : cclRules) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("clear ccl_rules;");
                stmt.execute(cclRule);
                final Connection anotherConnection = getPolardbxDirectConnection();
                Thread thread = new Thread() {
                    @SneakyThrows
                    public void run() {

                        try (Connection connection = anotherConnection; Statement stmt = connection.createStatement()) {
                            stmt.execute("select sleep(6)");
                        } catch (Exception e) {
                            System.out.println(ExceptionUtils.getFullStackTrace(e));
                        }
                    }
                };
                thread.start();
                Thread.sleep(1000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                        if (hasWait) {
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                Thread.sleep(8000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                        if (hasWait) {
                            break;
                        }
                    }
                    Assert.assertTrue(!hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                            break;
                        }
                    }
                    Assert.assertTrue(!hasWait);
                }
                ExecutorService executorService = Executors.newCachedThreadPool();
                anotherConnection.abort(executorService);
                thread.join();
                executorService.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testRunWaitTimeout() throws SQLException {
        String[] cclRules = {
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=1,WAIT_QUEUE_SIZE=2,LIGHT_WAIT=0,WAIT_TIMEOUT=7",
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=1,WAIT_QUEUE_SIZE=2,LIGHT_WAIT=1,WAIT_TIMEOUT=7"};
        for (String cclRule : cclRules) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("clear ccl_rules;");
                stmt.execute(cclRule);
                List<Thread> threads = Lists.newArrayList();
                List<Connection> connections = Lists.newArrayList();
                Map<Integer, String> result = Maps.newConcurrentMap();
                for (int i = 0; i < 7; i++) {
                    Connection anotherConnection = getPolardbxDirectConnection();
                    connections.add(anotherConnection);
                    int finalI = i;
                    Thread thread = new Thread() {
                        @SneakyThrows
                        public void run() {
                            try (Connection connection = anotherConnection;
                                Statement stmt = connection.createStatement()) {
                                stmt.execute("select sleep(5)");
                                result.put(finalI + 10000, "finish");
                            } catch (Exception e) {
                                e.printStackTrace();
                                result.put(finalI, e.getMessage());
                            }
                        }
                    };
                    thread.start();
                    threads.add(thread);
                }
                Thread.sleep(3000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    boolean hasRunning = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                        }
                        if (rs.getInt("Running") != 0) {
                            hasRunning = true;
                        }
                    }
                    Assert.assertTrue(hasWait);
                    Assert.assertTrue(hasRunning);
                }
                Thread.sleep(12000);
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    boolean hasRunning = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                        }
                        if (rs.getInt("Running") != 0) {
                            hasRunning = true;
                        }
                    }
                    Assert.assertTrue(!hasWait);
                    Assert.assertTrue(!hasRunning);
                }
                System.out.println("result------" + JSON.toJSONString(result));
                Assert.assertEquals(7, result.size());
                ExecutorService executorService = Executors.newCachedThreadPool();
                for (Connection connection : connections) {
                    try {
                        connection.abort(executorService);
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                executorService.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPrepareMatch() {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                    + " FILTER BY KEYWORD(\"dingfeng\")\n" + " WITH MAX_CONCURRENCY=0");

            try (PreparedStatement pStmt = tddlConnection.prepareStatement("select ?")) {
                pStmt.setString(1, "dingfeng");
                pStmt.executeQuery();
            }

        } catch (Exception e) {
            exception = e;
        }

        Assert.assertTrue(exception != null);
        Assert.assertTrue(exception.getMessage().contains("busu1118"));

    }

    @Test
    public void testNoMatchCache() {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                    + " FILTER BY KEYWORD('dingfeng')\n" + " WITH MAX_CONCURRENCY=0");
            try (PreparedStatement pStmt = tddlConnection.prepareStatement(
                "update " + CCL_TEST_TABLE_NAME + " set myname = ? where id = 1")) {
                pStmt.setString(1, "busu");
                pStmt.executeUpdate();
                pStmt.setString(1, "dingfeng");
                pStmt.executeUpdate();
            }

        } catch (Exception e) {
            exception = e;
        }
        Assert.assertTrue(exception == null);
    }

    @Test
    public void testCclFilterByNoUser() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON " + "*.*  " + "FOR SELECT "
                + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME + " where id = ?'\n"
                + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
        }
        Assert.assertTrue(exception != null);
        Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
    }

    @Test
    public void testExplainContainingTemplateId() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("explain select 1 as a")) {
                boolean hasTemplateId = false;
                while (rs.next()) {
                    String row = rs.getString(1);
                    if (row.contains("TemplateId")) {
//                        System.out.println(row);
                        hasTemplateId = true;
                    }
                }
                Assert.assertTrue(hasTemplateId);
            }

        }
    }

    @Test
    public void testCclFilterByQueryTemplate() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON " + "*.* TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                    + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME + " where id = ?'\n"
                    + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
        }
        Assert.assertTrue(exception != null);
        Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
    }

    @Test
    public void testCclFilterByCompleteQuery() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            String database = testNoMatchCacheDbName;
            stmt.execute("use " + database);
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON " + database + ".`*` TO '" + userName + "'@'%'\n"
                + "FOR SELECT \n" + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME + " where id = 1'\n"
                + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
            stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 2");
            exception = null;
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

    @Test
    public void testCclFilterBySemiQuery() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            String database = testNoMatchCacheDbName;
            stmt.execute("use " + database);
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON " + database + ".`*` TO '" + userName + "'@'%'\n"
                + "FOR SELECT \n" + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME
                + " where id = 1 and myname = ?'\n" + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1 and myname = 'busu'");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
            stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 2 and myname = 'busu'");
            exception = null;
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1 and myname = 'busu'");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

    @Test
    public void testTableUnquoteKeyword() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            String database = testNoMatchCacheDbName;
            stmt.execute("use " + database);
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON " + database + ".`" + CCL_TEST_TABLE_NAME + "` TO '"
                    + userName + "'@'%'\n" + "FOR SELECT \n" + " FILTER BY KEYWORD('" + keyword + "') "
                    + " WITH MAX_CONCURRENCY=0");

            for (int i = 0; i < 50; ++i) {
                try {
                    stmt.execute(
                        "select * from `" + CCL_TEST_TABLE_NAME + "` where id = 1 and myname = '" + keyword + "'");
                } catch (Exception e) {
                    e.printStackTrace();
                    exception = e;
                    break;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

    @Test
    public void testCreateShowCclBlocker() throws Exception {
        String createCclBlockerSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "when  affected_rows >= 0, response_time >= 0,fetch_rows>=0,phy_affected_rows>=0,physical_sql_count>=0,sql_type='select'\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with max_concurrency = 0";
        String showCclBlockerSql = "show ccl_blockers";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclBlockerSql);
            ResultSet resultSet = stmt.executeQuery(showCclBlockerSql);
            resultSet.next();
            Assert.assertEquals(resultSet.getString("TRIGGER_NAME"), "busucclblockersdf");
            Assert.assertEquals(resultSet.getInt("CCL_RULE_COUNT"), 0);
            Assert.assertEquals(resultSet.getString("DATABASE"), testNoMatchCacheDbName);
            Assert.assertEquals(resultSet.getString("CONDITIONS"),
                "AFFECTED_ROWS >= 0, RESPONSE_TIME >= 0, FETCH_ROWS >= 0, PHY_AFFECTED_ROWS >= 0, DN_REQUEST_COUNT >= 0, SQL_TYPE = 'SELECT'");
            Assert.assertEquals(resultSet.getString("RULE_CONFIG"), "MAX_CONCURRENCY = 0");
            Assert.assertEquals(resultSet.getInt("QUERY_RULE_UPGRADE"), 5);
            Assert.assertEquals(resultSet.getInt("MAX_CCL_RULE"), 10);
            Assert.assertEquals(resultSet.getInt("MAX_SQL_SIZE"), 4096);
        }
    }

    @Test
    public void testCreateCclBlocker() throws Exception {
        String createCclSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "when   response_time >= 0, sql_type='select'\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with max_concurrency = 0";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclSql);
            int i = 0;
            for (i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME);
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("Exceeding the max concurrency"));
                    break;
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(i < 5);
            String showCclBlockerSql = "show ccl_blocker busucclblockersdf";
            ResultSet resultSet = stmt.executeQuery(showCclBlockerSql);
            resultSet.next();
            int cclRuleCount = resultSet.getInt("CCL_RULE_COUNT");
            Assert.assertEquals(cclRuleCount, 1);
            for (int j = 0; j < 4; ++j) {
                boolean result = stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = " + j);
                Assert.assertTrue(result);
            }
            i = 0;
            for (i = 0; i < 5; ++i) {
                resultSet = stmt.executeQuery(showCclBlockerSql);
                resultSet.next();
                cclRuleCount = resultSet.getInt("CCL_RULE_COUNT");
                if (cclRuleCount == 5) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(i < 5);
            boolean result = stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = " + 5);
            Assert.assertTrue(result);
            i = 0;
            for (i = 0; i < 5; ++i) {
                resultSet = stmt.executeQuery(showCclBlockerSql);
                resultSet.next();
                cclRuleCount = resultSet.getInt("CCL_RULE_COUNT");
                if (cclRuleCount == 2) {
                    break;
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(i < 5);
            Exception exception = null;
            i = 0;
            for (i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = " + 6);
                } catch (Exception e) {
                    exception = e;
                    break;
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(i < 5);
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

    @Test
    public void testCreateCclBlockerWithDryRun() throws Exception {
        String createCclSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "when  affected_rows >= 0, response_time >= 0,fetch_rows>=0,phy_affected_rows>=0,physical_sql_count>=0,sql_type='select',active_session>=0\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with dry_run=1,max_concurrency = 0";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclSql);
            int i = 0;
            for (i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME);
                } catch (Exception e) {
                    throw new AssertionError("DRY RUN MODE will not kill sql");
                }
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testCreateCclBlockerWithTo() throws Exception {
        String createCclSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "to 'test1'@'%'"
            + "when  affected_rows >= 0, response_time >= 0,fetch_rows>=0,phy_affected_rows>=0,physical_sql_count>=0,sql_type='select',active_session>=0\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with max_concurrency = 0";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclSql);
            int i = 0;
            for (i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME);
                } catch (Exception e) {
                    throw new AssertionError("polardbx_root won't hits this blocker");
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(i == 5);
        }
    }

    @Test
    public void testCclBlockerExecutionTime() throws Exception {
        testConnection = getPolardbxConnection();
        String createCclSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "when  execution_time >= 3000,affected_rows >= 0, response_time >= 0,fetch_rows>=0,phy_affected_rows>=0,physical_sql_count>=0,sql_type='select',active_session>=0\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with max_concurrency = 0";
        try (Statement stmt = testConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("truncate table " + CCL_TEST_TABLE_NAME);
            stmt.execute("INSERT INTO " + CCL_TEST_TABLE_NAME + " (id, myname) VALUES\n"
                + "(1, 'aa'),\n"
                + "(2, 'bb'),\n"
                + "(3, 'cc');");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclSql);
            int i = 0;
            for (i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select id, sleep(5) from " + CCL_TEST_TABLE_NAME);
                } catch (Exception e) {
                    // the session has been killed, so we can't determine the exactly message
                    break;
                }
                Thread.sleep(3000);
            }
            Assert.assertTrue(i < 5);
        }
    }

    @Test
    public void testDropCclBlocker() throws Exception {
        String createCclSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "when  affected_rows >= 0, response_time >= 0,fetch_rows>=0,phy_affected_rows>=0,physical_sql_count>=0,sql_type='select'\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with max_concurrency = 0";
        String dropCclSql = "drop ccl_blocker `busucclblockersdf`";
        String showCclBlockerSql = "show ccl_blockers";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclSql);
            stmt.execute(dropCclSql);
            ResultSet allCclBlockerResultSet = stmt.executeQuery(showCclBlockerSql);
            Assert.assertTrue(allCclBlockerResultSet.next() == false);
        }
    }

    @Test
    public void testDropCclTrigger() throws Exception {
        String createCclSql = "create ccl_blocker `busucclblockersdf`  on `" + testNoMatchCacheDbName + "`\n"
            + "when  affected_rows >= 0, response_time >= 0,fetch_rows>=0,phy_affected_rows>=0,physical_sql_count>=0,sql_type='select'\n"
            + "limit query_rule_upgrade = 5, max_ccl_rule = 10\n"
            + "create ccl_rule\n"
            + "with max_concurrency = 0";
        String dropCclSql = "drop ccl_trigger `busucclblockersdf`";
        String showCclBlockerSql = "show ccl_blockers";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute(createCclSql);
            stmt.execute(dropCclSql);
            ResultSet allCclBlockerResultSet = stmt.executeQuery(showCclBlockerSql);
            Assert.assertTrue(allCclBlockerResultSet.next() == false);
        }
    }

    @Test
    public void testSlowSqlCcl() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicLong time = new AtomicLong(0);
        Thread thread = new Thread() {
            @SneakyThrows
            public void run() {
                countDownLatch.countDown();
                long startTs = System.currentTimeMillis();
                try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
                    Statement stmt = connection.createStatement()) {
                    stmt.execute("use " + testNoMatchCacheDbName);
                    stmt.execute("select sleep(10)");
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                } finally {
                    time.set(System.currentTimeMillis() - startTs);
                }
            }
        };
        thread.start();
        countDownLatch.await();
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
            Statement stmt = connection.createStatement()) {
            Thread.sleep(2000);
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("slow_sql_ccl go 'all' 0");
            Thread.sleep(3000);
            thread.join();
            ResultSet rs = stmt.executeQuery("slow_sql_ccl show");
            Assert.assertTrue(rs.next());
        }
    }

    @Test
    public void testSlowSqlCcl_1() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules");
            stmt.execute("clear ccl_blockers");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("slow_sql_ccl go 'select' 0 2000");
            stmt.execute("slow_sql_ccl go 'update' 0 2000");
            stmt.execute("slow_sql_ccl go 'delete' 0 2000");
            stmt.execute("slow_sql_ccl go 'insert' 0 2000");
            stmt.execute("slow_sql_ccl go 'all' 0 2000");
            Map<String, Map<String, String>> resultMap = Maps.newHashMap();
            Map<String, String> resultMapOfAll = new HashMap<String, String>() {
                {
                    put("NO.", "1");
                    put("TRIGGER_NAME", "_SYSTEM_SLOW_SQL_CCL_TRIGGER_ALL_");
                    put("CCL_RULE_COUNT", "0");
                    put("DATABASE", "*");
                    put("CONDITIONS", "RESPONSE_TIME >= 2000");
                    put("RULE_CONFIG", "MAX_CONCURRENCY = 0");
                    put("QUERY_RULE_UPGRADE", "1");
                    put("MAX_CCL_RULE", "1000");
                    put("MAX_SQL_SIZE", "4096");
                }
            };
            resultMap.put("_SYSTEM_SLOW_SQL_CCL_TRIGGER_ALL_", resultMapOfAll);
            Map<String, String> resultMapOfInsert = new HashMap<String, String>() {
                {
                    put("NO.", "2");
                    put("TRIGGER_NAME", "_SYSTEM_SLOW_SQL_CCL_TRIGGER_INSERT_");
                    put("CCL_RULE_COUNT", "0");
                    put("DATABASE", "*");
                    put("CONDITIONS", "RESPONSE_TIME >= 2000, SQL_TYPE = 'INSERT'");
                    put("RULE_CONFIG", "MAX_CONCURRENCY = 0");
                    put("QUERY_RULE_UPGRADE", "1");
                    put("MAX_CCL_RULE", "1000");
                    put("MAX_SQL_SIZE", "4096");
                }
            };
            resultMap.put("_SYSTEM_SLOW_SQL_CCL_TRIGGER_INSERT_", resultMapOfInsert);
            Map<String, String> resultMapOfDelete = new HashMap<String, String>() {
                {
                    put("NO.", "3");
                    put("TRIGGER_NAME", "_SYSTEM_SLOW_SQL_CCL_TRIGGER_DELETE_");
                    put("CCL_RULE_COUNT", "0");
                    put("DATABASE", "*");
                    put("CONDITIONS", "RESPONSE_TIME >= 2000, SQL_TYPE = 'DELETE'");
                    put("RULE_CONFIG", "MAX_CONCURRENCY = 0");
                    put("QUERY_RULE_UPGRADE", "1");
                    put("MAX_CCL_RULE", "1000");
                    put("MAX_SQL_SIZE", "4096");
                }
            };
            resultMap.put("_SYSTEM_SLOW_SQL_CCL_TRIGGER_DELETE_", resultMapOfDelete);
            Map<String, String> resultMapOfUpdate = new HashMap<String, String>() {
                {
                    put("NO.", "4");
                    put("TRIGGER_NAME", "_SYSTEM_SLOW_SQL_CCL_TRIGGER_UPDATE_");
                    put("CCL_RULE_COUNT", "0");
                    put("DATABASE", "*");
                    put("CONDITIONS", "RESPONSE_TIME >= 2000, SQL_TYPE = 'UPDATE'");
                    put("RULE_CONFIG", "MAX_CONCURRENCY = 0");
                    put("QUERY_RULE_UPGRADE", "1");
                    put("MAX_CCL_RULE", "1000");
                    put("MAX_SQL_SIZE", "4096");
                }
            };
            resultMap.put("_SYSTEM_SLOW_SQL_CCL_TRIGGER_UPDATE_", resultMapOfUpdate);
            Map<String, String> resultMapOfSelect = new HashMap<String, String>() {
                {
                    put("NO.", "5");
                    put("TRIGGER_NAME", "_SYSTEM_SLOW_SQL_CCL_TRIGGER_SELECT_");
                    put("CCL_RULE_COUNT", "0");
                    put("DATABASE", "*");
                    put("CONDITIONS", "RESPONSE_TIME >= 2000, SQL_TYPE = 'SELECT'");
                    put("RULE_CONFIG", "MAX_CONCURRENCY = 0");
                    put("QUERY_RULE_UPGRADE", "1");
                    put("MAX_CCL_RULE", "1000");
                    put("MAX_SQL_SIZE", "4096");
                }
            };
            resultMap.put("_SYSTEM_SLOW_SQL_CCL_TRIGGER_SELECT_", resultMapOfSelect);
            ResultSet rs = stmt.executeQuery("show ccl_blockers");
            while (rs.next()) {
                String blockerName = rs.getString("TRIGGER_NAME");
                Map<String, String> resultMapOfBlocker = resultMap.get(blockerName);
                Assert.assertTrue(resultMapOfBlocker != null);
                for (String mapKey : resultMapOfBlocker.keySet()) {
                    System.out.println(
                        String.format("%s %s %s", mapKey, resultMapOfBlocker.get(mapKey), rs.getString(mapKey)));
                    Assert.assertEquals(resultMapOfBlocker.get(mapKey), rs.getString(mapKey));
                }
            }
            rs.close();
        }
    }

    @Test
    public void testSlowSqlCcl_2() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules");
            stmt.execute("clear ccl_blockers");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("slow_sql_ccl go 'select' 0");
            ResultSet rs = stmt.executeQuery("show ccl_blockers");
            rs.next();
            Map<String, String> resultMapOfSelect = new HashMap<String, String>() {
                {
                    put("NO.", "1");
                    put("TRIGGER_NAME", "_SYSTEM_SLOW_SQL_CCL_TRIGGER_SELECT_");
                    put("CCL_RULE_COUNT", "0");
                    put("DATABASE", "*");
                    put("CONDITIONS", "RESPONSE_TIME >= 1000, SQL_TYPE = 'SELECT'");
                    put("RULE_CONFIG", "MAX_CONCURRENCY = 0");
                    put("QUERY_RULE_UPGRADE", "1");
                    put("MAX_CCL_RULE", "1000");
                    put("MAX_SQL_SIZE", "4096");
                }
            };
            for (String mapKey : resultMapOfSelect.keySet()) {
                Assert.assertEquals(rs.getString(mapKey), resultMapOfSelect.get(mapKey));
            }
            rs.close();
            stmt.execute("slow_sql_ccl go 'select' 2");
            resultMapOfSelect.put("RULE_CONFIG", "MAX_CONCURRENCY = 2");
            rs = stmt.executeQuery("show ccl_blockers");
            rs.next();
            for (String mapKey : resultMapOfSelect.keySet()) {
                Assert.assertEquals(rs.getString(mapKey), resultMapOfSelect.get(mapKey));
            }
        }
    }

    @Test
    public void testSlowSqlCcl_3() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules");
            stmt.execute("clear ccl_blockers");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("slow_sql_ccl go 'all' 0");
            stmt.execute("select sleep(2)");
            stmt.execute("select sleep(3)");
            Thread.sleep(5000);
            ResultSet rs = stmt.executeQuery("show ccl_rules");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getInt("MAX_CONCURRENCY_PER_NODE"), 0);
            String templateId = rs.getString("TEMPLATE_ID");
            Set<String> templateIdSet = Sets.newHashSet(Arrays.asList(StringUtils.split(templateId, ",")));
            Assert.assertEquals(templateIdSet.size(), 2);
            rs.close();
        }
    }
}
