package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

public class SetTypeTest extends ColumnarReadBaseTestCase {
    Logger logger = LoggerFactory.getLogger(SetTypeTest.class);
    public static String PRIMARY_TABLE_NAME = "set_type_create_test";
    public static String COLUMNAR_INDEX_NAME = "set_type_index";

    private static final List<String> SET_TYPE_TABLE_COLUMNS = Arrays.asList("id", "c1", "c2", "c_set");

    @Before
    public void prepareTable() {
        JdbcUtil.dropTable(tddlConnection, PRIMARY_TABLE_NAME);
        String createTableSql = "CREATE TABLE `" + PRIMARY_TABLE_NAME + "` ("
            + "`id` bigint NOT NULL AUTO_INCREMENT,"
            + "`c1` char(5) COLLATE utf8mb4_general_ci DEFAULT NULL,"
            + "`c2` varchar(5) COLLATE utf8mb4_general_ci DEFAULT NULL,"
            + "`c_set` set('a', 'b', 'c') COLLATE utf8mb4_general_ci DEFAULT NULL,"
            + "PRIMARY KEY (`id`)"
            + ") DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
    }

    @After
    public void dropTable() {
        //JdbcUtil.dropTable(tddlConnection, PRIMARY_TABLE_NAME);
    }

    @Test
    public void createIndexThenInsert() throws InterruptedException {
        ColumnarUtils.createColumnarIndex(tddlConnection,
            COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 4);

        List<String> insertSql = buildSetTypeTestInserts(PRIMARY_TABLE_NAME);

        for (String sql : insertSql) {
            JdbcUtil.executeUpdate(tddlConnection, sql, true, true);
        }

        waitForSync(tddlConnection);

        waitForRowCountEquals(tddlConnection, PRIMARY_TABLE_NAME, COLUMNAR_INDEX_NAME);

        checkColumnOneByOne();
        String columnarSql =
            "select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ") order by id";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary) order by id";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    @Test
    public void insertThenCreateIndex() throws InterruptedException {
        List<String> insertSql = buildSetTypeTestInserts(PRIMARY_TABLE_NAME);

        for (String sql : insertSql) {
            JdbcUtil.executeUpdate(tddlConnection, sql, true, true);
        }

        ColumnarUtils.createColumnarIndex(tddlConnection,
            COLUMNAR_INDEX_NAME, PRIMARY_TABLE_NAME, "id", "id", 4);

        waitForRowCountEquals(tddlConnection, PRIMARY_TABLE_NAME, COLUMNAR_INDEX_NAME);

        checkColumnOneByOne();
        String columnarSql =
            "select * from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ") order by id";
        String primarySql = "select * from " + PRIMARY_TABLE_NAME + " force index (primary) order by id";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }

    private static List<String> buildSetTypeTestInserts(String primaryTableName) {
        List<String> inserts = new ArrayList<>();

        // Insert 1: NULL values
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, null, null, null);");

        // Insert 2: Empty strings
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, '', '', '');");

        // Insert 3: Single values
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, 'hello', 'world', 'a');");

        // Insert 4: Multiple SET values
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, 'test', 'data', 'a,b');");

        // Insert 5: All SET values
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, 'char1', 'var1', 'a,b,c');");

        // Insert 6: Different combination
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, 'char2', 'var2', 'b,c');");

        // Insert 7: Single SET value 'c'
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, 'char3', 'var3', 'c');");

        // Insert 8: Single SET value 'b'
        inserts.add("insert into " + primaryTableName + "(id, c1, c2, c_set) values(null, 'char4', 'var4', 'b');");

        return inserts;
    }

    public static void waitForRowCountEquals(Connection polardbxConnection, String primaryTableName,
                                             String columnarIndexName) throws InterruptedException {

        String columnarSql =
            "select count(*) from " + primaryTableName + " force index (" + columnarIndexName + ")";
        String primarySql = "select count(*) from " + primaryTableName + " force index (primary)";
        try (Statement statement = polardbxConnection.createStatement()) {
            int waitTime = 0;
            while (waitTime < 60) {
                ResultSet rs1 = statement.executeQuery(primarySql);
                rs1.next();
                int innodbCount = rs1.getInt(1);
                ResultSet rs2 = statement.executeQuery(columnarSql);
                rs2.next();
                int columnarCount = rs2.getInt(1);
                if (innodbCount == columnarCount) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                    waitTime++;
                } catch (InterruptedException e) {
                    return;
                }
            }
            if (waitTime >= 60) {
                throw new AssertionError("Columnar delay is beyond 1 minute, abort test");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, polardbxConnection,
            polardbxConnection,
            false, false, false);
    }

    public static void waitForSync(Connection polardbxConnection) throws InterruptedException {
        //cdc延迟基本在1s以内，有小概率超过1s，导致polardbx的位点不包含已执行的语句
        Thread.sleep(10000);

        List<List<String>> result =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("SHOW COLUMNAR OFFSET", polardbxConnection), false, null);
        List<String> polardbxResult = result.stream().filter(list -> list.get(0).equalsIgnoreCase("CDC"))
            .collect(Collectors.toList()).get(0);
        String[] currentBinlogFileSplits = polardbxResult.get(1).split("\\.");
        int currentBinlogFileId = Integer.parseInt(currentBinlogFileSplits[currentBinlogFileSplits.length - 1]);
        long currentBinlogPosition = Long.parseLong(polardbxResult.get(2));

        while (true) {
            List<List<String>> offset =
                JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("SHOW COLUMNAR OFFSET", polardbxConnection), false,
                    null);
            List<String> columnarResult =
                offset.stream().filter(list -> list.get(0).equalsIgnoreCase("CN_MIN_LATENCY"))
                    .collect(Collectors.toList()).get(0);

            String[] columnarBinlogFileSplits = columnarResult.get(1).split("\\.");
            int columnarBinlogFileId = Integer.parseInt(columnarBinlogFileSplits[columnarBinlogFileSplits.length - 1]);
            long columnarBinlogPosition = Long.parseLong(columnarResult.get(2));
            if ((currentBinlogFileId == columnarBinlogFileId ?
                Long.compare(columnarBinlogPosition, currentBinlogPosition) :
                Integer.compare(columnarBinlogFileId, currentBinlogFileId)) >= 0) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }

        }
    }

    private void checkColumnOneByOne() {
        String columnarSqlFormat =
            "select %s from " + PRIMARY_TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ") order by id";
        String primarySqlFormat = "select %s from " + PRIMARY_TABLE_NAME + " force index (primary) order by id";
        List<String> checkFailedList = new ArrayList<>();
        for (String columnName : SET_TYPE_TABLE_COLUMNS) {
            boolean checkOk = true;
            try {
                DataValidator.selectContentSameAssertWithDiffSql(
                    String.format(columnarSqlFormat, columnName),
                    String.format(primarySqlFormat, columnName),
                    null, tddlConnection, tddlConnection, false, false, false);
            } catch (Throwable t) {
                logger.error(String.format("%s check failed: %s", columnName, t.getMessage()));
                checkOk = false;
                System.exit(1);
            }

            if (!checkOk) {
                checkFailedList.add(columnName);
            }
        }
        if (!checkFailedList.isEmpty()) {
            assertWithMessage(checkFailedList.stream().reduce("Check failed column: ", (a, b) -> a + ", " + b)).fail();
        }
    }
}
