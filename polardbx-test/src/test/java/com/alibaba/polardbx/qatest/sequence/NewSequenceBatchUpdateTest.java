package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class NewSequenceBatchUpdateTest extends DDLBaseNewDBTestCase {

    private static final String tmpTableName = "t_new_sequence_tmp_tb";
    private static final String tableName = "t_new_sequence_primary_tb";

    private static final String createTableSql =
        "create table %s (id int not null primary key auto_increment, b int) partition by key(id)";

    private static final String selectSql = "select id from %s order by id";
    private static final String selectMaxSql = "select max(id) from %s";

    @Before
    public void before() {
        dropTableIfExists(tmpTableName);
        dropTableIfExists(tableName);

        String sql = String.format(createTableSql, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testBatchInsert() {
        String sql = String.format("insert into %s(id) values %s", tableName,
            "(1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(10);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(11);
    }

    @Test
    public void testBatchInsertWithNullValues() {
        String sql = String.format("insert into %s(id) values %s", tableName,
            "(0), (2), (3), (null), (5), (6), (7), (null), (9), (10)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(10);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(11);
    }

    @Test
    public void testInsertIgnore() {
        String sql = String.format("insert ignore into %s(id) values %s", tableName,
            "(1), (2), (6), (7), (8)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert ignore into %s(id) values %s", tableName,
            "(1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(10);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(11);
    }

    @Test
    public void testInsertIgnoreWithNullValues() {
        String sql = String.format("insert ignore into %s(id) values %s", tableName,
            "(1), (2), (6)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert ignore into %s(id) values %s", tableName,
            "(1), (2), (3), (4), (5), (6), (null), (8), (0), (null)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(10);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(11);
    }

    @Test
    public void testReplace() {
        String sql = String.format("replace into %s(id) values %s", tableName,
            "(1), (2), (6), (7), (8)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("replace into %s(id) values %s", tableName,
            "(1), (2), (3), (4), (5), (6), (7), (8), (9), (10)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(10);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(11);
    }

    @Test
    public void testReplaceWithNullValues() {
        String sql = String.format("replace into %s(id) values %s", tableName,
            "(1), (2), (6)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("replace into %s(id) values %s", tableName,
            "(1), (2), (3), (4), (5), (6), (null), (8), (0), (null)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(10);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(11);
    }

    @Test
    public void testInsertSelect() {
        String sql = String.format("create table %s (id int, b int)", tmpTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // load data
        String insertSql = "insert into %s values (%s, %s)";
        for (int i = 0; i < 100; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tmpTableName, i + 1, i + 1));
        }

        sql = String.format("insert into %s select id, b from %s", tableName, tmpTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(100);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(101);
    }

    @Test
    public void testInsertSelectWithNullValues() {
        String sql = String.format("create table %s (id int, b int)", tmpTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        // load data
        String insertSql = "insert into %s values (%s, %s)";
        for (int i = 0; i < 100; i++) {
            if (i % 7 == 0) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tmpTableName, null, i + 1));
            } else if (i % 3 == 0) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tmpTableName, 0, i + 1));
            } else {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, tmpTableName, i + 1, i + 1));
            }
        }

        sql = String.format("insert into %s select id, b from %s order by b", tableName, tmpTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkResult(100);

        sql = String.format("insert into %s(b) values (1)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        checkMaxValue(101);
    }

    private void checkResult(int rowCount) {
        String sql = String.format(selectSql, tableName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<List<Object>> result = JdbcUtil.getAllResult(rs);
        for (int i = 0; i < rowCount; i++) {
            assertEquals(result.get(i).get(0).toString(), String.valueOf(i + 1));
        }
    }

    private void checkMaxValue(int maxValue) {
        String sql = String.format(selectMaxSql, tableName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<List<Object>> result = JdbcUtil.getAllResult(rs);
        assertEquals(result.get(0).get(0).toString(), String.valueOf(maxValue));
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
