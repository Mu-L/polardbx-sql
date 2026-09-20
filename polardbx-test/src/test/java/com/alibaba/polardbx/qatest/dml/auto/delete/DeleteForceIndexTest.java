package com.alibaba.polardbx.qatest.dml.auto.delete;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * single table delete force index test with dbname
 */
public class DeleteForceIndexTest extends AutoCrudBasedLockTestCase {

    private static final String TEST_TABLE_NAME = "delete_force_index_test_table";
    private static final String TEST_TABLE_INDEX_NAME = "delete_force_index_test_index";

    @Before
    public void prepare() throws Exception {
        dropTableIfExists(TEST_TABLE_NAME);
    }

    private void dropTableIfExists(String tableName) {
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, "drop table if exists " + tableName);
        } catch (Exception e) {
            // Ignore
        }
    }

    @Test
    public void testDeleteForceIndexOnPartitionTableWithAutoMode() throws Exception {
        String createSql = String.format(
            "CREATE TABLE %s (id INT PRIMARY KEY, age INT, name VARCHAR(50), LOCAL INDEX %s (age)) partition BY key(id)",
            TEST_TABLE_NAME, TEST_TABLE_INDEX_NAME);

        String mysqlCreateSql = String.format(
            "CREATE TABLE %s (id INT PRIMARY KEY, age INT, name VARCHAR(50))",
            TEST_TABLE_NAME);

        // Create table on both TDDL and MySQL
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreateSql);

        // Insert one record
        String insertSql =
            String.format("INSERT INTO %s (id, age, name) VALUES (%d, %d, %s)", TEST_TABLE_NAME, 1, 15, "'test_name'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, insertSql);

        String deleteSql =
            String.format("DELETE FROM %s.%s force index(%s) WHERE age = %d AND id = %d", polardbxOneDB,
                TEST_TABLE_NAME,
                TEST_TABLE_INDEX_NAME, 15, 1);
        String mysqlDeleteSql = String.format("DELETE FROM %s WHERE age = %d AND id = %d", TEST_TABLE_NAME, 15, 1);

        JdbcUtil.executeUpdateSuccess(tddlConnection, deleteSql);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlDeleteSql);

        // Verify data is deleted correctly
        String selectSql = "SELECT * FROM " + TEST_TABLE_NAME;
        selectContentSameAssert(selectSql, null, mysqlConnection,
            tddlConnection, true);
    }
}