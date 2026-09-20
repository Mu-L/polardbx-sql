package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class ColNameWithDotDMLTest extends AutoCrudBasedLockTestCase {
    @Test
    public void testInsertIntoTableWithColNameWithDot(){
        final String dropTableIfExists = "drop table if exists `testInsertIntoTableWithColNameWithDot`";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTableIfExists, null);

        final String createTableMysql = "CREATE TABLE IF NOT EXISTS `testInsertIntoTableWithColNameWithDot` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `test.long` bigint(20) NOT NULL DEFAULT '0',\n"
            + "  PRIMARY KEY(`id`)\n"
            + ") ENGINE=InnoDB";

        final String createTable = "CREATE TABLE IF NOT EXISTS `testInsertIntoTableWithColNameWithDot` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `test.long` bigint(20) NOT NULL DEFAULT '0',\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  global index g_i(`test.long`) partition by key(`test.long`) partitions 3\n"
            + ") ENGINE=InnoDB";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, createTableMysql, createTable, null,true);

        final String insertSql = "insert into `testInsertIntoTableWithColNameWithDot`(id, `test.long`) values(1, 1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, null);

        final String replaceSql = "replace into `testInsertIntoTableWithColNameWithDot`(id, `test.long`) values(2, 2)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replaceSql, null);

        final String updateSql = "update `testInsertIntoTableWithColNameWithDot` set `test.long` = 3 where id = 1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, updateSql, null);

        final String upsertSql = "insert into `testInsertIntoTableWithColNameWithDot`(id, `test.long`) values(1, 1) on duplicate key update `test.long` = 1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, upsertSql, null);

        final String selectSql = "select * from `testInsertIntoTableWithColNameWithDot`";
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection, true);

        final String dropTable = "drop table `testInsertIntoTableWithColNameWithDot`";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTable, null);
    }
}