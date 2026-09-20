package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@NotThreadSafe
public class ColumnOrdinalTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
        hint = "/*+TDDL:cmd_extra(FORCE_USING_OMC_30 = true)*/";
    }

    private static final String[] MODIFY_PARAMS = new String[] {
        "alter table %s modify column b bigint",
        "alter table %s modify column c bigint first",
        "alter table %s modify column d bigint after e",
    };

    private static final String[] CHANGE_PARAMS = new String[] {
        "alter table %s change column b bb bigint",
        "alter table %s change column c cc bigint first",
        "alter table %s change column d dd bigint after e",
        "alter table %s change column e `3` bigint after dd",
        "alter table %s change column `3` `\"f\"` int first",
        "alter table %s change column `dd` `UNIQUE` int after `\"f\"`",
    };

    private static final String MODIFY_COLUMNS = "a,b,c,d,e";

    private static final String CHANGE_COLUMNS = "a,bb,cc,`UNIQUE`,`\"f\"`";

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    public ColumnOrdinalTest() {
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testModifyColumnOrdinal() {
        String tableName = "omc_modify_column_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, MODIFY_PARAMS, MODIFY_COLUMNS);
    }

    @Test
    public void testChangeColumnOrdinal() {
        String tableName = "omc_change_column_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, CHANGE_PARAMS, CHANGE_COLUMNS);
    }

    private void testColumnOrdinalInternal(String tableName, String[] params, String columns) {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql =
            String.format("create table %s (a int primary key, b int, c int, d int, e int)", tableName);
        String partitionDef = " partition by hash(`a`) partitions 7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String insert = String.format("insert into %s values (1,2,3,4,5),(6,7,8,9,10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);

        for (int i = 0; i < params.length; i++) {
            String alterSql = hint + String.format(params[i], tableName);
            execDdlWithRetry(tddlDatabase1, tableName, alterSql + USE_OMC_ALGORITHM, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }

        insert = String.format("insert into %s values (2,3,4,5,6)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (13,4,5,6,7)", tableName, columns);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s values (4,5,6,7,8),(8,29,10,11,12)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (15,6,7,8,9),(19,10,11,12,13)", tableName, columns);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void generatedColumnTest() {
        String tableName = "dn_gen_col_alter_tbl";
        dropTableIfExists(tableName);
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a), d int, e int)", tableName);
        String partDef = " partition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);

        String alter = hint + String.format("alter table %s modify column b bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

        alter = hint + String.format("alter table %s modify column b bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

        alter = hint + String.format("alter table %s modify column d bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

        alter = hint + String.format("alter table %s modify column d bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

        alter = hint + String.format("alter table %s change column b f bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);

        alter = hint + String.format("alter table %s change column f f bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alter);
    }
}
