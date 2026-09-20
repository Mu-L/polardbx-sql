package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static org.hamcrest.Matchers.is;

// returning replace only for table with gsi
public class ReturningReplaceTest extends DDLBaseNewDBTestCase {
    @Override
    public boolean usingNewPartDb() {
        return false;
    }

    private static final String DISABLE_GET_DUP_USING_GSI = "DML_GET_DUP_USING_GSI=FALSE";
    private static final String FORCE_PUSHDOWN_RC_REPLACE = "DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE";
    private static final String DML_SKIP_IDENTICAL_ROW_CHECK = "DML_SKIP_IDENTICAL_ROW_CHECK=TRUE";
    private static final String DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK = "DML_SKIP_IDENTICAL_JSON_ROW_CHECK=FALSE";
    private static final String DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE = "DML_CHECK_JSON_BY_STRING_COMPARE=FALSE";
    private static final String ENABLE_OPTIMIZE_REPLACE_BY_RETURNING = "OPTIMIZE_REPLACE_BY_RETURNING=TRUE";

    // set DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=FALSE to enable optimize replace by returning
    private static final String DISABLE_DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN =
        "DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=FALSE";

    // set SEQUENTIAL_CONCURRENT_POLICY=TRUE to enable sequential concurrent policy
    private static final String ENABLE_SEQUENTIAL_CONCURRENT_POLICY = "SEQUENTIAL_CONCURRENT_POLICY=TRUE";

    private boolean useAffectedRows;
    private Connection oldTddl;
    private Connection oldMySql;

    public ReturningReplaceTest(boolean useAffectedRows) {
        this.useAffectedRows = useAffectedRows;
    }

    @Parameterized.Parameters(name = "{index}:useAffectedRows={0}")
    public static List<Boolean[]> prepareData() {
        return ImmutableList.of(new Boolean[] {false}, new Boolean[] {true});
    }

    @Before
    public void before() {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        if (useAffectedRows && !useXproto()) {
            useAffectedRows = false;
        }
        if (useAffectedRows) {
            oldTddl = tddlConnection;
            tddlConnection = ConnectionManager.getInstance().newPolarDBXConnectionWithUseAffectedRows();
            useDb(tddlConnection, tddlDatabase1);
            oldMySql = mysqlConnection;
            mysqlConnection = ConnectionManager.getInstance().newMysqlConnectionWithUseAffectedRows();
            useDb(mysqlConnection, mysqlDatabase1);
        }
    }

    @After
    public void after() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        if (useAffectedRows) {
            tddlConnection.close();
            tddlConnection = oldTddl;
            mysqlConnection.close();
            mysqlConnection = oldMySql;
        }
    }

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    private static String buildReturningReplace(String insert) {
        return
            buildCmdExtra(ENABLE_OPTIMIZE_REPLACE_BY_RETURNING, DISABLE_DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN)
                + insert;
    }

    private static boolean isReturningPath(List<List<String>> trace) {
        // Check if first trace row contains +returning_all hint
        return !trace.isEmpty() && trace.get(0).get(11).contains("+returning_all");
    }

    /*
     * 包含 GSI 的测试用例
     */

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，returning replace 直接下发replace , 不涉及fix
     */
    @Test
    public void tableWithPkNoUkWithGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci;\n";

        final String gsiName = "returning_g_replace_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        // todo：根据mysql官方文档 affected rows应返回3，但在dn80和mysql80验证实际返回的是6, 目前returning无法拿到从dn返回的affected rows，暂时忽略
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // 3 replace for primary , 1 replace for gsi
        Assert.assertThat(trace.size(), Matchers.is(3 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        // test sequential concurrent policy
        final String sequentialReturningInsert = buildCmdExtra(ENABLE_SEQUENTIAL_CONCURRENT_POLICY) + returningInsert;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + sequentialReturningInsert, null,
            false);
        final List<List<String>> sequentialTrace = getTrace(tddlConnection);
        // 3 replace for primary , 1 replace for gsi
        Assert.assertThat(sequentialTrace.size(), Matchers.is(3 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(sequentialTrace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * PK 未包含全部拆分键，returning replace 直接下发replace , 不涉及fix
     */
    @Test
    public void tableWithPkNoUkWithGsi2() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName = "returning_g_replace_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

//        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI, 主键拆分
     * 唯一键包含全部拆分键，不会走到logical handleReplace 通过原流程直接下推
     */
    @Test
    public void tableWithPkNoUkWithGsi_partitionByPk() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName = "returning_g_replace_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(6));
        Assert.assertFalse("Should use returning path when push down replace", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，每张表中都包含全部UK, returning replace 直接下发replace , 不涉及fix
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_two_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName1 = "returning_g_replace_two_c1";
        final String gsiName2 = "returning_g_replace_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c2`,`c5`) DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // 3 for primary ,2 for gsi1 , 1 for gsi2
        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    /**
     * 有 PK 无 UK, 两个 GSI, 主键拆分
     * 主键中缺少一个gsi的拆分键，returning replace 直接下发replace , 不涉及fix
     */
    @Test
    public void tableWithPkNoUkWithMultiGsi_partitionByPk2() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_two_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName1 = "returning_g_replace_two_c1";
        final String gsiName2 = "returning_g_replace_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName1
            + "(`c1`) COVERING(`c2`,`c5`) DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3,\n"
            + "  GLOBAL INDEX " + gsiName2
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // 3 for primary ,2 for gsi1 , 1 for gsi2
        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * returning replace分别对ugsi和主表下推,不涉及fix
     */
    @Test
    public void tableWithPkNoUkWithUgsi_usingGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c2(`c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName = "returning_ug_replace_one_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK conflicts (c1=3 repeated), should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * returning replace分别对ugsi和主表下推,不涉及fix
     */
    @Test
    public void tableWithPkNoUkWithUgsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c2(`c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName = "returning_ug_replace_one_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI, FORCE_PUSHDOWN_RC_REPLACE);

        final String insert = hint + "replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK conflicts (c1=3 repeated), should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，跳过 VALUES 去重步骤，直接下发 REPLACE, 不经过logical handleReplace
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk2() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY u_c1(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";

        final String gsiName = "returning_ug_replace_one_c1";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci\n";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(3, 'a', '2020-06-16 06:49:32'), (3, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(2));
        Assert.assertFalse("Should use returning path when push down replace", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * returning replace对主表和ugsi下发replace，对主表fix delete
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3_usingGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * returning replace对主表和ugsi下发replace，对主表fix delete
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk3() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE,DML_GET_DUP_USING_GSI=FALSE)*/replace into "
                + tableName
                + "(id, c1, c2, c5, c8) values"
                + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
                + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
                + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * returning replace对主表和ugsi下发replace，对主表fix delete
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32_usingGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/replace into " + tableName
            + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * returning replace对主表和ugsi下发replace，对主表fix delete
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk32() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi3";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c1_c2_3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI, FORCE_PUSHDOWN_RC_REPLACE);

        final String insert = hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI, 主键拆分
     * 每个唯一键中都包含全部拆分键，但只有索引表包含全部 UK
     * returning replace对主表和ugsi下发replace，对主表fix delete
     */
    @Test
    public void tableWithPkNoUkWithUgsi_partitionByPk4() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_with_ugsi4";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE KEY u_c1_c2_3(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c1_c2_4";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`, `c1`, `c2`),\n"
            + "  UNIQUE CLUSTERED INDEX " + gsiName
            + "(`c1`, `c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        String hint = buildCmdExtra(FORCE_PUSHDOWN_RC_REPLACE);
        final String insert = hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(2, 2, 3, 'b', '2020-06-16 06:49:32'), "
            + "(1, 2, 3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK, 一个 GSI
     * UK 未包含全部 Partition Key, REPLACE 转 SELECT + DELETE + INSERT
     * returning replace对主表和ugsi下发replace，不涉及fix
     */
    @Test
    public void tableWithPkWithUkWithGsi_partitionByPk() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_with_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY(`c2`,`c4`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_g_replace_with_uk_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  UNIQUE KEY i_c2_c4(`c2`,`c4`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c1`) COVERING(`c5`) DBPARTITION BY HASH(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK, 一个 UGSI
     * UGSI 包含全部唯一键，直接下发 REPLACE, 不经过Logical handleReplace
     */
    @Test
    public void tableWithPkWithUkWithUgsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_with_uk_one_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`,`c2`),\n"
            + "  UNIQUE KEY u_c2(`c2`,`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_one_c2_c1";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT '3',\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`,`c2`),\n"
            + "  UNIQUE KEY u_c2(`c2`,`c1`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`, `c1`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        Assert.assertThat(trace.size(), Matchers.is(4));
        Assert.assertFalse("Should not use returning path when push down replace", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * returning replace对主表和ugsi下发replace，对主表和 ugsi 均 fix delete
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi_usingGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_with_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c2_c3_with_ugsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * returning replace对主表和ugsi下发replace，对主表和 ugsi 均 fix delete
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi_usingSequential() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_with_uk_with_ugsi_seq";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c2_c3_seq";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "/*+TDDL:CMD_EXTRA(DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into " + tableName
            + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        final String returningInsert =
            buildCmdExtra(ENABLE_SEQUENTIAL_CONCURRENT_POLICY) + buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);
        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 有 UK 有 UGSI
     * 保持和 MySQL 行为一致，NULL 不产生冲突
     * returning replace对主表和ugsi下发replace，对主表和 ugsi 均 fix delete
     */
    @Test
    public void tableWithPkWithMultiUkWithUgsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_with_uk_with_ugsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE KEY u_g_c2_c3(`c2`,`c3`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_c2_c3";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY (`pk`),"
            + "  UNIQUE KEY u_c1_c2_1(`c1`,`c2`),"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`,`c3`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI, FORCE_PUSHDOWN_RC_REPLACE);

        final String insert = hint + "replace into " + tableName + "(c1, c2, c3, c5, c8) values"
            + "(1, 2, 3, 'a', '2020-06-16 06:49:32'), "
            + "(null, 2, 3, 'b', '2020-06-16 06:49:32'), " // u_c2_c3 冲突, replace
            + "(1, null, 3, 'c', '2020-06-16 06:49:32'), " // 不冲突
            + "(1, 2, null, 'd', '2020-06-16 06:49:32')," // u_c1_c2 与第一行冲突，但是第一行被 replace, 这行保留
            + "(1, 2, 4, 'e', '2020-06-16 06:49:32')," // u_c1_c2 冲突，replace
            + "(2, 2, 4, 'f', '2020-06-16 06:49:32')"; // u_c2_c3 冲突，replace
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have PK/UK conflicts, should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        selectContentSameAssert("select c1,c2,c3,c4,c5,c6,c7,c8 from " + tableName, null, mysqlConnection,
            tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 主键未包含全部拆分键，
     * 主表包含全部UK，REPLACE 转 SELECT + REPLACE + INSERT
     * 索引表，REPLACE 转 SELECT + DELETE + INSERT，DELETE_ONLY 模式默认忽略 INSERT
     * returning 优化只对public gsi 生效，下列测试同理
     */
    @Test
    @CdcIgnore(ignoreReason = "delete only gsi 和主表数据不一致 cdc check sum报错")
    public void tableWithPkNoUkWithGsi_deleteOnly() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_delete_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_g_replace_c2_delete_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus1\",DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, returningInsert, null, true);

        final String checkSql = "select * from " + tableName;
        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 1));
        Assert.assertFalse("Should not use returning path when have delete only GSI", isReturningPath(trace));

        selectContentSameAssert(checkSql, checkSql + " ignore index(" + gsiName + ")", null, mysqlConnection,
            tddlConnection);

        final ResultSet resultSet =
            JdbcUtil.executeQuery("select * from " + gsiName,
                tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertThat(allResult.size(), Matchers.is(0));
    }

    /**
     * 有 PK 无 UK, 一个 GSI
     * 所有UK包含全部拆分键，但有 WRITE_ONLY 阶段的 GSI，
     * 对主表和ugsi下发执行returning replace，write only阶段的ugsi会执行replace
     * <p>
     * WRITE_ONLY 阶段 GSI 不走 returning 优化
     */
    @Test
    public void tableWithPkNoUkWithGsi_writeOnly() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_tb_with_pk_no_uk_write_only_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_g_replace_c2_write_only";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT 2,\n"
            + "  `c2` bigint(20) NOT NULL DEFAULT 3,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`, `c2`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert =
            "/*+TDDL: cmd_extra(GSI_DEBUG=\"GsiStatus2\",DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_FORCE_PUSHDOWN_RC_REPLACE=TRUE)*/ replace into "
                + tableName
                + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningInsert = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningInsert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, "trace " + returningInsert, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);

        Assert.assertThat(trace.size(), Matchers.is(3 + 3 + 2));
        Assert.assertFalse("Should not use returning path when have write only GSI", isReturningPath(trace));

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键
     * UGSI 中包含全部唯一键
     * returning replace对主表和ugsi下发replace，对主表 fix delete
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    @CdcIgnore(ignoreReason = "不同分区存在重复主键")
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow_usingGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "return_replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String replace =
            "replace into " + tableName + "(id, c1, c2, c5, c8) values(2, 1, 1, 'd', '2020-06-16 06:49:32')";
        final String returningReplace = buildReturningReplace(replace);

        // 已知问题，对pk仅具有局部唯一约束
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + returningReplace, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // 1 replace for priamry + 1 replace for ugsi + 2 delete for primary
        Assert.assertThat(trace.size(), Matchers.is(1 + 1 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键
     * UGSI 中包含全部唯一键
     * returning replace对主表和ugsi下发replace，对主表 fix delete
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    @CdcIgnore(ignoreReason = "不同分区存在重复主键")
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "return_replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_two_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String replace =
            hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values(2, 1, 1, 'd', '2020-06-16 06:49:32')";
        final String returningReplace = buildReturningReplace(replace);

        // 已知问题，对pk仅具有局部唯一约束
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + returningReplace, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // 1 replace for priamry + 1 replace for ugsi + 1 delete for primary
        Assert.assertThat(trace.size(), Matchers.is(1 + 1 + 1));
        Assert.assertTrue("Should use returning path", isReturningPath(trace));

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键， 主表上
     * UGSI 中包含全部唯一键，UGSI
     * returning replace对主表和ugsi下发replace，对主表 fix delete
     * 涉及到不同分区存在重复主键场景
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow1_usingGsi() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "return_replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_three_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String replace =
            "replace into " + tableName + "(id, c1, c2, c5, c8) values"
                + "(4, 4, 4, 'e', '2020-06-16 06:49:32'),"
                + "(2, 1, 1, 'f', '2020-06-16 06:49:32'),"
                + "(5, 5, 5, 'g', '2020-06-16 06:49:32'),"
                + "(3, 1, 4, 'h', '2020-06-16 06:49:32')";
        final String returningReplace = buildReturningReplace(replace);

        // 已知问题，returning流程不会对主键做全分片扫描，允许不同物理分片存在相同主键，该场景中（3，1，4）和（3，3，3）可以共存
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + returningReplace, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have UK conflicts (c2: 4 appears twice), should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        // selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 有 PK 无 UK, 一个 UGSI
     * UGSI 中未包含主表拆分键
     * UGSI 中包含全部唯一键
     * returning replace对主表和ugsi下发replace，对主表 fix delete
     * <p>
     * 正确处理与多行冲突的情况
     */
    @Test
    public void tableWithPkNoUkWithUgsi_multiDuplicateRow1() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "return_replace_test_tb_with_pk_no_uk_with_ugsi_multi_duplicate";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE KEY u_c2(`c2`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_ug_replace_three_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        final String insert = "insert into " + tableName
            + "(id, c1, c2, c5, c8) values(1, 1, 1, 'a', '2020-06-16 06:49:32'), (2, 2, 2, 'b', '2020-06-16 06:49:32'), (3, 3, 3, 'c', '2020-06-16 06:49:32')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        checkGsi(tddlConnection, gsiName);

        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        final String hint = buildCmdExtra(DISABLE_GET_DUP_USING_GSI);

        final String replace =
            hint + "replace into " + tableName + "(id, c1, c2, c5, c8) values"
                + "(4, 4, 4, 'e', '2020-06-16 06:49:32'),"
                + "(2, 1, 1, 'f', '2020-06-16 06:49:32'),"
                + "(5, 5, 5, 'g', '2020-06-16 06:49:32'),"
                + "(3, 1, 4, 'h', '2020-06-16 06:49:32')";
        final String returningReplace = buildReturningReplace(replace);

        // 已知问题，returning流程不会对主键做全分片扫描，允许不同物理分片存在相同主键，该场景中（3，1，4）和（3，3，3）可以共存
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + returningReplace, null, false);
        final List<List<String>> trace = getTrace(tddlConnection);

        // Batch values have UK conflicts (c2: 4 appears twice), should not use returning path
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        // selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 重复执行四次，验证 affected rows 结果符合预期
     * 注意：由于是通过 JDBC 下发，useAffectedRows 默认为 false，也就是 CLIENT_FOUND_ROWS=1 ，
     * 因此返回的是 touched 而非 updated。与此对应的是官方命令行工具仅支持 CLIENT_FOUND_ROWS=0 ，
     * 因此返回的是 updated ，如果更新后取值无变化则返回 0
     */
    @Test
    public void checkAffectedRows() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_result_tb_with_pk_no_uk_one_gsi";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";

        final String gsiName = "returning_g_replace_result_c2";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL AUTO_INCREMENT,\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  `c4` bigint(20) DEFAULT NULL,\n"
            + "  `c5` varchar(255) DEFAULT NULL,\n"
            + "  `c6` datetime DEFAULT NULL,\n"
            + "  `c7` text,\n"
            + "  `c8` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  PRIMARY KEY(`c1`),\n"
            + "  GLOBAL INDEX " + gsiName
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY hash(`c1`) TBPARTITION BY HASH(`c1`) TBPARTITIONS 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        // one
        final String insert = "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:32'), (2, 'b', '2020-06-16 06:49:32'), (3, 'c', '2020-06-16 06:49:32')";
        final String returningReplace = buildReturningReplace(insert);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, returningReplace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);

        // two
        final String insert1 = "/*+TDDL:CMD_EXTRA(DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE)*/replace into " + tableName
            + "(c1, c5, c8) values(1, 'a', '2020-06-16 06:49:33'), (2, 'b', '2020-06-16 06:49:33'), (3, 'c', '2020-06-16 06:49:33')";
        final String returningReplace1 = buildReturningReplace(insert1);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert1, returningReplace1, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        checkGsi(tddlConnection, gsiName);

        // three
        // 同上对完全相同记录的行replace和mysql行为不一致
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert1, returningReplace1, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // four
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert1, returningReplace1, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        checkGsi(tddlConnection, gsiName);
    }

    /**
     * 在 RC 的隔离级别
     * returning流程依然会下发replace
     */
    @Test
    public void checkIsolationLevelRC() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_isolation_level";
        dropTableIfExists(tableName);
        final String createTable = "CREATE TABLE " + tableName + " (\n"
            + "  `a` int(11) NOT NULL,\n"
            + "  `b` int(11) DEFAULT NULL,\n"
            + "  `c` int(11) DEFAULT NULL,\n"
            + "  `d` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE KEY `b` (`b`),\n"
            + "  UNIQUE KEY `c` (`c`),\n"
            + "  KEY `auto_shard_key_d` USING BTREE (`d`),\n"
            + "  GLOBAL INDEX `returning_g`(`c`) COVERING (`a`, `d`) DBPARTITION BY HASH(`c`) TBPARTITION BY HASH(`c`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci DBPARTITION BY HASH(`d`) TBPARTITION BY HASH(`d`) TBPARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String insert = "insert into " + tableName + " values (1,2,3,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            String replace = "replace into " + tableName + " values (1,3,3,4)";
            final String returningReplace = "trace " + buildReturningReplace(replace);
            JdbcUtil.executeUpdateSuccess(conn, returningReplace);
            List<List<String>> trace = getTrace(conn);

            for (List<String> row : trace) {
                String phySql = row.get(row.size() - 4);
                Assert.assertTrue(phySql.contains("REPLACE"));
            }

            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    /**
     * 在 RR 的隔离级别
     * returning流程会下发 REPLACE
     */
    @Test
    public void checkIsolationLevelRR() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_isolation_level";
        dropTableIfExists(tableName);
        final String createTable = "CREATE TABLE " + tableName + " (\n"
            + "  `a` int(11) NOT NULL,\n"
            + "  `b` int(11) DEFAULT NULL,\n"
            + "  `c` int(11) DEFAULT NULL,\n"
            + "  `d` int(11) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE KEY `b` (`b`),\n"
            + "  UNIQUE KEY `c` (`c`),\n"
            + "  KEY `auto_shard_key_d` USING BTREE (`d`),\n"
            + "  GLOBAL INDEX `returning_g`(`c`) COVERING (`a`, `d`) DBPARTITION BY HASH(`c`) TBPARTITION BY HASH(`c`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci  DBPARTITION BY HASH(`d`) TBPARTITION BY HASH(`d`) TBPARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);
        String insert = "insert into " + tableName + " values (1,2,3,4)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            String replace = "replace into " + tableName + " values (1,3,3,4)";
            final String returningReplace = "trace " + buildReturningReplace(replace);
            JdbcUtil.executeUpdateSuccess(conn, returningReplace);
            List<List<String>> trace = getTrace(conn);
            boolean hasReplace = false;

            for (List<String> row : trace) {
                String phySql = row.get(row.size() - 4);
                hasReplace |= phySql.contains("REPLACE");
            }
            Assert.assertTrue(hasReplace);

            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    /**
     * 验证 Replace 在大小写不敏感编码时的正确性
     */
    @Test
    public void checkCaseInsensitive() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_replace_test_case_insensitive";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int(11) primary key,\n"
            + "  `b` varchar(20) unique key\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY HASH(`a`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);

        final String gsiName = "returning_replace_test_case_insensitive_gsi";
        final String createGsi =
            String.format("create global unique index %s on %s(b) DBPARTITION BY HASH(`b`)", gsiName,
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String insert = "insert into " + tableName + " values(1,'QQ')";
        String replace = "replace into " + tableName + " values(5,'qq')";
        String returningReplace = buildReturningReplace(replace);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, returningReplace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        replace = "replace into " + tableName + " values(2,'Qq')";
        returningReplace = buildReturningReplace(replace);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, returningReplace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        replace = "replace into " + tableName + " values(11,'tt'),(12,'TT')";
        returningReplace = buildReturningReplace(replace);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, returningReplace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void checkHugeBatchReplaceTraceId() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "replace_huge_batch_traceid_test";
        final String indexName = "replace_huge_batch_traceid_test_index";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (\n"
            + "  `a` int primary key,\n"
            + "  `b` int,\n"
            + "  `c` varchar(1024) \n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " DBPARTITION BY HASH(`a`) TBPARTITION BY HASH(`a`) TBPARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTable);
        final String createIndex =
            "create global index " + indexName + " on " + tableName
                + "(`b`) DBPARTITION BY HASH(`b`) TBPARTITION BY HASH(`b`) TBPARTITIONS 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createIndex);

        JdbcUtil.executeUpdate(tddlConnection, "set polardbx_server_id = 27149");

        final int batchSize = 1000;
        String pad = String.join("", Collections.nCopies(1000, "p"));
        StringBuilder sb = new StringBuilder();
        sb.append("replace into " + tableName + " values");
        for (int i = 0; i < batchSize; i++) {
            String value = "(" + i + "," + i + ",'" + pad + "')";
            if (i != batchSize - 1) {
                value += ",";
            }
            sb.append(value);
        }

        String replace = sb.toString();
        replace = buildReturningReplace(replace);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + replace, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertTrue("Should use returning path", isReturningPath(trace));
        checkPhySqlOrder(trace);

        // 已知问题，affected rows展示不一致
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
    }

    private static final String SOURCE_TABLE_NAME = "returning_replace_test_src_tbl";

    private void testComplexDmlInternal(String hint, String op, String tableName, String partitionDef, boolean withPk,
                                        boolean withUk, boolean withGsi, String[][] params) throws SQLException {
        // Create source table for insert select
        dropTableIfExists(SOURCE_TABLE_NAME);
        String createSourceTableSql =
            String.format(
                "create table if not exists %s (id int primary key, a int, b int)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci",
                SOURCE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSourceTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + SOURCE_TABLE_NAME + " values(100,101,101),(101,102,102),(102,103,103)");

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);
        String primaryDef = withPk ? "primary key" : "";
        String uniqueDef = withUk ? "unique key" : "";
        String createTableSql =
            String.format(
                "create table if not exists %s (id int %s, a int default 1, b int default 0 %s)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci",
                tableName,
                primaryDef, uniqueDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        System.out.println("--------------------");
        for (int i = 0; i < params.length; i++) {
            String insert = String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
            String mysqlInsert =
                String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
            System.out.println("mysql: " + mysqlInsert + "\ntddl: " + insert);
            // 已知问题，replace 无 PK，会有隐式主键更新导致 affected rows 实在 delete+insert 多1
            // 已知问题，UK非拆分键，会导致replace没有检测到其他表的unique冲突，少replace
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null, false);
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null, false);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);
        }
        if (withGsi) {
            String gsiName1 = tableName + "_gsi_a";
            String gsiName2 = tableName + "_gsi_b";
            String gsiName3 = tableName + "_gsi_ab";
            String gsiName4 = tableName + "_gsi_ba";
            String createGsiSql1 =
                String.format(
                    "create global index %s on %s(a) DBPARTITION BY HASH(`a`)",
                    gsiName1, tableName);
            String createGsiSql2 =
                String.format(
                    "create global index %s on %s(b) DBPARTITION BY HASH(`b`)",
                    gsiName2, tableName);
            String createGsiSql3 =
                String.format(
                    "create global index %s on %s(a) covering(id,b) DBPARTITION BY HASH(`a`)",
                    gsiName3,
                    tableName);
            String createGsiSql4 =
                String.format(
                    "create global index %s on %s(b) covering(id,a) DBPARTITION BY HASH(`b`)",
                    gsiName4,
                    tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql3);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql4);
            String deleteAll = "delete from " + tableName;
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, deleteAll, deleteAll, null, true);
            for (int i = 0; i < params.length; i++) {
                String insert =
                    String.format("%s %s %s %s", op, tableName, params[i][0], params[i][1]);
                String mysqlInsert =
                    String.format("%s %s %s %s", op, tableName, params[i][2], params[i][3]);
                System.out.println("mysql: " + mysqlInsert + "\ntddl: " + insert);
                // 已知问题，replace 无 PK，会有隐式主键更新导致 affected rows 实在 delete+insert 多1
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, insert, null,
                    false);
                executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlInsert, hint + insert, null,
                    false);
                selectContentSameAssert("select * from " + tableName, null, mysqlConnection,
                    tddlConnection);
                checkGsi(tddlConnection, gsiName1);
                checkGsi(tddlConnection, gsiName2);
                checkGsi(tddlConnection, gsiName3);
                checkGsi(tddlConnection, gsiName4);
            }
        }
    }

    private static final String[][] REPLACE_PARAMS_1 = new String[][] {
        new String[] {
            "(id,a,b)", "values (0,1,1),(1,2,2),(2,3,3),(100,101,101),(101,102,102)", "(id,a,b)",
            "values (0,1,1),(1,2,2),(2,3,3),(100,101,101),(101,102,102)"},
        new String[] {"(id)", "values (1)", "(id)", "values (1)"},
        new String[] {"(id,a,b)", "values (4,0+2,0+2)", "(id,a,b)", "values (4,2,2)"},
        new String[] {"(id,a,b)", "values (1,2,2),(2,3,3)", "(id,a,b)", "values (1,2,2),(2,3,3)"},
        new String[] {
            "(id,a,b)", String.format("select * from %s where id=100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (100,101,101)"},
        new String[] {
            "(id,a,b)", String.format("select * from %s where id>100", SOURCE_TABLE_NAME), "(id,a,b)",
            "values (101,102,102),(102,103,103)"}
    };

    @Test
    public void testLogicalReplaceWithoutFullTableScan() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        String hint =
            "/*+TDDL:CMD_EXTRA(DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=FALSE,OPTIMIZE_REPLACE_BY_RETURNING=TRUE)*/";

        // 与auto库参数不一致原因：分区方式不一致导致，而local uk/pk仅保证局部唯一约束
        testComplexDmlInternal(hint, "replace into", "returning_replace_test_tbl",
            " DBPARTITION BY HASH(`id`)", true, false, true,
            REPLACE_PARAMS_1);
        testComplexDmlInternal(hint, "replace into", "returning_replace_test_tbl_brd", " broadcast", false, true, false,
            REPLACE_PARAMS_1);
        testComplexDmlInternal(hint, "replace into", "returning_replace_test_tbl_single", " single", false, true, false,
            REPLACE_PARAMS_1);
        // 已知问题，对local uk仅保证局部唯一约束
        testComplexDmlInternal(hint, "replace into", "returning_replace_test_tbl",
            " DBPARTITION BY HASH(`id`)", true,
            false, true,
            REPLACE_PARAMS_1);
        testComplexDmlInternal(hint, "replace into", "returning_replace_test_tbl_brd", " broadcast", true, true, false,
            REPLACE_PARAMS_1);
        testComplexDmlInternal(hint, "replace into", "returning_replace_test_tbl_single", " single", true, true, false,
            REPLACE_PARAMS_1);
    }

    /**
     * 原replace测试中用于json类型不支持直接比较
     * 但在returning流程中，该场景不会涉及到对完整行比较，因此不会对比json类型导致报错
     */
    @Test
    public void testReplaceJson() {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "replace_json_tbl";
        final String indexName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String create =
            String.format(
                "create table %s (a int primary key, b int, c json, global index %s(b) dbpartition by hash(b))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci dbpartition by hash(a)",
                tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String replace =
            String.format("replace into %s values (1,2,'{\"b\": \"b\", \"a\": \"a\", \"c\": \"c\"}')", tableName);
        replace = buildReturningReplace(replace);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);
        replace = String.format("replace into %s values (1,2,'{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}')", tableName);
        replace = buildReturningReplace(replace);
        String hint =
            buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + replace);
        hint = buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DML_SKIP_IDENTICAL_ROW_CHECK);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + replace);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        System.out.println(allResult);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2"));
        Assert.assertTrue(allResult.get(0).get(2).equals("{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}"));
    }

    @Test
    public void testReplaceJson1() {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "replace_json_tbl1";
        final String indexName = tableName + "_gsi";
        dropTableIfExists(tableName);

        String create =
            String.format(
                "create table %s (a int primary key, b int, c json, global index %s(b) dbpartition by hash(b))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci dbpartition by hash(a)",
                tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String replace =
            String.format("replace into %s values (1,2,'{\"b\": \"b\", \"a\": \"a\", \"c\": \"c\"}')", tableName);
        replace = buildReturningReplace(replace);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);
        replace = String.format("replace into %s values (1,2,'{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}')", tableName);
        replace = buildReturningReplace(replace);
        String hint =
            buildCmdExtra(DISABLE_DML_SKIP_IDENTICAL_JSON_ROW_CHECK, DISABLE_DML_CHECK_JSON_BY_STRING_COMPARE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + replace);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);

        ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        List<List<String>> allResult = JdbcUtil.getStringResult(resultSet, true);
        System.out.println(allResult);
        Assert.assertThat(allResult.size(), Matchers.is(1));
        Assert.assertTrue(allResult.get(0).get(0).equals("1"));
        Assert.assertTrue(allResult.get(0).get(1).equals("2"));
        Assert.assertTrue(allResult.get(0).get(2).equals("{\"a\": \"b\", \"b\": \"a\", \"d\": \"c\"}"));
    }

    /**
     * replace select的下推行为，在returning优化中由于不会对replace select修改执行逻辑，因此还是原执行逻辑
     */
    @Test
    public void testReplacePushdown() {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName1 = "replace_pushdown_tbl1";
        final String tableName2 = "replace_pushdown_tbl2";
        final String tableName3 = "replace_pushdown_tbl3";
        dropTableIfExists(tableName1);
        dropTableIfExists(tableName2);
        dropTableIfExists(tableName3);

        String create = String.format(
            "create table %s(e int primary key)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci dbpartition by hash(e)",
            tableName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        create = String.format("CREATE TABLE %s (\n"
                + "  a bigint(20) NOT NULL, \n"
                + "  b bigint(20) NOT NULL, \n"
                + "  c bigint(20) NOT NULL, \n"
                + "  d int(11) NOT NULL, \n"
                + "  PRIMARY KEY (`a`)\n"
                + ")ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci DBPARTITION BY hash(`d`)",
            tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        create = String.format("CREATE TABLE %s (\n"
                + "  b bigint(20) NOT NULL, \n"
                + "  d int(11) NOT NULL, \n"
                + "  PRIMARY KEY (`b`)\n"
                + ")ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci DBPARTITION BY hash(`d`);",
            tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String insert = String.format("insert into %s values (1,1,1,1)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        insert = String.format("insert into %s values (1,1)", tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        String replace = MessageFormat.format(
            "REPLACE INTO {0}(e) SELECT 1 FROM {1} INNER JOIN {2} ON {1}.b = {2}.b AND {2}.d = 1 WHERE {1}.d = 1 AND {1}.c = 1;\n",
            tableName1, tableName2, tableName3);
        replace = buildReturningReplace(replace);
        JdbcUtil.executeUpdateSuccess(tddlConnection, replace);

        String select = String.format("select * from %s", tableName1);
        ResultSet rs = JdbcUtil.executeQuery(select, tddlConnection);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);

        Assert.assertEquals(1, objects.size());
        Assert.assertEquals("1", objects.get(0).get(0).toString());
    }

    @Test
    public void testReplaceWithUgsiAndJson() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }
        final String tableName = "returning_test_tb_replace_with_json";
        dropTableIfExists(tableName);

        final String gsiName = tableName + "_gsi";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `pk` bigint(11) NOT NULL,\n"
            + "  `c1` bigint(20) DEFAULT NULL,\n"
            + "  `c2` bigint(20) DEFAULT NULL ,\n"
            + "  `c3` bigint(20) DEFAULT NULL ,\n"
            + "  `c4` json DEFAULT NULL ,\n"
            + "  PRIMARY KEY (`pk`), \n"
            + "  UNIQUE GLOBAL INDEX " + gsiName + "(`c1`) covering(`c2`) DBPARTITION BY HASH(`c1`), \n"
            + "  UNIQUE INDEX l1 on g1(`c2`) "
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE = utf8mb4_general_ci";
        final String partitionDef = " dbpartition by hash(`c3`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String sql = String.format("insert into %s values (1,1,1,4,null)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("replace into %s values (1,2,3,4,'{\"a\":\"b\"}')", tableName);
        sql = buildReturningReplace(sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        checkGsi(tddlConnection, gsiName);

        final ResultSet resultSet = JdbcUtil.executeQuery("select * from " + tableName, tddlConnection);
        final List<List<Object>> allResult = JdbcUtil.getAllResult(resultSet);

        Assert.assertEquals("2", allResult.get(0).get(1).toString());
        Assert.assertEquals("3", allResult.get(0).get(2).toString());
        Assert.assertEquals("4", allResult.get(0).get(3).toString());
        Assert.assertEquals("{\"a\": \"b\"}", allResult.get(0).get(4).toString());
    }

    /**
     * 测试覆盖了所有sk的全局索引表的Batch replace
     * values中出现自冲突，不经过returning优化
     */
    @Test
    public void testCoveringIndexReplace() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }

        final String tableName = "returning_test_tb_batch_replace_cover_index";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String mysqlCreatTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `a` int NOT NULL,\n"
            + "  `b` int DEFAULT NULL,\n"
            + "  `c` int DEFAULT NULL,\n"
            + "  `d` int DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE INDEX l1(`b`),\n"
            + "  UNIQUE INDEX l2(`d`,`c`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n";

        final String gsiName1 = "gib";
        final String gsiName2 = "gic";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `a` int NOT NULL,\n"
            + "  `b` int DEFAULT NULL,\n"
            + "  `c` int DEFAULT NULL,\n"
            + "  `d` int DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`b`) COVERING(`c`,`d`) DBPARTITION BY HASH(`b`) TBPARTITION BY HASH(`b`) TBPARTITIONS 3,\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName2
            + "(`d`,`c`) COVERING(`b`) DBPARTITION BY HASH(`d`,`c`) TBPARTITION BY HASH(`d`,`c`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n";
        final String partitionDef = " DBPARTITION BY HASH(`a`) TBPARTITION BY HASH(`a`) TBPARTITIONS 3";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreatTable);

        // 执行 REPLACE INTO 操作
        String replace = "replace into " + tableName + " values(1,1,3,4),(1,1,5,6),(1,2,3,4)";
        replace = buildReturningReplace(replace);

        // 执行带 trace 的 REPLACE INTO 操作并校验 trace 记录数量
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, "trace " + replace, null, true);
        final List<List<String>> trace = getTrace(tddlConnection);

        // 校验 trace 记录数量
        Assert.assertThat(trace.size(), Matchers.greaterThanOrEqualTo(1 + 2 + 2 + 1 + 1));
        Assert.assertFalse("Should not use returning path when batch values have PK/UK conflicts",
            isReturningPath(trace));

        // 执行不带 trace 的 REPLACE INTO 操作并进行 MySQL 数据库查询对比
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, replace, null, true);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        // 校验全局索引的一致性
        checkGsi(tddlConnection, gsiName1);
        checkGsi(tddlConnection, gsiName2);
    }

    /**
     * 测试未覆盖所有sk的全局索引表的Batch replace
     * 应当不能利用replace returning优化
     */
    @Test
    public void testNotCoveringIndexReplace() throws SQLException {
        if (!isMySQL80() || !useXproto()) {
            return;
        }

        final String tableName = "returning_test_tb_batch_replace_not_cover_index";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String gsiName1 = "gib_not_cover";
        final String gsiName2 = "gic_not_cover";
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` (\n"
            + "  `a` int NOT NULL,\n"
            + "  `b` int DEFAULT NULL,\n"
            + "  `c` int DEFAULT NULL,\n"
            + "  PRIMARY KEY (`a`),\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName1
            + "(`b`) DBPARTITION BY HASH(`b`) TBPARTITION BY HASH(`b`) TBPARTITIONS 3,\n"
            + "  UNIQUE GLOBAL INDEX " + gsiName2
            + "(`c`) DBPARTITION BY HASH(`c`) TBPARTITION BY HASH(`c`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n";
        final String partitionDef = " DBPARTITION BY HASH(`a`) TBPARTITION BY HASH(`a`) TBPARTITIONS 3";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        // 执行 REPLACE INTO 操作
        String replace = buildReturningReplace("replace into " + tableName + " values(1,1,1),(1,2,2),(2,1,2)");

        // 执行explain该sql并获取explain结果
        String explainSql = "explain " + replace;
        ResultSet explainRs = JdbcUtil.executeQuery(explainSql, tddlConnection);
        List<List<String>> explainResult = JdbcUtil.getStringResult(explainRs, false);

        // 验证explain结果中不包含"isReturning=true"字段
        boolean foundIsReturning = false;
        for (List<String> row : explainResult) {
            for (String cell : row) {
                if (cell != null && cell.contains("isReturning=true")) {
                    foundIsReturning = true;
                    break;
                }
            }
            if (foundIsReturning) {
                break;
            }
        }
        Assert.assertFalse("explain result should not contains isReturning=true", foundIsReturning);
    }
}