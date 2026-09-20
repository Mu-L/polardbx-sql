package com.alibaba.polardbx.qatest.NotThreadSafe.udf.sql;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.BROADCAST_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.data.ExecuteTableName.ONE_DB_ONE_TB_SUFFIX;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepare;

@NotThreadSafe
public class SelectWithUdfTest extends CrudBasedLockTestCase {

    private static final String FUNCTION_NAME = "test_select_udf_add_one";
    private static final String DROP_FUNCTION = "drop function if exists %s";
    private static final String CREATE_FUNCTION = "create function %s(num int) returns int no sql return num + 1";

    @Parameterized.Parameters(name = "{index}:{0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_ONE_TB_SUFFIX},
            new String[] {ExecuteTableName.UPDATE_DELETE_BASE + ONE_DB_MUTIL_TB_SUFFIX},
            new String[] {ExecuteTableName.UPDATE_DELETE_BASE + MULTI_DB_ONE_TB_SUFFIX},
            new String[] {ExecuteTableName.UPDATE_DELETE_BASE + MUlTI_DB_MUTIL_TB_SUFFIX},
            new String[] {ExecuteTableName.UPDATE_DELETE_BASE + BROADCAST_TB_SUFFIX});
    }

    public SelectWithUdfTest(String tableName) {
        this.baseOneTableName = tableName;
    }

    @Before
    public void initData() throws Exception {
        tableDataPrepare(baseOneTableName, 20,
            TableColumnGenerator.getAllTypeColum(), PK_COLUMN_NAME, mysqlConnection,
            tddlConnection, columnDataGenerator);
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_FUNCTION, FUNCTION_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, FUNCTION_NAME));
        JdbcUtil.executeSuccess(mysqlConnection, "set global log_bin_trust_function_creators = on");
        JdbcUtil.executeSuccess(tddlConnection, "set global log_bin_trust_function_creators = on");
        JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_FUNCTION, FUNCTION_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_FUNCTION, FUNCTION_NAME));
    }

    @After
    public void cleanUp() {
        JdbcUtil.executeSuccess(mysqlConnection, String.format(DROP_FUNCTION, FUNCTION_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, FUNCTION_NAME));
    }

    @Test
    public void testSelectProjectionWithUdf() {
        String sql = String.format("select pk, %s(integer_test) from %s order by pk", FUNCTION_NAME, baseOneTableName);
        DataValidator.selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
    }

    @Test
    public void testSelectMultiUdfCalls() {
        String sql = String.format(
            "select pk, %s(integer_test), %s(%s(integer_test)) from %s order by pk",
            FUNCTION_NAME, FUNCTION_NAME, FUNCTION_NAME, baseOneTableName);
        DataValidator.selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
    }

    @Test
    public void testSelectUdfWithAggregate() {
        String sql = String.format("select sum(%s(integer_test)) from %s", FUNCTION_NAME, baseOneTableName);
        DataValidator.selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
    }
}
