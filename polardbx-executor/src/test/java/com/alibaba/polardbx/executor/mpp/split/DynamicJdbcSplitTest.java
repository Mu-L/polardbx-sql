package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.mpp.metadata.SplitType;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.test.JSONParseTest.buildParams;
import static com.alibaba.polardbx.executor.test.JSONParseTest.buildTableNames;

public class DynamicJdbcSplitTest {

    private JdbcSplit jdbcSplit;

    @Before
    public void setUp() throws Exception {

        ExecutionContext context = new ExecutionContext();
        context.setClientIp("127.1");
        context.setTraceId("testTrace");
        byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(context);
        String sql = "select pk from ? as tb1 where pk in (?) and 'bka_magic' = 'bka_magic'";
        BytesSql bytesSql = BytesSql.getBytesSql(sql);
        List<List<ParameterContext>> params = buildParams();
        List<List<String>> tableNames = buildTableNames();
        jdbcSplit =
            new JdbcSplit("ca", "sc", "db0", hint, bytesSql, null, null, params, "127.1", tableNames,
                ITransaction.RW.WRITE,
                true, null, new byte[] {0x01, 0x02, 0x03}, true, null, null);

    }

    @After
    public void tearDown() throws Exception {
        jdbcSplit = null;
    }

    private SqlNode buildCondition() {
        SqlIdentifier sqlIdentifier = new SqlIdentifier("a", SqlParserPos.ZERO);
        SqlDynamicParam dynamicParam = new SqlDynamicParam(4, SqlParserPos.ZERO);
        final SqlNode paramRow =
            new SqlBasicCall(SqlStdOperatorTable.ROW, new SqlNode[] {dynamicParam}, SqlParserPos.ZERO);

        return new SqlBasicCall(SqlStdOperatorTable.IN,
            new SqlNode[] {sqlIdentifier, paramRow},
            SqlParserPos.ZERO);
    }

    @Test
    public void test1() throws Exception {

        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        List<ParameterContext> parameterContexts = dynamicJdbcSplit.getFlattedParams();
        Assert.assertTrue(parameterContexts.size() == 20);
    }

    @Test
    public void test2() throws Exception {

        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(false);
        Assert.assertTrue(bytesSql.size() == 710);
    }

    @Test
    public void test3() throws Exception {

        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(true);
        Assert.assertTrue(bytesSql.size() == 70);
    }

    // New test cases

    @Test
    public void testConstructorWithSingleCondition() throws Exception {
        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);

        // Verify that the constructor works correctly
        Assert.assertNotNull(dynamicJdbcSplit);
        Assert.assertEquals(jdbcSplit.getCatalogName(), dynamicJdbcSplit.getCatalogName());
        Assert.assertEquals(jdbcSplit.getSchemaName(), dynamicJdbcSplit.getSchemaName());
        Assert.assertEquals(jdbcSplit.getDbIndex(), dynamicJdbcSplit.getDbIndex());
    }

    @Test
    public void testConstructorWithMultipleConditions() throws Exception {
        List<SqlNode> conditions = new ArrayList<>();
        for (int i = 0; i < jdbcSplit.getTableNames().size(); i++) {
            conditions.add(buildCondition());
        }

        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, conditions);

        // Verify that the constructor works correctly
        Assert.assertNotNull(dynamicJdbcSplit);
        Assert.assertEquals(jdbcSplit.getCatalogName(), dynamicJdbcSplit.getCatalogName());
        Assert.assertEquals(jdbcSplit.getSchemaName(), dynamicJdbcSplit.getSchemaName());
        Assert.assertEquals(jdbcSplit.getDbIndex(), dynamicJdbcSplit.getDbIndex());
    }

    @Test
    public void testGetFlattedParamsWithNullConditions() throws Exception {
        List<SqlNode> conditions = new ArrayList<>();
        for (int i = 0; i < jdbcSplit.getTableNames().size(); i++) {
            conditions.add(null); // Add null conditions
        }

        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, conditions);
        List<ParameterContext> parameterContexts = dynamicJdbcSplit.getFlattedParams();

        // With all null conditions, we should have no parameters
        Assert.assertTrue(parameterContexts.isEmpty());
    }

    @Test
    public void testGetFlattedParamsWithMixedConditions() throws Exception {
        List<SqlNode> conditions = new ArrayList<>();
        for (int i = 0; i < jdbcSplit.getTableNames().size(); i++) {
            if (i % 2 == 0) {
                conditions.add(buildCondition()); // Add condition for even indices
            } else {
                conditions.add(null); // Add null for odd indices
            }
        }

        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, conditions);
        List<ParameterContext> parameterContexts = dynamicJdbcSplit.getFlattedParams();

        // We should have parameters only for the non-null conditions
        Assert.assertTrue(parameterContexts.size() > 0);
        Assert.assertTrue(parameterContexts.size() < 20); // Less than the full set
    }

    @Test
    public void testGetUnionBytesSqlWithIgnoreTrue() throws Exception {
        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(true);

        // Verify that we get a result
        Assert.assertNotNull(bytesSql);
        Assert.assertTrue(bytesSql.size() > 0);
    }

    @Test
    public void testGetUnionBytesSqlWithIgnoreFalse() throws Exception {
        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(false);

        // Verify that we get a result
        Assert.assertNotNull(bytesSql);
        Assert.assertTrue(bytesSql.size() > 0);
    }

    @Test
    public void testGetUnionBytesSqlWithLocalIndexExists() throws Exception {
        SqlNode sqlNode = buildCondition();
        List<SqlNode> conditions = new ArrayList<>();
        for (int i = 0; i < jdbcSplit.getTableNames().size(); i++) {
            conditions.add(sqlNode);
        }
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, conditions);
        BytesSql bytesSql = dynamicJdbcSplit.getUnionBytesSql(false);

        // Verify that we get a result
        Assert.assertNotNull(bytesSql);
        Assert.assertTrue(bytesSql.size() > 0);
    }

    @Test
    public void testReset() throws Exception {
        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);

        // Reset the split without calling any other methods first
        dynamicJdbcSplit.reset();

        // After reset, both lookupConditions and hintSql are null
        // getSplitType should still work
        Assert.assertEquals(SplitType.DYNAMIC_JDBC, dynamicJdbcSplit.getSplitType());
    }

    @Test
    public void testGetSplitType() throws Exception {
        SqlNode sqlNode = buildCondition();
        DynamicJdbcSplit dynamicJdbcSplit = new DynamicJdbcSplit(jdbcSplit, sqlNode);

        // Verify that the split type is correct
        Assert.assertEquals(SplitType.DYNAMIC_JDBC, dynamicJdbcSplit.getSplitType());
    }
}