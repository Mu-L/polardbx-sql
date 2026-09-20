package com.alibaba.polardbx.qatest.dql.auto.advisor;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnarIndexAdvisorTest extends AutoReadBaseTestCase {

    public static boolean local_debug = false;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (local_debug) {
            return;
        }
        Connection connection = getPolardbxConnection0();
        InputStream inputStream =
            Thread.currentThread().getContextClassLoader().getResourceAsStream("dql/tpch_1t.prepare");
        String sql = IOUtils.toString(inputStream, "UTF-8");
        connection.createStatement().execute(sql);
    }

    @Test
    public void testTPCHColumnarIndexAdvisor() throws Exception {
        Connection connection = getPolardbxConnection("tpch_1t");
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("dql/tpch_1t.sql");
        List<String> tpchSqls = IOUtils.readLines(inputStream, "UTF-8");
        for (String tpchSql : tpchSqls) {
            String explainAdvisorSql =
                "/*+TDDL:ENABLE_CHECK_STATISTICS_EXPIRE=false ADVISE_TYPE=COLUMNAR_INDEX*/explain advisor " + tpchSql;
            System.out.println(explainAdvisorSql);
            ResultSet rs = JdbcUtil.executeQuery(explainAdvisorSql, connection);
            Assert.assertTrue(rs.next());
            Assert.assertNotNull(rs.getString("ADVISE_INDEX"));
            Assert.assertNotNull(rs.getString("NEW_PLAN"));
            String improveValue = rs.getString("IMPROVE_VALUE").trim();
            Assert.assertTrue(improveValue.endsWith("%"));
            Assert.assertTrue(Double.parseDouble(improveValue.substring(0, improveValue.length() - 1)) >= 0);

        }
    }

    @Test
    public void testColumnarIndexAdvisor() throws Exception {
        Connection connection = getPolardbxConnection("tpch_1t");
        testIndexAdvisor(connection, "select * from lineitem where l_partkey = 10000000",
            Arrays.asList(Arrays.asList("lineitem", "l_partkey", "l_partkey")));
        testIndexAdvisor(connection, "select * from lineitem where l_partkey > 10000000",
            Arrays.asList(Arrays.asList("lineitem", "l_partkey", "l_partkey")));
        testIndexAdvisor(connection, "select * from lineitem where l_suppkey > 1000 and l_partkey = 10000000",
            Arrays.asList(Arrays.asList("lineitem", "l_partkey", "l_partkey")));
        testIndexAdvisor(connection, "select * from lineitem where l_suppkey = 1000 and l_partkey > 10000000",
            Arrays.asList(Arrays.asList("lineitem", "l_suppkey", "l_suppkey")));

        //testIndexAdvisor(connection, "select * from lineitem where l_suppkey < 1000 or l_partkey = 10000000", Arrays.asList(Arrays.asList("lineitem", "l_partkey", "l_partkey")));

        //groupby列暂时无法体现在索引推荐中
//        testIndexAdvisor(connection, "select max(l_linenumber) from lineitem where l_suppkey > 1000 group by l_partkey",
//            Arrays.asList(Arrays.asList("lineitem", "l_suppkey", "l_partkey")));
//        testIndexAdvisor(connection, "select max(l_linenumber) from lineitem group by l_partkey",
//            Arrays.asList(Arrays.asList("lineitem", "l_orderkey", "l_partkey")));

        //orderby列暂时无法体现在索引推荐中
        //testIndexAdvisor(connection, "select max(l_linenumber) from lineitem order by l_partkey", "l_orderkey", "l_partkey");

        testIndexAdvisor(connection, "select * from lineitem l join orders o on l.l_partkey = o.o_custkey",
            Arrays.asList(Arrays.asList("lineitem", "l_orderkey", "l_partkey"),
                Arrays.asList("orders", "o_orderkey", "o_custkey"))
        );
        testIndexAdvisor(connection,
            "select * from lineitem l join orders o on l.l_partkey = o.o_custkey where l.l_suppkey > 1000 and o.o_orderdate < '1990-12-12'",
            Arrays.asList(Arrays.asList("lineitem", "l_suppkey", "l_partkey"),
                Arrays.asList("orders", "o_orderdate", "o_custkey"))
        );

        testIndexAdvisor(connection,
                "select l_suppkey, o_custkey  from lineitem l join orders o on l.l_partkey = o.o_custkey where l.l_suppkey > 1000 and o.o_orderdate < '1990-12-12' union " +
                        " select p_size, ps_suppkey from part p join partsupp ps on p.p_partkey = ps.ps_partkey where p.p_size > 100 and ps.ps_suppkey < 1000",
                Arrays.asList(Arrays.asList("lineitem", "l_suppkey", "l_partkey"),
                        Arrays.asList("orders", "o_orderdate", "o_custkey"),
                        Arrays.asList("part", "p_size", "p_partkey"),
                        Arrays.asList("partsupp", "ps_suppkey", "ps_partkey"))
        );

    }

    public void testIndexAdvisor(Connection connection, String sql, List<List<String>> expectIndexes) throws Exception {
        Map<String, Pair<String, String>> tableExpectIndexes = new HashMap<>();
        for (List<String> expectIndex : expectIndexes) {
            tableExpectIndexes.put(expectIndex.get(0), Pair.of(expectIndex.get(1), expectIndex.get(2)));
        }
        testIndexAdvisor(connection, sql, tableExpectIndexes);
    }

    public void testIndexAdvisor(Connection connection, String sql,
                                 Map<String, Pair<String, String>> tableExpectIndexes) throws Exception {
        String explainAdvisorSql =
            "/*+TDDL:ENABLE_CHECK_STATISTICS_EXPIRE=false ADVISE_TYPE=COLUMNAR_INDEX*/explain advisor " + sql;
        System.out.println(explainAdvisorSql);
        ResultSet rs = JdbcUtil.executeQuery(explainAdvisorSql, connection);
        Assert.assertTrue(rs.next());
        String adviseIndex = rs.getString("ADVISE_INDEX");
        for (SQLStatement stmt : FastsqlUtils.parseSql(adviseIndex)) {
            SQLAlterTableStatement alterStmt = (SQLAlterTableStatement) stmt;

            String tableName =
                removeBacktick(alterStmt.getTableSource().getExpr().toString().toLowerCase().split("\\.")[1]);
            SQLAlterTableAddIndex addIndex = (SQLAlterTableAddIndex) alterStmt.getItems().get(0);

            String indexCols = addIndex.getIndexDefinition().getColumns().stream()
                .map(item -> removeBacktick(item.getExpr().toString())).collect(Collectors.joining());
            String partCols = addIndex.getIndexDefinition().getPartitioning().getColumns().stream()
                .map(item -> removeBacktick(item.toString())).collect(Collectors.joining());
            Assert.assertEquals(indexCols, tableExpectIndexes.get(tableName).getKey());
            Assert.assertEquals(partCols, tableExpectIndexes.get(tableName).getValue());
        }
    }

    public static String removeBacktick(String s) {
        if (s == null) {
            return s;
        }
        if (s.startsWith("`") && s.endsWith("`")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

}
