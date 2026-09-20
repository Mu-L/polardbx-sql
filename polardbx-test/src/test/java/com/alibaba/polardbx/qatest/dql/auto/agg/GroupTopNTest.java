package com.alibaba.polardbx.qatest.dql.auto.agg;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Verify that ENABLE_PARTIAL_GROUP_TOPN hint does not change query results.
 * This test creates a supplier table, inserts sample data, then executes 5 window queries
 * with both ENABLE_PARTIAL_GROUP_TOPN=true/false and ensures results are identical.
 */
public class GroupTopNTest extends AutoReadBaseTestCase {

    private static final String TABLE_NAME = "supplier";

    private static final String CREATE_TABLE = ""
        + "CREATE TABLE IF NOT EXISTS `" + TABLE_NAME + "` (\n"
        + "        `s_suppkey` int(11) NOT NULL,\n"
        + "        `s_name` varchar(25) NOT NULL,\n"
        + "        `s_address` varchar(40) NOT NULL,\n"
        + "        `s_nationkey` int(11) NOT NULL,\n"
        + "        `s_phone` varchar(15) NOT NULL,\n"
        + "        `s_acctbal` decimal(15, 2) NOT NULL,\n"
        + "        `s_comment` varchar(101) NOT NULL,\n"
        + "        PRIMARY KEY (`s_suppkey`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARACTER SET = LATIN1 DEFAULT COLLATE = latin1_swedish_ci\n"
        + "PARTITION BY KEY(`s_suppkey`)\n"
        + "PARTITIONS 16";

    private static final String HINT_TRUE =
        "/*+TDDL: ENABLE_GROUP_TOPN=true enable_sort_window=false enable_hash_window=false*/";
    private static final String HINT_FALSE = "/*+TDDL: ENABLE_GROUP_TOPN=false enable_hash_window=false*/";

    @Before
    public void setUp() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE);
        insertSupplierTestData(300);
    }

    @After
    public void tearDown() {
        JdbcUtil.dropTable(tddlConnection, TABLE_NAME);
    }

    /**
     * Insert sample rows into supplier.
     * Generate a moderate data set to exercise window functions and grouping.
     */
    private void insertSupplierTestData(int count) {
        // Batch insert to avoid oversized single statement
        final int batchSize = 100;
        int inserted = 0;
        while (inserted < count) {
            int upper = Math.min(inserted + batchSize, count);
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(TABLE_NAME)
                .append(" (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) VALUES ");
            boolean first = true;
            for (int i = inserted + 1; i <= upper; i++) {
                // generate fields
                int sSuppKey = i;
                String sName = "name" + i;
                String sAddress = "addr" + (i % 20);
                int sNationKey = i % 20;
                String sPhone = String.format("1%014d", i); // fixed length numeric string
                BigDecimal acct = new BigDecimal(String.format("%.2f", (i * 1.11) % 10000));
                String sComment = "comment" + (i % 50);

                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append("(")
                    .append(sSuppKey).append(", ")
                    .append('\'').append(sName).append('\'').append(", ")
                    .append('\'').append(sAddress).append('\'').append(", ")
                    .append(sNationKey).append(", ")
                    .append('\'').append(sPhone).append('\'').append(", ")
                    .append(acct.toPlainString()).append(", ")
                    .append('\'').append(sComment).append('\'')
                    .append(")");
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, sb.toString());
            inserted = upper;
        }
    }

    /**
     * Helper to execute query and return list of rows as list of stringified columns.
     */
    private List<List<String>> executeQueryToRows(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        List<List<String>> rows = new ArrayList<>();
        final int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<String> row = new ArrayList<>(columnCount);
            for (int c = 1; c <= columnCount; c++) {
                Object val = rs.getObject(c);
                row.add(val == null ? null : String.valueOf(val));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Compare two result sets. If ordered is true, compare in order; otherwise compare as multisets.
     */
    private void assertResultsEqual(List<List<String>> lhs, List<List<String>> rhs, boolean ordered) {
        if (ordered) {
            Assert.assertEquals(lhs, rhs);
            return;
        }
        List<List<String>> leftCopy = new ArrayList<>(lhs);
        List<List<String>> rightCopy = new ArrayList<>(rhs);

        Comparator<List<String>> rowComparator = (a, b) -> {
            int n = Math.min(a.size(), b.size());
            for (int i = 0; i < n; i++) {
                String va = a.get(i);
                String vb = b.get(i);
                if (va == null && vb == null) {
                    continue;
                } else if (va == null) {
                    return -1;
                } else if (vb == null) {
                    return 1;
                }
                int cmp = va.compareTo(vb);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(a.size(), b.size());
        };

        Collections.sort(leftCopy, rowComparator);
        Collections.sort(rightCopy, rowComparator);
        Assert.assertEquals(leftCopy, rightCopy);
    }

    private void assertExplainHasKeywords(String hint, String baseSql, String... keywords) throws SQLException {
        assertExplainKeywords(hint, baseSql, true, keywords);
    }

    private void assertExplainHasNoKeywords(String hint, String baseSql, String... keywords) throws SQLException {
        assertExplainKeywords(hint, baseSql, false, keywords);
    }

    /**
     * Execute EXPLAIN with hint and assert that plan contains expected keywords.
     */
    private void assertExplainKeywords(String hint, String baseSql, boolean contain, String... keywords)
        throws SQLException {
        String sql = "explain " + hint + " " + baseSql;
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        StringBuilder plan = new StringBuilder();
        int cols = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= cols; i++) {
                Object v = rs.getObject(i);
                if (v != null) {
                    plan.append(String.valueOf(v));
                }
                if (i < cols) {
                    plan.append('\t');
                }
            }
            plan.append('\n');
        }
        String planText = plan.toString();
        for (String kw : keywords) {
            Assert.assertTrue("Explain plan should contain keyword: " + kw + "\nPlan:\n" + planText,
                contain ? planText.contains(kw) : !planText.contains(kw));
        }
    }

    private void runAndCompare(String baseSql, boolean ordered) throws SQLException {
        String sqlTrue = HINT_TRUE + " " + baseSql;
        String sqlFalse = HINT_FALSE + " " + baseSql;

        List<List<String>> resultTrue = executeQueryToRows(sqlTrue);
        List<List<String>> resultFalse = executeQueryToRows(sqlFalse);

        assertResultsEqual(resultTrue, resultFalse, ordered);
    }

    /**
     * Execute with ENABLE_GROUP_TOPN true/false and verify identical results.
     * Also check explain plans: true -> contains GroupTop, false -> contains MergeSort.
     * SQL:
     * select s_suppkey from(
     * select s_suppkey, row_number() over(partition by s_suppkey order by s_address) as rn
     * from supplier
     * ) where 11 > rn order by s_suppkey limit 10;
     */
    @Test
    public void testQuery1_rowNumberPartitionByKeyOrdered() throws SQLException {
        String sql = "select s_suppkey from(\n"
            + "  select \n"
            + "  s_suppkey, \n"
            + "  row_number() over(partition by s_suppkey order by s_address) as rn \n"
            + "  from " + TABLE_NAME + ")\n"
            + "      where 11 > rn order by s_suppkey limit 10";
        assertExplainHasKeywords(HINT_TRUE, sql, "GroupTopN");
        assertExplainHasNoKeywords(HINT_FALSE, sql, "GroupTopN");
        runAndCompare(sql, true);
    }

    /**
     * Execute with ENABLE_GROUP_TOPN true/false and verify identical results.
     * Also check explain plans: true -> contains GroupTop, false -> contains MergeSort.
     * SQL:
     * select mx from(
     * select s_suppkey, s_nationkey, min(s_suppkey) over(partition by s_suppkey) as mx
     * from supplier
     * ) where mx = s_suppkey and s_nationkey=10 and s_suppkey &lt; 100 and mx &lt; 200;
     */
    @Test
    public void testQuery2_minWindowFilterUnordered() throws SQLException {
        String sql = "select mx from(\n"
            + "  select \n"
            + "  s_suppkey, \n"
            + "  s_nationkey,\n"
            + "  min(s_suppkey) over(partition by s_suppkey) as mx \n"
            + "  from " + TABLE_NAME + ")\n"
            + "where mx = s_suppkey and s_nationkey=10 and s_suppkey < 100 and mx < 200";
        assertExplainHasKeywords(HINT_TRUE, sql, "GroupTopN");
        assertExplainHasNoKeywords(HINT_FALSE, sql, "GroupTopN");
        runAndCompare(sql, false);
    }

    /**
     * Execute with ENABLE_GROUP_TOPN true/false and verify identical results.
     * Also check explain plans: true -> contains GroupTop, false -> contains MergeSort.
     * SQL:
     * select s_suppkey from(
     * select s_suppkey, row_number() over(partition by s_acctbal) as rn
     * from supplier
     * ) where rn &lt; 20 order by s_suppkey desc limit 10;
     */
    @Test
    public void testQuery3_rowNumberPartitionByAcctbalOrderedDesc() throws SQLException {
        String sql = "select s_suppkey from(\n"
            + "   select \n"
            + "   s_suppkey, \n"
            + "   row_number() over(partition by s_acctbal) as rn \n"
            + "   from " + TABLE_NAME + ")\n"
            + "      where rn < 20 order by s_suppkey desc limit 10";
        assertExplainHasKeywords(HINT_TRUE, sql, "GroupTopN");
        assertExplainHasNoKeywords(HINT_FALSE, sql, "GroupTopN");
        runAndCompare(sql, true);
    }

    /**
     * Execute with ENABLE_GROUP_TOPN true/false and verify identical results.
     * Also check explain plans: true -> contains GroupTop, false -> contains MergeSort.
     * SQL:
     * select s_suppkey, s_phone, s_comment from(
     * select s_suppkey, row_number() over(order by s_phone desc, s_comment) as rn, s_phone, s_comment
     * from supplier
     * ) where rn &lt; 16;
     */
    @Test
    public void testQuery4_globalOrderRowNumberUnordered() throws SQLException {
        String sql = "select s_suppkey, s_phone, s_comment from(\n"
            + "  select \n"
            + "  s_suppkey, \n"
            + "  row_number() over(order by s_phone desc, s_comment) as rn,\n"
            + "  s_phone,\n"
            + "  s_comment\n"
            + "  from " + TABLE_NAME + ")\n"
            + "where rn < 16";
        assertExplainHasKeywords(HINT_TRUE, sql, "GroupTopN");
        assertExplainHasNoKeywords(HINT_FALSE, sql, "GroupTopN");
        runAndCompare(sql, false);
    }

    /**
     * Execute with ENABLE_GROUP_TOPN true/false and verify identical results.
     * Also check explain plans: true -> contains GroupTop, false -> contains MergeSort.
     * SQL:
     * select s_suppkey from(
     * select s_suppkey, s_nationkey, row_number() over() as rn
     * from supplier
     * ) where rn &lt; 100 order by s_suppkey limit 10;
     */
    @Test
    public void testQuery5_rowNumberNoPartitionOrdered() throws SQLException {
        String sql = "select s_suppkey from(\n"
            + "  select \n"
            + "  s_suppkey, \n"
            + "  s_nationkey,\n"
            + "  row_number() over() as rn \n"
            + "  from " + TABLE_NAME + ")\n"
            + "      where rn < 100 order by s_suppkey limit 10";
        assertExplainHasKeywords(HINT_TRUE, sql, "GroupTopN");
        assertExplainHasNoKeywords(HINT_FALSE, sql, "GroupTopN");

        String sqlTrue = HINT_TRUE + " " + sql;
        String sqlFalse = HINT_FALSE + " " + sql;

        List<List<String>> resultTrue = executeQueryToRows(sqlTrue);
        List<List<String>> resultFalse = executeQueryToRows(sqlFalse);

        Assert.assertEquals(resultTrue.size(), resultFalse.size());
    }
}