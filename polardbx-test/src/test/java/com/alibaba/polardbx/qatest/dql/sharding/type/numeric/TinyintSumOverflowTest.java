package com.alibaba.polardbx.qatest.dql.sharding.type.numeric;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * AONE-85360670: sum(tinyint) aggregated directly on CN (SortAgg) derives the
 * accumulator type from the first input value (java.lang.Byte), so a group sum
 * larger than 127 throws ERR_CONVERTOR: "Integer value '131' is too large for
 * java.lang.Byte". Pushing aggregation down is disabled so that CN aggregates
 * raw rows, reproducing the columnar AP path without a columnar cluster.
 * <p>
 * Besides plain SUM(tinyint), the planner-derived return type must also protect
 * SUM(DISTINCT tinyint), SUM(smallint/int/bigint) whose totals exceed the Java
 * narrow type of the input, AVG/STDDEV_POP/VAR_POP(tinyint) (expanded to
 * SUM/COUNT during optimization), and the window-function path.
 */
public class TinyintSumOverflowTest extends AutoCrudBasedLockTestCase {
    protected static final String TEST_TABLE = "t_tinyint_sum_overflow";
    protected static final String SQL_CREATE_TEST_TABLE = "create table if not exists %s (\n"
        + "  pk bigint auto_increment,\n"
        + "  grp int,\n"
        + "  val tinyint,\n"
        + "  val_s smallint,\n"
        + "  val_i int,\n"
        + "  val_b bigint,\n"
        + "  primary key (pk)) %s";

    protected static final String SUFFIX_IN_NEW_PART = "partition by key(pk) partitions 8";

    protected static final String INSERTION_FORMAT =
        "insert into %s (grp, val, val_s, val_i, val_b) values\n"
            + "  (1, 100, 30000, 2000000000, 9000000000000000000),\n"
            + "  (1, 31, 30000, 2000000000, 9000000000000000000),\n"
            + "  (2, 127, 32767, 1073741824, 4611686018427387904),\n"
            + "  (2, 1, 32767, 1073741824, 4611686018427387904),\n"
            + "  (2, 1, 32767, 1073741824, 4611686018427387904),\n"
            + "  (3, 100, 30000, 2000000000, 9000000000000000000),\n"
            + "  (3, 100, 30000, 2000000000, 9000000000000000000),\n"
            + "  (3, 100, 30000, 2000000000, 9000000000000000000),\n"
            + "  (4, 120, 32767, 1500000000, 6000000000000000000),\n"
            + "  (4, 120, 32767, 1500000000, 6000000000000000000),\n"
            + "  (4, 120, 32767, 1500000000, 6000000000000000000),\n"
            + "  (4, 120, 32767, 1500000000, 6000000000000000000)";

    @Before
    public void prepareMySQLTable() {
        String mysqlSql = String.format(SQL_CREATE_TEST_TABLE, TEST_TABLE, "");
        JdbcUtil.executeSuccess(mysqlConnection, mysqlSql);
        JdbcUtil.executeSuccess(mysqlConnection, String.format("delete from %s where 1=1", TEST_TABLE));

        JdbcUtil.executeSuccess(mysqlConnection, String.format(INSERTION_FORMAT, TEST_TABLE));
    }

    @Before
    public void preparePolarDBXTable() {
        String tddlSql = String.format(SQL_CREATE_TEST_TABLE, TEST_TABLE, SUFFIX_IN_NEW_PART);
        JdbcUtil.executeSuccess(tddlConnection, tddlSql);
        JdbcUtil.executeSuccess(tddlConnection, String.format("delete from %s where 1=1", TEST_TABLE));

        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERTION_FORMAT, TEST_TABLE));
    }

    @After
    public void afterTable() {
        JdbcUtil.dropTable(tddlConnection, TEST_TABLE);
        JdbcUtil.dropTable(mysqlConnection, TEST_TABLE);
    }

    @Test
    public void testGroupBySumTinyint() {
        String[] sqlList = {
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, sum(val) from t_tinyint_sum_overflow group by grp order by grp;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, sum(val), count(val) from t_tinyint_sum_overflow group by grp order by grp;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select sum(val) from t_tinyint_sum_overflow;"
        };
        for (String sql : sqlList) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testGroupBySumDistinctTinyint() {
        String[] sqlList = {
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, sum(distinct val) from t_tinyint_sum_overflow group by grp order by grp;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select sum(distinct val) from t_tinyint_sum_overflow;"
        };
        for (String sql : sqlList) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testGroupBySumWiderIntegerTypes() {
        String[] sqlList = {
            // smallint sum 131068 > Short.MAX_VALUE
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, sum(val_s) from t_tinyint_sum_overflow group by grp order by grp;",
            // int sum 4000000000 > Integer.MAX_VALUE
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, sum(val_i) from t_tinyint_sum_overflow group by grp order by grp;",
            // bigint sum 18000000000000000000 > Long.MAX_VALUE
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, sum(val_b) from t_tinyint_sum_overflow group by grp order by grp;"
        };
        for (String sql : sqlList) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testGroupByAvgAndVarianceTinyint() {
        String[] sqlList = {
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select grp, avg(val), stddev_pop(val), var_pop(val)"
                + " from t_tinyint_sum_overflow group by grp order by grp;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false ENABLE_SORT_AGG=true*/"
                + " select avg(val), stddev_pop(val), var_pop(val) from t_tinyint_sum_overflow;"
        };
        for (String sql : sqlList) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    @Test
    public void testWindowSumTinyint() {
        // selectContentSameAssert sends the SQL to the comparison MySQL first,
        // which may not support window functions before 8.0.
        if (!isMySQL80()) {
            return;
        }
        String[] sqlList = {
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/"
                + " select grp, sum(val) over (partition by grp order by pk)"
                + " from t_tinyint_sum_overflow order by grp, pk;",
            "/*+TDDL:ENABLE_PUSH_AGG=false ENABLE_CBO_PUSH_AGG=false*/"
                + " select sum(val) over () from t_tinyint_sum_overflow order by pk;"
        };
        for (String sql : sqlList) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }
}
