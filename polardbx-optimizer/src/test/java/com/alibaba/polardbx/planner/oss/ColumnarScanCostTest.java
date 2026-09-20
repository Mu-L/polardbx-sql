package com.alibaba.polardbx.planner.oss;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ColumnarScanCostTest extends PlanTestCommon {

    public ColumnarScanCostTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ColumnarScanCostTest.class);
    }

    static final String columnar_scan_hint = "/*+TDDL:cmd_extra(workload_type=ap optimizer_type=columnar)*/ ";

    String l_od_shipdate_orderkey_pt_orderkey = "lineitem_col_index_orderby_shipdate_and_orderkey_partitionby_orderkey";
    String l_od_orderkey_shipdate_pt_orderkey = "lineitem_col_index_orderby_orderkey_and_shipdate_partitionby_orderkey";

    String l_od_orderkey_pt_orderkey = "lineitem_col_index_orderby_orderkey_partitionby_orderkey";
    String l_od_partkey_pt_orderkey = "lineitem_col_index_orderby_partkey_partitionby_orderkey";
    String l_od_suppkey_pt_orderkey = "lineitem_col_index_orderby_suppkey_partitionby_orderkey";
    String l_od_shipdate_pt_orderkey = "lineitem_col_index_orderby_shipdate_partitionby_orderkey";

    String l_od_orderkey_pt_partkey = "lineitem_col_index_orderby_orderkey_partitionby_partkey";
    String l_od_partkey_pt_partkey = "lineitem_col_index_orderby_partkey_partitionby_partkey";
    String l_od_suppkey_pt_partkey = "lineitem_col_index_orderby_suppkey_partitionby_partkey";
    String l_od_shipdate_pt_partkey = "lineitem_col_index_orderby_shipdate_partitionby_partkey";

    String l_od_orderkey_pt_suppkey_partkey = "lineitem_col_index_orderby_orderkey_partitionby_suppkey_and_partkey";
    String l_od_partkey_pt_suppkey_orderkey = "lineitem_col_index_orderby_partkey_partitionby_suppkey_and_orderkey";

    Set<String> lineitemAllCci = new HashSet<>();

    Set<String> lineitemAllCciOrderBy = new HashSet<>();

    {
        lineitemAllCci.add(l_od_shipdate_orderkey_pt_orderkey);
        lineitemAllCci.add(l_od_orderkey_shipdate_pt_orderkey);

        lineitemAllCci.add(l_od_orderkey_pt_orderkey);
        lineitemAllCci.add(l_od_partkey_pt_orderkey);
        lineitemAllCci.add(l_od_suppkey_pt_orderkey);
        lineitemAllCci.add(l_od_shipdate_pt_orderkey);

        lineitemAllCci.add(l_od_orderkey_pt_partkey);
        lineitemAllCci.add(l_od_partkey_pt_partkey);
        lineitemAllCci.add(l_od_suppkey_pt_partkey);
        lineitemAllCci.add(l_od_shipdate_pt_partkey);

        lineitemAllCci.add(l_od_orderkey_pt_suppkey_partkey);
        lineitemAllCci.add(l_od_partkey_pt_suppkey_orderkey);
    }

    @Test
    public void testAllScanCost() {
        String allScanSql1 = columnar_scan_hint + "select * from lineitem force index(%s)";
        checkCciCostAllEqual(allScanSql1, lineitemAllCci);

        String allScanSql2 = columnar_scan_hint + "select l_orderkey, l_extendedprice  from lineitem force index(%s)";
        checkCciCostAllEqual(allScanSql2, lineitemAllCci);

        String allScanSql3 = columnar_scan_hint
            + "select l_orderkey, l_extendedprice, l_quantity, l_commitdate  from lineitem force index(%s)";
        checkCciCostAllEqual(allScanSql3, lineitemAllCci);

        checkSqlCostOnSameCci(allScanSql3, allScanSql1, l_od_shipdate_orderkey_pt_orderkey);
        checkSqlCostOnSameCci(allScanSql2, allScanSql1, l_od_shipdate_orderkey_pt_orderkey);

    }

    @Test
    public void testCciPartitionKey() {
        String sql = columnar_scan_hint + "select l_orderkey, l_extendedprice from lineitem force index(%s) ";

        String whereOrderkeyEqualSql = sql + " where l_orderkey = 1447";
        //相同排序键，不同分区键
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_partkey_pt_orderkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_partkey_pt_orderkey, l_od_partkey_pt_suppkey_orderkey);

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_shipdate_pt_orderkey, l_od_shipdate_pt_partkey);

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_suppkey_pt_orderkey, l_od_suppkey_pt_partkey);

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_orderkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_orderkey_pt_suppkey_partkey);

        checkCciCostAllEqual(whereOrderkeyEqualSql, new String[] {
            l_od_partkey_pt_orderkey, l_od_shipdate_pt_orderkey, l_od_suppkey_pt_orderkey
        });

        String whereOrderkeyInSql = sql + " where l_orderkey in (1447, 2855, 4388, 6022)";
        //相同排序键，不同分区键
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_partkey_pt_orderkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_partkey_pt_orderkey, l_od_partkey_pt_suppkey_orderkey);

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_shipdate_pt_orderkey, l_od_shipdate_pt_partkey);

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_suppkey_pt_orderkey, l_od_suppkey_pt_partkey);

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_orderkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_orderkey_pt_suppkey_partkey);

        checkCciCostAllEqual(whereOrderkeyEqualSql, new String[] {
            l_od_partkey_pt_orderkey, l_od_shipdate_pt_orderkey, l_od_suppkey_pt_orderkey
        });

        String wherePartkeyAndSuppkeyEqualSql = sql + " where l_partkey = 3083750 and l_suppkey = 299702";
        //相同排序键，不同分区键
        checkCciCostInSameSql(wherePartkeyAndSuppkeyEqualSql, l_od_orderkey_pt_suppkey_partkey,
            l_od_orderkey_pt_orderkey);
        checkCciCostAllEqual(wherePartkeyAndSuppkeyEqualSql,
            new String[] {l_od_orderkey_pt_suppkey_partkey, l_od_orderkey_pt_partkey});

        String whereSuppkeyAndOrdereyEqualSql = sql + " where l_suppkey = 299702 and l_orderkey = 1447";
        //相同排序键，不同分区键
        checkCciCostInSameSql(whereSuppkeyAndOrdereyEqualSql, l_od_partkey_pt_suppkey_orderkey,
            l_od_partkey_pt_partkey);
        checkCciCostAllEqual(whereSuppkeyAndOrdereyEqualSql,
            new String[] {l_od_partkey_pt_suppkey_orderkey, l_od_partkey_pt_orderkey});

        //Or无法进行分区裁剪
        String whereSuppkeyOrOrdereyEqualSql = sql + " where l_suppkey = 299702 or l_orderkey = 1447";
        //相同排序键，不同分区键
        checkCciCostAllEqual(whereSuppkeyOrOrdereyEqualSql, new String[] {
            l_od_partkey_pt_suppkey_orderkey,
            l_od_partkey_pt_partkey, l_od_partkey_pt_orderkey
        });

        String whereOrderkeyRangeSql = sql + " where l_orderkey > 1447";
        //范围查询，无法进行分区裁剪
        checkCciCostAllEqual(whereOrderkeyRangeSql, new String[] {
            l_od_partkey_pt_orderkey, l_od_suppkey_pt_orderkey, l_od_shipdate_pt_orderkey,
            l_od_partkey_pt_partkey, l_od_suppkey_pt_partkey, l_od_shipdate_pt_partkey,
            l_od_partkey_pt_suppkey_orderkey, l_od_shipdate_orderkey_pt_orderkey
        });

        whereOrderkeyRangeSql = sql + " where l_orderkey < 2447";
        //范围查询，无法进行分区裁剪
        checkCciCostAllEqual(whereOrderkeyRangeSql, new String[] {
            l_od_partkey_pt_orderkey, l_od_suppkey_pt_orderkey, l_od_shipdate_pt_orderkey,
            l_od_partkey_pt_partkey, l_od_suppkey_pt_partkey, l_od_shipdate_pt_partkey,
            l_od_partkey_pt_suppkey_orderkey, l_od_shipdate_orderkey_pt_orderkey
        });

        whereOrderkeyRangeSql = sql + " where l_orderkey > 1447 and l_orderkey < 2447";
        //范围查询，无法进行分区裁剪
        checkCciCostAllEqual(whereOrderkeyRangeSql, new String[] {
            l_od_partkey_pt_orderkey, l_od_suppkey_pt_orderkey, l_od_shipdate_pt_orderkey,
            l_od_partkey_pt_partkey, l_od_suppkey_pt_partkey, l_od_shipdate_pt_partkey,
            l_od_partkey_pt_suppkey_orderkey, l_od_shipdate_orderkey_pt_orderkey
        });

        whereOrderkeyRangeSql = sql + " where l_orderkey between 1447 and 2447";
        //范围查询，无法进行分区裁剪
        checkCciCostAllEqual(whereOrderkeyRangeSql, new String[] {
            l_od_partkey_pt_orderkey, l_od_suppkey_pt_orderkey, l_od_shipdate_pt_orderkey,
            l_od_partkey_pt_partkey, l_od_suppkey_pt_partkey, l_od_shipdate_pt_partkey,
            l_od_partkey_pt_suppkey_orderkey, l_od_shipdate_orderkey_pt_orderkey
        });

    }

    @Test
    public void testCciSortKey() {
        String sql = columnar_scan_hint
            + "select l_orderkey, l_extendedprice, l_partkey, l_suppkey, l_shipdate from lineitem force index(%s) ";

        String whereOrderkeyEqualSql = sql + " where l_orderkey = 1447";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_shipdate_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_orderkey, l_od_shipdate_orderkey_pt_orderkey);
        checkCciCostAllEqual(whereOrderkeyEqualSql,
            new String[] {l_od_orderkey_pt_orderkey, l_od_orderkey_shipdate_pt_orderkey});

        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_partkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_partkey, l_od_suppkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyEqualSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);

        String whereOrderkeyInSql = sql + " where l_orderkey in (1447, 2855, 4388, 6022)";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_orderkey, l_od_shipdate_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_orderkey, l_od_shipdate_orderkey_pt_orderkey);
        checkCciCostAllEqual(whereOrderkeyInSql,
            new String[] {l_od_orderkey_pt_orderkey, l_od_orderkey_shipdate_pt_orderkey});

        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_partkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_partkey, l_od_suppkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyInSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);

        String whereOrderkeyRangeSql = sql + " where l_orderkey > 1447";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_shipdate_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_shipdate_orderkey_pt_orderkey);
        checkCciCostAllEqual(whereOrderkeyRangeSql,
            new String[] {l_od_orderkey_pt_orderkey, l_od_orderkey_shipdate_pt_orderkey});

        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_suppkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);

        whereOrderkeyRangeSql = sql + " where l_orderkey < 2447";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_shipdate_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_shipdate_orderkey_pt_orderkey);
        checkCciCostAllEqual(whereOrderkeyRangeSql,
            new String[] {l_od_orderkey_pt_orderkey, l_od_orderkey_shipdate_pt_orderkey});

        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_suppkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);

        whereOrderkeyRangeSql = sql + " where l_orderkey between 1447 and 2447";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_shipdate_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_orderkey, l_od_shipdate_orderkey_pt_orderkey);
        checkCciCostAllEqual(whereOrderkeyRangeSql,
            new String[] {l_od_orderkey_pt_orderkey, l_od_orderkey_shipdate_pt_orderkey});

        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_suppkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);

        //orderkey 谓词的过滤性比shipdate谓词的过滤性高
        String whereOrderkeyInAndShipdateRangeSql =
            sql + " where l_orderkey in (1447, 2855, 4388, 6022) and l_shipdate > '1996-03-09'";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_orderkey_shipdate_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_orderkey_shipdate_pt_orderkey,
            l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey,
            l_od_suppkey_pt_orderkey);

        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_shipdate_pt_orderkey, l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_shipdate_pt_orderkey, l_od_suppkey_pt_orderkey);

        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyInAndShipdateRangeSql, l_od_orderkey_shipdate_pt_orderkey,
            l_od_shipdate_orderkey_pt_orderkey);

        //orderkey 谓词的过滤性比shipdate谓词的过滤性低
        String whereOrderkeyRangeAndShipdateRangeSql = sql + " where l_orderkey > 8000 and l_shipdate < '1900-03-09'";

        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_orderkey_shipdate_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_orderkey_shipdate_pt_orderkey,
            l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey,
            l_od_suppkey_pt_orderkey);

        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_orderkey_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_orderkey_pt_orderkey,
            l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_shipdate_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_shipdate_pt_orderkey,
            l_od_suppkey_pt_orderkey);

        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_shipdate_pt_partkey,
            l_od_orderkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeAndShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey,
            l_od_orderkey_shipdate_pt_orderkey);

        String whereOrderkeyRangeOrOrderkeyRangeSql = sql + " where l_orderkey > 10000 or l_orderkey < 5000";
        //不同排序键，相同分区键
        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_orderkey,
            l_od_partkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_orderkey,
            l_od_suppkey_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_orderkey,
            l_od_shipdate_pt_orderkey);
        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_orderkey,
            l_od_shipdate_orderkey_pt_orderkey);
        checkCciCostAllEqual(whereOrderkeyRangeOrOrderkeyRangeSql,
            new String[] {l_od_orderkey_pt_orderkey, l_od_orderkey_shipdate_pt_orderkey});

        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_partkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_suppkey_pt_partkey);
        checkCciCostInSameSql(whereOrderkeyRangeOrOrderkeyRangeSql, l_od_orderkey_pt_partkey, l_od_shipdate_pt_partkey);

        //or查询，因为一个列存文件只有一个排序键，所以当or查询有两个列的范围查询时，必有至少一个列是无法走sortkey裁剪的，导致整体的代价为NoIndexIO
        String whereOrderkeyRangeOrShipdateRangeSql = sql + " where l_orderkey > 10000 or l_shipdate < '1996-03-09'";
        checkCciCostAllEqual(whereOrderkeyRangeOrShipdateRangeSql,
            new String[] {l_od_orderkey_shipdate_pt_orderkey, l_od_partkey_pt_orderkey});
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_orderkey_shipdate_pt_orderkey, l_od_suppkey_pt_orderkey);
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_shipdate_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
//
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_orderkey_pt_orderkey, l_od_partkey_pt_orderkey);
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_orderkey_pt_orderkey, l_od_suppkey_pt_orderkey);
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_shipdate_pt_orderkey, l_od_partkey_pt_orderkey);
//        checkCciCostInSameSql(whereOrderkeyRangeOrShipdateRangeSql, l_od_shipdate_pt_orderkey, l_od_suppkey_pt_orderkey);

        //具有更多过滤谓词的Cost更低，范围查询的基础估计逻辑是：选择过滤性最高的一个范围谓词做基数估计
//        String whereOrderkeyRangeAndShipdateRangeSql0 = sql + " where l_orderkey > 8000 and l_shipdate < '1997-03-09'";
//        String whereOrderkeyRangeAndShipdateRangeSql1 = sql + " where l_orderkey > 8000";
//        String whereOrderkeyRangeAndShipdateRangeSql2 = sql + " where l_shipdate < '1997-03-09'";
//        checkSqlCostOnSameCci(whereOrderkeyRangeAndShipdateRangeSql0, whereOrderkeyRangeAndShipdateRangeSql1, l_od_orderkey_pt_orderkey);
//        checkSqlCostOnSameCci(whereOrderkeyRangeAndShipdateRangeSql0, whereOrderkeyRangeAndShipdateRangeSql2, l_od_orderkey_pt_orderkey);
//        checkSqlCostOnSameCci(whereOrderkeyRangeAndShipdateRangeSql0, whereOrderkeyRangeAndShipdateRangeSql1, l_od_shipdate_pt_orderkey);
//        checkSqlCostOnSameCci(whereOrderkeyRangeAndShipdateRangeSql0, whereOrderkeyRangeAndShipdateRangeSql2, l_od_shipdate_pt_orderkey);

    }

    private void printLogicalViewCost(String sql, boolean ossTableScan) {
        RelOptCost cost1 = getLogicalViewCost(sql, ossTableScan);
        System.out.println((ossTableScan ? "OSSTableScan" : "LogicalView") + " : " + cost1);

    }

    private RelOptCost getLogicalViewCost(String sql, boolean ossTableScan) {
        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.getParamManager().getProps().put(ConnectionProperties.ENABLE_COLUMNAR_SCAN_COST, "true");
        RelNode plan = getExecutionPlan(sql, executionContext).getPlan();
        LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
        plan.accept(logicalViewFinder);
        LogicalView logicalView = logicalViewFinder.getResult().get(0);
        Assert.assertEquals(ossTableScan, logicalView instanceof OSSTableScan);
        return logicalView.computeSelfCost(plan.getCluster().getPlanner(), plan.getCluster().getMetadataQuery());
    }

    private void checkCciCostInSameSql(String sql, Set<String> allCci, String minCostCci) {
        if (!allCci.contains(minCostCci)) {
            Assert.fail();
        }

        RelOptCost minCost = getLogicalViewCost(String.format(sql, minCostCci), true);
        for (String cci : allCci) {
            if (cci.equals(minCostCci)) {
                continue;
            }
            RelOptCost cost = getLogicalViewCost(String.format(sql, cci), true);
            Assert.assertTrue(cci, minCost.isLt(cost));
        }
    }

    private void checkCciCostInSameSql(String sql, String[] allCci, String minCostCci) {
        Set<String> allCciSet = new HashSet<>();
        for (String cci : allCci) {
            allCciSet.add(cci);
        }
        allCciSet.add(minCostCci);
        checkCciCostInSameSql(sql, allCciSet, minCostCci);
    }

    private void checkCciCostInSameSql(String sql, String lowerCostCci, String higherCostCci) {
        RelOptCost cost1 = getLogicalViewCost(String.format(sql, lowerCostCci), true);
        RelOptCost cost2 = getLogicalViewCost(String.format(sql, higherCostCci), true);
        Assert.assertTrue(cost1.isLt(cost2));
    }

    private void checkCciCostAllEqual(String sql, Set<String> allCci) {
        Iterator<String> iterator = allCci.iterator();
        if (iterator.hasNext()) {
            RelOptCost cost = getLogicalViewCost(String.format(sql, iterator.next()), true);
            while (iterator.hasNext()) {
                Assert.assertTrue(cost.equals(getLogicalViewCost(String.format(sql, iterator.next()), true)));
            }
        }
    }

    private void checkCciCostAllEqual(String sql, String[] allCci) {
        Set<String> allCciSet = new HashSet<>();
        for (String cci : allCci) {
            allCciSet.add(cci);
        }
        checkCciCostAllEqual(sql, allCciSet);
    }

    private void checkSqlCostOnSameCci(String sqlWithLowerCost, String sqlWithHigherCost, String cci) {
        RelOptCost cost1 = getLogicalViewCost(String.format(sqlWithLowerCost, cci), true);
        RelOptCost cost2 = getLogicalViewCost(String.format(sqlWithHigherCost, cci), true);
        Assert.assertTrue(cost1.isLt(cost2));
    }

}
