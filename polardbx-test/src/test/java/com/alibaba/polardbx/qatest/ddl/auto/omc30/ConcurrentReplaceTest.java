package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.auto.omc30.omc30Utils.Omc30DmlBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

public class ConcurrentReplaceTest extends Omc30DmlBaseTest {

    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
        org.junit.Assume.assumeTrue(isMySQL80());
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void modifyWithReplace1() throws Exception {
        String tableName = "omc_with_replace_1";
        String colDef = "int";
        String alterSql = "alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s values(%d, %d + %d,null,null)", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void modifyWithReplace2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = "alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, ENABLE_LOCAL_UK_FULL_TABLE_SCAN)
                + "replace into %%s(a,b,c,d) values(%d + %d, %d,null,null)",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void modifyWithReplace3() throws Exception {
        String tableName = "omc_with_replace_3";
        String colDef = "int default 3";
        String alterSql = "alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker,
            true);
    }

    @Test
    public void modifyMultiWithReplace1() throws Exception {
        String tableName = "omc_multi_with_replace_1";
        String colDef = "int";
        String alterSql = "alter table %s modify column b bigint, modify column c varchar(20)";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a,b,c,d) values(%d, %d + %d, %d, %d+%d)",
            count, count, FILL_COUNT, count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB && (colD.equalsIgnoreCase("dbc")
                || Float.parseFloat(colC) + FILL_COUNT == Float.parseFloat(colD)));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void modifyMultiWithReplace2() throws Exception {
        String tableName = "omc_multi_with_replace_2";
        String colDef = "int";
        String alterSql = "alter table %s modify column c char(10) default 'dbc'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a,b) values(%d, %d + %d)", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB && (colC.equalsIgnoreCase("dbc")
                || colC.equalsIgnoreCase("abc")));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, true);
    }

    @Test
    public void changeWithReplace1() throws Exception {
        String tableName = "omc_with_replace_1";
        String colDef = "int";
        String alterSql = "alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s values(%d, %d + %d,null,null)", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void changeWithReplace2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = "alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, ENABLE_LOCAL_UK_FULL_TABLE_SCAN)
                + "replace into %%s values(%d + %d, %d,null,null)",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void changeWithReplace3() throws Exception {
        String tableName = "omc_with_replace_3";
        String colDef = "int default 3";
        String alterSql = "alter table %s change column b e bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, true);
    }

    @Test
    public void changeMultiWithReplace1() throws Exception {
        String tableName = "omc_with_replace_1";
        String colDef = "int";
        String alterSql =
            "alter table %s change column b e bigint, change column c cc char(10) character set utf8, modify column d varchar(30) not null";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s values(%d, %d + %d, %d, %d + %d)",
            count, count, FILL_COUNT, count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB
                && Float.parseFloat(colC) + FILL_COUNT == Float.parseFloat(colD));
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void changeMultiWithReplace2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = "alter table %s change column b e bigint, change column c cc char(10) character set utf8";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, ENABLE_LOCAL_UK_FULL_TABLE_SCAN)
                + "replace into %%s values(%d + %d, %d, \"def c\", \"def d\")",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA - FILL_COUNT == colB) && colC.equalsIgnoreCase("def c")
                && colD.equalsIgnoreCase("def d");
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void changeMultiWithReplace3() throws Exception {
        String tableName = "omc_with_replace_3";
        String colDef = "int default 3";
        String alterSql =
            "alter table %s change column b e bigint default 4, change column c f char(10) character set utf8mb4 default 'wumu'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("wumu"));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, true);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void singleChangeMultiWithReplace() throws Exception {
        String tableName = "omc_single_with_replace";
        String colDef = "int default 3";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") single",
            colDef);
        String alterSql =
            "alter table %s change column b e bigint default 4, change column c f char(10) character set utf8mb4 default 'wumu'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("wumu"));
        concurrentTestInternal30WithCreateTable(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true,
            createSql);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void broadcastChangeMultiWithReplace() throws Exception {
        String tableName = "omc_broadcast_with_replace";
        String colDef = "int default 3";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") broadcast",
            colDef);
        String alterSql =
            "alter table %s change column b e bigint default 4, change column c f char(10) character set utf8mb4 default 'wumu'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("wumu"));
        concurrentTestInternal30WithCreateTable(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true,
            createSql);
    }

    @Test
    @CdcIgnore(ignoreReason = "omc 精度变更导致cdc数据校验无法通过")
    public void changeWithReplace4() throws Exception {
        String tableName = "omc_with_replace_4";
        String colDef = "float(8,2)";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b decimal(9,3)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator = (count) -> String.format(
            "replace into %%s(a,b,c,d) values(%d, %f + %d,null,null)", count, count / 7.0, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;

        concurrentTestInternal30WithNotStrict(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true);
    }
}
