package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.auto.omc30.omc30Utils.Omc30DmlBaseTest;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

public class ConcurrentInsertTest extends Omc30DmlBaseTest {

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(isMySQL80());
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    @CdcIgnore(ignoreReason = "分布式加的列有DEFAULT CURRENT_TIMESTAMP时有可能导致上下游数据不一致")
    public void addColumnWithInsert() throws Exception {
        String tableName = "add_column_with_insert_1";
        String colDef = "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        String alterSql = " alter table %s add column e int";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,e) values (%d,%d+1)", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    @CdcIgnore(ignoreReason = "分布式加的列有DEFAULT CURRENT_TIMESTAMP时有可能导致上下游数据不一致")
    public void addColumnWithInsert2() throws Exception {
        String tableName = "add_column_with_insert_2";
        String colDef = "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        String alterSql =
            " alter table %s add column e timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,e) values (%d,now())", count);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void modifyWithInsert1() throws Exception {
        String tableName = "omc_with_insert_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyWithInsert2() throws Exception {
        String tableName = "omc_with_insert_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colB == 0;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyWithInsert3() throws Exception {
        String tableName = "omc_with_insert_3";
        String colDef = "int not null";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint not null";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyWithInsert4() throws Exception {
        String tableName = "omc_with_insert_4";
        String colDef = "int not null";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint not null";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values (%d,%d+1,null,null)", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, false);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void modifyWithInsert5() throws Exception {
        String tableName = "omc_with_insert_5";
        String colDef = "int not null default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint not null default 4";
        String selectSql = "select * from %s";
        Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4);
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void modifyWithInsert6() throws Exception {
        String tableName = "omc_with_insert_6";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default null";
        String selectSql = "select * from %s";
        Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 0);
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyWithInsert7() throws Exception {
        String tableName = "omc_with_insert_7";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                count * 2 + 1, count * 2 + 2);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyWithInsert8() throws Exception {
        String tableName = "omc_with_insert_8";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyMultiWithInsert1() throws Exception {
        String tableName = "omc_multi_with_insert_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column d varchar(20) default 'abc' after b";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB && colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyMultiWithInsert2() throws Exception {
        String tableName = "omc_multi_with_insert_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column d varchar(20)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b,c,d) values (%d,%d+1,%d,%d+1)", count, count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB && Float.parseFloat(colC) + 1 == Float.parseFloat(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void modifyMultiWithInsert3() throws Exception {
        String tableName = "omc_multi_with_insert_3";
        String colDef = "int not null";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column d varchar(2)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b,c,d) values (%d,%d+1,%d,%d)", count, count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB && (colC.equals(colD) || colC.substring(0, 2).equals(colD)));
        concurrentTestInternal30WithNotStrict(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            false);
    }

    @Test
    public void modifyMultiWithInsert4() throws Exception {
        String tableName = "omc_multi_with_insert_4";
        String colDef = "int not null default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column d varchar(10) default 'bcd'";
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s(a,b,c) values (%d,%d+1,%d)", count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB && Float.parseFloat(colC) == colA && (
                colD.equalsIgnoreCase("abc") || colD.equalsIgnoreCase("bcd")));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, false);
    }

    @Test
    public void changeWithInsert1() throws Exception {
        String tableName = "omc_with_insert_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d,%d+1)", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void changeWithInsert2() throws Exception {
        String tableName = "omc_with_insert_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 = (count) -> String.format("insert into %%s(a) values (%d)", count);
        Function<Integer, String> generator2 = (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colB == 0;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void changeWithInsert3() throws Exception {
        String tableName = "omc_with_insert_3";
        String colDef = "int not null";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint not null";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d,%d+1)", count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void changeWithInsert5() throws Exception {
        String tableName = "omc_with_insert_5";
        String colDef = "int not null default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint not null default 4";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 = (count) -> String.format("insert into %%s(a) values (%d)", count);
        Function<Integer, String> generator2 = (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4);
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void changeWithInsert6() throws Exception {
        String tableName = "omc_with_insert_6";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default null";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 = (count) -> String.format("insert into %%s(a) values (%d)", count);
        Function<Integer, String> generator2 = (count) -> String.format("insert into %%s(a) values (%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 0);
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void changeWithInsert7() throws Exception {
        String tableName = "omc_with_insert_7";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                count * 2 + 1, count * 2 + 2);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                count * 2 + 1, count * 2 + 2);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void changeWithInsert8() throws Exception {
        String tableName = "omc_with_insert_8";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> colA + 1 == colB;
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @CdcIgnore(ignoreReason = "改列名，default 值 cdc 会以逻辑表的为准，造成上下游不一致")
    @Test
    public void changeMultiWithInsert1() throws Exception {
        String tableName = "omc_multi_with_insert_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c cc varchar(20) default 'new default'";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count,
                count, count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count,
                count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB);
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void changeMultiWithInsert2() throws Exception {
        String tableName = "omc_multi_with_insert_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c d varchar(20), change column d c varchar(20)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format(
                "insert into %%s(a,b,c,d) values (%d*2,%d*2+1,%d*2,%d*2+1),(%d*2+1,%d*2+2,%d*2,%d*2+1)",
                count, count, count, count, count, count, count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format(
                "insert into %%s(a,e,d,c) values (%d*2,%d*2+1,%d*2,%d*2+1),(%d*2+1,%d*2+2,%d*2,%d*2+1)",
                count, count, count, count, count, count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB) && (Float.parseFloat(colC) + 1 == Float.parseFloat(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void changeMultiWithInsert3() throws Exception {
        String tableName = "omc_multi_with_insert_3";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c d int, change column d c int";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format(
                "insert into %%s(a,b,c,d) values (%d*2,%d*2+1,%d*2,%d*2+1),(%d*2+1,%d*2+2,%d*2,%d*2+1)",
                count, count, count, count, count, count, count, count);
        Function<Integer, String> generator2 =
            (count) -> String.format(
                "insert into %%s(a,e,d,c) values (%d*2,%d*2+1,%d*2,%d*2+1),(%d*2+1,%d*2+2,%d*2,%d*2+1)",
                count, count, count, count, count, count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + 1 == colB) && (Float.parseFloat(colC) + 1 == Float.parseFloat(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    public void omcWithFloatPk() throws Exception {
        String tableName = "omc_with_float_pk";
        String colDef = "float primary key";
        String createSql = String.format(
            "create table %%s ("
                + "a int, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") partition by hash(`a`) partitions 3",
            colDef);
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column c int," + USE_OMC_ALGORITHM;
        String selectSql = "select * from %s";
        Function<Integer, String> generator =
            (count) -> String.format(
                "insert into %%s(a,b,c,d) values (%d*2,%d*2+1,%d*2,%d*2+1),(%d*2+1,%d*2+2,%d*2,%d*2+1)",
                count, count, count, count, count, count, count, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;
        concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            false, false, createSql);
    }

    @Test
    @CdcIgnore(ignoreReason = "omc 精度变更导致cdc数据校验无法通过")
    public void changeWithInsert9() throws Exception {
        String tableName = "omc_with_insert_9";
        String colDef = "decimal(8,2)";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e decimal(9,3)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%f)", count, count / 7.0);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d,%f)", count, count / 7.0);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;

        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    @CdcIgnore(ignoreReason = "omc 精度变更导致cdc数据校验无法通过")
    public void changeWithInsert10() throws Exception {
        String tableName = "omc_with_insert_10";
        String colDef = "decimal(8,2)";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b decimal(9,3)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%f)", count, count / 7.0);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;

        concurrentTestInternal30WithNotStrict(tableName, colDef, alterSql, selectSql, generator1, generator1, checker,
            false);
    }

    @Test
    @CdcIgnore(ignoreReason = "omc 精度变更导致cdc数据校验无法通过")
    public void changeWithInsert11() throws Exception {
        String tableName = "omc_with_insert_11";
        String colDef = "float(8,2)";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e decimal(9,3)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%f)", count, count / 7.0);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s(a,e) values (%d,%f)", count, count / 7.0);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;

        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false);
    }

    @Test
    @CdcIgnore(ignoreReason = "omc 精度变更导致cdc数据校验无法通过")
    public void changeWithInsert12() throws Exception {
        String tableName = "omc_with_insert_12";
        String colDef = "float(8,2)";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b decimal(9,3)";
        String selectSql = "select * from %s";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s(a,b) values (%d,%f)", count, count / 7.0);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;

        concurrentTestInternal30WithNotStrict(tableName, colDef, alterSql, selectSql, generator1, generator1, checker,
            false);
    }
}