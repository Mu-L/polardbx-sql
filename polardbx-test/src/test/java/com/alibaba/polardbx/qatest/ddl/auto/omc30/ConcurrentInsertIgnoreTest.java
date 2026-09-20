package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.ddl.auto.omc.ConcurrentDMLBaseTest;
import com.alibaba.polardbx.qatest.ddl.auto.omc30.omc30Utils.Omc30DmlBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;

public class ConcurrentInsertIgnoreTest extends Omc30DmlBaseTest {
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
    public void modifyWithInsertIgnore1() throws Exception {
        String tableName = "omc_with_insert_ignore_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void modifyWithInsertIgnore2() throws Exception {
        String tableName = "omc_with_insert_ignore_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c char(10) after d";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void changeWithInsertIgnore1() throws Exception {
        String tableName = "omc_with_insert_ignore_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void changeWithInsertIgnore2() throws Exception {
        String tableName = "omc_with_insert_ignore_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void changeMultiWithInsertIgnore1() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_1";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c d char(10), change column d c varchar(20)";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30WithoutGenerateCol(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true);
    }

    @Test
    public void singleChangeMultiWithInsertIgnore() throws Exception {
        String tableName = "omc_single_multi_with_insert_ignore";
        String colDef = "int unique key";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") single", colDef);
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column d f char(10) default 'xyz'";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB));
        concurrentTestInternal30WithCreateTable(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true, createSql);
    }

    @Test
    public void broadcastChangeMultiWithInsertIgnore() throws Exception {
        String tableName = "omc_single_multi_with_insert_ignore";
        String colDef = "int unique key";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") broadcast", colDef);
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column d f char(10) default 'xyz'";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB));
        concurrentTestInternal30WithCreateTable(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true, createSql);
    }

    @Test
    public void modifyWithInsertIgnore3() throws Exception {
        String tableName = "omc_with_insert_ignore_3";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s(a,b,c,d) values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, true);
    }

    @Test
    public void modifyWithInsertIgnore4() throws Exception {
        String tableName = "omc_with_insert_ignore_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c char(10) after d";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator, checker, true);
    }

    @Test
    public void changeWithInsertIgnore3() throws Exception {
        String tableName = "omc_with_insert_ignore_3";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s(a,b,c,d) values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        Function<Integer, String> generator2 = (count) -> String.format(
            "insert ignore into %%s(a,e,c,d) values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true);
    }

    @Test
    public void changeWithInsertIgnore4() throws Exception {
        String tableName = "omc_with_insert_ignore_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        Function<Integer, String> generator2 = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,e,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true);
    }

    @Test
    public void changeMultiWithInsertIgnore3() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_3";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c d char(10), change column d c varchar(20)";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        Function<Integer, String> generator2 = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,e,d,c) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true);
    }

    @Test
    public void changeMultiWithInsertIgnore4() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column d f char(10) default 'xyz' after c";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        Function<Integer, String> generator2 = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,e,c,f) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        ConcurrentDMLBaseTest.QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal30(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true);
    }
}