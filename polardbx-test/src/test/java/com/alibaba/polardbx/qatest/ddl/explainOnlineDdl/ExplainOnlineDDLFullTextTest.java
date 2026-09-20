package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainOnlineDDLFullTextTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_full_text";
    String localIndexName = "f_idx_c";

    @Before
    public void prepare() {
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, fulltext index %s(c)) partition by key(a)",
                tableName, localIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        setSupportInstant(tddlConnection);
    }

    @After
    public void clean() {
        String dropTableSql = String.format("drop table %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testOptimizeTable() {
        String sql = String.format("explain online_ddl optimize table %s", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAlterTableAddColumn() {
        String sql = String.format("explain online_ddl alter table %s add column gg int", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAlterTableDropColumn() {
        String sql = String.format("explain online_ddl alter table %s drop column b", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAlterTableModifyColumn() {
        String sql = String.format("explain online_ddl alter table %s modify column b varchar(10) not null", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(10)", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(11)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(300)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAddIndex() {
        String sql = String.format("explain online_ddl alter table %s add index(b)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyOptions() {
        String sql =
            String.format("explain online_ddl alter table %s comment \"test table\"", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyRowFormat() {
        String sql =
            String.format("explain online_ddl alter table %s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAddVirtualColumn() {
        String sql =
            String.format("explain online_ddl alter table %s add column d int generated always as (a + 1) virtual",
                tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }
}
