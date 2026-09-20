package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@ReplicaIgnore(ignoreReason = "set session variables")
public class ExplainAdvisorFullTextTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_advisor_full_text";
    String localIndexName = "f_idx_c";

    @Before
    public void prepare() {
        String createTableSql =
            String.format("create table %s (a int, b varchar(10), c text, fulltext index %s(c)) partition by key(a)",
                tableName, localIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        setSupportInstant(tddlConnection);

        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
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
        String sql = String.format("explain advisor optimize table %s", tableName);
        String expected = String.format("/*+tddl:cmd_extra(force_using_omc=true)*/optimize table %s", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC20);
    }

    @Test
    public void testOptimizeTable2() {
        String sql = String.format("explain advisor alter table %s engine = innodb", tableName);
        String expected = String.format("alter table %s algorithm = omc, engine = innodb", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAlterTableAddColumn() {
        String sql = String.format("explain advisor alter table %s add column gg int", tableName);
        String expected = String.format("alter table %s add column gg int, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAlterTableDropColumn() {
        String sql = String.format("explain advisor alter table %s drop column b", tableName);
        String expected = String.format("alter table %s drop column b, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAlterTableModifyColumnVarchar1() {
        String sql = String.format("explain advisor alter table %s modify column b varchar(10) not null", tableName);
        String expected =
            String.format("alter table %s modify column b varchar(10) not null, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAlterTableModifyColumnVarchar2() {
        String sql = String.format("explain advisor alter table %s modify column b varchar(10)", tableName);
        String expected = String.format("ALTER TABLE %s MODIFY COLUMN b varchar(10)", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, expectAlgorithm);
    }

    @Test
    public void testAlterTableModifyColumnVarchar3() {
        String sql = String.format("explain advisor alter table %s modify column b varchar(11)", tableName);
        String expected = String.format("ALTER TABLE %s MODIFY COLUMN b varchar(11)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAlterTableModifyColumnVarchar4() {
        String sql = String.format("explain advisor alter table %s modify column b varchar(300)", tableName);
        String expected = String.format("alter table %s modify column b varchar(300), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAddIndex() {
        String sql = String.format("explain advisor alter table %s add index(b)", tableName);
        String expected = String.format("ALTER TABLE %s ADD INDEX (b)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyOptions() {
        String sql = String.format("explain advisor alter table %s comment \"test table\"", tableName);
        String expected = String.format("ALTER TABLE %s COMMENT = 'test table'", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyRowFormat() {
        String sql =
            String.format("explain advisor alter table %s ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4", tableName);
        String expected =
            String.format("alter table %s algorithm = omc, row_format = compressed key_block_size = 4", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAddVirtualColumn() {
        String sql =
            String.format("explain advisor alter table %s add column d int generated always as (a + 1) virtual",
                tableName);
        String expected =
            String.format("ALTER TABLE %s ADD COLUMN d int GENERATED ALWAYS AS (a + 1) VIRTUAL", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, expectAlgorithm);
    }
}
