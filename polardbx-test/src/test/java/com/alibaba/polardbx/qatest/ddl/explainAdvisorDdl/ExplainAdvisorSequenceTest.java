package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

public class ExplainAdvisorSequenceTest extends ExplainOnlineDDLBaseTest {

    @Test
    public void testCreateSequence() {
        String sql = "explain advisor create sequence test_seq_1";
        String expectedSql = "CREATE SEQUENCE test_seq_1";
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);

        sql = "explain advisor create new sequence test_seq_2";
        expectedSql = "CREATE SEQUENCE test_seq_2";
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);

        sql = "drop sequence test_seq_1";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "drop sequence test_seq_2";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Test
    public void testDropSequence() {
        String sql = "create sequence test_seq_3";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "explain advisor drop sequence test_seq_3";
        String expectedSql = "DROP SEQUENCE test_seq_3";
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testAlterSequence() {
        String sql = "create new sequence test_seq_4";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "explain advisor alter sequence test_seq_4 START WITH 100";
        String expectedSql = "ALTER SEQUENCE test_seq_4 START WITH 100";
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);

        sql = "drop sequence test_seq_4";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    /**
     * 会全局修改sequence，会影响其他用例
     */
    @Ignore
    @Test
    public void testConvertSequence() {
        String sql = "create sequence test_seq_5";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "explain advisor CONVERT ALL SEQUENCES FROM GROUP to NEW";
        String expectedSql = "CONVERT ALL SEQUENCES FROM GROUP to NEW";
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.META_ONLY);

        sql = "drop sequence test_seq_5";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
