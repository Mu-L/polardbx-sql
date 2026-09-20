package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class ExplainOnlineDDLSequenceTest extends ExplainOnlineDDLBaseTest {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testCreateSequence() {
        String sql = "explain online_ddl create sequence test_seq_1";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);

        sql = "explain online_ddl create new sequence test_seq_2";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
    }

    @Test
    public void testDropSequence() {
        String sql = "create sequence test_seq_3";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "explain online_ddl drop sequence test_seq_3";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);

        sql = "drop sequence test_seq_3";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Test
    public void testAlterSequence() {
        String sql = "create new sequence test_seq_4";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "explain online_ddl alter sequence test_seq_4 START WITH 100";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);

        sql = "drop sequence test_seq_4";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Test
    public void testConvertSequence() {
        String sql = "create sequence test_seq_5";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "explain online_ddl CONVERT ALL SEQUENCES FROM GROUP to NEW";
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);

        sql = "drop sequence test_seq_5";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
