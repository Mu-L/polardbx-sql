package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.cdc.CdcBaseTest;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupCciTest extends CdcBaseTest {

    private static final String DATABASE_NAME = "rebuild_cleanup_cci_test";
    private static final String TABLE_NAME = RebuildCleanupTestSupport.TABLE_NAME;

    private int initialCleanupMarkCount;

    @Before
    public void setUpRebuildCleanup() {
        RebuildCleanupTestSupport.prepareTable(tddlConnection, DATABASE_NAME);
        initialCleanupMarkCount = getRebuildCleanupMarks().size();
    }

    @After
    public void tearDownRebuildCleanup() {
        RebuildCleanupTestSupport.dropDatabase(tddlConnection, DATABASE_NAME);
    }

    @Test
    public void testTableWithCciRequiresSameForceHintAndNotifiesColumnar() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL:CMD_EXTRA(ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=true,"
                + "SKIP_DDL_TASKS=WaitColumnarTableCreationTask)*/"
                + "alter table " + TABLE_NAME
                + " add clustered columnar index cleanup_orders_cci(status)"
                + " partition by key(id) partitions 3");

        String cleanupSql = RebuildCleanupTestSupport.forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateFailed(tddlConnection, cleanupSql, "does not support GSI or CCI");

        Assert.assertEquals(12,
            RebuildCleanupTestSupport.queryCount(tddlConnection, "select count(*) from " + TABLE_NAME));
        Assert.assertEquals(initialCleanupMarkCount, getRebuildCleanupMarks().size());

        String forcedCleanupSql = RebuildCleanupTestSupport.forceOmc30WithGsiHint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, forcedCleanupSql);

        Assert.assertEquals(8,
            RebuildCleanupTestSupport.queryCount(tddlConnection, "select count(*) from " + TABLE_NAME));
        Assert.assertEquals(0, RebuildCleanupTestSupport.queryCount(
            tddlConnection, "select count(*) from " + TABLE_NAME + " where status = 'deleted'"));

        List<DdlRecordInfo> marks = getRebuildCleanupMarks();
        Assert.assertEquals(initialCleanupMarkCount + 1, marks.size());
        Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(), marks.get(0).getVisibility());
        Assert.assertEquals("REBUILD_CLEANUP", marks.get(0).getSqlKind());
    }

    private List<DdlRecordInfo> getRebuildCleanupMarks() {
        return getDdlRecordInfoList(DATABASE_NAME, TABLE_NAME).stream()
            .filter(mark -> "REBUILD_CLEANUP".equals(mark.getSqlKind()))
            .collect(Collectors.toList());
    }
}
