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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupCdcTest extends CdcBaseTest {

    private static final String DATABASE_NAME = "rebuild_cleanup_cdc_test";
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
    public void testDryRunDoesNotEmitCdcMark() throws SQLException {
        String dryRunSql = RebuildCleanupTestSupport.forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted' dry run";
        try (Statement statement = tddlConnection.createStatement()) {
            Assert.assertTrue("DRY RUN must return a result set", statement.execute(dryRunSql));
            try (ResultSet resultSet = statement.getResultSet()) {
                Assert.assertTrue(resultSet.next());
            }
        }

        Assert.assertEquals(12,
            RebuildCleanupTestSupport.queryCount(tddlConnection, "select count(*) from " + TABLE_NAME));
        Assert.assertEquals("DRY RUN must not emit a CDC DDL mark",
            initialCleanupMarkCount, getRebuildCleanupMarks().size());
    }

    @Test
    public void testCleanupEmitsProtectedCdcMark() {
        String cleanupSql = RebuildCleanupTestSupport.forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, cleanupSql);

        List<DdlRecordInfo> marks = getRebuildCleanupMarks();
        Assert.assertEquals(initialCleanupMarkCount + 1, marks.size());
        Assert.assertEquals(CdcDdlMarkVisibility.Protected.getValue(), marks.get(0).getVisibility());
        Assert.assertEquals("REBUILD_CLEANUP", marks.get(0).getSqlKind());
        Assert.assertEquals(DATABASE_NAME, marks.get(0).getSchemaName());
        Assert.assertEquals(TABLE_NAME, marks.get(0).getTableName());
        Assert.assertTrue(marks.get(0).getDdlSql().toLowerCase().contains("rebuild cleanup"));
    }

    @Test
    public void testSkipCdcTaskHintDoesNotEmitCdcMark() {
        String cleanupSql = RebuildCleanupTestSupport.forceOmc30SkipCdcHint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, cleanupSql);

        Assert.assertEquals(8,
            RebuildCleanupTestSupport.queryCount(tddlConnection, "select count(*) from " + TABLE_NAME));
        Assert.assertEquals(0, RebuildCleanupTestSupport.queryCount(
            tddlConnection, "select count(*) from " + TABLE_NAME + " where status = 'deleted'"));
        Assert.assertEquals("Skipping the CDC task must not emit a REBUILD_CLEANUP mark",
            initialCleanupMarkCount, getRebuildCleanupMarks().size());
    }

    private List<DdlRecordInfo> getRebuildCleanupMarks() {
        return getDdlRecordInfoList(DATABASE_NAME, TABLE_NAME).stream()
            .filter(mark -> "REBUILD_CLEANUP".equals(mark.getSqlKind()))
            .collect(Collectors.toList());
    }
}
