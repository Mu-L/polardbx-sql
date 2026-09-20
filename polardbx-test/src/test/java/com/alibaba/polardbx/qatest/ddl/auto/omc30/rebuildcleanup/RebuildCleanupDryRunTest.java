package com.alibaba.polardbx.qatest.ddl.auto.omc30.rebuildcleanup;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.qatest.CdcIgnore;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

@CdcIgnore(ignoreReason = "REBUILD CLEANUP 不产生逐行 DELETE binlog，CDC 下游数据校验会不一致")
public class RebuildCleanupDryRunTest extends RebuildCleanupTestBase {

    private static final String DATABASE_NAME = "rebuild_cleanup_dry_run_test";

    @Override
    protected String databaseName() {
        return DATABASE_NAME;
    }

    @Test
    public void testDryRunReturnsPredicatesAndBoundedSamplesWithoutChangingData() throws SQLException {
        String dryRunSql = forceOmc30Hint()
            + "alter table " + TABLE_NAME
            + " rebuild cleanup where status = 'deleted' dry run";

        int sampleRows = 0;
        int cleanupSampleRows = 0;
        int keepSampleRows = 0;
        boolean cleanupPredicateFound = false;
        boolean keepPredicateFound = false;
        Set<String> sampledPhysicalTables = new HashSet<>();
        try (Statement statement = tddlConnection.createStatement()) {
            Assert.assertTrue("DRY RUN must return a result set", statement.execute(dryRunSql));
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    String type = resultSet.getString("TYPE");
                    String content = resultSet.getString("CONTENT");
                    if ("CLEANUP_PREDICATE".equals(type)) {
                        cleanupPredicateFound = true;
                        Assert.assertTrue(content.contains("status"));
                        Assert.assertTrue(content.contains("deleted"));
                    } else if ("KEEP_PREDICATE".equals(type)) {
                        keepPredicateFound = true;
                        Assert.assertTrue(content.contains("IS NOT TRUE"));
                    } else if ("SAMPLE_CLEANUP".equals(type)) {
                        sampleRows++;
                        cleanupSampleRows++;
                        sampledPhysicalTables.add(resultSet.getString("PHYSICAL_TABLE"));
                        RebuildCleanupTestSupport.assertSampleColumn(content, "id");
                        RebuildCleanupTestSupport.assertSampleColumn(content, "status");
                        Assert.assertEquals("deleted", JSONObject.parseObject(content).getString("status"));
                    } else if ("SAMPLE_KEEP".equals(type)) {
                        sampleRows++;
                        keepSampleRows++;
                        sampledPhysicalTables.add(resultSet.getString("PHYSICAL_TABLE"));
                        RebuildCleanupTestSupport.assertSampleColumn(content, "id");
                        RebuildCleanupTestSupport.assertSampleColumn(content, "status");
                        Assert.assertFalse("deleted".equals(JSONObject.parseObject(content).getString("status")));
                    } else {
                        Assert.fail("Unexpected DRY RUN result type: " + type + ", content: " + content);
                    }
                }
            }
        }

        Assert.assertTrue(cleanupPredicateFound);
        Assert.assertTrue(keepPredicateFound);
        Assert.assertTrue("DRY RUN should return cleanup samples", cleanupSampleRows > 0);
        Assert.assertTrue("DRY RUN should return keep samples", keepSampleRows > 0);
        Assert.assertTrue("DRY RUN must return no more than 5 cleanup sample rows", cleanupSampleRows <= 5);
        Assert.assertTrue("DRY RUN must return no more than 5 keep sample rows", keepSampleRows <= 5);
        Assert.assertTrue("DRY RUN must return no more than 10 sample rows", sampleRows <= 10);
        Assert.assertTrue("DRY RUN must sample no more than 2 physical partitions",
            sampledPhysicalTables.size() <= 2);
        Assert.assertEquals(12, queryCount("select count(*) from " + TABLE_NAME));
    }
}
