package com.alibaba.polardbx.optimizer.parse;

import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableRebuildCleanup;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

public class RebuildCleanupParserTest {

    @Test
    public void testParseRebuildCleanup() {
        SqlNode sqlNode = new FastsqlParser()
            .parse("ALTER TABLE test.t REBUILD CLEANUP WHERE status = 'deleted' DRY RUN")
            .get(0);

        SqlAlterTable alterTable = (SqlAlterTable) sqlNode;
        Assert.assertTrue(alterTable.isRebuildCleanup());
        SqlAlterTableRebuildCleanup cleanup =
            (SqlAlterTableRebuildCleanup) alterTable.getAlters().get(0);
        Assert.assertTrue(cleanup.isDryRun());
        Assert.assertEquals("(`status` = 'deleted')", cleanup.getCleanupPredicate().toString());
    }
}
