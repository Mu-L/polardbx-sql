package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.task.omc.OmcPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

public class LogicalAlterTableRebuildCleanupHandlerTest {

    @Test
    public void testBuildKeepFilterPreservesUnknownRows() {
        String keepFilter =
            LogicalAlterTableRebuildCleanupHandler.buildKeepFilter("status = 'deleted'");

        Assert.assertEquals("((status = 'deleted') IS NOT TRUE)", keepFilter);
        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        columns.addAll(Arrays.asList("id", "status"));
        Assert.assertEquals(
            new TreeSet<>(Arrays.asList("status")),
            OmcUtils.validateKeepFilter(keepFilter, columns));
    }

    @Test
    public void testBuildSampleQueryIsBoundedWithoutCount() {
        String sql = LogicalAlterTableRebuildCleanupHandler.buildSampleQuery(
            "t_00001",
            Arrays.asList("id", "status"),
            "`status` = 'deleted'",
            5);

        Assert.assertEquals(
            "SELECT /*+ MAX_EXECUTION_TIME(1000) */ `id`, `status` FROM `t_00001` "
                + "WHERE ((`status` = 'deleted') IS TRUE) ORDER BY `id` LIMIT 5",
            sql);
        Assert.assertFalse(sql.toUpperCase().contains("COUNT("));
    }

    @Test
    public void testCleanupKeepFilterIsPersistedInOmcTask() {
        String keepFilter = "((`status` = 'deleted') IS NOT TRUE)";
        OmcPhyDdlTask task = new OmcPhyDdlTask(
            "s",
            "t",
            "ALTER TABLE `t` ENGINE=INNODB",
            "ALTER TABLE t REBUILD CLEANUP WHERE status = 'deleted'",
            Collections.emptyMap(),
            false,
            keepFilter);

        Assert.assertEquals(keepFilter, task.getCleanupKeepFilter());
    }
}
