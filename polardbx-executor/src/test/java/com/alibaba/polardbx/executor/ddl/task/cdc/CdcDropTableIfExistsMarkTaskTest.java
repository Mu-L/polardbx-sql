package com.alibaba.polardbx.executor.ddl.task.cdc;

import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropTableIfExistsMarkTask;
import org.junit.Test;

public class CdcDropTableIfExistsMarkTaskTest {
    @Test
    public void testCheckTableName() {
        CdcDropTableIfExistsMarkTask.checkTableName("/*!50001 DROP TABLE IF EXISTS `verifydata` */;");
        CdcDropTableIfExistsMarkTask.checkTableName("DROP TABLE IF EXISTS `verifydata`;");
    }
}
