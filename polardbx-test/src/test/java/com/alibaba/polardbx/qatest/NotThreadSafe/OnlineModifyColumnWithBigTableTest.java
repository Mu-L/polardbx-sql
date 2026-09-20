package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class OnlineModifyColumnWithBigTableTest extends DDLBaseNewDBTestCase {

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";

    @Test
    public void testDrdsBigTableWithSplit() throws ExecutionException, InterruptedException {
        AtomicBoolean stop = new AtomicBoolean(false);
        FutureTask<Collection<String>> futureTask = checkInvalidTrx(stop);
        new Thread(futureTask).start();
        try {
            String tableName = "omc_drds_big_table";
            dropTableIfExists(tableName);
            String sql = String.format("create table %s ("
                + "a int primary key auto_increment, "
                + "b int, "
                + "c text, "
                + "index idx_b(b)"
                + ") dbpartition by hash(a)", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            // prepare data
            sql = String.format("insert into %s(b, c) values(1, REPEAT('a', 5000))", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = String.format("insert into %s(b, c) select b, c from %s", tableName, tableName);
            for (int i = 0; i < 17; ++i) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            }

            // analyze table
            sql = String.format("analyze table %s", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

            String hint = "/*+TDDL:CMD_EXTRA("
                + "PHYSICAL_TABLE_START_SPLIT_SIZE = 100, PHYSICAL_TABLE_BACKFILL_PARALLELISM = 2, "
                + "ENABLE_SLIDE_WINDOW_BACKFILL = true, SLIDE_WINDOW_SPLIT_SIZE = 2, SLIDE_WINDOW_TIME_INTERVAL = 1000)*/";

            // do omc
            sql = hint + String.format("alter table %s modify column b bigint null,", tableName) + USE_OMC_ALGORITHM;
            execDdlWithRetry(tddlDatabase1, tableName, sql, tddlConnection);
        } finally {
            stop.set(true);
        }
        Collection<String> invalidTrx = futureTask.get();
        if (!invalidTrx.isEmpty()) {
            for (String trx : invalidTrx) {
                System.out.println(trx);
            }
        }
    }

}
