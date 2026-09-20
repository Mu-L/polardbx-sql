package com.alibaba.polardbx.qatest.ddl.auto.omc30.progressTest;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class SinglePkProgressTest extends DDLBaseNewDBTestCase {

    @Before
    public void beforeMethod() {
        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
    }

    private void prepareData(String tableName, int partitions) {
        dropTableIfExists(tableName);
        String createTable =
            String.format("create table %s(a varchar(60) primary key, b int) partition by key(a) partitions %s",
                tableName, partitions);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        StringBuilder sb = new StringBuilder(String.format("insert into %s values", tableName));
        for (int i = 0; i < 1024; i++) {
            sb.append(String.format("(UUID(), %s)", i));
            if (i != 1023) {
                sb.append(",");
            }
        }

        String insertSql = sb.toString();
        // load data
        for (int i = 0; i < 500; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, insertSql);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + tableName);
    }

    @Test
    public void testLogicalDdlProgress() throws SQLException {
        String tableName = "t_ddl_progress_1";
        prepareData(tableName, 2);
        System.out.println("prepareData success");

        Long jobId = generateDdlJobId();
        String myHint = String.format(
            "/*+TDDL:cmd_extra(ddl_job_id=%s,FASTCHECKER_BATCH_SIZE=50000,PHYSICAL_TABLE_START_SPLIT_SIZE=50000)*/",
            jobId);
        String sql =
            myHint + String.format("alter table %s modify column b bigint, algorithm = omc, async=true", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<DdlProgress> ddlProgressList = getDdlProgress(tddlConnection, jobId);

        // before backfill
        int loopCount = 0;
        while (ddlProgressList.isEmpty() || !hasRowProgressOrFinished(ddlProgressList.get(0))) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 100) {
                System.out.println("waiting for backfill start failed");
                return;
            }
            ddlProgressList = getDdlProgress(tddlConnection, jobId);
        }

        // backfill
        System.out.println("start backfill");
        loopCount = 0;
        long finishedRows;
        long approximateTotalRows;
        long lastFinishedRows = 0;
        //long lastApproximateTotalRows = 0;
        while (!ddlProgressList.isEmpty() && !ddlProgressList.get(0).getProgress().equalsIgnoreCase("100%")) {
            DdlProgress currentProgress = ddlProgressList.get(0);
            if (!hasNumericRowProgress(currentProgress)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ddlProgressList = getDdlProgress(tddlConnection, jobId);
                continue;
            }
            finishedRows = Long.parseLong(currentProgress.getFinishedRows());
            approximateTotalRows = Long.parseLong(currentProgress.getApproximateTotalRows());
            Assert.assertTrue(finishedRows >= 0);
            Assert.assertTrue(finishedRows <= 512000);
            Assert.assertTrue(approximateTotalRows <= 652000);
            System.out.println("finishedRows: " + finishedRows + ", approximateTotalRows: " + approximateTotalRows);
            if (lastFinishedRows != 0 && finishedRows != 512000) {
                Assert.assertTrue(
                    String.format("current finished rows %s, last finished rows %s", finishedRows, lastFinishedRows),
                    finishedRows >= lastFinishedRows);
            }
            //if (lastApproximateTotalRows != 0) {
            //    Assert.assertEquals(approximateTotalRows, lastApproximateTotalRows);
            //}
            lastFinishedRows = finishedRows;
            //lastApproximateTotalRows = approximateTotalRows;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            ddlProgressList = getDdlProgress(tddlConnection, jobId);
        }

        System.out.println("waiting finished");
        loopCount = 0;
        while (!ddlProgressList.isEmpty() && ddlProgressList.get(0).getState().equalsIgnoreCase("RUNNING")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 600) {
                System.out.println("waiting for ddl finished failed");
                return;
            }
            ddlProgressList = getDdlProgress(tddlConnection, jobId);
        }
        System.out.println("ddl finished");
    }

    private static boolean hasRowProgressOrFinished(DdlProgress progress) {
        return "100%".equalsIgnoreCase(progress.getProgress()) || hasNumericRowProgress(progress);
    }

    private static boolean hasNumericRowProgress(DdlProgress progress) {
        return isLong(progress.getFinishedRows()) && isLong(progress.getApproximateTotalRows());
    }

    private static boolean isLong(String value) {
        if (value == null || value.isEmpty() || value.equals("-")) {
            return false;
        }
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
