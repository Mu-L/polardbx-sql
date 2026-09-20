package com.alibaba.polardbx.qatest.ddl.ddlProgress;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class DdlProgressMultiPkTest extends DDLBaseNewDBTestCase {
    @Before
    public void beforeMethod() {
        JdbcUtil.executeSuccess(tddlConnection, "set ENABLE_OMC_30 = false");
    }

    private void prepareData(String tableName, int partitions) {
        dropTableIfExists(tableName);
        String createTable =
            String.format(
                "create table %s(a varchar(60), b int, c int,  primary key(a,b)) partition by key(a) partitions %s",
                tableName, partitions);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        StringBuilder sb = new StringBuilder(String.format("insert into %s values", tableName));
        for (int i = 0; i < 1024; i++) {
            sb.append(String.format("(UUID(), %s, %s)", i, i));
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
        String tableName = "t_ddl_progress_m_pk";
        prepareData(tableName, 3);
        System.out.println("prepareData success");

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s)*/", jobId);
        String sql =
            myHint + String.format("alter table %s modify column b bigint, algorithm = omc, async=true", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<DdlProgress> ddlProgressList = getDdlProgress(tddlConnection, jobId);

        // 验证 ddl 进度的几个阶段
        // before backfill
        int loopCount = 0;
        while (ddlProgressList.isEmpty() || ddlProgressList.get(0).getProgress().equalsIgnoreCase("-")) {
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
        long lastApproximateTotalRows = 0;
        while (!ddlProgressList.isEmpty() && !ddlProgressList.get(0).getProgress().equalsIgnoreCase("100%")) {
            finishedRows = Long.parseLong(ddlProgressList.get(0).getFinishedRows());
            approximateTotalRows = Long.parseLong(ddlProgressList.get(0).getApproximateTotalRows());
            Assert.assertTrue(finishedRows >= 0);
            Assert.assertTrue(finishedRows <= 512000);
            Assert.assertTrue(approximateTotalRows <= 652000);
            System.out.println("finishedRows: " + finishedRows);
            if (lastFinishedRows != 0 && finishedRows != 512000) {
                Assert.assertTrue(
                    String.format("current finished rows %s, last finished rows %s", finishedRows, lastFinishedRows),
                    finishedRows >= lastFinishedRows);
            }
            if (lastApproximateTotalRows != 0) {
                Assert.assertEquals(approximateTotalRows, lastApproximateTotalRows);
            }
            lastFinishedRows = finishedRows;
            lastApproximateTotalRows = approximateTotalRows;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            ddlProgressList = getDdlProgress(tddlConnection, jobId);
        }

        // show ddl status
        List<String> metricList =
            JdbcUtil.executeQueryAndGetColumnResult("show ddl status", tddlConnection, 1);
        Assert.assertTrue(metricList.contains("OMC_THREAD_POOL_SIZE"));
        Assert.assertTrue(metricList.contains("OMC_THREAD_POOL_NUM"));

        System.out.println("start checker");
        loopCount = 0;
        while (!ddlProgressList.isEmpty() && !ddlProgressList.get(0).getCheckProgress().equalsIgnoreCase("100%")) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 100) {
                System.out.println("waiting for checker finished failed");
                return;
            }
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

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}