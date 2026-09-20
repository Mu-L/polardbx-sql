package com.alibaba.polardbx.qatest.ddl.auto.omc30.progressTest;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OmcConcurrentExecuteTest extends DDLBaseNewDBTestCase {

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
    public void testSequential() throws SQLException {
        String tableName = "t_omc_progress_1";
        prepareData(tableName, 2);
        System.out.println("prepareData success");

        Long jobId = generateDdlJobId();
        String myHint = String.format(
            "/*+TDDL:cmd_extra(ddl_job_id=%s,FASTCHECKER_BATCH_SIZE=50000,PHYSICAL_TABLE_START_SPLIT_SIZE=50000, OMC_SEQUENTIAL_POLICY=true)*/",
            jobId);
        String sql =
            myHint + String.format("alter table %s modify column b bigint, algorithm = omc, async=true", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<OmcProgress> omcProgressList = getOmcProgress(tddlConnection, jobId);

        // wait start
        int loopCount = 0;
        while (omcProgressList.isEmpty()) {
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
            omcProgressList = getOmcProgress(tddlConnection, jobId);
        }

        // wait finished
        for (int idx = 0; idx < 2; idx++) {
            int loop = 0;
            do {
                omcProgressList = getOmcProgress(tddlConnection, jobId);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                loop++;
                if (loop > 10000) {
                    throw new RuntimeException("waiting for omc phy ddl schedule failed");
                }
            } while (idx > omcProgressList.size() - 1);
            Assert.assertEquals("RUNNING", omcProgressList.get(idx).getState());

            // check backfill
            System.out.println("start backfill");
            loopCount = 0;
            long finishedRows;
            long approximateTotalRows;
            long lastFinishedRows = 0;
            long lastApproximateTotalRows = 0;
            while (!omcProgressList.isEmpty() && !omcProgressList.get(idx).getProgress().equalsIgnoreCase("100%")
                && Objects.equals(omcProgressList.get(idx).getOmcState(), "BACKFILL")) {
                finishedRows = Long.parseLong(omcProgressList.get(0).getFinishedRows());
                approximateTotalRows = Long.parseLong(omcProgressList.get(0).getApproximateTotalRows());
                Assert.assertTrue(finishedRows > 0);
                Assert.assertTrue(finishedRows < 512000);
                Assert.assertTrue(approximateTotalRows < 512000);
                if (lastFinishedRows != 0) {
                    Assert.assertTrue(finishedRows >= lastFinishedRows);
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
                omcProgressList = getOmcProgress(tddlConnection, jobId);
            }

            System.out.println("start checker");
            loopCount = 0;
            while (!omcProgressList.isEmpty()
                && !omcProgressList.get(idx).getCheckProgress().equalsIgnoreCase("100%")
                && Objects.equals(omcProgressList.get(idx).getOmcState(), "CHECKER")) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                loopCount++;
                if (loopCount > 30) {
                    System.out.println("waiting for checker finished failed");
                    return;
                }
                omcProgressList = getOmcProgress(tddlConnection, jobId);
            }

            System.out.println("waiting finished");
            loopCount = 0;
            while (!omcProgressList.isEmpty() && omcProgressList.get(idx).getState().equalsIgnoreCase("RUNNING")) {
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
                omcProgressList = getOmcProgress(tddlConnection, jobId);
            }
            System.out.println("ghost finished");
        }

        System.out.println("ddl finished");
    }

    @Test
    public void testInstanceConcurrent() throws SQLException {
        String tableName = "t_omc_progress_2";
        prepareData(tableName, 16);
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

        List<OmcProgress> omcProgressList = getOmcProgress(tddlConnection, jobId);

        // wait start
        int loopCount = 0;
        while (omcProgressList.isEmpty()) {
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
            omcProgressList = getOmcProgress(tddlConnection, jobId);
        }

        // wait finished
        long maxRunningCount = 0;
        while (!omcProgressList.isEmpty()) {

            long runningCount = omcProgressList.stream().filter(e -> Objects.equals(e.state, "RUNNING")).count();
            Assert.assertTrue(String.format("running count is %s which should less then or equal 2", runningCount),
                runningCount <= 2);
            maxRunningCount = Math.max(maxRunningCount, runningCount);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 1000) {
                System.out.println("waiting for finished start failed");
                return;
            }
            omcProgressList = getOmcProgress(tddlConnection, jobId);
        }

        Assert.assertTrue(String.format("max running count is %s which should bigger then 1", maxRunningCount),
            maxRunningCount > 1);
    }

    @Test
    public void testDdlConcurrent() throws SQLException {
        String tableName = "t_omc_progress_3";
        prepareData(tableName, 16);
        System.out.println("prepareData success");

        Long jobId = generateDdlJobId();
        String myHint = String.format(
            "/*+TDDL:cmd_extra(ddl_job_id=%s,FASTCHECKER_BATCH_SIZE=50000,PHYSICAL_TABLE_START_SPLIT_SIZE=50000, OMC_FULL_CONCURRENT_POLICY=true,OMC_PREFETCH_SHARDS=2)*/",
            jobId);
        String sql =
            myHint + String.format("alter table %s modify column b bigint, algorithm = omc, async=true", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        List<OmcProgress> omcProgressList = getOmcProgress(tddlConnection, jobId);

        // wait start
        int loopCount = 0;
        while (omcProgressList.isEmpty()) {
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
            omcProgressList = getOmcProgress(tddlConnection, jobId);
        }

        // wait finished
        long maxRunningCount = 0;
        while (!omcProgressList.isEmpty()) {

            long runningCount = omcProgressList.stream().filter(e -> Objects.equals(e.state, "RUNNING")).count();
            Assert.assertTrue(String.format("running count is %s which should less then or equal 4", runningCount),
                runningCount <= 4);
            maxRunningCount = Math.max(maxRunningCount, runningCount);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            loopCount++;
            if (loopCount > 2000) {
                System.out.println("waiting for finished start failed");
                return;
            }
            omcProgressList = getOmcProgress(tddlConnection, jobId);
        }

        Assert.assertTrue(String.format("max running count is %s which should bigger then 1", maxRunningCount),
            maxRunningCount > 2);
    }

    @Data
    public static class OmcProgress {
        String tableSchema;
        String tableName;
        Long jobId;
        String backfillId;
        String state;
        String omcState;
        String finishedRows;
        String approximateTotalRows;
        String progress;
        String checkProgress;
    }

    protected List<OmcProgress> getOmcProgress(Connection connection, Long jobId) throws SQLException {
        List<OmcProgress> progresses = new ArrayList<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection,
            String.format("select * from information_schema.omc_progress where job_id = %s order by STATE desc",
                jobId))) {
            while (rs.next()) {
                OmcProgress ddlProgress = new OmcProgress();
                ddlProgress.jobId = rs.getLong("JOB_ID");
                ddlProgress.backfillId = rs.getString("BACKFILL_ID");
                ddlProgress.tableSchema = rs.getString("TABLE_SCHEMA");
                ddlProgress.tableName = rs.getString("TABLE_NAME");
                ddlProgress.state = rs.getString("STATE");
                ddlProgress.omcState = rs.getString("OMC_STATE");
                ddlProgress.finishedRows = rs.getString("FINISHED_ROWS");
                ddlProgress.approximateTotalRows = rs.getString("APPROXIMATE_TOTAL_ROWS");
                ddlProgress.progress = rs.getString("PROGRESS");
                ddlProgress.checkProgress = rs.getString("CHECKER_PROGRESS");
                progresses.add(ddlProgress);
            }
        }
        return progresses;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}