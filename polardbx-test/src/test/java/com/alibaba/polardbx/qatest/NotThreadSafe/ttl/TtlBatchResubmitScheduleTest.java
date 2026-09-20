package com.alibaba.polardbx.qatest.NotThreadSafe.ttl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests for the batch-resubmit scheduling mode introduced in CleanAndPrepareExpiredDataTask.
 * <p>
 * Covered scenarios:
 * 1. Multi-round batch-resubmit with small TTL_JOB_DEFAULT_BATCH_SIZE (verifies resubmit loop).
 * 2. TTL_MAX_WORKER_COUNT_EACH_DN hint (configuredMax > 0 branch).
 * 3. TTL_ENABLE_BATCH_RESUBMIT_SCHEDULE=false fallback to zigzag mode.
 * 4. Single-partition table falls back to zigzag mode (taskRunners.size() <= 1).
 * 5. TTL_ENABLE_INTRA_TASK_INFO_LOG=true exercises log code paths.
 * <p>
 * Because test B1 modifies the global TTL_JOB_DEFAULT_BATCH_SIZE setting, these tests
 * must not run concurrently with other tests; hence they reside in the NotThreadSafe package.
 */
public class TtlBatchResubmitScheduleTest extends PartitionAutoLoadSqlTestBase {

    public TtlBatchResubmitScheduleTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TtlBatchResubmitScheduleTest.class, 0, false);
    }
}
