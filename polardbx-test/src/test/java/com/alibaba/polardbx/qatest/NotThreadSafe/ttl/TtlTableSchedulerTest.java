package com.alibaba.polardbx.qatest.NotThreadSafe.ttl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

public class TtlTableSchedulerTest extends PartitionAutoLoadSqlTestBase {
    public TtlTableSchedulerTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TtlTableSchedulerTest.class, 0, false);
    }
}
