package com.alibaba.polardbx.qatest.ddl.auto.partition;
import com.alibaba.polardbx.qatest.CdcIgnore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

public class ReplicasTableDMLTest extends PartitionAutoLoadSqlTestBase {
    public ReplicasTableDMLTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持ReplicasTable相关用例")
    public void runTest() throws Exception {
        super.runTest();
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(ReplicasTableDMLTest.class, 0, false);
    }
}