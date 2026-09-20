package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

public class DbleWithoutPartitionKeyTest extends PartitionAutoLoadSqlTestBase {

    public DbleWithoutPartitionKeyTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持GH相关用例")
    public void runTest() throws Exception {
        super.runTest();
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(DbleWithoutPartitionKeyTest.class, 3, true);
    }
}