package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

public class ToggleFullScanTest extends PartitionAutoLoadSqlTestBase {

    public ToggleFullScanTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Test
    @CdcIgnore(ignoreReason = "该测试用例禁止部分表全表扫描，会让导致CDC框架里的全扫sql报错")
    public void runTest() throws Exception {
        super.runTest();
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(ToggleFullScanTest.class, 3, true);
    }
}