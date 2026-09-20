package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

@CdcIgnore(ignoreReason = "存在隐藏主键变更")
public class PrimaryKeyTest extends PartitionAutoLoadSqlTestBase {

    public PrimaryKeyTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Test
    @CdcIgnore(ignoreReason = "存在隐藏主键变更")
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        runOneTestCaseInner(this.params);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PrimaryKeyTest.class, 3, true);
    }
}