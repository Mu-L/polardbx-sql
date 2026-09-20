package com.alibaba.polardbx.qatest.ddl.auto.partition;
import org.junit.runners.Parameterized;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Test;

import java.util.List;

@CdcIgnore(ignoreReason = "本地自增列会出现重复主键，在预期内故忽略")
public class LocalAutoIncTest extends PartitionAutoLoadSqlTestBase {
    public LocalAutoIncTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Test
    @CdcIgnore(ignoreReason = "本地自增列会出现重复主键，在预期内故忽略")
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        runOneTestCaseInner(this.params);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(LocalAutoIncTest.class, 3, true);
    }
}