package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

@ReplicaIgnore(ignoreReason = "set session variables")
public class MultiPartDdlOmcTest extends PartitionAutoLoadSqlTestBase {
    public MultiPartDdlOmcTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @ReplicaIgnore(ignoreReason = "set session variables")
    @Test
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        runOneTestCaseInner(this.params);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(MultiPartDdlOmcTest.class, 0, false);
    }
}
