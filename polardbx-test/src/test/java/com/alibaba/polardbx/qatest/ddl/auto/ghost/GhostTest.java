package com.alibaba.polardbx.qatest.ddl.auto.ghost;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

@ReplicaIgnore(ignoreReason = "set session variables")
public class GhostTest extends GhostTestBase {
    public String resourceFile;

    public GhostTest(String resourceFile) {
        this.resourceFile = resourceFile;
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<String> parameters() {
        return getParameters(GhostTest.class);
    }

    @ReplicaIgnore(ignoreReason = "set session variables")
    @Test
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.resourceFile)) {
            return;
        }
        runTestCase(this.resourceFile);
    }
}
