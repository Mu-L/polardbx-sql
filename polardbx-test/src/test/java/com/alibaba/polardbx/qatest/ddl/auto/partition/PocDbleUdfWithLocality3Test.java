package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.server.util.StringUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */

@NotThreadSafe
public class PocDbleUdfWithLocality3Test extends PartitionAutoLoadSqlTestBase {

    public PocDbleUdfWithLocality3Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
        this.udfDelimiter = NEW_UDF_DELIMITER;
    }

    @Before
    public void beforeLoadCaseTestCase() {
        List<String> storageList = LocalityTestUtils.getDatanodes(tddlConnection);
        LocalityTestUtils.flushStorageLabel(storageList, tddlConnection);
    }

    @Test
    @CdcIgnore(ignoreReason = "CDC不支持UDF相关用例")
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        runOneTestCaseInner(this.params);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PocDbleUdfWithLocality3Test.class, 0, false);
    }
}
