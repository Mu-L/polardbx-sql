package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.IcbcIgnore;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
@IcbcIgnore(ignoreReason = "explicit_defaults_for_timestamp")
public class PartitionTableDdlTest extends PartitionAutoLoadSqlTestBase {

    public PartitionTableDdlTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTableDdlTest.class, 3, true);
    }
}
