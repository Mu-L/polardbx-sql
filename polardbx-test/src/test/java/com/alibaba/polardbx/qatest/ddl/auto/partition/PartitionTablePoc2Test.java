package com.alibaba.polardbx.qatest.ddl.auto.partition;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */

@NotThreadSafe
public class PartitionTablePoc2Test extends PartitionAutoLoadSqlTestBase {

    public PartitionTablePoc2Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTablePoc2Test.class, 0, false);
    }
}
