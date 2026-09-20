package com.alibaba.polardbx.qatest.NotThreadSafe.ttl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

public class TtlTableWarningScannerTest extends PartitionAutoLoadSqlTestBase {
    public TtlTableWarningScannerTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TtlTableWarningScannerTest.class, 0, false);
    }
}
