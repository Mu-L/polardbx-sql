package com.alibaba.polardbx.qatest.ddl.ttl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

public class TtlPartitionTable7Test extends PartitionAutoLoadSqlTestBase {
    public TtlPartitionTable7Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
        parameter.setIgnoreAutoIncrement(true);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TtlPartitionTable7Test.class, 0, false);
    }
}
