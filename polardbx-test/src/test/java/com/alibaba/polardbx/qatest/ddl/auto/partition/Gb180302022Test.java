package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@CdcIgnore(ignoreReason = "开源 MySQL 不支持的字符集")
public class Gb180302022Test extends PartitionAutoLoadSqlTestBase {
    public Gb180302022Test(AutoLoadSqlTestCaseParams params) {
        super(params);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        if (!isMySQL80()) {
            // only test for 8.0 DN
            return new ArrayList<>();
        }
        return getParameters(Gb180302022Test.class, 0, false);
    }
}
