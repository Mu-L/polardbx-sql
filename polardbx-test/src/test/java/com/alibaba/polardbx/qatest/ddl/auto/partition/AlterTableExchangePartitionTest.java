package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */

public class AlterTableExchangePartitionTest extends PartitionAutoLoadSqlTestBase {

    public AlterTableExchangePartitionTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(AlterTableExchangePartitionTest.class, 3, true);
    }
}
