package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.util.List;

public class AlterTableChangeMetaTest extends PartitionAutoLoadSqlTestBase {

    @Before
    public void before() {
        columnNameUpperCase = true;
    }

    public AlterTableChangeMetaTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(AlterTableChangeMetaTest.class, 3, true);
    }
}
