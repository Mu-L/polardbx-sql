package com.alibaba.polardbx.qatest.ddl.auto.partition;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * for show statement with unpredictable result like 'show variables'
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
public class ShowDalVariableTest extends PartitionAutoLoadSqlTestBase {

    public ShowDalVariableTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(ShowDalVariableTest.class, 0, false);
    }

}
