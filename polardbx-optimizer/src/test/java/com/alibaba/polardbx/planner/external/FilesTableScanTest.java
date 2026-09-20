package com.alibaba.polardbx.planner.external;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Plan-level unit tests for FILES() function.
 * Uses mock connector to verify plan shape without external dependencies.
 */
public class FilesTableScanTest extends ParameterizedTestCommon {

    public FilesTableScanTest(String caseName, int sqlIndex, String sql,
                              String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(FilesTableScanTest.class);
    }
}
