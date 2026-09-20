package com.alibaba.polardbx.qatest.ddl.ttl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import com.google.common.collect.ImmutableSet;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Set;

public class TtlPartitionTable6Test extends PartitionAutoLoadSqlTestBase {

    /**
     * Cases that fail on MySQL 5.7 DN due to date-dependent TTL assertions
     * tied to integer column encodings (require MySQL 8.0 DN behavior).
     */
    private static final Set<String> CASES_REQUIRE_MYSQL80 = ImmutableSet.of(
        "test_ttl_iso_dt_int",
        "test_ttl_todays_int",
        "test_ttl_unix_ts_int",
        "test_ttl_unix_ts_ms_int"
    );

    public TtlPartitionTable6Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TtlPartitionTable6Test.class, 0, false);
    }

    @Override
    @Test
    public void runTest() throws Exception {
        if (CASES_REQUIRE_MYSQL80.contains(params.tcName)) {
            Assume.assumeTrue(
                "TTL partition case " + params.tcName + " requires MySQL 8.0 DN",
                isMySQL80());
        }
        super.runTest();
    }
}
