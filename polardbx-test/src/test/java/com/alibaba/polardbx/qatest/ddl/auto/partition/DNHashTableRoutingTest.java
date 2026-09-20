package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Test case for verifying consistency between CN part_hash function and DN polardbx_hasher function.
 * This test ensures that the same input values produce identical hash results on both CN and DN sides,
 * which is critical for correct data routing in PolarDB-X distributed database.
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
public class DNHashTableRoutingTest extends PartitionAutoLoadSqlTestBase {

    public DNHashTableRoutingTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        List<AutoLoadSqlTestCaseParams> paramsList = getParameters(DNHashTableRoutingTest.class, 0, false);
        for (AutoLoadSqlTestCaseParams params : paramsList) {
            params.dropDbAfterCheck = false;
        }
        return paramsList;
    }

    @Test
    @Override
    public void runTest() throws Exception {
        if (StringUtil.isEmpty(this.params.tcName)) {
            return;
        }
        if (!isMySQL80()) {
            return;
        }
        runOneTestCaseInner(this.params);
        String partHashSql =
            "SELECT id, age, CONVERT(TRIM('(' FROM TRIM(')' FROM part_hash('', '%s', id))), signed) AS hash_value FROM %s order by age,hash_value";
        String polardbxHasherSql =
            "/*+TDDL:scan()*/select id, age, CONVERT(polardbx_hasher(id),signed) AS hash_value from %s order by age,hash_value";
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, params.testDbName);
            List<String> tableNames = showTables(connection);
            for (String tableName : tableNames) {
                ResultSet rs = JdbcUtil.executeQuery(String.format(partHashSql, tableName, tableName), connection);
                List<Long> cnHashValues = new ArrayList<>();
                List<Long> dnHashValues = new ArrayList<>();
                while (rs.next()) {
                    cnHashValues.add(rs.getLong(3));
                }
                ResultSet rs1 = JdbcUtil.executeQuery(String.format(polardbxHasherSql, tableName), connection);
                while (rs1.next()) {
                    dnHashValues.add(rs1.getLong(3));
                }
                if (!Objects.equals(cnHashValues, dnHashValues)) {
                    System.out.println(
                        String.format("Table %s has inconsistent hash values between CN and DN", tableName));
                }
                Assert.assertEquals(String.format(polardbxHasherSql, tableName), cnHashValues, dnHashValues);
            }
        }
    }
}