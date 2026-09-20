/*
 * Copyright [1999-2024] Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class LocalPkDupCheckPruningTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "local_pk_dup_check_pruning";
    private static final String GSI_NAME = "g_gsi_key";
    private static final String PLAN_CACHE_TABLE_NAME = "local_pk_plan_cache_pruning";
    private static final String PLAN_CACHE_EXISTING_GSI_NAME = "g_plan_cache_existing";
    private static final String PLAN_CACHE_ADDED_GSI_NAME = "g_plan_cache_added";
    private static final String HINT = buildHint(true);
    private static final String DISABLED_HINT = buildHint(false);

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void createTable() {
        dropTableIfExists(TABLE_NAME);
        dropTableIfExists(PLAN_CACHE_TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table " + TABLE_NAME + " ("
                + "id bigint not null,"
                + "job_id int not null,"
                + "gsi_key int not null,"
                + "status int not null,"
                + "primary key(id),"
                + "global index " + GSI_NAME + "(gsi_key) covering(job_id,status) "
                + "partition by range(gsi_key) ("
                + "partition gp0 values less than (100),"
                + "partition gp1 values less than (200),"
                + "partition gp2 values less than (300),"
                + "partition gp3 values less than maxvalue)"
                + ") partition by range(job_id) ("
                + "partition p0 values less than (100),"
                + "partition p1 values less than (200),"
                + "partition p2 values less than (300),"
                + "partition p3 values less than maxvalue)");
    }

    @After
    public void dropTable() {
        dropTableIfExists(TABLE_NAME);
        dropTableIfExists(PLAN_CACHE_TABLE_NAME);
    }

    @Test
    @CdcIgnore(ignoreReason = "不同分区存在重复主键")
    public void testCrossPartitionLocalPkAllowedForComplexDml() throws SQLException {
        assertCrossPartitionDuplicateAllowed(
            "insert ignore into " + TABLE_NAME + " values(1,110,110,2)");
        assertCrossPartitionDuplicateAllowed(
            "replace into " + TABLE_NAME + " values(1,110,110,2)");
        assertCrossPartitionDuplicateAllowed(
            "insert into " + TABLE_NAME + " values(1,110,110,2) "
                + "on duplicate key update status=values(status)");
    }

    @Test
    public void testSamePrimaryPartitionConflictHandled() throws SQLException {
        assertSamePrimaryPartitionConflict(
            "insert ignore into " + TABLE_NAME + " values(1,110,110,2)", 1);
        assertSamePrimaryPartitionConflict(
            "replace into " + TABLE_NAME + " values(1,110,110,2)", 2);
        assertSamePrimaryPartitionConflict(
            "insert into " + TABLE_NAME + " values(1,110,110,2) "
                + "on duplicate key update status=values(status)", 2);
    }

    @Test
    public void testSameGsiPartitionPhysicalPkConflictHandled() throws SQLException {
        assertGsiPhysicalPkConflict(
            "insert ignore into " + TABLE_NAME + " values(1,110,50,2)", 10, 1);
        assertGsiPhysicalPkConflict(
            "replace into " + TABLE_NAME + " values(1,110,50,2)", 110, 2);
        assertGsiPhysicalPkConflict(
            "insert into " + TABLE_NAME + " values(1,110,50,2) "
                + "on duplicate key update status=values(status)", 10, 2);
    }

    @Test
    public void testLocalPkSwitchOffKeepsLegacyScan() throws SQLException {
        resetAndInsert(1, 10, 10, 1);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace " + DISABLED_HINT + " insert ignore into " + TABLE_NAME + " values(1,110,110,2)");

        Assert.assertTrue("Disabled LOCAL PK pruning should retain the legacy multi-partition check",
            getPhysicalSelects().size() > 1);
        assertRows(1, new long[][] {{1, 10, 10, 1}});
    }

    @Test
    public void testAddingGsiInvalidatesCachedLocalPkCandidate() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set DML_PARTITION_LOCAL_PK_DUP_CHECK=true");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table " + PLAN_CACHE_TABLE_NAME + " ("
                + "id bigint not null,"
                + "job_id int not null,"
                + "gsi_key int not null,"
                + "new_gsi_key int not null,"
                + "status int not null,"
                + "primary key(id),"
                + "global index " + PLAN_CACHE_EXISTING_GSI_NAME
                + "(gsi_key) covering(job_id,new_gsi_key,status) partition by range(gsi_key) ("
                + "partition egp0 values less than (100),"
                + "partition egp1 values less than (200),"
                + "partition egp2 values less than (300),"
                + "partition egp3 values less than maxvalue)"
                + ") partition by range(job_id) ("
                + "partition p0 values less than (100),"
                + "partition p1 values less than (200),"
                + "partition p2 values less than (300),"
                + "partition p3 values less than maxvalue)");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + PLAN_CACHE_TABLE_NAME + " values(1,10,10,50,1)");
        final String cacheDml =
            "insert into " + PLAN_CACHE_TABLE_NAME + " values(2,110,110,50,2) "
                + "on duplicate key update status=values(status)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, cacheDml);
        JdbcUtil.executeUpdateSuccess(tddlConnection, cacheDml);
        final long hitCount = getPlanCacheHitCount(PLAN_CACHE_TABLE_NAME);
        Assert.assertTrue("The second execution should hit the cached LOCAL PK plan, hitCount=" + hitCount,
            hitCount > 0);

        final String nonConflictTraceDml =
            "insert into " + PLAN_CACHE_TABLE_NAME + " values(3,210,210,60,3) "
                + "on duplicate key update status=values(status)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + nonConflictTraceDml);
        List<String> physicalSelects = getPhysicalSelects();
        final String realExistingGsiName =
            getRealGsiName(tddlConnection, PLAN_CACHE_TABLE_NAME, PLAN_CACHE_EXISTING_GSI_NAME);
        Assert.assertEquals("The cached plan should contain the primary and existing GSI LOCAL PK scopes",
            2, physicalSelects.size());
        assertOneSelectOnTable(physicalSelects, PLAN_CACHE_TABLE_NAME);
        assertOneSelectOnTable(physicalSelects, realExistingGsiName);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create global index " + PLAN_CACHE_ADDED_GSI_NAME + " on " + PLAN_CACHE_TABLE_NAME
                + "(new_gsi_key) covering(job_id,gsi_key,status) partition by range(new_gsi_key) ("
                + "partition gp0 values less than (100),"
                + "partition gp1 values less than (200),"
                + "partition gp2 values less than (300),"
                + "partition gp3 values less than maxvalue)");

        final String gsiConflictDml =
            "insert into " + PLAN_CACHE_TABLE_NAME + " values(1,110,110,50,2) "
                + "on duplicate key update status=values(status)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, gsiConflictDml);
        assertPlanCacheGsiPkConflictHandled();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + gsiConflictDml);
        physicalSelects = getPhysicalSelects();
        final String realAddedGsiName =
            getRealGsiName(tddlConnection, PLAN_CACHE_TABLE_NAME, PLAN_CACHE_ADDED_GSI_NAME);
        Assert.assertEquals("Adding GSI should rebuild three LOCAL PK scopes and look up the primary row",
            4, physicalSelects.size());
        assertSelectCountOnTable(physicalSelects, PLAN_CACHE_TABLE_NAME, 2);
        assertOneSelectOnTable(physicalSelects, realExistingGsiName);
        assertOneSelectOnTable(physicalSelects, realAddedGsiName);
        checkGsi(tddlConnection, realExistingGsiName);
        checkGsi(tddlConnection, realAddedGsiName);
    }

    private void assertPlanCacheGsiPkConflictHandled() throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select job_id,gsi_key,status from " + PLAN_CACHE_TABLE_NAME + " where id=1")) {
            Assert.assertTrue("Expected the original row with the conflicting physical GSI PK", resultSet.next());
            Assert.assertEquals(10, resultSet.getInt(1));
            Assert.assertEquals(10, resultSet.getInt(2));
            Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertFalse("The stale primary-only plan must not insert another row with id=1", resultSet.next());
        }
    }

    private long getPlanCacheHitCount(String tableName) throws SQLException {
        final String sql = "select coalesce(max(hit_count),-1) from information_schema.plan_cache "
            + "where schema_name=database() and upper(table_names) like '%"
            + tableName.toUpperCase(Locale.ROOT) + "%'";
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            Assert.assertTrue("Expected a plan cache row for " + tableName, resultSet.next());
            return resultSet.getLong(1);
        }
    }

    private void assertCrossPartitionDuplicateAllowed(String dml) throws SQLException {
        resetAndInsert(1, 10, 10, 1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + HINT + " " + dml);

        final String realGsiName = getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME);
        final List<String> physicalSelects = getPhysicalSelects();
        Assert.assertEquals("Each LOCAL PK physical scope should access one target partition",
            2, physicalSelects.size());
        assertOneSelectOnTable(physicalSelects, TABLE_NAME);
        assertOneSelectOnTable(physicalSelects, realGsiName);
        assertRows(1, new long[][] {{1, 10, 10, 1}, {1, 110, 110, 2}});
        checkGsi(tddlConnection, realGsiName);
    }

    private void assertSamePrimaryPartitionConflict(String dml, int expectedStatus) throws SQLException {
        resetAndInsert(1, 110, 110, 1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + " " + dml);

        assertRows(1, new long[][] {{1, 110, 110, expectedStatus}});
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME));
    }

    private void assertGsiPhysicalPkConflict(String dml, int expectedJobId, int expectedStatus) throws SQLException {
        resetAndInsert(1, 10, 50, 1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + " " + dml);

        assertRows(1, new long[][] {{1, expectedJobId, 50, expectedStatus}});
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME));
    }

    private void resetAndInsert(long id, int jobId, int gsiKey, int status) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + TABLE_NAME + " values(" + id + "," + jobId + "," + gsiKey + "," + status + ")");
    }

    private void assertRows(long id, long[][] expectedRows) throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select id,job_id,gsi_key,status from " + TABLE_NAME + " where id=" + id + " order by job_id")) {
            for (long[] expectedRow : expectedRows) {
                Assert.assertTrue("Missing expected LOCAL PK row", resultSet.next());
                Assert.assertArrayEquals(expectedRow, new long[] {
                    resultSet.getLong(1), resultSet.getLong(2), resultSet.getLong(3), resultSet.getLong(4)});
            }
            Assert.assertFalse("Found unexpected LOCAL PK row", resultSet.next());
        }
    }

    private static void assertOneSelectOnTable(List<String> physicalSelects, String tableName) {
        assertSelectCountOnTable(physicalSelects, tableName, 1);
    }

    private static void assertSelectCountOnTable(List<String> physicalSelects, String tableName,
                                                 long expectedCount) {
        Assert.assertEquals("Unexpected LOCAL PK SELECT count on " + tableName,
            expectedCount, physicalSelects.stream()
                .filter(trace -> trace.contains(tableName.toUpperCase(Locale.ROOT)))
                .count());
    }

    private List<String> getPhysicalSelects() throws SQLException {
        final List<String> physicalSelects = new ArrayList<>();
        try (ResultSet trace = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace")) {
            while (trace.next()) {
                final String statement = trace.getString("STATEMENT");
                if (statement == null || !statement.toUpperCase(Locale.ROOT).contains("SELECT")) {
                    continue;
                }
                final String params = trace.getString("PARAMS");
                physicalSelects.add((statement + " " + (params == null ? "" : params))
                    .toUpperCase(Locale.ROOT));
            }
        }
        return physicalSelects;
    }

    private static String buildHint(boolean enableLocalPk) {
        return "/*+TDDL:CMD_EXTRA(PLAN_CACHE=FALSE,DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE,"
            + "OPTIMIZE_REPLACE_BY_RETURNING=FALSE,DML_GET_DUP_USING_GSI=TRUE,"
            + "DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY=FALSE,"
            + "DML_PARTITION_LOCAL_UK_DUP_CHECK=FALSE,DML_PARTITION_LOCAL_PK_DUP_CHECK="
            + Boolean.toString(enableLocalPk).toUpperCase(Locale.ROOT) + ")*/";
    }
}
