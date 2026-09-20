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

public class LocalUkDupCheckPruningTest extends DDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "local_uk_dup_check_pruning";
    private static final String GSI_NAME = "g_job_id";
    private static final String GSI_LOCAL_UK_NAME = "uk_run_id_gsi";
    private static final String HINT =
        "/*+TDDL:CMD_EXTRA(PLAN_CACHE=FALSE,DML_EXECUTION_STRATEGY=LOGICAL,DML_USE_RETURNING=FALSE,"
            + "OPTIMIZE_REPLACE_BY_RETURNING=FALSE,DML_GET_DUP_USING_GSI=TRUE,"
            + "DML_SKIP_DUPLICATE_CHECK_FOR_PK=TRUE,DML_PARTITION_LOCAL_UK_DUP_CHECK=TRUE,"
            + "DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=FALSE)*/";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void createTable() {
        dropTableIfExists(TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table " + TABLE_NAME + " ("
                + "id bigint unsigned not null auto_increment,"
                + "job_id int not null,"
                + "run_id varchar(64) not null,"
                + "status int not null,"
                + "primary key(id),"
                + "unique local key uk_run_id(run_id),"
                + "global index " + GSI_NAME + "(job_id) covering(run_id,status) "
                + "partition by range(job_id) ("
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
    }

    @Test
    @CdcIgnore(ignoreReason = "不同分区存在重复 LOCAL UK，CDC 下游无法保持局部唯一语义")
    public void testSingleRowComplexDmlUsesPartitionLocalSemantics() throws SQLException {
        assertCrossPartitionDuplicateAllowed(
            "insert ignore into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            TABLE_NAME);
        assertCrossPartitionDuplicateAllowed(
            "replace into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            TABLE_NAME);
        assertCrossPartitionDuplicateAllowed(
            "insert into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2) "
                + "on duplicate key update status=values(status)",
            TABLE_NAME);
    }

    @Test
    @CdcIgnore(ignoreReason = "不同分区存在重复 LOCAL UK，CDC 下游无法保持局部唯一语义")
    public void testSingleRowComplexDmlChecksMatchingLocalUkOnGsi() throws SQLException {
        final String realGsiName = getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create local unique index " + GSI_LOCAL_UK_NAME + " on " + realGsiName + "(run_id)");

        assertCrossPartitionDuplicateAllowed(
            "insert ignore into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            TABLE_NAME, realGsiName);
        assertCrossPartitionDuplicateAllowed(
            "replace into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            TABLE_NAME, realGsiName);
        assertCrossPartitionDuplicateAllowed(
            "insert into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2) "
                + "on duplicate key update status=values(status)",
            TABLE_NAME, realGsiName);
    }

    @Test
    public void testSingleRowComplexDmlHandlesSamePartitionConflict() throws SQLException {
        assertSamePartitionDuplicateHandled(
            "insert ignore into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            1, true);
        assertSamePartitionDuplicateHandled(
            "replace into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            2, false);
        assertSamePartitionDuplicateHandled(
            "insert into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2) "
                + "on duplicate key update status=values(status)",
            2, true);
    }

    @Test
    public void testSingleRowComplexDmlHandlesSamePartitionConflictOnGsi() throws SQLException {
        final String realGsiName = getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create local unique index " + GSI_LOCAL_UK_NAME + " on " + realGsiName + "(run_id)");

        assertSamePartitionDuplicateHandled(
            "insert ignore into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            1, true);
        assertSamePartitionDuplicateHandled(
            "replace into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2)",
            2, false);
        assertSamePartitionDuplicateHandled(
            "insert into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',2) "
                + "on duplicate key update status=values(status)",
            2, true);
    }

    @Test
    public void testMultiRowValuesKeepsLegacyScan() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "truncate table " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "trace " + HINT + " insert ignore into " + TABLE_NAME + "(job_id,run_id,status) values"
                + "(110,'run-id-1',1),(210,'run-id-2',2)");
        Assert.assertTrue("multi-row VALUES should keep the legacy multi-partition duplicate check",
            getPhysicalSelects().size() > 1);
    }

    private void assertCrossPartitionDuplicateAllowed(String dml, String... expectedSelectTables) throws SQLException {
        // TRUNCATE rebuilds the GSI and drops local indexes created directly on its internal table.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + TABLE_NAME + "(job_id,run_id,status) values(10,'same-run-id',1)");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + HINT + " " + dml);
        final List<String> physicalSelects = getPhysicalSelects();
        Assert.assertEquals("Each LOCAL UK scope should only access one target partition",
            expectedSelectTables.length, physicalSelects.size());
        for (String expectedSelectTable : expectedSelectTables) {
            Assert.assertEquals("LOCAL UK duplicate check should access one target partition of "
                    + expectedSelectTable,
                1, physicalSelects.stream()
                    .filter(trace -> trace.contains(expectedSelectTable.toUpperCase(Locale.ROOT)))
                    .count());
        }

        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select count(*) from " + TABLE_NAME + " where run_id='same-run-id'")) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(2, resultSet.getLong(1));
        }
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME));
    }

    private void assertSamePartitionDuplicateHandled(String dml, int expectedStatus, boolean expectSameId)
        throws SQLException {
        // TRUNCATE rebuilds the GSI and drops local indexes created directly on its internal table.
        JdbcUtil.executeUpdateSuccess(tddlConnection, "delete from " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + TABLE_NAME + "(job_id,run_id,status) values(110,'same-run-id',1)");
        final long originalId = getSingleRunIdRow()[0];

        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + HINT + " " + dml);

        final long[] actualRow = getSingleRunIdRow();
        Assert.assertEquals(expectedStatus, actualRow[1]);
        if (expectSameId) {
            Assert.assertEquals("INSERT IGNORE and UPSERT should keep the original row id",
                originalId, actualRow[0]);
        } else {
            Assert.assertNotEquals("REPLACE should delete the conflicting row and insert a new row",
                originalId, actualRow[0]);
        }
        checkGsi(tddlConnection, getRealGsiName(tddlConnection, TABLE_NAME, GSI_NAME));
    }

    private long[] getSingleRunIdRow() throws SQLException {
        try (ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "select id,status from " + TABLE_NAME + " where run_id='same-run-id'")) {
            Assert.assertTrue("Expected one row for the conflicting LOCAL UK value", resultSet.next());
            final long[] row = {resultSet.getLong(1), resultSet.getLong(2)};
            Assert.assertFalse("Expected exactly one row in the target partition", resultSet.next());
            return row;
        }
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
                final String traceContent = statement + " " + (params == null ? "" : params);
                physicalSelects.add(traceContent.toUpperCase(Locale.ROOT));
            }
        }
        return physicalSelects;
    }
}
