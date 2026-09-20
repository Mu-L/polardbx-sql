/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl.ExplainAdvisorCciDdl;

import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;

/**
 * Test class for EXPLAIN ADVISOR on rebuild CCI operations
 */
public class ExplainAdvisorRebuildCciTest extends ExplainOnlineDDLBaseTest {

    String tableName = "explain_advisor_cci_test";
    String cciName = "cci_test_index";
    String hint = "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/";

    @Before
    public void prepare() {
        // Set up test environment
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");

        // Create table with CCI
        String createTableSql = String.format(
            "CREATE TABLE `%s` (\n" +
                "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "    `order_id` varchar(20) DEFAULT NULL,\n" +
                "    `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "    `seller_id` varchar(20) DEFAULT NULL,\n" +
                "    `order_snapshot` longtext,\n" +
                "    PRIMARY KEY (`id`),\n" +
                "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`order_id`)\n" +
                ") ENGINE = InnoDB PARTITION BY KEY(`id`)",
            tableName, cciName);

        createCciSuccess(createTableSql);
    }

    @After
    public void clean() {
        String dropTableSql = String.format("DROP TABLE IF EXISTS %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * Test EXPLAIN ADVISOR for direct rebuild CCI with different sort key
     */
    @Test
    public void testRebuildCciWithDifferentSortKey() {
        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s REBUILD CLUSTERED COLUMNAR INDEX `%s`(`id`) PARTITION BY KEY(`order_id`)",
            tableName, cciName);

        String expectedSql = String.format(
            "ALTER TABLE %s REBUILD  CLUSTERED COLUMNAR INDEX `%s` (`id`) PARTITION BY KEY (`order_id`)",
            tableName, cciName);

        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC, hint);
    }

    /**
     * Test EXPLAIN ADVISOR for direct rebuild CCI with different partition key
     */
    @Test
    public void testRebuildCciWithDifferentPartitionKey() {
        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s REBUILD CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)",
            tableName, cciName);

        String expectedSql = String.format(
            "ALTER TABLE %s REBUILD  CLUSTERED COLUMNAR INDEX `%s` (`buyer_id`) PARTITION BY KEY (`id`)",
            tableName, cciName);

        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC, hint);
    }

    /**
     * Test EXPLAIN ADVISOR for modify column that triggers CCI rebuild
     */
    @Test
    public void testModifyColumnTriggersCciRebuild() {
        // Set rebuild strategy to auto
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s MODIFY COLUMN buyer_id int",
            tableName);

        String expectedSql = String.format(
            "ALTER TABLE %s MODIFY COLUMN buyer_id int, algorithm = omc",
            tableName);

        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.OMC30 : DdlAlgorithm.OMC20;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm, hint);
    }

    /**
     * Test EXPLAIN ADVISOR for modify partition key column that triggers CCI rebuild
     */
    @Test
    public void testModifyPartitionKeyTriggersCciRebuild() {
        // Set rebuild strategy to auto
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s MODIFY COLUMN order_id int",
            tableName);

        String expectedSql = String.format(
            "ALTER TABLE %s MODIFY COLUMN order_id int, algorithm = omc",
            tableName);

        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.OMC30 : DdlAlgorithm.OMC20;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm, hint);
    }

    /**
     * Test EXPLAIN ADVISOR for modify primary key column that triggers CCI rebuild
     */
    @Test
    public void testModifyPrimaryKeyTriggersCciRebuild() {
        // Set rebuild strategy to auto
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s MODIFY COLUMN id int",
            tableName);

        String expectedSql = String.format(
            "ALTER TABLE %s MODIFY COLUMN id int",
            tableName);

        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC20, hint);
    }

    /**
     * Test EXPLAIN ADVISOR for modify non-critical column (should not trigger rebuild)
     */
    @Test
    public void testModifyNonCriticalColumn() {
        // Set rebuild strategy to auto
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s MODIFY COLUMN seller_id varchar(30)",
            tableName);

        String expectedSql = String.format(
            "ALTER TABLE %s MODIFY COLUMN seller_id varchar(30)",
            tableName);

        // Non-critical column changes do not trigger CCI rebuild.
        // The DDL algorithm depends on the actual DN capability:
        // - MySQL 8.0 DN supports INSTANT for metadata-only changes (e.g., widening varchar)
        // - MySQL 5.7 DN falls back to INPLACE
        // isMySQL80() from qatest.properties may not match the actual DN version,
        // so we accept both INSTANT and INPLACE here.
        DdlType ddlType = null;
        DdlAlgorithm algorithm = null;
        String adviseSql = null;
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            if (rs.next()) {
                ddlType = DdlType.valueOf(rs.getString(1));
                adviseSql = rs.getString(2);
                algorithm = DdlAlgorithm.valueOf(rs.getString(3));
            }
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertSame(DdlType.ONLINE_DDL, ddlType);
        Assert.assertNotNull(adviseSql);
        Assert.assertEquals(expectedSql.toLowerCase(), adviseSql.toLowerCase());
        Assert.assertTrue("Expected INSTANT or INPLACE but got " + algorithm,
            algorithm == DdlAlgorithm.INSTANT || algorithm == DdlAlgorithm.INPLACE);
        // Execute the advised DDL
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + expectedSql);
    }

    /**
     * Test EXPLAIN ADVISOR with forced rebuild strategy
     */
    @Test
    public void testForcedRebuildStrategy() {
        // Set rebuild strategy to force rebuild
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 2");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String sql = String.format(
            "EXPLAIN ADVISOR ALTER TABLE %s MODIFY COLUMN buyer_id varchar(20) COMMENT 'test'",
            tableName);

        String expectedSql = String.format(
            "ALTER TABLE %s MODIFY COLUMN buyer_id varchar(20) COMMENT 'test'",
            tableName);

        // Even minor changes should trigger rebuild with forced strategy.
        // The DDL algorithm depends on the actual DN capability:
        // - MySQL 8.0 DN supports INSTANT for metadata-only changes
        // - MySQL 5.7 DN falls back to INPLACE
        // We cannot rely on isMySQL80() from qatest.properties because it may not
        // match the actual DN version (e.g., XDB57 lab has CN configured as 8.0
        // but DN is MySQL 5.7). The EXPLAIN ADVISOR correctly probes the DN to
        // determine the algorithm, so we accept both INSTANT and INPLACE here.
        DdlType ddlType = null;
        DdlAlgorithm algorithm = null;
        String adviseSql = null;
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            if (rs.next()) {
                ddlType = DdlType.valueOf(rs.getString(1));
                adviseSql = rs.getString(2);
                algorithm = DdlAlgorithm.valueOf(rs.getString(3));
            }
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertSame(DdlType.ONLINE_DDL, ddlType);
        Assert.assertNotNull(adviseSql);
        Assert.assertEquals(expectedSql.toLowerCase(), adviseSql.toLowerCase());
        Assert.assertTrue("Expected INSTANT or INPLACE but got " + algorithm,
            algorithm == DdlAlgorithm.INSTANT || algorithm == DdlAlgorithm.INPLACE);
        // Execute the advised DDL
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + expectedSql);
    }
}