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
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for EXPLAIN ADVISOR on CREATE COLUMNAR INDEX FOR TABLES IN DATABASE operations
 */
public class ExplainAdvisorCreateCciInDbTest extends ExplainOnlineDDLBaseTest {

    String tableName1 = "explain_advisor_cci_db_test1";
    String tableName2 = "explain_advisor_cci_db_test2";
    String tableName3 = "explain_advisor_cci_db_test3";
    String cciName = "cci_db_test_index";
    String hint = "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/";

    @Before
    public void prepare() {
        // Set up test environment
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");

        // Create multiple test tables without CCI
        String createTableSql1 = String.format(
            "CREATE TABLE `%s` (\n" +
                "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "    `order_id` varchar(20) DEFAULT NULL,\n" +
                "    `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "    `seller_id` varchar(20) DEFAULT NULL,\n" +
                "    `order_snapshot` longtext,\n" +
                "    PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB PARTITION BY KEY(`id`)",
            tableName1);

        String createTableSql2 = String.format(
            "CREATE TABLE `%s` (\n" +
                "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "    `order_id` varchar(20) DEFAULT NULL,\n" +
                "    `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "    `seller_id` varchar(20) DEFAULT NULL,\n" +
                "    `order_snapshot` longtext,\n" +
                "    PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB PARTITION BY KEY(`id`)",
            tableName2);

        String createTableSql3 = String.format(
            "CREATE TABLE `%s` (\n" +
                "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n" +
                "    `order_id` varchar(20) DEFAULT NULL,\n" +
                "    `buyer_id` varchar(20) DEFAULT NULL,\n" +
                "    `seller_id` varchar(20) DEFAULT NULL,\n" +
                "    `order_snapshot` longtext,\n" +
                "    PRIMARY KEY (`id`)\n" +
                ") ENGINE = InnoDB PARTITION BY KEY(`id`)",
            tableName3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql3);
    }

    @After
    public void clean() {
        String dropTableSql1 = String.format("DROP TABLE IF EXISTS %s", tableName1);
        String dropTableSql2 = String.format("DROP TABLE IF EXISTS %s", tableName2);
        String dropTableSql3 = String.format("DROP TABLE IF EXISTS %s", tableName3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTableSql1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTableSql2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropTableSql3);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * Test EXPLAIN ADVISOR for CREATE COLUMNAR INDEX FOR TABLES IN DATABASE
     */
    @Test
    public void testCreateColumnarIndexForTablesInDatabase() {
        String sql = String.format(
            "EXPLAIN ADVISOR CREATE COLUMNAR INDEX FOR TABLES IN %s", tddlDatabase1);

        String expectedSql = String.format(
            "CREATE COLUMNAR INDEX FOR TABLES IN %s", tddlDatabase1);

        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC, hint);
    }

    /**
     * Test EXPLAIN ADVISOR for DROP COLUMNAR INDEX FOR TABLES IN DATABASE
     */
    @Test
    public void testDropColumnarIndexForTablesInDatabase() {
        createCciSuccess(String.format(
            "CREATE COLUMNAR INDEX FOR TABLES IN %s", tddlDatabase1));

        String sql = String.format(
            "EXPLAIN ADVISOR DROP COLUMNAR INDEX FOR TABLES IN %s", tddlDatabase1);

        String expectedSql = String.format(
            "DROP COLUMNAR INDEX FOR TABLES IN %s", tddlDatabase1);

        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OSC, hint);
    }
}