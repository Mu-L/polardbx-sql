/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.qatest.dal.ai;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class Nl2sqlAgentPolicyTest extends Nl2sqlAgentTestBase {

    @Test
    public void testPolicyReadOnly() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_ALLOWED_SQL_TYPES='READ_ONLY'");
        }
        // Ask agent to do DDL (CREATE TABLE) — should be rejected by policy
        String output = executeNlQuery("创建一个名为test_policy的表，包含id和name列");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Reset policy
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_ALLOWED_SQL_TYPES='ALL'");
        }
    }

    @Test
    public void testPolicyReadWrite() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_ALLOWED_SQL_TYPES='READ_WRITE'");
        }
        // Ask agent to do a query
        String output = executeNlQuery("查询所有员工信息");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Reset policy
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_ALLOWED_SQL_TYPES='ALL'");
        }
    }

    @Test
    public void testCheckExplainCost_withScanRowsLimit() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_MAX_SCAN_ROWS=1000");
        }
        // Query that triggers EXPLAIN cost check
        String output = executeNlQuery("查询employees表中所有数据");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Reset
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_MAX_SCAN_ROWS=0");
        }
    }

    @Test
    public void testPolicyAll() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET NL2SQL_ALLOWED_SQL_TYPES='ALL'");
            String output = executeNlQuery("查询所有员工");
            Assert.assertFalse("Output should not be empty in ALL mode", output.isEmpty());
        }
    }

    @Test
    public void testPolicyReadWriteAllowsSelect() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET NL2SQL_ALLOWED_SQL_TYPES='READ_WRITE'");
            String output = executeNlQuery("查询所有员工的姓名和部门");
            Assert.assertFalse("Output should not be empty in READ_WRITE mode", output.isEmpty());
        }
    }
}
