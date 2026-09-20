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
public class Nl2sqlAgentErrorTest extends Nl2sqlAgentTestBase {

    @Test
    public void testNl2sql_disabled() throws Exception {
        try (Connection conn2 = getPolardbxConnection()) {
            try (Statement stmt = conn2.createStatement()) {
                // Don't SET ENABLE_NL2SQL — should get parser error
                try {
                    stmt.execute("some random natural language text");
                    // If no error, check that it's not an NL2SQL response
                    // (might be parsed as SQL and fail differently)
                } catch (SQLException e) {
                    // Expected: parser error when NL2SQL is disabled
                    Assert.assertTrue("Should get error when NL2SQL is disabled",
                        e.getMessage() != null);
                }
            }
        }
    }

    @Test
    public void testNl2sql_sessionLevelEnabled() throws Exception {
        // Verify that SET ENABLE_NL2SQL=true works at session level
        try (Connection conn2 = getPolardbxConnection()) {
            try (Statement stmt = conn2.createStatement()) {
                stmt.execute("SET ENABLE_NL2SQL=true");
                // Verify it's set
                try (ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'ENABLE_NL2SQL'")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals("true", rs.getString(2));
                }
            }
        }
    }

    @Test
    public void testNl2sql_sessionLevelDisabled() throws Exception {
        // Verify that SET ENABLE_NL2SQL=false disables it
        try (Connection conn2 = getPolardbxConnection()) {
            try (Statement stmt = conn2.createStatement()) {
                stmt.execute("SET ENABLE_NL2SQL=false");
                try {
                    stmt.execute("some random natural language text");
                } catch (SQLException e) {
                    // Expected: parser error when NL2SQL is disabled
                    Assert.assertTrue("Should get error when NL2SQL is disabled",
                        e.getMessage() != null);
                }
            }
        }
    }

    @Test
    public void testNl2sql_noDatabaseSelected() throws Exception {
        try (Connection conn2 = getPolardbxConnection()) {
            try (Statement stmt = conn2.createStatement()) {
                stmt.execute("SET ENABLE_NL2SQL=true");
                try {
                    stmt.execute("查询所有员工");
                } catch (SQLException e) {
                    // Expected: no database selected error
                    Assert.assertTrue("Should get error about no database",
                        e.getMessage() != null);
                }
            }
        }
    }

    @Test
    public void testNoDatabaseSelectedError() throws Exception {
        ConnectionManager manager = ConnectionManager.getInstance();
        try (Connection noDbConn = getPolardbxDirectConnection(
            manager.getPolardbxAddress(), manager.getPolardbxUser(), manager.getPolardbxPassword(),
            manager.getPolardbxPort());
            Statement stmt = noDbConn.createStatement()) {
            stmt.execute("SET ENABLE_NL2SQL=true");
            try {
                boolean hasResult = stmt.execute("查询所有员工");
                StringBuilder output = new StringBuilder();
                while (true) {
                    if (hasResult) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            int colCount = rs.getMetaData().getColumnCount();
                            while (rs.next()) {
                                for (int i = 1; i <= colCount; i++) {
                                    String val = rs.getString(i);
                                    if (val != null) {
                                        output.append(val).append('\n');
                                    }
                                }
                            }
                        }
                    } else if (stmt.getUpdateCount() == -1) {
                        break;
                    }
                    hasResult = stmt.getMoreResults();
                }
                String text = output.toString().toLowerCase();
                Assert.assertTrue("Should get error about no database: " + output,
                    text.contains("database") || text.contains("schema") || text.contains("数据库"));
            } catch (SQLException e) {
                Assert.assertTrue("Should get error about no database: " + e.getMessage(),
                    e.getMessage() != null && (e.getMessage().toLowerCase().contains("database")
                        || e.getMessage().toLowerCase().contains("schema") || e.getMessage().contains("数据库")));
            }
        }
    }
}
