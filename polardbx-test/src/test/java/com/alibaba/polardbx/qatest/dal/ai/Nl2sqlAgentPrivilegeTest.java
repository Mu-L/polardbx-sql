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
public class Nl2sqlAgentPrivilegeTest extends Nl2sqlAgentTestBase {

    @Test
    public void testNonRootUserNl2sql() throws Exception {
        cleanupNormalUsers();
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE USER '" + NL2SQL_USER + "'@'%' IDENTIFIED BY '" + NORMAL_USER_PASSWORD + "'");
            stmt.execute("GRANT SELECT ON " + TEST_DB + ".* TO '" + NL2SQL_USER + "'@'%'");
            stmt.execute("GRANT NL2SQL ON *.* TO '" + NL2SQL_USER + "'@'%'");
        }
        try (Connection userConn = getNormalUserConnection(NL2SQL_USER);
            Statement stmt = userConn.createStatement()) {
            stmt.execute("SET ENABLE_NL2SQL=true");
            boolean hasResult = stmt.execute("查询所有员工");
            StringBuilder sb = new StringBuilder();
            if (hasResult) {
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        sb.append(rs.getString(1));
                    }
                }
            }
            Assert.assertFalse("Non-root user with NL2SQL privilege should get NL2SQL response", sb.length() == 0);
        } finally {
            cleanupNormalUsers();
        }
    }

    @Test
    public void testNonPrivilegedUser() throws Exception {
        cleanupNormalUsers();
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE USER '" + NL2SQL_USER_NO_PRIV + "'@'%' IDENTIFIED BY '" + NORMAL_USER_PASSWORD + "'");
            stmt.execute("GRANT SELECT ON " + TEST_DB + ".* TO '" + NL2SQL_USER_NO_PRIV + "'@'%'");
        }
        try (Connection userConn = getNormalUserConnection(NL2SQL_USER_NO_PRIV);
            Statement stmt = userConn.createStatement()) {
            stmt.execute("SET ENABLE_NL2SQL=true");
            try {
                stmt.execute("查询所有员工");
                Assert.fail("User without NL2SQL privilege should not execute natural-language input");
            } catch (SQLException e) {
                Assert.assertTrue("Expected parser or privilege error: " + e.getMessage(),
                    e.getMessage() != null && !e.getMessage().isEmpty());
            }
        } finally {
            cleanupNormalUsers();
        }
    }
}
