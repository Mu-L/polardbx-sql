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

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CheckGsiBackfillTest extends DDLBaseNewDBTestCase {
    private final String TABLE_NAME = "test_gsi_check_backfill";
    private final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
        + " id int primary key auto_increment, "
        + " name varchar(255) "
        + ") partition by key (id)";
    private final String HINT = "/*+TDDL:cmd_extra(FB_CHECK_IN_BACK_FILL=true)*/ ";
    private final String ADD_GSI =
        "ALTER TABLE " + TABLE_NAME + " ADD GLOBAL INDEX `gsi_check_test` (`id`) partition by key(`id`)";

    @Test
    public void testFailure() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GSI_BACKFILL_USE_FASTCHECKER = FALSE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_SQL);
        JdbcUtil.executeUpdateFailed(tddlConnection, HINT + ADD_GSI, "FB_CHECK_IN_BACK_FILL");
    }

    @Test
    public void testCheckGsiBackfillWithDirtyData() throws SQLException, InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GSI_BACKFILL_USE_FASTCHECKER = FALSE");
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE_SQL);

        // 插入数据
        String insertSql = "INSERT INTO " + TABLE_NAME + " (name) VALUES (?)";

        try (PreparedStatement ps = tddlConnection.prepareStatement(insertSql)) {
            for (int i = 0; i < 2000; i++) {
                ps.setString(1, String.valueOf(i));
                ps.addBatch();

                if (i > 0 && i % 1000 == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }

            ps.executeBatch();
        }

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(GSI_BACKFILL_BATCH_SIZE=1, ddl_job_id=%s)*/", jobId);
        String sql = myHint + ADD_GSI;

        // 预期因为脏数据backfill check失败
        Thread ddlThread = new Thread(() -> {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "GSI checker found error when creating GSI");
        });

        // 启动 DDL 线程
        ddlThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Thread dirtyDataThread = new Thread(() -> {
            try (Connection newConn = getNewTddlConnection1()) {
                // 导入脏数据
                JdbcUtil.executeUpdateSuccess(newConn, "SET PUSHDOWN_HINT_ON_GSI = TRUE");

                int loopCount = 0;

                String gsiTableName = "";
                while (gsiTableName.isEmpty()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    loopCount++;
                    if (loopCount > 100) {
                        System.out.println("gsi table not found");
                        return;
                    }
                    ResultSet rs = JdbcUtil.executeQuery("/*+TDDL:node(0)*/ show tables", newConn);
                    while (rs.next()) {
                        String tableName = rs.getString(1);
                        if (tableName.startsWith("gsi_check_test")) {
                            gsiTableName = tableName;
                            rs.close();
                            break;
                        }
                    }
                }

                JdbcUtil.executeUpdateSuccess(newConn,
                    "/*+TDDL:node(0)*/ insert into " + gsiTableName + " (id) values (5001)");
            } catch (SQLException e) {
                throw new RuntimeException("Error inserting dirty data: " + e.getMessage(), e);
            }
        });

        // 启动插入脏数据线程
        dirtyDataThread.start();

        ddlThread.join();
        dirtyDataThread.join();

        // DDL执行失败
        Assert.assertTrue(checkDDLError(tddlConnection, jobId));
        // 无GSI
        ResultSet rs = JdbcUtil.executeQuery("show global index from " + TABLE_NAME, tddlConnection);
        Assert.assertFalse(rs.next());
        rs.close();

        System.out.println("ddl finished");
    }

    private static boolean checkDDLError(Connection connection, Long jobId) throws SQLException {
        String sql = "show ddl result " + jobId;
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            String ddlState = rs.getString("RESULT_TYPE");
            rs.close();
            return ddlState.equalsIgnoreCase("ERROR");
        }
        return false;
    }

    public boolean usingNewPartDb() {
        return true;
    }
}
