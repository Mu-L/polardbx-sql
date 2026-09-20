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

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;

/**
 * 测试列存节点存在性验证逻辑
 * 针对 ColumnarNodeStatusUtils.validateColumnarNodeExists 方法的集成测试
 */
public class ValidateColumnarNodeExistsTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME = "validate_columnar_node_test_table";
    private static final String INDEX_NAME = "validate_columnar_node_test_cci";

    private static final String CREATE_TABLE_TMPL = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`)"
        + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

    private static final String CREATE_CCI_TMPL =
        "ALTER TABLE %s ADD CLUSTERED columnar index %s(`buyer_id`) PARTITION BY KEY(`ID`)";

    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME);
        createTestTable();
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME);
    }

    private void createTestTable() {
        try {
            final String sqlCreateTable = String.format(CREATE_TABLE_TMPL, PRIMARY_TABLE_NAME);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    protected void createCciSuccess(String sql) {
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } catch (Exception e) {
            throw new RuntimeException("CREATE CCI statement execution failed!", e);
        }
    }

    private void createCciFailed(String sql, String expectedError) {
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, expectedError);
        } catch (Exception e) {
            throw new RuntimeException("CREATE CCI failed test execution failed!", e);
        }
    }

    /**
     * 测试当启用 ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE 参数时，
     * 即使没有列存节点也应该允许创建CCI
     */
    @Test
    public void testCreateCciWithoutColumnarNodeEnabled() {
        String hint = buildCmdExtra("ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=true",
            "SKIP_DDL_TASKS=WaitColumnarTableCreationTask");
        final String sqlCreateCci = String.format(CREATE_CCI_TMPL, PRIMARY_TABLE_NAME, INDEX_NAME);

        try {
            // 当启用参数时，应该能够成功创建CCI，即使没有列存节点
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sqlCreateCci);

            // 验证CCI是否创建成功
            String showIndexSql = String.format("SHOW COLUMNAR INDEX FROM %s", PRIMARY_TABLE_NAME);
            boolean cciExists = false;
            ResultSet rs = JdbcUtil.executeQuery(showIndexSql, tddlConnection);
            try {
                while (rs.next()) {
                    if (rs.getString("INDEX_NAME").contains(INDEX_NAME)) {
                        cciExists = true;
                        break;
                    }
                }
            } finally {
                rs.close();
            }

            if (!cciExists) {
                throw new RuntimeException(
                    "CCI should be created successfully when ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=true");
            }

        } catch (Exception e) {
            throw new RuntimeException("Test failed: " + e.getMessage(), e);
        } finally {
            // 清理创建的CCI
            try {
                String dropCciSql = String.format("ALTER TABLE %s DROP INDEX %s", PRIMARY_TABLE_NAME, INDEX_NAME);
                JdbcUtil.executeUpdateSuccess(tddlConnection, dropCciSql);
            } catch (Exception ignored) {
                // 忽略清理失败
            }
        }
    }

    /**
     * 测试当禁用 ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE 参数且没有列存节点时，
     * 应该禁止创建CCI
     */
    @Test
    public void testCreateCciWithoutColumnarNodeDisabled() {
        String hint = buildCmdExtra("ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=false");
        final String sqlCreateCci = String.format(CREATE_CCI_TMPL, PRIMARY_TABLE_NAME, INDEX_NAME);

        try {
            // 当禁用参数且没有列存节点时，应该创建CCI失败
            createCciFailed(hint + sqlCreateCci,
                "Cannot create columnar index because no columnar node is available");
        } catch (Exception e) {
            throw new RuntimeException("Test failed: " + e.getMessage(), e);
        }
    }

    /**
     * 测试跳过DDL任务的情况
     * 当跳过 WaitColumnarTableCreationTask 时，应该允许创建CCI
     */
    @Test
    public void testCreateCciWithSkipDdlTask() {
        String hint = buildCmdExtra(
            "ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=false",
            "SKIP_DDL_TASKS=WaitColumnarTableCreationTask"
        );
        final String sqlCreateCci = String.format(CREATE_CCI_TMPL, PRIMARY_TABLE_NAME, INDEX_NAME);

        try {
            // 当跳过DDL任务时，应该能够成功创建CCI
            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sqlCreateCci);

            // 验证CCI是否创建成功
            String showIndexSql = String.format("SHOW COLUMNAR INDEX FROM %s", PRIMARY_TABLE_NAME);
            boolean cciExists = false;
            ResultSet rs = JdbcUtil.executeQuery(showIndexSql, tddlConnection);
            try {
                while (rs.next()) {
                    if (rs.getString("INDEX_NAME").contains(INDEX_NAME)) {
                        cciExists = true;
                        break;
                    }
                }
            } finally {
                rs.close();
            }

            if (!cciExists) {
                throw new RuntimeException("CCI should be created successfully when skipping DDL tasks");
            }

        } catch (Exception e) {
            throw new RuntimeException("Test failed: " + e.getMessage(), e);
        } finally {
            // 清理创建的CCI
            try {
                String dropCciSql = String.format("ALTER TABLE %s DROP INDEX %s", PRIMARY_TABLE_NAME, INDEX_NAME);
                JdbcUtil.executeUpdateSuccess(tddlConnection, dropCciSql);
            } catch (Exception ignored) {
                // 忽略清理失败
            }
        }
    }

    /**
     * 测试创建表时同时创建CCI的情况
     */
    @Test
    public void testCreateTableWithCciDirectly() {
        String testTableName = PRIMARY_TABLE_NAME + "_direct";
        String testIndexName = INDEX_NAME + "_direct";

        try {
            dropTableIfExists(testTableName);

            // 测试启用参数时创建表同时创建CCI
            String hint = buildCmdExtra("ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=true",
                "SKIP_DDL_TASKS=WaitColumnarTableCreationTask");
            String sqlCreateTableWithCci = String.format(
                "CREATE TABLE `%s` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`ID`),\n"
                    + "    PRIMARY KEY (`id`)"
                    + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n",
                testTableName, testIndexName);

            JdbcUtil.executeUpdateSuccess(tddlConnection, hint + sqlCreateTableWithCci);

            // 验证表和CCI都创建成功
            String showTablesSql = String.format("SHOW TABLES LIKE '%s'", testTableName);
            ResultSet rs = JdbcUtil.executeQuery(showTablesSql, tddlConnection);
            boolean tableExists = rs.next();
            rs.close();

            if (!tableExists) {
                throw new RuntimeException("Table should be created successfully");
            }

            String showIndexSql = String.format("SHOW COLUMNAR INDEX FROM %s", testTableName);
            boolean cciExists = false;
            rs = JdbcUtil.executeQuery(showIndexSql, tddlConnection);
            try {
                while (rs.next()) {
                    if (rs.getString("INDEX_NAME").contains(INDEX_NAME)) {
                        cciExists = true;
                        break;
                    }
                }
            } finally {
                rs.close();
            }

            if (!cciExists) {
                throw new RuntimeException("CCI should be created successfully with table");
            }

        } catch (Exception e) {
            throw new RuntimeException("Test create table with CCI failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(testTableName);
        }
    }

    /**
     * 测试禁用参数时创建表同时创建CCI应该失败
     */
    @Test
    public void testCreateTableWithCciDirectlyFailed() {
        String testTableName = PRIMARY_TABLE_NAME + "_direct_fail";
        String testIndexName = INDEX_NAME + "_direct_fail";

        try {
            dropTableIfExists(testTableName);

            // 测试禁用参数时创建表同时创建CCI应该失败
            String hint = buildCmdExtra("ENABLE_CREATE_CCI_WITHOUT_COLUMNAR_NODE=false");
            String sqlCreateTableWithCci = String.format(
                "CREATE TABLE `%s` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`ID`),\n"
                    + "    PRIMARY KEY (`id`)"
                    + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n",
                testTableName, testIndexName);

            createCciFailed(hint + sqlCreateTableWithCci,
                "Cannot create columnar index because no columnar node is available");

        } catch (Exception e) {
            throw new RuntimeException("Test create table with CCI failed test failed: " + e.getMessage(), e);
        } finally {
            dropTableIfExists(testTableName);
        }
    }
}