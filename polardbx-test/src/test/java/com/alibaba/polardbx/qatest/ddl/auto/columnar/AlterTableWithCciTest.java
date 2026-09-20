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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.ddl.auto.ddl.AlterTableTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class AlterTableWithCciTest extends ColumnarDdlCompareBase {
    final static Log log = LogFactory.getLog(AlterTableTest.class);

    protected static final String PRIMARY_TABLE_PREFIX = "alter_table_with_cci_prim";
    protected static final String INDEX_PREFIX = "alter_table_with_cci_cci";
    protected static final String PRIMARY_TABLE_NAME1 = PRIMARY_TABLE_PREFIX + "_1";
    protected static final String INDEX_NAME1 = INDEX_PREFIX + "_1";
    protected static final String PRIMARY_TABLE_NAME2 = PRIMARY_TABLE_PREFIX + "_2";
    protected static final String INDEX_NAME2 = INDEX_PREFIX + "_2";
    protected static final String PRIMARY_TABLE_NAME3 = PRIMARY_TABLE_PREFIX + "_3";
    protected static final String INDEX_NAME3 = INDEX_PREFIX + "_2";

    private static final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
        + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

    private static final String creatTableTmpl2 = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext,\n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`)\n"
        + ") PARTITION BY RANGE(id) \n"
        + "(partition p0 values less than (1000)\n"
        + ",partition p1 values less than (2000)\n"
        + ");\n";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @After
    public void after() {
//        dropTableIfExists(PRIMARY_TABLE_NAME1);
//        dropTableIfExists(PRIMARY_TABLE_NAME2);
//        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testAddColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2"));
        checkCciMeta(indexName);

        sql = "alter table %s add c3 int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c3"));
        checkCciMeta(indexName);

        sql = "alter table %s add c4 int after c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c4"));
        checkCciMeta(indexName);
    }

    @Test
    public void testDropColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s drop seller_id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("seller_id"));
        checkCciMeta(indexName);

        // drop last column
        sql = "alter table %s drop order_snapshot";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("order_snapshot"));
        checkCciMeta(indexName);
    }

    @Test
    public void testModifyColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s modify column seller_id longtext";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id varchar(64) after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id varchar(64) first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);
    }

    @Test
    public void testChangeColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s change column seller_id seller_id_1 varchar(64)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id_1", "seller_id")));
        checkCciMeta(indexName);
    }

    @Test
    public void testMultiAlters() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s add column c2 int, add column c3 int first, add column c4 int after c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2", "c3", "c4"));

//        sql = "alter table %s add c2 int";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
//        sql = "alter table %s add c3 int first";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
//        sql = "alter table %s add c4 int after c3";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c2 bigint, modify column c3 int first, change column c4 c40 int after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("c40", "c4")));
        checkCciMeta(indexName);

        sql = "alter table %s change column c40 c4 int, drop column c2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop column c2, drop column c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("c2, c3"));
        checkCciMeta(indexName);

        checkColumnarColumnEvolutionConsistency(schemaName, tableName);
    }

    @Test
    public void testRepartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);
        String originCciName = fetchCciSysTable(schemaName, tableName, indexName);

        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        String afterCciName = fetchCciSysTable(schemaName, tableName, indexName);

        Assert.assertTrue(originCciName.equals(afterCciName));

        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        afterCciName = fetchCciSysTable(schemaName, tableName, indexName);
        Assert.assertTrue(originCciName.equals(afterCciName));
        checkCciMeta(indexName);
    }

    @Test
    public void testOMC() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // SINGLE STATEMENT
        String sql = "alter table %s modify column seller_id longtext, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id varchar(64) after id, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql = "alter table %s modify column seller_id longtext first, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql = "alter table %s change column seller_id seller_id_1 varchar(64), algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id_1", "seller_id")));
        checkCciMeta(indexName);

        // MULTI STATEMENTS
        sql =
            "alter table %s modify column seller_id_1 longtext, add column c2 int, add column c3 int first, add column c4 int, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        sql =
            "alter table %s modify column c2 bigint, modify column c3 int first, change column c4 c40 int after id, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("c40", "c4")));
        checkCciMeta(indexName);

        sql = "alter table %s change column c40 c4 int, drop column c2, add column tmp int algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);
    }

    @Test
    public void testRenameTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String newTableName = PRIMARY_TABLE_NAME2;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s rename to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, newTableName));

        // show primary table
        String showCreateTableSql = String.format("show create table %s", newTableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        String createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(indexName));

        compareIndexRecords(schemaName, newTableName, Collections.singletonList(indexName));

        sql = "rename table %s to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, newTableName, tableName));

        showCreateTableSql = String.format("show create table %s", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(indexName));

        compareIndexRecords(schemaName, tableName, Collections.singletonList(indexName));
    }

    @Test
    public void testRenameCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;
        String newIndexName = INDEX_NAME2;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s rename index %s to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, indexName, newIndexName));

        // show primary table
        String showCreateTableSql = String.format("show create table %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        String createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName));

        // show cci table
        showCreateTableSql = String.format("show create table %s", newIndexName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName));

        compareIndexRecords(schemaName, tableName, Collections.singletonList(newIndexName));
    }

    @Test
    public void testRebuildTable() throws SQLException {
        // 特殊的情况，修改分区键（即使不更改分区键的长度）会重建主表，而不是走alter table的路径
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s modify column order_id varchar(20) comment 'test'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);
    }

    @Test
    public final void testAddColumnAfterRepartition() throws SQLException {
        /**
         * 测试在经过repartition(move partition, split partition, etc.)后indexes系统表的version列变更
         * 在老代码中，再创建新的列，其version列值为0，导致排序的时候被列为第一列，失去了CCI的标识和CLUSTER属性
         * 新代码会将新列的version列值设置为和原表一致，避免这个问题。
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl2,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // check last version & seq
        Pair<Long, Long> versionAndSeqBefore = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqBefore);

        // split partition
        String sql =
            "ALTER TABLE %s SPLIT PARTITION p1 INTO  (PARTITION p10 VALUES LESS THAN (1500), PARTITION p11 VALUES LESS THAN(2000))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        Pair<Long, Long> versionAndSeqAfter = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqAfter);
        assertEquals(versionAndSeqBefore.getKey(), versionAndSeqAfter.getKey());

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2"));
        checkCciMeta(indexName);

        sql = "alter table %s add c3 int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c3"));
        checkCciMeta(indexName);

        // test omc
        versionAndSeqBefore = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqBefore);

        // split partition
        sql =
            "ALTER TABLE %s SPLIT PARTITION p11 INTO  (PARTITION p110 VALUES LESS THAN (1700), PARTITION p111 VALUES LESS THAN(2000))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c3 bigint, add c4 int after c3, algorithm = omc";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        versionAndSeqAfter = checkVersionAndSeq(schemaName, tableName);
        Assert.assertNotNull(versionAndSeqAfter);
        assertEquals(versionAndSeqBefore.getKey(), versionAndSeqAfter.getKey());

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c4"));
        checkCciMeta(indexName);
    }

    @Test
    public void testCaseInsensitiveColumnEvolution() throws SQLException {
        /**
         * 测试大小写不敏感的列名变更情况下，columnar_column_evolution表中的元数据一致性
         * 确保同一个列在不同大小写变更后，不会在columnar_column_evolution表中产生额外的field_id
         * 验证SQL: SELECT table_id,column_name,COUNT(DISTINCT field_id) AS unique_field_count
         *          FROM columnar_column_evolution GROUP BY table_id, column_name HAVING unique_field_count >= 2;
         * 应该返回空结果
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // Test case 1: Change column name with different case
        String sql = "alter table %s change column seller_id SELLER_ID varchar(20)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("SELLER_ID", "seller_id")));
        checkCciMeta(indexName);

        // Verify no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 2: Change back to lowercase
        sql = "alter table %s change column SEllER_ID seller_id varchar(20)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id", "SEllER_ID")));
        checkCciMeta(indexName);

        // Verify no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 3: Add column with mixed case, then change case
        sql = "alter table %s add column Test_Column int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("Test_Column"));
        checkCciMeta(indexName);

        // Verify no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 4: Change the added column to different case
        sql = "alter table %s change column Test_Column test_column int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("test_column", "Test_Column")));
        checkCciMeta(indexName);

        // Final verification: no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 5: Multiple case changes in one statement
        sql =
            "alter table %s change column order_snapshot ORDER_SNAPSHOT varchar(20), change column order_id ORDER_ID varchar(20)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(
            new Pair<>("ORDER_SNAPSHOT", "order_snapshot"),
            new Pair<>("ORDER_ID", "order_id")
        ));
        checkCciMeta(indexName);
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 6: Test modify column with case changes (position changes)
        sql = "alter table %s modify column test_column int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        // Verify no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 7: Test modify column with case changes (data type changes)
        sql = "alter table %s modify column test_column bigint";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        // Verify no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 8: Test modify column with mixed case column name and position change
        sql = "alter table %s modify column TEST_column varchar(50) after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        // Verify no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);

        // Test case 9: Test modify column with different case variations
        sql = "alter table %s modify column TeSt_CoLuMn int after buyer_id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        checkCciMeta(indexName);

        // Final verification: no duplicate field_id for same column name (case insensitive)
        checkColumnarColumnEvolutionConsistency(schemaName, tableName);
    }

    /**
     * 检查columnar_column_evolution表中是否存在同一列名（大小写不敏感）对应多个不同field_id的情况
     * 如果存在，说明大小写不敏感处理有问题
     */
    private void checkColumnarColumnEvolutionConsistency(String schemaName, String tableName) throws SQLException {
        String sql =
            "SELECT \n"
                + "    table_id,\n"
                + "    LOWER(column_name) AS column_name,\n"
                + "    GROUP_CONCAT(DISTINCT column_name ORDER BY column_name) AS case_variants,\n"
                + "    COUNT(DISTINCT field_id) AS unique_field_count,\n"
                + "    GROUP_CONCAT(DISTINCT field_id ORDER BY field_id) AS field_ids\n"
                + "FROM \n"
                + "    columnar_column_evolution\n"
                + "GROUP BY \n"
                + "    table_id, \n"
                + "    LOWER(column_name)\n"
                + "HAVING \n"
                + "    COUNT(DISTINCT BINARY column_name) >= 2\n"
                + "   AND COUNT(DISTINCT field_id) >= 2\n"
                + "ORDER BY \n"
                + "    table_id, column_name;\n";

        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                long tableId = rs.getLong("table_id");
                String columnName = rs.getString("column_name");
                int uniqueFieldCount = rs.getInt("unique_field_count");

                Assert.fail(String.format(
                    "Found inconsistent field_id for column '%s' in table_id %d. " +
                        "Column has %d distinct field_id values, but should have only 1. " +
                        "This indicates case-insensitive handling is not working properly.",
                    columnName, tableId, uniqueFieldCount));
            }

            // If we reach here, the query returned no results, which is expected
            log.info(
                "columnar_column_evolution consistency check passed - no duplicate field_id found for same column names");
        }
    }
}