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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class AlterTableWithMultiCciTest extends AlterTableWithCciTest {
    // 表上有两个 CCI
    private static final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`id`),\n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`id`) PARTITION BY KEY(`order_id`)\n"
        + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";

    @Override
    @Test
    public void testAddColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2"));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s add c3 int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c3"));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s add c4 int after c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c4"));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Override
    @Test
    public void testDropColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s drop seller_id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("seller_id"));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        // drop last column
        sql = "alter table %s drop order_snapshot";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("order_snapshot"));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Override
    @Test
    public void testModifyColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s modify column seller_id longtext";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s modify column seller_id varchar(64) after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s modify column seller_id varchar(64) first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Override
    @Test
    public void testChangeColumn() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s change column seller_id seller_id_1 varchar(64)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id_1", "seller_id")));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Override
    @Test
    public void testMultiAlters() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // DO NOT SUPPORT MIXED ALTERS ON CLUSTERED INDEX
//        String sql = "alter table %s add column c2 int, add column c3 int first, add column c4 int after c3";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
//        compareColumnPositions(schemaName, tableName, indexNames);
//        compareColumnRecords(schemaName, tableName);
//        checkAddIndexesRecords(schemaName, tableName, ImmutableList.of("c2, c3, c4"));

        String sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        sql = "alter table %s add c3 int first";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        sql = "alter table %s add c4 int after c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column c2 bigint, modify column c3 int first, change column c4 c40 int after id";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("c40", "c4")));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s change column c40 c4 int, drop column c2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s add c2 int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s drop column c2, drop column c3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkDropIndexesRecords(schemaName, tableName, ImmutableList.of("c2, c3"));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Test
    public void testRepartition() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);
        String originCciName1 = fetchCciSysTable(schemaName, tableName, indexName1);
        String originCciName2 = fetchCciSysTable(schemaName, tableName, indexName2);

        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        String afterCciName1 = fetchCciSysTable(schemaName, tableName, indexName1);
        String afterCciName2 = fetchCciSysTable(schemaName, tableName, indexName2);
        Assert.assertTrue(originCciName1.equals(afterCciName1));
        Assert.assertTrue(originCciName2.equals(afterCciName2));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        afterCciName1 = fetchCciSysTable(schemaName, tableName, indexName1);
        afterCciName2 = fetchCciSysTable(schemaName, tableName, indexName2);
        Assert.assertTrue(originCciName1.equals(afterCciName1));
        Assert.assertTrue(originCciName2.equals(afterCciName2));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Test
    public void testOMC() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // SINGLE STATEMENT
        String sql = "alter table %s modify column seller_id longtext, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s modify column seller_id varchar(64) after id, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s modify column seller_id longtext first, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s change column seller_id seller_id_1 varchar(64), algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("seller_id_1", "seller_id")));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        // MULTI STATEMENTS
        sql =
            "alter table %s modify column seller_id_1 longtext, add column c2 int, add column c3 int first, add column c4 int, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql =
            "alter table %s modify column c2 bigint, modify column c3 int first, change column c4 c40 int after id, algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkChangeIndexesRecords(schemaName, tableName, ImmutableList.of(new Pair<>("c40", "c4")));
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);

        sql = "alter table %s change column c40 c4 int, drop column c2, add column tmp int algorithm = 'omc'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, indexNames);
        compareColumnRecords(schemaName, tableName, indexNames);
        checkCciMeta(indexName1);
        checkCciMeta(indexName2);
    }

    @Test
    public void testRenameTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String newTableName = PRIMARY_TABLE_NAME2;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        List<String> indexNames = ImmutableList.of(indexName1, indexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s rename to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, newTableName));

        // show primary table
        String showCreateTableSql = String.format("show create table %s", newTableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        String createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(indexName1));
        Assert.assertTrue(createTableString.contains(indexName2));

        compareIndexRecords(schemaName, newTableName, indexNames);

        sql = "rename table %s to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, newTableName, tableName));

        showCreateTableSql = String.format("show create table %s", tableName);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(indexName1));
        Assert.assertTrue(createTableString.contains(indexName2));

        compareIndexRecords(schemaName, tableName, indexNames);
    }

    @Test
    public void testRenameCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 2");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        String newIndexName1 = indexName1 + "_new";
        String newIndexName2 = indexName2 + "_new";
        List<String> newIndexNames = ImmutableList.of(newIndexName1, newIndexName2);

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1,
            indexName2);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        String sql = "alter table %s rename index %s to %s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, indexName1, newIndexName1));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName, indexName2, newIndexName2));

        // show primary table
        String showCreateTableSql = String.format("show create table %s", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        String createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName1));
        Assert.assertTrue(createTableString.contains(newIndexName2));

        // show cci table
        showCreateTableSql = String.format("show create table %s", newIndexName1);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName1));

        showCreateTableSql = String.format("show create table %s", newIndexName2);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        Assert.assertTrue(rs.next());
        createTableString = rs.getString(2);
        Assert.assertTrue(createTableString.contains(newIndexName2));

        compareIndexRecords(schemaName, tableName, newIndexNames);
    }

}
