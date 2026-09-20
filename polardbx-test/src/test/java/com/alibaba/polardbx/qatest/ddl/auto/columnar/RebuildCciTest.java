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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableIdVersionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableIdVersionRecord;
import com.alibaba.polardbx.qatest.ColumnarRelatedMetaTest;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@ReplicaIgnore(ignoreReason = "暂不支持列存相关DDL的同步")
@ColumnarRelatedMetaTest
public class RebuildCciTest extends ColumnarDdlCompareBase {
    final static Log log = LogFactory.getLog(RebuildCciTest.class);

    protected static final String PRIMARY_TABLE_PREFIX = "rebuild_cci_prim";
    protected static final String INDEX_PREFIX = "rebuild_cci_cci";
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
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`order_id`)\n"
        + ") ENGINE = InnoDB PARTITION BY KEY(`id`);\n";

    private static final String creatTableTmpl1 = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`order_id`)\n"
        + ") ENGINE = InnoDB PARTITION BY KEY(`seller_id`);\n";

    private static final String creatTableSnapshotTmpl = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`order_id`) columnar_options='{\"type\":\"snapshot\"}'\n"
        + ") ENGINE = InnoDB PARTITION BY KEY(`id`);\n";

    private static final String creatTableArchiveTmpl = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `seller_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`), \n"
        + "    CLUSTERED COLUMNAR INDEX `%s`(`buyer_id`) PARTITION BY KEY(`order_id`) columnar_options='{\"type\":\"archive\"}'\n"
        + ") ENGINE = InnoDB PARTITION BY KEY(`id`);\n";

    private static final String rebuildCciSortKey = "alter table %s rebuild \n"
        + "CLUSTERED COLUMNAR INDEX `%s`(`id`) PARTITION BY KEY(`order_id`)";

    private static final String rebuildCciPartKey = "alter table %s rebuild \n"
        + "CLUSTERED COLUMNAR INDEX `%s`(`id`) PARTITION BY KEY(`id`)";

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
    public void testAlterTableRebuildCciFailed() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        /**
         * 自动选择选择重建策略
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");

        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s drop column buyer_id";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName),
            "Do not support drop sort key of clustered columnar index");

        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s drop column order_id";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName),
            "Do not support drop partition key of clustered columnar index");

        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s drop column id";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Do not support drop");
    }

    @Test
    public void testAlterTableRebuildArchiveCciFailed() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;
        final String sqlCreateTable1 = String.format(
            creatTableArchiveTmpl,
            tableName,
            indexName1);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        /**
         * 自动选择选择重建策略
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");

        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id int";

        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName),
            "Do not support modify column rebuild archive columnar index");
    }

    @Test
    public void testModifyColumnRebuildCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        /**
         * 强制不重建cci
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 1");

        // 对于只改了 comment 这种场景
        long tableIdBefore = getTableId(schemaName, tableName, indexName1);
        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id varchar(20) comment 'test'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        long tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertEquals(tableIdBefore, tableIdAfter);
        checkAll(schemaName, tableName, indexName1);

        /**
         * 强制重建 cci
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 2");

        // 修改排序键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        checkAll(schemaName, tableName, indexName1);

        // 修改主键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        checkAll(schemaName, tableName, indexName1);

        /**
         * 自动选择选择重建策略
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // 对于只改了 comment 这种场景
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id varchar(20) comment 'test'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertEquals(tableIdBefore, tableIdAfter);
        checkAll(schemaName, tableName, indexName1);

        // 修改排序键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        checkAll(schemaName, tableName, indexName1);

        // 修改分区键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column order_id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        checkAll(schemaName, tableName, indexName1);

        // 修改主键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName1));

        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
    }

    @Test
    public void testChangeColumnRebuildCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl1,
            tableName,
            indexName1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        /**
         * 自动选择选择重建策略
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // 修改排序键，被禁止，需要用rebuild
        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s change column buyer_id buyer_id_new int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName),
            "Do not support change sort key of clustered columnar index");

        // 修改分区键，被禁止，需要用rebuild
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s change column order_id order_id_new int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName),
            "Do not support change partition key of clustered columnar index");

        // 修改主表主键，被禁止，需要用rebuild
        long tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s change column id id_new int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName),
            "Do not support change primary key of clustered columnar index");
    }

    @Test
    public void testAlterColumnRebuildSnapshotCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableSnapshotTmpl,
            tableName,
            indexName1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        /**
         * 自动选择选择重建策略
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // 对于只改了 comment 这种场景
        long tableIdBefore = getTableId(schemaName, tableName, indexName1);
        String sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id varchar(20) comment 'test'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName1));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName1));
        compareIndexRecords(schemaName, tableName, Collections.singletonList(indexName1));
        comparePartitionRecords(schemaName, tableName, indexName1);
        checkCciMeta(indexName1);
        long tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertEquals(tableIdBefore, tableIdAfter);

        // 修改排序键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column buyer_id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        String oldCciName1 = getCciName(tableIdBefore);
        Assert.assertTrue(isValidUUID(oldCciName1));
        Assert.assertTrue(checkCciIgnore(tableIdBefore));
        checkAll(schemaName, tableName, indexName1);
        checkAll(schemaName, tableName, oldCciName1);

        // 修改分区键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column order_id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        String oldCciName2 = getCciName(tableIdBefore);
        Assert.assertTrue(isValidUUID(oldCciName2));
        Assert.assertTrue(checkCciIgnore(tableIdBefore));
        checkAll(schemaName, tableName, indexName1);
        checkAll(schemaName, tableName, oldCciName1);
        checkAll(schemaName, tableName, oldCciName2);

        // 修改主键
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        sql = SKIP_WAIT_CCI_CREATION_HINT + "alter table %s modify column id int";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);
        String oldCciName3 = getCciName(tableIdBefore);
        Assert.assertTrue(isValidUUID(oldCciName3));
        Assert.assertTrue(checkCciIgnore(tableIdBefore));
        checkAll(schemaName, tableName, indexName1);
        checkAll(schemaName, tableName, oldCciName1);
        checkAll(schemaName, tableName, oldCciName2);
        checkAll(schemaName, tableName, oldCciName3);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show full create table " + tableName);
        Assert.assertTrue(rs.next());
        String fullCreateTable = rs.getString("Create Table");
        Assert.assertTrue(
            fullCreateTable.contains(oldCciName1) &&
                fullCreateTable.contains(oldCciName2) &&
                fullCreateTable.contains(oldCciName3) &&
                fullCreateTable.contains("\"COLUMNAR_IGNORE\":\"TRUE\""));
    }

    @Test
    public void testRebuildCci() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");

        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName = PRIMARY_TABLE_NAME1;
        String indexName1 = INDEX_NAME1;

        final String sqlCreateTable1 = String.format(
            creatTableTmpl,
            tableName,
            indexName1);

        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        // 重建排序键
        final String rebuildCci1 = String.format(rebuildCciSortKey, tableName, indexName1);
        long tableIdBefore = getTableId(schemaName, tableName, indexName1);
        createCciSuccess(rebuildCci1);
        long tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);

        String createTableString = showCreateTable(tddlConnection, tableName);
        assertEquals("CREATE TABLE `rebuild_cci_prim_1` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tCLUSTERED COLUMNAR INDEX `rebuild_cci_cci_1` (`id`) \n"
            + "\t\tPARTITION BY KEY(`order_id`)\n"
            + "\t\tPARTITIONS 16\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`id`)\n"
            + "PARTITIONS 3", createTableString);

        // 重建分区健
        final String rebuildCci2 = String.format(rebuildCciPartKey, tableName, indexName1);
        tableIdBefore = getTableId(schemaName, tableName, indexName1);
        createCciSuccess(rebuildCci2);
        tableIdAfter = getTableId(schemaName, tableName, indexName1);
        Assert.assertNotEquals(tableIdBefore, tableIdAfter);
        compareMultiTableId(tableIdAfter, tableIdBefore);

        createTableString = showCreateTable(tddlConnection, tableName);
        assertEquals("CREATE TABLE `rebuild_cci_prim_1` (\n"
            + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "\t`order_id` varchar(20) DEFAULT NULL,\n"
            + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
            + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
            + "\t`order_snapshot` longtext,\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tCLUSTERED COLUMNAR INDEX `rebuild_cci_cci_1` (`id`) \n"
            + "\t\tPARTITION BY KEY(`id`)\n"
            + "\t\tPARTITIONS 16\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`id`)\n"
            + "PARTITIONS 3", createTableString);
    }

    @Test
    public void testRebuildCciFailed() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET FORBID_DDL_WITH_CCI = false");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET MAX_CCI_COUNT = 10");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_MODIFY_CCI_CRITICAL_COLUMN = true");
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName1 = PRIMARY_TABLE_NAME1;
        String tableName2 = PRIMARY_TABLE_NAME2;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        final String sqlCreateTable1 = String.format(
            creatTableSnapshotTmpl,
            tableName1,
            indexName1);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        final String sqlCreateTable2 = String.format(
            creatTableArchiveTmpl,
            tableName2,
            indexName2);
        // Create table with cci
        createCciSuccess(sqlCreateTable2);

        /**
         * 自动选择选择重建策略
         */
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET REBUILD_CCI_STRATEGY = 0");

        final String rebuildCci1 = String.format(rebuildCciSortKey, tableName1, indexName1);
        final String rebuildCci2 = String.format(rebuildCciSortKey, tableName2, indexName2);

        createCciFailed(rebuildCci1,
            "Do not support rebuild snapshot columnar index", "The DDL job has been rollback");
        createCciFailed(rebuildCci2,
            "Do not support rebuild archive columnar index", "The DDL job has been rollback");
    }

    @Test
    public void testIgnoreCci() throws SQLException {
        String schemaName = TStringUtil.isBlank(tddlDatabase2) ? tddlDatabase1 : tddlDatabase2;
        String tableName1 = PRIMARY_TABLE_NAME1;
        String tableName2 = PRIMARY_TABLE_NAME2;
        String indexName1 = INDEX_NAME1;
        String indexName2 = INDEX_NAME2;
        final String sqlCreateTable1 = String.format(
            creatTableSnapshotTmpl,
            tableName1,
            indexName1);
        // Create table with cci
        createCciSuccess(sqlCreateTable1);

        final String sqlCreateTable2 = String.format(
            creatTableArchiveTmpl,
            tableName2,
            indexName2);
        // Create table with cci
        createCciSuccess(sqlCreateTable2);

        long tableId1 = getTableId(schemaName, tableName1, indexName1);
        long tableId2 = getTableId(schemaName, tableName2, indexName2);

        ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, String.format("call polardbx.columnar_ignore(%s)", tableId1));
        Assert.assertTrue(rs.next());
        String status = rs.getString("status");
        Assert.assertEquals("FAIL: NOT ALLOW TO IGNORE CCI", status);

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_COLUMNAR_IGNORE = true");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("call polardbx.columnar_ignore(%s)", tableId1));
        Assert.assertTrue(rs.next());
        status = rs.getString("status");
        Assert.assertEquals("OK", status);
        Assert.assertTrue(checkCciIgnore(tableId1));

        // un-ignore
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("call polardbx.columnar_unignore(%s)", tableId1));
        Assert.assertTrue(rs.next());
        status = rs.getString("status");
        Assert.assertEquals("OK", status);
        Assert.assertFalse(checkCciIgnore(tableId1));

        // multi table id
        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("call polardbx.columnar_ignore(%s, %s)", tableId1, tableId2));
        Assert.assertTrue(rs.next());
        status = rs.getString("status");
        Assert.assertEquals("OK", status);
        Assert.assertTrue(checkCciIgnore(tableId1));
        Assert.assertTrue(checkCciIgnore(tableId2));

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET GLOBAL ENABLE_COLUMNAR_IGNORE = false");
    }

    protected void compareMultiTableId(long newTableId, long oldTableId) throws SQLException {
        ColumnarTableIdVersionRecord record = fetchFromTableIdVersionTable(newTableId);
        assert record.newTableId == newTableId && record.oldTableId == oldTableId;
    }

    protected ColumnarTableIdVersionRecord fetchFromTableIdVersionTable(long newTableId)
        throws SQLException {
        List<ColumnarTableIdVersionRecord> records;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableIdVersionAccessor accessor = new ColumnarTableIdVersionAccessor();
            accessor.setConnection(metaDbConn);
            records = accessor.queryByNewTableId(newTableId);
            assert records.size() == 1;
        }
        return records.get(0);
    }

    protected String getCciName(long tableId) throws SQLException {
        String sql = "select index_name from columnar_table_mapping where table_id=%d";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, tableId))) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return "";
    }

    public static boolean isValidUUID(String str) {
        if (str == null) {
            return false;
        }
        try {
            UUID uuid = UUID.fromString(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    protected boolean checkCciIgnore(long tableId) throws SQLException {
        String sql =
            "select config_value from columnar_config where table_id = %d and config_key like 'COLUMNAR_IGNORE'";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, tableId))) {
            if (rs.next()) {
                return rs.getBoolean(1);
            }
        }
        return false;
    }

    /**
     * Check all meta data of cci
     * 1. column position
     * 2. column record
     * 3. index record
     * 4. partition record
     * 5. cci meta
     */
    public void checkAll(String schemaName, String tableName, String indexName) throws SQLException {
        compareColumnPositions(schemaName, tableName, Collections.singletonList(indexName));
        compareColumnRecords(schemaName, tableName, Collections.singletonList(indexName));
        compareIndexRecords(schemaName, tableName, Collections.singletonList(indexName));
        comparePartitionRecords(schemaName, tableName, indexName);
//        checkCciMeta(indexName);
    }
}
