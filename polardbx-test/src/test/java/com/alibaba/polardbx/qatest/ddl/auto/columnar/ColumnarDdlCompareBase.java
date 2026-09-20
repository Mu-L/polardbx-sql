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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarIndexEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesAccessor;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.ddl.AlterTableTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ColumnarDdlCompareBase extends DDLBaseNewDBTestCase {
    final static Log log = LogFactory.getLog(AlterTableTest.class);

    public Pair<Long, Long> checkVersionAndSeq(String schemaName, String tableName) throws SQLException {
        String sql = "select version, seq_in_index from indexes "
            + "where table_schema='%s' and table_name='%s' and index_location = 1 order by version,seq_in_index desc limit 1";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            if (rs.next()) {
                return new Pair<>(rs.getLong(1), rs.getLong(2));
            }
        }
        return null;
    }

    protected long getTableId(String schemaName, String tableName, String indexName) throws SQLException {
        String sql = "select table_id from columnar_table_mapping "
            + "where table_schema='%s' and table_name='%s' and index_name like '%s%%' order by latest_version_id desc limit 1";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName, indexName))) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return -1;
    }

    protected void compareColumnPositions(String schemaName, String tableName, List<String> indexNames)
        throws SQLException {
        Map<Integer, String> sysTableColumnPositions = fetchColumnPositionsFromSysTable(schemaName, tableName);
        for (String indexName : indexNames) {
            Map<Integer, String>
                evolutionTableColumnPositions =
                fetchColumnPositionsFromEvolutionTable(schemaName, tableName, indexName);
            compareColumnPositions(sysTableColumnPositions, evolutionTableColumnPositions);
        }
    }

    protected Map<Integer, String> fetchColumnPositionsFromSysTable(String schemaName, String tableName)
        throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        String sql = "select ordinal_position, column_name from columns "
            + "where table_schema='%s' and table_name='%s' order by ordinal_position";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName))) {
            while (rs.next()) {
                columnPositions.put(rs.getInt(1), rs.getString(2));
            }
        }
        return columnPositions;
    }

    protected Map<Integer, String> fetchColumnPositionsFromEvolutionTable(String schemaName, String tableName,
                                                                          String indexName)
        throws SQLException {
        Map<Integer, String> columnPositions = new HashMap<>();
        String sql1 = "select columns from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' and index_name like '%s' order by `version_id` desc limit 1";
        String sql2 = "select column_name from columnar_column_evolution where id=%s";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql1, schemaName, tableName, indexName + '%'))) {
            Assert.assertTrue(rs.next());
            List<Long> columnIds = ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1));
            for (int i = 0; i < columnIds.size(); i++) {
                ResultSet rs1 = stmt.executeQuery(String.format(sql2, columnIds.get(i)));
                Assert.assertTrue(rs1.next());
                columnPositions.put(i + 1, rs1.getString(1));
            }

        }
        return columnPositions;
    }

    protected void compareColumnPositions(Map<Integer, String> sysTableColumnPositions,
                                          Map<Integer, String> evolutionTableColumnPositions) {
        printColumnPositions(sysTableColumnPositions, "Columns System Table");
        printColumnPositions(evolutionTableColumnPositions, "Columnar Table Evolution");

        if (sysTableColumnPositions == null || sysTableColumnPositions.isEmpty() ||
            evolutionTableColumnPositions == null || evolutionTableColumnPositions.isEmpty()) {
            Assert.fail("Invalid column names and positions");
        }

        if (sysTableColumnPositions.size() != evolutionTableColumnPositions.size()) {
            Assert.fail("Different column sizes");
        }

        for (Integer columnPos : sysTableColumnPositions.keySet()) {
            if (!TStringUtil.equalsIgnoreCase(sysTableColumnPositions.get(columnPos),
                evolutionTableColumnPositions.get(columnPos))) {
                Assert.fail("Different column names '" + sysTableColumnPositions.get(columnPos) + "' and '"
                    + evolutionTableColumnPositions.get(columnPos) + "' at " + columnPos);
            }
        }
    }

    protected void printColumnPositions(Map<Integer, String> columnPositions, String tableInfo) {
        if (MapUtils.isNotEmpty(columnPositions)) {
            StringBuilder buf = new StringBuilder();
            buf.append("\n").append("Column Positions from ").append(tableInfo).append(":\n");
            for (Map.Entry<Integer, String> entry : columnPositions.entrySet()) {
                buf.append(entry.getKey()).append(":").append(entry.getValue()).append(", ");
            }
            log.info(buf);
        } else {
            log.info("No Column Positions from " + tableInfo);
        }
    }

    protected void compareColumnRecords(String schemaName, String tableName, List<String> indexNames)
        throws SQLException {
        List<ColumnsRecord> sysTableColumnRecords = fetchColumnRecordsFromSysTable(schemaName, tableName);
        for (String indexName : indexNames) {
            List<ColumnsRecord> evolutionTableColumnRecords =
                fetchColumnRecordsFromEvolutionTable(schemaName, tableName, indexName);
            compareColumnRecords(sysTableColumnRecords, evolutionTableColumnRecords);
        }
    }

    protected List<ColumnsRecord> fetchColumnRecordsFromSysTable(String schemaName, String tableName)
        throws SQLException {
        List<ColumnsRecord> columnsRecords;
        try (Connection metaDbConn = getMetaConnection();) {
            ColumnsAccessor columnsAccessor = new ColumnsAccessor();
            columnsAccessor.setConnection(metaDbConn);
            columnsRecords = columnsAccessor.query(schemaName, tableName);
        }
        return columnsRecords;
    }

    protected List<ColumnsRecord> fetchColumnRecordsFromEvolutionTable(String schemaName, String tableName,
                                                                       String indexName)
        throws SQLException {
        List<ColumnsRecord> columnsRecords = new ArrayList<>();
        String sql1 = "select columns from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' and index_name like '%s' order by `version_id` desc limit 1";
        String sql2 = "select columns_record from columnar_column_evolution where id=%s";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql1, schemaName, tableName, indexName + '%'))) {
            Assert.assertTrue(rs.next());
            List<Long> columnIds = ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1));
            for (Long columnId : columnIds) {
                ResultSet rs1 = stmt.executeQuery(String.format(sql2, columnId));
                Assert.assertTrue(rs1.next());
                columnsRecords.add(ColumnarColumnEvolutionRecord.deserializeFromJson(rs1.getString(1)));
            }

        }
        return columnsRecords;
    }

    protected void compareColumnRecords(List<ColumnsRecord> sysTableColumnRecords,
                                        List<ColumnsRecord> evolutionTableColumnRecords) {

        if (sysTableColumnRecords == null || sysTableColumnRecords.isEmpty() ||
            evolutionTableColumnRecords == null || evolutionTableColumnRecords.isEmpty()) {
            Assert.fail("Invalid column records");
        }

        if (sysTableColumnRecords.size() != evolutionTableColumnRecords.size()) {
            Assert.fail("Different column sizes");
        }

        for (int i = 0; i < sysTableColumnRecords.size(); i++) {
            ColumnsRecord sysRecord = sysTableColumnRecords.get(i);
            ColumnsRecord evolutionRecord = evolutionTableColumnRecords.get(i);
            if (!ColumnsRecord.equalsColumnRecord(sysRecord, evolutionRecord)) {
                Assert.fail("Different column records in '" + sysTableColumnRecords.get(i).columnName);
            }
        }
    }

    protected void compareIndexRecords(String schemaName, String tableName, List<String> indexNames)
        throws SQLException {
        for (String indexName : indexNames) {
            List<IndexesRecord> sysTableIndexesRecords =
                fetchIndexesRecordsFromSysTable(schemaName, tableName, indexName);
            List<IndexesRecord> evolutionTableIndexesRecords =
                fetchIndexesRecordsFromEvolutionTable(schemaName, tableName, indexName);
            compareIndexRecords(sysTableIndexesRecords, evolutionTableIndexesRecords);
        }
    }

    protected List<IndexesRecord> fetchIndexesRecordsFromSysTable(String schemaName, String tableName, String indexName)
        throws SQLException {
        List<IndexesRecord> indexesRecords = new ArrayList<>();
        try (Connection metaDbConn = getMetaConnection();) {
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            indexesAccessor.setConnection(metaDbConn);
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);

            final String fullIndexName =
                columnarTableMappingAccessor.queryBySchemaTableIndexLike(schemaName, tableName, indexName + "%",
                    ColumnarTableStatus.PUBLIC.name()).get(0).indexName;

            final List<IndexesRecord> primaryKeyRecords =
                indexesAccessor.queryPrimaryKeyBySchemaAndTable(schemaName, tableName);
            final List<IndexesRecord> sortKeyRecords =
                indexesAccessor.queryColumnarIndexColumnsByName(schemaName, fullIndexName);

            indexesRecords.addAll(primaryKeyRecords);
            indexesRecords.addAll(sortKeyRecords);
        }
        return indexesRecords;
    }

    protected List<IndexesRecord> fetchIndexesRecordsFromEvolutionTable(String schemaName, String tableName,
                                                                        String indexName)
        throws SQLException {
        List<IndexesRecord> indexesRecords = new ArrayList<>();
        String sql1 = "select primary_keys from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' and index_name like '%s' order by `version_id` desc limit 1";
        String sql2 = "select sort_keys from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' and index_name like '%s' order by `version_id` desc limit 1";
        String sql3 = "select index_record from columnar_index_evolution where id=%s";
        try (Connection metaDbConn = getMetaConnection()) {
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql1, schemaName, tableName, indexName + '%'));
            Assert.assertTrue(rs.next());
            List<Long> indexIds = ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1));
            rs = stmt.executeQuery(String.format(sql2, schemaName, tableName, indexName + '%'));
            Assert.assertTrue(rs.next());
            indexIds.addAll(ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1)));
            for (Long indexId : indexIds) {
                ResultSet rs1 = stmt.executeQuery(String.format(sql3, indexId));
                Assert.assertTrue(rs1.next());
                indexesRecords.add(ColumnarIndexEvolutionRecord.deserializeFromJson(rs1.getString(1)));
            }

        }
        return indexesRecords;
    }

    protected void compareIndexRecords(List<IndexesRecord> sysTableIndexesRecords,
                                       List<IndexesRecord> evolutionTableIndexesRecords) {

        if (sysTableIndexesRecords == null || sysTableIndexesRecords.isEmpty() ||
            evolutionTableIndexesRecords == null || evolutionTableIndexesRecords.isEmpty()) {
            Assert.fail("Invalid index records");
        }

        if (sysTableIndexesRecords.size() != evolutionTableIndexesRecords.size()) {
            Assert.fail("Different index sizes");
        }

        for (int i = 0; i < sysTableIndexesRecords.size(); i++) {
            IndexesRecord sysRecord = sysTableIndexesRecords.get(i);
            IndexesRecord evolutionRecord = evolutionTableIndexesRecords.get(i);
            if (!IndexesRecord.equalsIndexRecord(sysRecord, evolutionRecord)) {
                Assert.fail("Different index records in '" + sysTableIndexesRecords.get(i).columnName + "'");
            }
        }
    }

    protected void checkAddIndexesRecords(String schemaName, String tableName, List<String> addColumns)
        throws SQLException {
        List<IndexesRecord> indexesRecords;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            indexesAccessor.setConnection(metaDbConn);

            List<String> indexes =
                columnarTableMappingAccessor.querySchemaTable(schemaName, tableName).stream().map(c -> c.indexName)
                    .collect(
                        Collectors.toList());
            for (String index : indexes) {
                indexesRecords = indexesAccessor.query(schemaName, tableName, index);
                if (GeneralUtil.isEmpty(indexesRecords)) {
                    continue;
                }
                Assert.assertTrue(indexesRecords.stream().anyMatch(record -> addColumns.contains(record.columnName)));
            }
        }
    }

    protected void checkDropIndexesRecords(String schemaName, String tableName, List<String> dropColumns)
        throws SQLException {
        List<IndexesRecord> indexesRecords;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            indexesAccessor.setConnection(metaDbConn);

            List<String> indexes =
                columnarTableMappingAccessor.querySchemaTable(schemaName, tableName).stream().map(c -> c.indexName)
                    .collect(
                        Collectors.toList());
            for (String index : indexes) {
                indexesRecords = indexesAccessor.query(schemaName, tableName, index);
                if (GeneralUtil.isEmpty(indexesRecords)) {
                    continue;
                }
                Assert.assertTrue(indexesRecords.stream().noneMatch(record -> dropColumns.contains(record.columnName)));
            }
        }
    }

    protected void checkChangeIndexesRecords(String schemaName, String tableName, List<Pair<String, String>> columNames)
        throws SQLException {
        List<IndexesRecord> indexesRecords;
        try (Connection metaDbConn = getMetaConnection()) {
            ColumnarTableMappingAccessor columnarTableMappingAccessor = new ColumnarTableMappingAccessor();
            IndexesAccessor indexesAccessor = new IndexesAccessor();
            columnarTableMappingAccessor.setConnection(metaDbConn);
            indexesAccessor.setConnection(metaDbConn);

            List<String> newColumnsNames = columNames.stream().map(Pair::getKey).collect(Collectors.toList());
            List<String> oldColumnsNames = columNames.stream().map(Pair::getValue).collect(Collectors.toList());

            List<String> indexes =
                columnarTableMappingAccessor.querySchemaTable(schemaName, tableName).stream().map(c -> c.indexName)
                    .collect(
                        Collectors.toList());
            for (String index : indexes) {
                indexesRecords = indexesAccessor.query(schemaName, tableName, index);
                if (GeneralUtil.isEmpty(indexesRecords)) {
                    continue;
                }
                Assert.assertTrue(
                    indexesRecords.stream().anyMatch(record -> newColumnsNames.contains(record.columnName)));
                Assert.assertTrue(
                    indexesRecords.stream().noneMatch(record -> oldColumnsNames.contains(record.columnName)));
            }
        }
    }

    protected String fetchCciSysTable(String schemaName, String tableName, String columnarName)
        throws SQLException {
        String sql = "select index_name from columnar_table_mapping "
            + "where table_schema='%s' and table_name='%s' and index_name like '%%%s%%' order by latest_version_id desc limit 1";
        try (Connection metaDbConn = getMetaConnection();
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, tableName, columnarName))) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return "";
    }

    protected void checkCciMeta(String indexName) throws SQLException {
        String sql = String.format("check columnar index `%s` meta", indexName);
        ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        org.junit.Assert.assertTrue(resultSet.next());
        String detail = resultSet.getString("details");
        Assert.assertTrue(detail.startsWith("OK"));
    }

    protected void comparePartitionRecords(String schemaName, String tableName, String indexName)
        throws SQLException {
        List<TablePartitionRecord> sysTablePartitionRecords =
            fetchPartitionRecordsFromSysTable(schemaName, tableName, indexName);
        List<TablePartitionRecord> evolutionTablePartitionRecords =
            fetchPartitionRecordsFromEvolutionTable(schemaName, tableName, indexName);
        comparePartitionRecords(sysTablePartitionRecords, evolutionTablePartitionRecords);
    }

    protected List<TablePartitionRecord> fetchPartitionRecordsFromSysTable(String schemaName, String tableName,
                                                                           String indexName)
        throws SQLException {
        List<TablePartitionRecord> partitionRecords;
        try (Connection metaDbConn = getMetaConnection();) {
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            tablePartitionAccessor.setConnection(metaDbConn);

            String realCciName = getRealCciName(tableName, indexName);
            partitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, realCciName, false);
        }
        return partitionRecords;
    }

    protected List<TablePartitionRecord> fetchPartitionRecordsFromEvolutionTable(String schemaName, String tableName,
                                                                                 String indexName)
        throws SQLException {
        List<TablePartitionRecord> partitionRecords = new ArrayList<>();
        String sql1 = "select partitions from columnar_table_evolution "
            + "where table_schema='%s' and table_name='%s' and index_name like '%s' order by `version_id` desc limit 1";
        String sql2 = "select partition_record from columnar_partition_evolution where id=%s";
        try (Connection metaDbConn = getMetaConnection()) {
            Statement stmt = metaDbConn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql1, schemaName, tableName, indexName + '%'));
            Assert.assertTrue(rs.next());
            List<Long> partitionIds = ColumnarTableEvolutionRecord.deserializeListFromJson(rs.getString(1));
            for (Long partitionId : partitionIds) {
                ResultSet rs1 = stmt.executeQuery(String.format(sql2, partitionId));
                Assert.assertTrue(rs1.next());
                partitionRecords.add(ColumnarPartitionEvolutionRecord.deserializeFromJson(rs1.getString(1)));
            }

        }
        return partitionRecords;
    }

    protected void comparePartitionRecords(List<TablePartitionRecord> sysTablePartitionRecords,
                                           List<TablePartitionRecord> evolutionTablePartitionRecords) {

        if (sysTablePartitionRecords == null || sysTablePartitionRecords.isEmpty() ||
            evolutionTablePartitionRecords == null || evolutionTablePartitionRecords.isEmpty()) {
            Assert.fail("Invalid partition records");
        }

        if (sysTablePartitionRecords.size() != evolutionTablePartitionRecords.size()) {
            Assert.fail("Different partition sizes");
        }

        for (int i = 0; i < sysTablePartitionRecords.size(); i++) {
            TablePartitionRecord sysRecord = sysTablePartitionRecords.get(i);
            TablePartitionRecord evolutionRecord = evolutionTablePartitionRecords.get(i);
            if (!TablePartitionRecord.isPartitionRecordEqual(sysRecord, evolutionRecord)) {
                Assert.fail("Different partition records in '" + sysTablePartitionRecords.get(i).partName + "'");
            }
        }
    }
}
