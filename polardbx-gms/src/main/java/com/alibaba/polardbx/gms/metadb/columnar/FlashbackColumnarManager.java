package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.ColumnarPartitionPrunedSnapshot;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableIdVersionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableIdVersionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

public class FlashbackColumnarManager {

    private final Map<String, List<Pair<String, Long>>> deletePositionMap = new HashMap<>();
    private final Map<String, Pair<List<String>, List<Pair<String, Long>>>> snapshotInfo = new HashMap<>();
    private final Map<String, Long> schemaTsoMap = new HashMap<>();
    private Long columnarDeltaCheckpointTso = null;

    public FlashbackColumnarManager(long flashbackTso, String logicalSchema, String logicalTable,
                                    boolean autoPosition) {
        try (Connection connection = MetaDbUtil.getConnection()) {

            List<Long> tableIds = getAllTableIds(connection, logicalSchema, logicalTable);
            long actualTableId = -1;

            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);

            List<FilesRecordSimplified> snapshotFiles = new ArrayList<>();
            for (Long tableId : tableIds) {
                snapshotFiles = filesAccessor
                    .queryColumnarSnapshotFilesByTsoAndTableId(flashbackTso, logicalSchema, String.valueOf(tableId));
                // tableId从大到小，直到找到对应版本的tableId
                if (GeneralUtil.isNotEmpty(snapshotFiles)) {
                    actualTableId = tableId;
                    break;
                }
            }

            if (actualTableId == -1) {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                    String.format("Columnar index not found, schema: %s, table: %s", logicalSchema, logicalTable));
            }

            if (autoPosition) {
                ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
                checkpointsAccessor.setConnection(connection);

                List<ColumnarCheckpointsRecord> checkpoints =
                    checkpointsAccessor.queryColumnarTsoByBinlogTsoAndCheckpointTsoAsc(flashbackTso);
                if (checkpoints == null || checkpoints.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                        String.format(
                            "No valid columnar checkpoint found for binlog tso %d, schema: %s, table: %s. "
                                + "Please check if columnar delta checkpoint has been triggered.",
                            flashbackTso, logicalSchema, logicalTable));
                }
                this.columnarDeltaCheckpointTso = checkpoints.get(0).checkpointTso;
            }

            for (FilesRecordSimplified record : snapshotFiles) {
                String fileName = record.fileName;
                String partName = record.partitionName;
                Long schemaTso = record.schemaTs;
                String suffix = fileName.substring(fileName.lastIndexOf('.') + 1);
                ColumnarFileType columnarFileType = ColumnarFileType.of(suffix);

                if (columnarFileType == ColumnarFileType.ORC) {
                    snapshotInfo.computeIfAbsent(
                        partName,
                        s -> Pair.of(new ArrayList<>(), new ArrayList<>())
                    ).getKey().add(fileName);
                    schemaTsoMap.put(fileName, schemaTso);
                } else if (columnarFileType.isDeltaFile()) {
                    schemaTsoMap.put(fileName, schemaTso);

                    if (autoPosition) {
                        if (columnarFileType == ColumnarFileType.CSV) {
                            snapshotInfo.computeIfAbsent(
                                partName,
                                s -> Pair.of(new ArrayList<>(), new ArrayList<>())
                            ).getValue().add(Pair.of(fileName, -1L));
                        } else if (columnarFileType == ColumnarFileType.DEL) {
                            deletePositionMap.computeIfAbsent(
                                partName,
                                s -> new ArrayList<>()
                            ).add(Pair.of(fileName, -1L));
                        }
                    }
                }
            }

            if (autoPosition) {
                // for auto position, we get the read position from the csv file content, rather than GMS
                return;
            }

            ColumnarAppendedFilesAccessor appendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            appendedFilesAccessor.setConnection(connection);

            List<ColumnarAppendedFilesRecord> appendedFilesRecords =
                appendedFilesAccessor.queryLastValidAppendByTsoAndTableId(flashbackTso, logicalSchema,
                    String.valueOf(actualTableId));

            for (ColumnarAppendedFilesRecord record : appendedFilesRecords) {
                String fileName = record.fileName;
                String partName = record.partName;
                long position = record.appendOffset + record.appendLength;
                String suffix = fileName.substring(fileName.lastIndexOf('.') + 1);
                ColumnarFileType columnarFileType = ColumnarFileType.of(suffix);

                if (columnarFileType == ColumnarFileType.CSV) {
                    snapshotInfo.computeIfAbsent(
                        partName,
                        s -> Pair.of(new ArrayList<>(), new ArrayList<>())
                    ).getValue().add(Pair.of(fileName, position));
                }

                if (columnarFileType == ColumnarFileType.DEL) {
                    deletePositionMap.computeIfAbsent(
                        partName,
                        s -> new ArrayList<>()
                    ).add(Pair.of(fileName, position));
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                String.format("Failed to generate columnar snapshot, tso: %d, schema: %s, table: %s",
                    flashbackTso, logicalSchema, logicalTable));
        }
    }

    /**
     * @return all versioned table ids, order from large to small
     */
    public List<Long> getAllTableIds(Connection connection, String logicalSchema, String logicalTable) {
        ColumnarTableMappingAccessor mappingAccessor = new ColumnarTableMappingAccessor();
        ColumnarTableIdVersionAccessor versionAccessor = new ColumnarTableIdVersionAccessor();
        mappingAccessor.setConnection(connection);
        versionAccessor.setConnection(connection);

        Set<Long> tableIdsSet = new TreeSet<>(Collections.reverseOrder());
        List<ColumnarTableMappingRecord> records = mappingAccessor.querySchemaIndex(logicalSchema, logicalTable);
        if (GeneralUtil.isNotEmpty(records)) {
            long tableId = records.get(0).tableId;
            // 已访问的tableId集合
            Set<Long> visitedTableIds = new HashSet<>();
            List<ColumnarTableIdVersionRecord> tableIdVersionRecords = versionAccessor.queryByNewTableId(tableId);
            if (GeneralUtil.isNotEmpty(tableIdVersionRecords)) {
                while (GeneralUtil.isNotEmpty(tableIdVersionRecords)) {
                    ColumnarTableIdVersionRecord tableIdVersionRecord = tableIdVersionRecords.get(0);
                    // 检查是否存在循环引用
                    if (!visitedTableIds.add(tableIdVersionRecord.newTableId)) {
                        throw new RuntimeException("Detected cycle in tableIdVersion for tableId: "
                            + tableIdVersionRecord.newTableId);
                    }
                    tableIdsSet.add(tableIdVersionRecord.newTableId);
                    tableIdsSet.add(tableIdVersionRecord.oldTableId);
                    tableId = tableIdVersionRecord.oldTableId;
                    tableIdVersionRecords = versionAccessor.queryByNewTableId(tableId);
                }
            } else {
                tableIdsSet.add(tableId);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                String.format("Columnar index not found, schema: %s, table: %s", logicalSchema, logicalTable));
        }

        return new ArrayList<>(tableIdsSet);
    }

    /**
     * @return partName -> ([orcFiles](name), [csvFiles](name, pos))
     */
    public Map<String, Pair<List<String>, List<Pair<String, Long>>>> getSnapshotInfo() {
        return snapshotInfo;
    }

    public Map<String, List<Pair<String, Long>>> getDeletePositions() {
        return deletePositionMap;
    }

    public Long getColumnarDeltaCheckpointTso() {
        return columnarDeltaCheckpointTso;
    }

    public Map<String, ColumnarPartitionPrunedSnapshot> getSnapshotInfo(
        SortedMap<Long, Set<String>> partitionResult) {
        Map<String, ColumnarPartitionPrunedSnapshot> result = new HashMap<>();
        snapshotInfo.forEach((partName, partSnapshot) -> {
            for (String orcFileName : partSnapshot.getKey()) {
                Long schemaTso = schemaTsoMap.get(orcFileName);
                SortedMap<Long, Set<String>> headMap = partitionResult.headMap(schemaTso + 1);
                if (!headMap.isEmpty()) {
                    Set<String> partitionSet = headMap.get(headMap.lastKey());
                    if (partitionSet != null && partitionSet.contains(partName)) {
                        result.computeIfAbsent(partName,
                            s -> new ColumnarPartitionPrunedSnapshot()
                        ).getOrcFilesAndSchemaTs().add(Pair.of(orcFileName, schemaTso));
                    }
                }
            }

            for (Pair<String, Long> csvNameAndPos : partSnapshot.getValue()) {
                Long schemaTso = schemaTsoMap.get(csvNameAndPos.getKey());
                SortedMap<Long, Set<String>> headMap = partitionResult.headMap(schemaTso + 1);
                if (!headMap.isEmpty()) {
                    Set<String> partitionSet = headMap.get(headMap.lastKey());
                    if (partitionSet != null && partitionSet.contains(partName)) {
                        result.computeIfAbsent(partName,
                                s -> new ColumnarPartitionPrunedSnapshot()
                            ).getCsvFilesAndSchemaTsWithPos()
                            .add(Pair.of(csvNameAndPos.getKey(), Pair.of(schemaTso, csvNameAndPos.getValue())));
                    }
                }
            }
        });
        return result;
    }

    public Map<String, List<Pair<String, Long>>> getDeletePositions(
        SortedMap<Long, Set<String>> partitionResult) {
        Map<String, List<Pair<String, Long>>> result = new HashMap<>();
        deletePositionMap.forEach((partName, partDeletePositions) -> {
            for (Pair<String, Long> deletePosition : partDeletePositions) {
                Long schemaTso = schemaTsoMap.get(deletePosition.getKey());
                SortedMap<Long, Set<String>> headMap = partitionResult.headMap(schemaTso + 1);
                if (!headMap.isEmpty()) {
                    Set<String> partitionSet = headMap.get(headMap.lastKey());
                    if (partitionSet != null && partitionSet.contains(partName)) {
                        result.computeIfAbsent(partName, s -> new ArrayList<>()).add(deletePosition);
                    }
                }
            }
        });
        return result;
    }
}
