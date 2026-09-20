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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.InplaceSplitUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;

public class LogicalAlterTableSplitPartition extends BaseDdlOperation {

    protected AlterTableGroupSplitPartitionPreparedData preparedData;

    public LogicalAlterTableSplitPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableSplitPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    @Override
    public boolean isSupportedByCci(ExecutionContext ec) {
        String schemaName = this.schemaName;
        String tblName = Util.last(((SqlIdentifier) relDdl.getTableName()).names);

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            TableMeta tblMeta = ec.getSchemaManager(schemaName).getTable(tblName);
            if (tblMeta.isColumnar()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec, boolean supportChangeset) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartition;
        SqlAlterTableSplitPartition sqlAlterTableSplitPartition =
            (SqlAlterTableSplitPartition) sqlAlterTable.getAlters().get(0);

        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        boolean ignoreNameAndLocality = GeneralUtil.isEmpty(sqlAlterTableSplitPartition.getNewPartitions());
        boolean hasSubPartDef = false;
        if (!ignoreNameAndLocality) {
            hasSubPartDef =
                GeneralUtil.isNotEmpty(sqlAlterTableSplitPartition.getNewPartitions().get(0).getSubPartitions());
        }

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        PartitionInfo curPartitionInfo = tableMeta.getPartitionInfo();

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        String partNamePrefix = StringUtils.EMPTY;
        if (sqlAlterTableSplitPartition.getNewPartitionPrefix() != null) {
            partNamePrefix =
                SQLUtils.normalizeNoTrim(sqlAlterTableSplitPartition.getNewPartitionPrefix().toString());
        }
        List<SqlNode> splitPartitionNameNodes = sqlAlterTableSplitPartition.getSplitPartitionNames();
        boolean isMultiPartitionSplit = splitPartitionNameNodes != null && splitPartitionNameNodes.size() > 1;

        List<String> splitPartitions = new ArrayList<>();
        String splitPartitionName;
        if (isMultiPartitionSplit) {
            for (SqlNode nameNode : splitPartitionNameNodes) {
                splitPartitions.add(Util.last(((SqlIdentifier) nameNode).names));
            }
            splitPartitionName = splitPartitions.get(0);
            normalizeMultiSplitPartition(sqlAlterTableSplitPartition, tableGroupName, logicalTableName,
                splitPartitions, ec);
        } else {
            splitPartitionName =
                Util.last(((SqlIdentifier) (sqlAlterTableSplitPartition.getSplitPartitionName())).names);
            splitPartitions.add(splitPartitionName);
            normalizeSqlSplitPartition(sqlAlterTableSplitPartition, tableGroupName, logicalTableName,
                splitPartitionName, ec);
        }

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);
        PartitionByDefinition partitionBy = curPartitionInfo.getPartitionBy();

        if (sqlAlterTableSplitPartition.isSubPartitionsSplit() && partitionBy.getSubPartitionBy() != null) {
            partitionBy = partitionBy.getSubPartitionBy();
        }

        preparedData = new AlterTableSplitPartitionPreparedData();
        boolean inplaceBackfill =
            InplaceSplitUtils.supportInplaceBackfill(schemaName, logicalTableName, partitionBy, supportChangeset, ec);

        boolean splitSubPartition = sqlAlterTableSplitPartition.isSubPartitionsSplit();
        preparedData.setInplaceBackfill(inplaceBackfill);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setSplitSubPartition(splitSubPartition);
        preparedData.setOperateOnSubPartition(sqlAlterTableSplitPartition.isSubPartitionsSplit());
        preparedData.setUseTemplatePart(curPartitionInfo.getPartitionBy().getSubPartitionBy() != null ?
            curPartitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate() : false);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableSplitPartition.getNewPartitions());
        preparedData.setIncludeFullPartitionDefinition(sqlAlterTableSplitPartition.getAtValue() == null);
        preparedData.setTargetStorageInstIds(null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());
        preparedData.setAtVal(sqlAlterTableSplitPartition.getAtValue());
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        if (preparedData.isUseTemplatePart() && preparedData.isSplitSubPartition()) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : curPartitionInfo.getPartitionBy().getPartitions()) {
                assert partitionSpec.isLogical();
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
        }
        preparedData.setTargetImplicitTableGroupName(sqlAlterTable.getTargetImplicitTableGroupName());

        //call this method before preparePartitionRelationship and prepareInvisiblePartitionGroup
        preparedData.checkAndResetInplaceBackfill(tableMeta, ec);

        Boolean hasSubPartition = curPartitionInfo.getPartitionBy().getSubPartitionBy() != null;
        preparedData.preparePartitionRelationship(curPartitionInfo, null);
        preparedData.prepareInvisiblePartitionGroup(hasSubPartition);

        SqlConverter sqlConverter = SqlConverter.getInstance(curPartitionInfo.getTableSchema(), ec);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(this.getCluster());
        Map<SqlNode, RexNode> partRexInfoCtx =
            sqlConverter.getRexInfoFromSqlAlterSpec(sqlAlterTable, ImmutableList.of(sqlAlterTableSplitPartition),
                plannerContext);
        preparedData.getPartBoundExprInfo().putAll(partRexInfoCtx);

        if (preparedData.needFindCandidateTableGroup()) {
            List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
            Map<String, Pair<String, String>> mockOrderedTargetTableLocations =
                new TreeMap<>(String::compareToIgnoreCase);
            int newPartCount = newPartitionGroups.size();
            if (ignoreNameAndLocality) {
                for (int j = 0; j < newPartCount; j++) {
                    Pair<String, String> pair = new Pair<>("", "");
                    mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, pair);
                }
                flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
            } else {
                for (int j = 0; j < newPartCount; j++) {
                    String mockTableName = "";
                    mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name,
                        new Pair<>(mockTableName, newPartitionGroups.get(j).getGroup_Name()));
                }
            }

            boolean changeFlag =
                curPartitionInfo.getPartitionBy().getSubPartitionBy() != null && !preparedData.isUseTemplatePart()
                    && !preparedData.isSplitSubPartition() && ((flag & IGNORE_PARTNAME_LOCALITY)
                    != IGNORE_PARTNAME_LOCALITY) && !hasSubPartDef;

            if (changeFlag) {
                flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
            }

            PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                    preparedData,
                    curPartitionInfo,
                    false,
                    sqlAlterTableSplitPartition,
                    preparedData.getOldPartitionNames(),
                    preparedData.getNewPartitionNames(),
                    preparedData.getTableGroupName(),
                    splitPartitionName,
                    preparedData.getInvisiblePartitionGroups(),
                    mockOrderedTargetTableLocations,
                    ec);

            if (preparedData.isInplaceBackfill()) {
                flag |= PartitionInfoUtil.COMPARE_NEW_PART_LOCATION;
            }
            preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo,
                sqlAlterTableSplitPartition.getNewPartitions(), partNamePrefix, flag, ec);

            preparedData.rebuildPartitionRelationship(curPartitionInfo, newPartInfo);
        }
    }

    public void normalizeSqlSplitPartition(SqlAlterTableSplitPartition sqlAlterTableGroupSplitPartition,
                                           String tableGroupName,
                                           String logicalTableName,
                                           String splitPartitionName,
                                           ExecutionContext ec) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        //for example, for key hash/key strategy partition split, alter table t1 split p1
        if (GeneralUtil.isEmpty(sqlAlterTableGroupSplitPartition.getNewPartitions())) {
            int newNameCount = 2;
            if (sqlAlterTableGroupSplitPartition.getNewPartitionNum() != null) {
                newNameCount =
                    ((SqlNumericLiteral) (sqlAlterTableGroupSplitPartition.getNewPartitionNum())).intValue(true);
            }
            List<String> newPartitionNames;
            if (sqlAlterTableGroupSplitPartition.getNewPartitionPrefix() != null) {
                String partNamePrefix =
                    SQLUtils.normalizeNoTrim(sqlAlterTableGroupSplitPartition.getNewPartitionPrefix().toString());
                newPartitionNames =
                    PartitionNameUtil.autoGeneratePartitionNamesWithUserDefPrefix(partNamePrefix, newNameCount);
            } else {
                TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
                List<String> partNames = new ArrayList<>();
                List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
                PartitionInfoUtil.getPartitionName(partitionInfo, partNames, subPartNamePairs);
                newPartitionNames =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs,
                        newNameCount,
                        new TreeSet<>(String::compareToIgnoreCase),
                        sqlAlterTableGroupSplitPartition.isSubPartitionsSplit());
            }

            assert newPartitionNames.size() == newNameCount;
            for (int i = 0; i < newNameCount; i++) {
                SqlIdentifier name = new SqlIdentifier(newPartitionNames.get(i), SqlParserPos.ZERO);
                SqlPartition sqlPartition = new SqlPartition(name, null, SqlParserPos.ZERO);
                sqlAlterTableGroupSplitPartition.getNewPartitions().add(sqlPartition);
            }

        }
        if (!sqlAlterTableGroupSplitPartition.isSubPartitionsSplit() && StringUtils.isNotEmpty(logicalTableName)) {
            PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();
            if (subPartBy == null) {
                return;
            }

            //split logical partition, will inherit the same subPartition definition as the source logical partition
            if (!subPartBy.isUseSubPartTemplate()) {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    int subPartCntOfTargetPart = partitionSpec.getSubPartitions().size();
                    if (partitionSpec.getName().equalsIgnoreCase(splitPartitionName)) {

                        /**
                         * Fetch all the existed subPartNames of the target partitions to be split
                         * by ast of ddl provided user
                         */
                        Set<String> existsNames = new TreeSet<>(String::compareToIgnoreCase);
                        /**
                         * hasSubPartDefinition is stat that all the target partitions to be split
                         * that contain subpartDefinitions
                         */
                        int hasSubPartDefinition = 0;
                        for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
                            for (SqlNode subPart : GeneralUtil.emptyIfNull(sqlPartition.getSubPartitions())) {
                                existsNames.add(((SqlIdentifier) ((SqlSubPartition) subPart).getName()).getSimple());
                            }
                            if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                                hasSubPartDefinition++;
                            }
                        }

                        if (hasSubPartDefinition < sqlAlterTableGroupSplitPartition.getNewPartitions().size()) {
                            /**
                             * Where hasSubPartDefinition < the size of all the parts of ddl ast to be split,
                             * that means some 1st-level part of ddl ast has no explicit subpart bound definitions
                             */

                            List<Integer> newPartIdxListThatDefineSubPartBnds = new ArrayList<>();
                            List<Integer> newPartIdxLIstThatDefineOnlySubPartCnt = new ArrayList<>();
                            List<Integer> newPartIdxListThatHasNoAnySubPartDefines = new ArrayList<>();
                            /**
                             * key: newSplitPartIdx
                             * Val: left:subPartCnt, right: hasBndDefinition
                             */
                            Map<Integer, Pair<Integer, Boolean>> newPartIdx2SubPartCntMapping = new HashMap<>();
                            Set<Integer> newPartIdxSetOfCopySubPartDefineByTargetPart = new HashSet<>();
                            List<SqlPartition> newSplitSqlPartAstList =
                                sqlAlterTableGroupSplitPartition.getNewPartitions();
                            for (int i = 0; i < newSplitSqlPartAstList.size(); i++) {
                                SqlPartition partAst = newSplitSqlPartAstList.get(i);
                                int subPartCntVal = 0;
                                boolean hasBndDefinitions = false;
                                SqlNode subPartCntAst = partAst.getSubPartitionCount();
                                List<SqlNode> subPartBndList = partAst.getSubPartitions();
                                if (subPartCntAst == null) {
                                    if (subPartBndList == null || subPartBndList.isEmpty()) {
                                        newPartIdxListThatHasNoAnySubPartDefines.add(i);
                                    } else {
                                        newPartIdxListThatDefineSubPartBnds.add(i);
                                        subPartCntVal = subPartBndList.size();
                                        hasBndDefinitions = true;
                                    }
                                } else {
                                    if (subPartBndList == null || subPartBndList.isEmpty()) {
                                        if (subPartCntAst instanceof SqlNumericLiteral) {
                                            subPartCntVal = Integer.valueOf(subPartCntAst.toString());
                                            if (subPartCntVal > 0) {
                                                newPartIdxLIstThatDefineOnlySubPartCnt.add(i);
                                            } else {
                                                newPartIdxListThatHasNoAnySubPartDefines.add(i);
                                            }
                                        }
                                    } else {
                                        newPartIdxListThatDefineSubPartBnds.add(i);
                                        subPartCntVal = subPartBndList.size();
                                        hasBndDefinitions = true;
                                    }
                                }
                                if (subPartCntVal == 0 && !hasBndDefinitions) {
                                    /**
                                     * all the part without any subpart defines
                                     * use the same subpart definitions as target part
                                     */
                                    subPartCntVal = subPartCntOfTargetPart;
                                    newPartIdxSetOfCopySubPartDefineByTargetPart.add(i);
                                }
                                Pair<Integer, Boolean> val = new Pair<>(subPartCntVal, hasBndDefinitions);
                                newPartIdx2SubPartCntMapping.put(i, val);
                            }

                            int newSubPartCntToGen = 0;
                            for (Map.Entry<Integer, Pair<Integer, Boolean>> item : newPartIdx2SubPartCntMapping.entrySet()) {
                                Integer spCnt = item.getValue().getKey();
                                newSubPartCntToGen += spCnt;
                            }

                            TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
                            List<String> partNames = new ArrayList<>();
                            List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
                            /**
                             * Fetch all the current partNames and subPartNames from partInfo
                             */
                            PartitionInfoUtil.getPartitionName(partitionInfo, partNames, subPartNamePairs);

                            List<String> newSubPartitionNames =
                                PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames,
                                    subPartNamePairs, newSubPartCntToGen, existsNames, true);

                            int j = 0;
                            for (int i = 0; i < newSplitSqlPartAstList.size(); i++) {
                                SqlPartition sqlPartAst = newSplitSqlPartAstList.get(i);
                                List<SqlNode> subPartListOfPartAst = sqlPartAst.getSubPartitions();
                                String newSplitPartName = ((SqlIdentifier) sqlPartAst.getName()).getLastName();
                                Pair<Integer, Boolean> subPartCntStatItem = newPartIdx2SubPartCntMapping.get(i);
                                Integer spCntVal = subPartCntStatItem.getKey();
                                Boolean hasBnds = subPartCntStatItem.getValue();
                                if (hasBnds) {
                                    continue;
                                }
                                if (newPartIdxSetOfCopySubPartDefineByTargetPart.contains(i)) {
                                    for (int k = 0; k < spCntVal; k++) {
                                        String newGenSubPartName = newSubPartitionNames.get(j);
                                        String realSubPartName =
                                            PartitionNameUtil.autoBuildSubPartitionName(newSplitPartName,
                                                newGenSubPartName);
                                        PartitionSpec subPartitionSpec = partitionSpec.getSubPartitions().get(k);
                                        SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                                            new SqlIdentifier(realSubPartName, SqlParserPos.ZERO),
                                            (SqlPartitionValue) subPartitionSpec.getBoundSpec().getBoundRawValue()
                                                .clone(SqlParserPos.ZERO));
                                        subPartListOfPartAst.add(sqlSubPartition);
                                        j++;
                                    }
                                } else {
                                    for (int k = 0; k < spCntVal; k++) {
                                        String newGenSubPartName = newSubPartitionNames.get(j);
                                        String realSubPartName =
                                            PartitionNameUtil.autoBuildSubPartitionName(newSplitPartName,
                                                newGenSubPartName);
                                        SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                                            new SqlIdentifier(realSubPartName, SqlParserPos.ZERO), null);
                                        subPartListOfPartAst.add(sqlSubPartition);
                                        j++;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            } else {
                PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().get(0);
                for (SqlPartition sqlPartition : sqlAlterTableGroupSplitPartition.getNewPartitions()) {
                    String logicalPartName = ((SqlIdentifier) (sqlPartition.getName())).getSimple();
                    for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                        String subPartTempName = subPartitionSpec.getTemplateName();
                        String realSubPartName =
                            PartitionNameUtil.autoBuildSubPartitionName(logicalPartName, subPartTempName);
                        SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                            new SqlIdentifier(realSubPartName, SqlParserPos.ZERO),
                            (SqlPartitionValue) subPartitionSpec.getBoundSpec().getBoundRawValue()
                                .clone(SqlParserPos.ZERO));
                        sqlPartition.getSubPartitions().add(sqlSubPartition);
                    }
                }
            }
        }

    }

    /**
     * Normalize multi-partition split: generate new partition names for all source partitions.
     * Each source partition gets newNameCount new partitions (from INTO PARTITIONS N or default 2).
     */
    private void normalizeMultiSplitPartition(SqlAlterTableSplitPartition sqlAlterTableSplitPartition,
                                              String tableGroupName,
                                              String logicalTableName,
                                              List<String> splitPartitions,
                                              ExecutionContext ec) {
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);

        int newNameCount = 2;
        if (sqlAlterTableSplitPartition.getNewPartitionNum() != null) {
            newNameCount =
                ((SqlNumericLiteral) (sqlAlterTableSplitPartition.getNewPartitionNum())).intValue(true);
        }

        int totalNewCount = splitPartitions.size() * newNameCount;
        List<String> allNewNames;
        if (sqlAlterTableSplitPartition.getNewPartitionPrefix() != null) {
            String partNamePrefix =
                SQLUtils.normalizeNoTrim(sqlAlterTableSplitPartition.getNewPartitionPrefix().toString());
            allNewNames =
                PartitionNameUtil.autoGeneratePartitionNamesWithUserDefPrefix(partNamePrefix, totalNewCount);
        } else {
            TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
            List<String> partNames = new ArrayList<>();
            List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
            PartitionInfoUtil.getPartitionName(partitionInfo, partNames, subPartNamePairs);
            allNewNames =
                PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs,
                    totalNewCount, new TreeSet<>(String::compareToIgnoreCase),
                    sqlAlterTableSplitPartition.isSubPartitionsSplit());
        }

        if (allNewNames.size() != totalNewCount) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "Generated partition name count (" + allNewNames.size()
                    + ") does not match expected count (" + totalNewCount + ")");
        }
        int idx = 0;
        for (String sourcePart : splitPartitions) {
            for (int i = 0; i < newNameCount; i++) {
                SqlIdentifier name = new SqlIdentifier(allNewNames.get(idx++), SqlParserPos.ZERO);
                SqlPartition sqlPartition = new SqlPartition(name, null, SqlParserPos.ZERO);
                sqlAlterTableSplitPartition.getNewPartitions().add(sqlPartition);
            }
        }

        // For tables with subpartitions, generate subpartition definitions for each new partition.
        // Without this, the new partitions would inherit copied subpartition names from the source,
        // causing duplicate subpartition name errors during validation.
        if (!sqlAlterTableSplitPartition.isSubPartitionsSplit()) {
            PartitionByDefinition subPartBy = partitionInfo.getPartitionBy().getSubPartitionBy();
            if (subPartBy != null) {
                if (!subPartBy.isUseSubPartTemplate()) {
                    // Non-templated: each source partition may have a different number of subpartitions.
                    Set<String> existsNames = new TreeSet<>(String::compareToIgnoreCase);
                    List<String> partNames = new ArrayList<>();
                    List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
                    PartitionInfoUtil.getPartitionName(partitionInfo, partNames, subPartNamePairs);
                    TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();

                    // Calculate total subpartitions needed across all source partitions
                    int totalSubPartsNeeded = 0;
                    List<PartitionSpec> sourcePartSpecs = new ArrayList<>();
                    for (String sourceName : splitPartitions) {
                        PartitionSpec found = null;
                        for (PartitionSpec ps : partitionInfo.getPartitionBy().getPartitions()) {
                            if (ps.getName().equalsIgnoreCase(sourceName)) {
                                found = ps;
                                break;
                            }
                        }
                        sourcePartSpecs.add(found);
                        if (found != null) {
                            totalSubPartsNeeded += newNameCount * found.getSubPartitions().size();
                        }
                    }

                    // Generate all unique subpartition template names at once
                    List<String> allNewSubPartNames = PartitionNameUtil.autoGeneratePartitionNames(
                        tableGroupRecord, partNames, subPartNamePairs,
                        totalSubPartsNeeded, existsNames, true);

                    // Distribute subpartition definitions to each new SqlPartition
                    int nameIdx = 0;
                    int partIdx = 0;
                    for (int s = 0; s < splitPartitions.size(); s++) {
                        PartitionSpec sourcePartSpec = sourcePartSpecs.get(s);
                        if (sourcePartSpec == null) {
                            partIdx += newNameCount;
                            continue;
                        }
                        int subPartCnt = sourcePartSpec.getSubPartitions().size();
                        for (int i = 0; i < newNameCount; i++) {
                            SqlPartition sqlPart = sqlAlterTableSplitPartition.getNewPartitions().get(partIdx);
                            String newPartName = ((SqlIdentifier) sqlPart.getName()).getLastName();
                            for (int k = 0; k < subPartCnt; k++) {
                                String newGenSubPartName = allNewSubPartNames.get(nameIdx++);
                                String realSubPartName = PartitionNameUtil.autoBuildSubPartitionName(
                                    newPartName, newGenSubPartName);
                                PartitionSpec subPartSpec = sourcePartSpec.getSubPartitions().get(k);
                                SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                                    new SqlIdentifier(realSubPartName, SqlParserPos.ZERO),
                                    (SqlPartitionValue) subPartSpec.getBoundSpec().getBoundRawValue()
                                        .clone(SqlParserPos.ZERO));
                                sqlPart.getSubPartitions().add(sqlSubPartition);
                            }
                            partIdx++;
                        }
                    }
                } else {
                    // Templated: all partitions share the same subpartition template.
                    PartitionSpec templatePartSpec = partitionInfo.getPartitionBy().getPartitions().get(0);
                    for (SqlPartition sqlPartition : sqlAlterTableSplitPartition.getNewPartitions()) {
                        String logicalPartName = ((SqlIdentifier) sqlPartition.getName()).getSimple();
                        for (PartitionSpec subPartSpec : templatePartSpec.getSubPartitions()) {
                            String subPartTempName = subPartSpec.getTemplateName();
                            String realSubPartName = PartitionNameUtil.autoBuildSubPartitionName(
                                logicalPartName, subPartTempName);
                            SqlSubPartition sqlSubPartition = new SqlSubPartition(SqlParserPos.ZERO,
                                new SqlIdentifier(realSubPartName, SqlParserPos.ZERO),
                                (SqlPartitionValue) subPartSpec.getBoundSpec().getBoundRawValue()
                                    .clone(SqlParserPos.ZERO));
                            sqlPartition.getSubPartitions().add(sqlSubPartition);
                        }
                    }
                }
            }
        }
    }

    public AlterTableGroupSplitPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSplitPartition create(DDL ddl) {
        return new LogicalAlterTableSplitPartition(ddl);
    }

}
