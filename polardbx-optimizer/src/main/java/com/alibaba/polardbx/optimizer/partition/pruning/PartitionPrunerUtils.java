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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.field.FieldCheckLevel;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.core.rel.util.LogicalViewCommonGroupInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.DirectPlanCommonGroupInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;
import com.alibaba.polardbx.optimizer.partition.util.StepExplainItem;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public class PartitionPrunerUtils {

    /**
     * The log for storage check ha log
     */
    public static final Logger PRUNER_LOG = LoggerFactory.getLogger(PartitionPrunerUtils.class);

    protected static final RelDataTypeFactory typeFactory =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    protected static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    //=========== Some tool methods for TargetDB ============
    /**
     * Full topology of one tbl : PartitionInfo.getPhysicalPartitionTopology() & PartitionInfo.getTopology()
     */

    /**
     * Convert one PartPrunedResult to TargetDB
     */
    public static List<TargetDB> buildTargetDbsByPartPrunedResults(PartPrunedResult result) {

        List<TargetDB> targetDbList = new ArrayList<>();
        Map<String, Map<String, Field>> targetDbInfo = new HashMap<>();

        List<PhysicalPartitionInfo> prunedParts = result.getPrunedPartitions();
        for (int j = 0; j < prunedParts.size(); j++) {
            PhysicalPartitionInfo prunedPart = prunedParts.get(j);

            String grpKey = prunedPart.getGroupKey();
            String phyTb = prunedPart.getPhyTable();
            Map<String, Field> phyTables = targetDbInfo.get(grpKey);
            if (phyTables == null) {
                phyTables = new HashMap<>();
                targetDbInfo.put(grpKey, phyTables);
            }
            phyTables.put(phyTb, null);
        }
        targetDbInfo.forEach((k, v) -> {
            TargetDB targetDB = new TargetDB();
            targetDB.setDbIndex(k);
            targetDB.setTableNames(v);
            targetDB.setLogTblName(result.getLogicalTableName());
            targetDbList.add(targetDB);
        });
        return targetDbList;
    }

//    @Data
//    protected static class TableTopology {
//
//        /**
//         * PartInfo
//         */
//        protected PartitionInfo partInfo;
//
//        /**
//         *
//         */
//        protected List<PhysicalPartitionInfo> partTblPrunedParts = new ArrayList<>();
//
//        /**
//         * Partition Table / Single Table PhyInfo Mappings
//         */
//        protected Map<String, Set<String>> partTblGrpPhyTblListMapping= new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//
//        /**
//         * Broadcast Table (Including Replicas Table) PhyInfo Mappings
//         */
//        protected Map<String, String> broTblGrpPhyTblMappings = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//
//        public TableTopology() {
//        }
//    }
//
//    public static Map<String, List<List<String>>> buildTargetTablesByPartPrunedResults2(List<PartPrunedResult> results) {
//
//        /**
//         * key: grpKey
//         * val:
//         *      List of phyTblList to be join that has different partition idx
//         *          List of phyTbl of the same partition idx of each logTbl
//         */
//        Map<String, List<List<String>>> phyGrpInfoMap = new HashMap<>();
//
//        List<TableTopology> tableTopologyList = new ArrayList<>();
//
//        for (int i = 0; i < results.size(); i++) {
//            TableTopology tblTopology = new TableTopology();
//            PartPrunedResult prunedResult = results.get(i);
//            PartitionInfo partInfo = prunedResult.getPartInfo();
//            tblTopology.setPartInfo(partInfo);
//            if (partInfo.isBroadcastTable() || partInfo.isReplicasTable()) {
//                partInfo.getTopology();
//            }
//
//            List<PhysicalPartitionInfo> prunedParts = prunedResult.getPrunedPartitions();
//        }
//
//
//        return null;
//    }

//    /**
//     * Convert the list of PartPrunedResult to TargetTables
//     */
//    public static Map<String, List<List<String>>> buildTargetTablesByPartPrunedResultsBackup(List<PartPrunedResult> results) {
//
//        /**
//         * key: grpKey
//         * val:
//         *      List of phyTblList to be join that has different partition idx
//         *          List of phyTbl of the same partition idx of each logTbl
//         */
//        Map<String, List<List<String>>> phyGrpInfoMap = new HashMap<>();
//
//        /**
//         * key: grpKey
//         * val:
//         *     partIdxPhyTbListMap:
//         *          key: partIdx
//         *          val: phyTbList that has same phy idx
//         */
//        Map<String, Map<Integer, List<String>>> allPhyInfos = new HashMap<>();
//
//        List<Map<String, Set<String>>> broadcastTopologyList = new ArrayList<>();
//
//        for (int i = 0; i < results.size(); i++) {
//            PartPrunedResult result = results.get(i);
//            if (result.getPrunedPartitions().isEmpty()) {
//                return phyGrpInfoMap;
//            }
//        }
//
//        for (int i = 0; i < results.size(); i++) {
//            PartPrunedResult result = results.get(i);
//            if (result.getPartInfo().isBroadcastTable()) {
//                broadcastTopologyList.add(result.getPartInfo().getTopology());
//                continue;
//            }
//            List<PhysicalPartitionInfo> prunedParts = result.getPrunedPartitions();
//            for (int j = 0; j < prunedParts.size(); j++) {
//                PhysicalPartitionInfo prunedPart = prunedParts.get(j);
//                String grpKey = prunedPart.getGroupKey();
//                String phyTb = prunedPart.getPhyTable();
//                int partBitSetIdx = prunedPart.getPartBitSetIdx();
//
//                // Get phyInfos of one Group
//                Map<Integer, List<String>> phyInfosOfOneGrp = allPhyInfos.get(grpKey);
//                if (phyInfosOfOneGrp == null) {
//                    phyInfosOfOneGrp = new HashMap<>();
//                    allPhyInfos.put(grpKey, phyInfosOfOneGrp);
//                }
//
//                // Get phyInfos that is the same partIdx
//                List<String> phyTbListHasSameIdx = phyInfosOfOneGrp.get(partBitSetIdx);
//                if (phyTbListHasSameIdx == null) {
//                    phyTbListHasSameIdx = new ArrayList<>();
//                    phyInfosOfOneGrp.put(partBitSetIdx, phyTbListHasSameIdx);
//                }
//                for (int k = 0; k < broadcastTopologyList.size(); k++) {
//                    String phyTable = broadcastTopologyList.get(k).get(grpKey).iterator().next();
//                    phyTbListHasSameIdx.add(phyTable);
//                }
//                phyTbListHasSameIdx.add(phyTb);
//            }
//            broadcastTopologyList = new ArrayList<>();
//        }
//
//        if (!broadcastTopologyList.isEmpty()) {
//            if (allPhyInfos.isEmpty()) {
//                // only broadcast
//                List<String> phyInfosOfSameIdx = new ArrayList<>();
//                List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
//                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
//                // TODO: broadcast table random access each group
//                String groupKey = broadcastTopologyList.get(0).keySet().stream().findFirst().get();
//                for (int i = 0; i < broadcastTopologyList.size(); i++) {
//                    String phyTable = broadcastTopologyList.get(i).get(groupKey).iterator().next();
//                    phyInfosOfSameIdx.add(phyTable);
//                }
//                phyGrpInfoMap.put(groupKey, phyInfosOfOneGrp);
//                return phyGrpInfoMap;
//            }
//        }
//        // not only broadcast
//        for (Map.Entry<String, Map<Integer, List<String>>> phyInfosOfOneGrpItem : allPhyInfos.entrySet()) {
//            String grpKey = phyInfosOfOneGrpItem.getKey();
//            Map<Integer, List<String>> phyInfosOfOneGrpMap = phyInfosOfOneGrpItem.getValue();
//            List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
//            for (Map.Entry<Integer, List<String>> phyInfosOfSameIdxItem : phyInfosOfOneGrpMap.entrySet()) {
//                List<String> phyInfosOfSameIdx = phyInfosOfSameIdxItem.getValue();
//                for (int i = 0; i < broadcastTopologyList.size(); i++) {
//                    String phyTable = broadcastTopologyList.get(i).get(grpKey).iterator().next();
//                    phyInfosOfSameIdx.add(phyTable);
//                }
//                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
//            }
//            phyGrpInfoMap.put(grpKey, phyInfosOfOneGrp);
//        }
//        return phyGrpInfoMap;
//    }
//
//    public static Map<String, List<List<String>>> buildTargetTablesByPartPrunedResultsNew(List<PartPrunedResult> results) {
//
////        t1: p1,p2,p3
////        b1: p1,p2,p3,p4
////        r1: p1,p2,p3
////        t2: p1,p2,p3
////        b2: p1,p2,p3,p4
////        r2: p1,p2,p3
////
////            ==>
////
////        t1: p1,p2,p3
////          g1: t1_p1
////              t1_p3
////          g2: t1_p2
////
////        b1: p1,p2,p3,p4
////            g1:b1_p1,
////            g2:b1_p2,
////            g3:b1_p3,
////            g4:b1_p4
////
////        r1: p1,p2,p3
////           g1: r1_p1,
////           g2: r1_p2,
////           g3: r1_p3
////
////        t2: p1,p2,p3
////          g1: t2_p1
////              t2_p3
////          g2: t2_p2
////
////
////        b2: p1,p2,p3,p4
////            g1:b2_p1,
////            g2:b2_p2,
////            g3:b2_p3,
////            g4:b2_p4
////
////
////        r2: p1,p2,p3
////        g1: r2_p1,
////            g2:r2_p2,
////            g3:r2_p3
////
////
////            ==>
////
////        g1:
////        t1_p1,b1,r1,t2_p1,b2,r2
////        t1_p3,b1,r1,t2_p3,b2,r2
////
////        g2:
////        t1_p2,b1,r1,t2_p2,b2,r2
//
//        /**
//         * key: grpKey
//         * val:
//         *      List of phyTblList to be join that has different partition idx
//         *          List of phyTbl of the same partition idx of each logTbl
//         */
//        Map<String, List<List<String>>> phyGrpInfoMap = new HashMap<>();
//
//        /**
//         * key: grpKey
//         * val:
//         *     partIdxPhyTbListMap:
//         *          key: partIdx
//         *          val: phyTbList that has same phy idx
//         */
//        Map<String, Map<Integer, List<String>>> allPhyInfos = new HashMap<>();
//
//        /**
//         * List
//         *    item of list:
//         *      the topology of the ith-table of broadcast/replicas/single
//         *          since last partitioned table in join-sql
//         *
//         */
//        List<Map<String, Set<String>>> autoBroadcastTableTopologyList = new ArrayList<>();
//
//        /**
//         * index list of broadcast tables topology in auto_broadcast_tbl_list
//         */
//        List<Integer> broadcastTableTopologyIndexList = new ArrayList<>();
//        /**
//         * index list of replicas tables topology in auto_broadcast_tbl_list
//         */
//        List<Integer> replicasTableTopologyIndexList = new ArrayList<>();
//        /**
//         * index list of single tables topology in auto_broadcast_tbl_list
//         */
//        List<Integer> singleTableTopologyIndexList = new ArrayList<>();
//
//        for (int i = 0; i < results.size(); i++) {
//            PartPrunedResult result = results.get(i);
//            if (result.getPrunedPartitions().isEmpty()) {
//                return phyGrpInfoMap;
//            }
//        }
//
//        for (int i = 0; i < results.size(); i++) {
//            PartPrunedResult result = results.get(i);
//            if (result.getPartInfo().isBroadcastTable()) {
//                /**
//                 * Here save topologies of each bro_tbl temporality
//                 * which are between two part_tables
//                 */
//                autoBroadcastTableTopologyList.add(result.getPartInfo().getTopology());
//                broadcastTableTopologyIndexList.add(autoBroadcastTableTopologyList.size() - 1);
//                continue;
//            }
//            if (result.getPartInfo().isReplicasTable()) {
//                /**
//                 * Here save topologies of each bro_tbl temporality
//                 * which are between two part_tables
//                 */
//                autoBroadcastTableTopologyList.add(result.getPartInfo().getTopology());
//                replicasTableTopologyIndexList.add(autoBroadcastTableTopologyList.size() - 1);
//                continue;
//            }
//            if (result.getPartInfo().isSingleTable()) {
//                /**
//                 * Here save topologies of each bro_tbl temporality
//                 * which are between two part_tables
//                 */
//                autoBroadcastTableTopologyList.add(result.getPartInfo().getTopology());
//                singleTableTopologyIndexList.add(autoBroadcastTableTopologyList.size() - 1);
//                continue;
//            }
//            boolean isSingleTbl = result.getPartInfo().isSingleTable();
//            boolean isPartTbl = result.getPartInfo().isPartitionedTable();
//            List<PhysicalPartitionInfo> prunedParts = result.getPrunedPartitions();
//            for (int j = 0; j < prunedParts.size(); j++) {
//                PhysicalPartitionInfo prunedPart = prunedParts.get(j);
//                String grpKey = prunedPart.getGroupKey();
//                String phyTb = prunedPart.getPhyTable();
//
//
//                // Get phyInfos of one Group
//                Map<Integer, List<String>> phyInfosOfOneGrp = allPhyInfos.get(grpKey);
//                if (phyInfosOfOneGrp == null) {
//                    phyInfosOfOneGrp = new HashMap<>();
//                    allPhyInfos.put(grpKey, phyInfosOfOneGrp);
//                }
//
//                int partBitSetIdx = prunedPart.getPartBitSetIdx();
//
//                // Get phyInfos that is the same partIdx
//                List<String> phyTbListHasSameIdx = phyInfosOfOneGrp.get(partBitSetIdx);
//                if (phyTbListHasSameIdx == null) {
//                    phyTbListHasSameIdx = new ArrayList<>();
//                    phyInfosOfOneGrp.put(partBitSetIdx, phyTbListHasSameIdx);
//                }
//                /**
//                 * By using the topologies of each bro_tbl above
//                 * which are between two part_tables,
//                 * first put them into the result of allPhyInfos,
//                 * and so keep the order of phytables of each group are the same as log tables in join
//                 */
//                if (!autoBroadcastTableTopologyList.isEmpty()) {
//                    for (int k = 0; k < autoBroadcastTableTopologyList.size(); k++) {
//                        String phyTable = autoBroadcastTableTopologyList.get(k).get(grpKey).iterator().next();
//                        phyTbListHasSameIdx.add(phyTable);
//                    }
//                }
//                phyTbListHasSameIdx.add(phyTb);
//            }
//            /**
//             * For each part, clear all topologies above and prepare next time
//             */
//            autoBroadcastTableTopologyList = new ArrayList<>();
//            broadcastTableTopologyIndexList = new ArrayList<>();
//            replicasTableTopologyIndexList = new ArrayList<>();
//            singleTableTopologyIndexList = new ArrayList<>();
//        }
//
//        boolean containBroadcastTables = !broadcastTableTopologyIndexList.isEmpty();
//        boolean containReplicasTables = !replicasTableTopologyIndexList.isEmpty();
//        boolean containSingleTables = !singleTableTopologyIndexList.isEmpty();
//
//        if (!autoBroadcastTableTopologyList.isEmpty()) {
//
//            /**
//             * Maybe bro_tbl is the last table of one join sql or bro_tbl is no join,
//             * so here need check if broadcastTopologyList is empty and add it into
//             * the result of allPhyInfos
//             */
//
//            if (allPhyInfos.isEmpty()) {
//
//                String targetGrpKey = null;
//                List<String> phyInfosOfSameIdx = new ArrayList<>();
//                List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
//                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
//
//
//                if (containSingleTables) {
//                    /**
//                     * Find at least one single tables, so use the group key of the single first
//                     */
//                    int firstSingleIndex = singleTableTopologyIndexList.get(0);
//                    targetGrpKey = autoBroadcastTableTopologyList.get(firstSingleIndex).keySet().stream().findFirst().get();
//                } else if (containReplicasTables) {
//                    /**
//                     * No found single table,
//                     * but found at least one replicas tables, so use the group key of the replicas first
//                     */
//                    int firstReplicasIndex = replicasTableTopologyIndexList.get(0);
//                    targetGrpKey = autoBroadcastTableTopologyList.get(firstReplicasIndex).keySet().stream().findFirst().get();
//                } else {
//                    /**
//                     * No found single tables and replicas tables,
//                     * Here process the only broadcast, so use the group key of the broadcast first
//                     *
//                     */
//                    int firstBroadcastIndex = broadcastTableTopologyIndexList.get(0);
//                    targetGrpKey = autoBroadcastTableTopologyList.get(firstBroadcastIndex).keySet().stream().findFirst().get();
//                }
//                for (int i = 0; i < autoBroadcastTableTopologyList.size(); i++) {
//                    String phyTable = autoBroadcastTableTopologyList.get(i).get(targetGrpKey).iterator().next();
//                    phyInfosOfSameIdx.add(phyTable);
//                }
//                phyGrpInfoMap.put(targetGrpKey, phyInfosOfOneGrp);
//
////                /**
////                 * Here process the only broadcast
////                 */
////                // only broadcast
////                List<String> phyInfosOfSameIdx = new ArrayList<>();
////                List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
////                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
////                // TODO: broadcast table random access each group
////                String groupKey = autoBroadcastTableTopologyList.get(0).keySet().stream().findFirst().get();
////                for (int i = 0; i < autoBroadcastTableTopologyList.size(); i++) {
////                    String phyTable = autoBroadcastTableTopologyList.get(i).get(groupKey).iterator().next();
////                    phyInfosOfSameIdx.add(phyTable);
////                }
////                phyGrpInfoMap.put(groupKey, phyInfosOfOneGrp);
//
//                return phyGrpInfoMap;
//            }
//        }
//
//        /**
//         * Here process the not-only broadcast, broadcast is the last table of join sql
//         */
//
//        // not only broadcast
//        for (Map.Entry<String, Map<Integer, List<String>>> phyInfosOfOneGrpItem : allPhyInfos.entrySet()) {
//            String grpKey = phyInfosOfOneGrpItem.getKey();
//            Map<Integer, List<String>> phyInfosOfOneGrpMap = phyInfosOfOneGrpItem.getValue();
//            List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
//            for (Map.Entry<Integer, List<String>> phyInfosOfSameIdxItem : phyInfosOfOneGrpMap.entrySet()) {
//                List<String> phyInfosOfSameIdx = phyInfosOfSameIdxItem.getValue();
//                for (int i = 0; i < autoBroadcastTableTopologyList.size(); i++) {
//                    String phyTable = autoBroadcastTableTopologyList.get(i).get(grpKey).iterator().next();
//                    phyInfosOfSameIdx.add(phyTable);
//                }
//                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
//            }
//            phyGrpInfoMap.put(grpKey, phyInfosOfOneGrp);
//        }
//        return phyGrpInfoMap;
//    }

    /**
     * Convert the list of PartPrunedResult to TargetTables
     */
    public static Map<String, List<List<String>>> buildTargetTablesByPartPrunedResults(List<PartPrunedResult> results,
                                                                                       ExecutionContext ec) {
        TargetTableInfo targetTableInfo = buildTargetTableInfoByPartPrunedResults(results, ec, true);
        return targetTableInfo.getTargetTables();
    }

    /**
     * Convert the list of PartPrunedResult to TargetTableInfo which can be targetDbs
     */
    public static TargetTableInfo buildTargetTableInfoByPartPrunedResults(List<PartPrunedResult> prunedResults,
                                                                          ExecutionContext executionContext,
                                                                          boolean ignoreBuildTargetTableInfo) {
        boolean ignoreTopologyInvalidInfo = false;
        if (executionContext != null) {
            ignoreTopologyInvalidInfo = executionContext.getParamManager()
                .getBoolean(ConnectionParams.IGNORE_INVALID_TOPOLOGY_IN_POST_PLANNER);
        }

        DirectPlanCommonGroupInfo directPlanCommonGroupInfo = new DirectPlanCommonGroupInfo();
        boolean allInOneGroup = PartitionPrunerUtils.calcCommonGroupKeySetFromAllPrunedResults(
            prunedResults, executionContext, directPlanCommonGroupInfo);
        Set<String> commonGroupKeyOutputOfAllPruningResults = directPlanCommonGroupInfo.getCommonGroupKeySet();
        TargetTableInfo targetTableInfo = new TargetTableInfo();

        /**
         * key: grpKey
         * val:
         *      List of phyTblList to be join that has different partition idx
         *          List of phyTbl of the same partition idx of each logTbl
         */
        Map<String, List<List<String>>> phyGrpInfoMap = new HashMap<>();
        List<TargetTableInfoOneTable> targetTableInfoOneTableList = new ArrayList<>();
        targetTableInfo.setTargetTables(phyGrpInfoMap);
        targetTableInfo.setTargetTableInfoList(targetTableInfoOneTableList);

        /**
         * key: grpKey
         * val:
         *     partIdxPhyTbListMap:
         *          key: partIdx
         *          val: phyTbList that has same phy idx
         */
        Map<String, Map<Integer, List<String>>> allPhyInfos = new HashMap<>();

        /**
         * Temp List of tables which are need auto broadcast into each partitions
         *    item of list:
         *      the topology of the ith-table of broadcast/replicas/single
         *          since last partitioned table in join-sql
         *
         */
        List<Map<String, Set<String>>> autoBroadcastTableTopologyList = new ArrayList<>();

        /**
         * index list of broadcast tables topology in auto_broadcast_tbl_list
         */
        List<Integer> broadcastTableTopologyIndexList = new ArrayList<>();
        /**
         * index list of replicas tables topology in auto_broadcast_tbl_list
         */
        List<Integer> replicasTableTopologyIndexList = new ArrayList<>();
        /**
         * index list of single tables topology in auto_broadcast_tbl_list
         */
        List<Integer> singleTableTopologyIndexList = new ArrayList<>();

        /**
         * key: partName
         * Val: List of subPartNames
         */
        Map<String, List<String>> part2SubPartListMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        for (int i = 0; i < prunedResults.size(); i++) {
            PartPrunedResult result = prunedResults.get(i);
            PartitionInfo partInfo = result.getPartInfo();
            if (result.getPrunedPartitions().isEmpty()) {
                if (!ignoreBuildTargetTableInfo) {
                    TargetTableInfoOneTable targetTableInfoOneTable = new TargetTableInfoOneTable();
                    targetTableInfoOneTable.setPartInfo(partInfo);
                    targetTableInfoOneTable.setUseSubPart(partInfo.getPartitionBy().getSubPartitionBy() != null);
                    targetTableInfoOneTable.setAllPartSorted(false);
                    targetTableInfoOneTable.setAllSubPartSorted(false);
                    targetTableInfoOneTable.setAllPrunedPartContainOnlyOneSubPart(false);
                    targetTableInfoOneTableList.add(targetTableInfoOneTable);
                }
                return targetTableInfo;
            }
        }

        boolean firstFoundPartTable = true;
        PartPruneStepPruningExtraInfo pruningExtraInfo = null;
        for (int i = 0; i < prunedResults.size(); i++) {
            PartPrunedResult result = prunedResults.get(i);
            PartitionInfo partInfo = result.getPartInfo();

            Set<String> parentPartNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            TargetTableInfoOneTable targetTableInfoOneTable = null;
            if (!ignoreBuildTargetTableInfo) {
                targetTableInfoOneTable = new TargetTableInfoOneTable();
                targetTableInfoOneTable.setPartInfo(partInfo);
                targetTableInfoOneTableList.add(targetTableInfoOneTable);
            }

            if (partInfo.isBroadcastTable()) {
                autoBroadcastTableTopologyList.add(result.getPartInfo().getTopology(ignoreTopologyInvalidInfo));
                broadcastTableTopologyIndexList.add(autoBroadcastTableTopologyList.size() - 1);
                continue;
            }
            if (partInfo.isReplicasTable()) {
                Map<String, Set<String>> fullGrpToPhyTbSetMapOfReplicasTbl =
                    result.getPartInfo().getTopology(ignoreTopologyInvalidInfo);
                PartPruneStepPruningExtraInfo extraInfo = result.getPruningExtraInfo();
                if (pruningExtraInfo == null) {
                    pruningExtraInfo = extraInfo;
                }
                if (pruningExtraInfo != null) {
                    /**
                     * If found the pruningExtraInfo, that means replicas table only scan the parts on the common groupKeySet
                     */
                    Map<String, Set<String>> newGrpToPhyTbSetMap =
                        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                    Set<String> commonGroupKeySet = pruningExtraInfo.getCommonGroupKeyInfo().getCommonGroupKeySet();
                    for (Map.Entry<String, Set<String>> grpAndTbSetItem : fullGrpToPhyTbSetMapOfReplicasTbl.entrySet()) {
                        String grpKey = grpAndTbSetItem.getKey();
                        if (commonGroupKeySet.contains(grpKey)) {
                            newGrpToPhyTbSetMap.put(grpKey, grpAndTbSetItem.getValue());
                        }
                    }
                    autoBroadcastTableTopologyList.add(newGrpToPhyTbSetMap);
                } else {
                    /**
                     * If no found the pruningExtraInfo,
                     *  that means replicas table scan the parts of its topology
                     */
                    autoBroadcastTableTopologyList.add(fullGrpToPhyTbSetMapOfReplicasTbl);
                }
                replicasTableTopologyIndexList.add(autoBroadcastTableTopologyList.size() - 1);
                continue;
            }
            if (partInfo.isSingleTable()) {
                autoBroadcastTableTopologyList.add(result.getPartInfo().getTopology(ignoreTopologyInvalidInfo));
                singleTableTopologyIndexList.add(autoBroadcastTableTopologyList.size() - 1);
                continue;
            }

            List<PhysicalPartitionInfo> prunedParts = result.getPrunedPartitions();
            PartKeyLevel phyPartLevel = null;
            for (int j = 0; j < prunedParts.size(); j++) {
                PhysicalPartitionInfo prunedPart = prunedParts.get(j);
                String partName = prunedPart.getPartName();
                if (phyPartLevel == null) {
                    phyPartLevel = prunedPart.getPartLevel();
                }

                String grpKey = prunedPart.getGroupKey();
                String phyTb = prunedPart.getPhyTable();
                int partBitSetIdx = prunedPart.getPartBitSetIdx();

                if (!ignoreBuildTargetTableInfo) {
                    String parentPartName = prunedPart.getParentPartName();
                    if (!StringUtils.isEmpty(parentPartName)) {
                        /**
                         * When parentPartName is NOT empty, the prunedPart must be a subpart
                         */
                        if (!parentPartNameSet.contains(parentPartName)) {
                            parentPartNameSet.add(parentPartName);
                        }

                        List<String> subPartNames = part2SubPartListMapping.get(parentPartName);
                        if (subPartNames == null) {
                            subPartNames = new ArrayList<>();
                            part2SubPartListMapping.put(parentPartName, subPartNames);
                        }
                        subPartNames.add(partName);
                    }
                }

                // Get phyInfos of one Group
                Map<Integer, List<String>> phyInfosOfOneGrp = allPhyInfos.get(grpKey);
                if (phyInfosOfOneGrp == null) {
                    if (firstFoundPartTable) {
                        phyInfosOfOneGrp = new HashMap<>();
                        allPhyInfos.put(grpKey, phyInfosOfOneGrp);
                    } else {
                        /**
                         * Do not have the same group, something is wrong.
                         */
                        targetTableInfo.setTargetTables(MapUtils.EMPTY_MAP);
                        return targetTableInfo;
                    }
                }

                // Get phyInfos that is the same partIdx
                List<String> phyTbListHasSameIdx = phyInfosOfOneGrp.get(partBitSetIdx);
                if (phyTbListHasSameIdx == null) {
                    phyTbListHasSameIdx = new ArrayList<>();
                    phyInfosOfOneGrp.put(partBitSetIdx, phyTbListHasSameIdx);
                }
                for (int k = 0; k < autoBroadcastTableTopologyList.size(); k++) {
                    Set<String> phyTbSetOfGrpOfLogTbl = autoBroadcastTableTopologyList.get(k).get(grpKey);
                    if (phyTbSetOfGrpOfLogTbl == null || phyTbSetOfGrpOfLogTbl.isEmpty()) {
                        /**
                         * Do not have the same group, something is wrong.
                         */
                        targetTableInfo.setTargetTables(MapUtils.EMPTY_MAP);
                        return targetTableInfo;
                    }
                    String phyTable = phyTbSetOfGrpOfLogTbl.iterator().next();
                    phyTbListHasSameIdx.add(phyTable);
                }
                phyTbListHasSameIdx.add(phyTb);
            }
            firstFoundPartTable = false;
            autoBroadcastTableTopologyList = new ArrayList<>();
            broadcastTableTopologyIndexList = new ArrayList<>();
            replicasTableTopologyIndexList = new ArrayList<>();
            singleTableTopologyIndexList = new ArrayList<>();

            if (!ignoreBuildTargetTableInfo && targetTableInfoOneTable != null) {
                targetTableInfoOneTable.setPrunedFirstLevelPartCount(parentPartNameSet.size());
                if (phyPartLevel == PartKeyLevel.SUBPARTITION_KEY) {
                    targetTableInfoOneTable.setUseSubPart(true);
                    if (targetTableInfoOneTable.getPrunedFirstLevelPartCount() == 1) {
                        if (checkPartitionsSortedByPartitionColumns(partInfo, PartKeyLevel.SUBPARTITION_KEY, null)) {
                            targetTableInfoOneTable.setAllPartSorted(false);
                            targetTableInfoOneTable.setPartColList(new ArrayList<>());
                            targetTableInfoOneTable.setAllSubPartSorted(true);
                            targetTableInfoOneTable.setSubpartColList(
                                partInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList());
                        }
                    } else if (targetTableInfoOneTable.getPrunedFirstLevelPartCount() > 1) {
                        if (checkPartitionsSortedByPartitionColumns(partInfo, PartKeyLevel.PARTITION_KEY, null)) {
                            targetTableInfoOneTable.setAllPartSorted(true);
                            targetTableInfoOneTable.setPartColList(
                                partInfo.getPartitionBy().getPartitionColumnNameList());
                            targetTableInfoOneTable.setAllSubPartSorted(false);
                            targetTableInfoOneTable.setSubpartColList(new ArrayList<>());

                            if (partInfo.getPartitionBy().getSubPartitionBy() != null) {
                                // use subpart
                                boolean allPartContainOnlyOneSubPart = true;
                                for (Map.Entry<String, List<String>> part2SubPartListItem : part2SubPartListMapping.entrySet()) {
                                    List<String> subPartNames = part2SubPartListItem.getValue();
                                    if (subPartNames != null && subPartNames.size() > 1) {
                                        allPartContainOnlyOneSubPart = false;
                                        break;
                                    }
                                }
                                targetTableInfoOneTable.setAllPrunedPartContainOnlyOneSubPart(
                                    allPartContainOnlyOneSubPart);
                            } else {
                                // no use subpart
                                targetTableInfoOneTable.setAllPrunedPartContainOnlyOneSubPart(false);
                            }
                        }
                    }
                } else if (phyPartLevel == PartKeyLevel.PARTITION_KEY) {
                    if (checkPartitionsSortedByPartitionColumns(partInfo, PartKeyLevel.PARTITION_KEY, null)) {
                        targetTableInfoOneTable.setAllPartSorted(true);
                        targetTableInfoOneTable.setPartColList(partInfo.getPartitionBy().getPartitionColumnNameList());
                    }
                    targetTableInfoOneTable.setAllSubPartSorted(false);
                    targetTableInfoOneTable.setSubpartColList(new ArrayList<>());
                }
            }

        }

        boolean containReplicasTables = !replicasTableTopologyIndexList.isEmpty();
        boolean containSingleTables = !singleTableTopologyIndexList.isEmpty();

        if (!autoBroadcastTableTopologyList.isEmpty()) {
            if (allPhyInfos.isEmpty()) {

                String targetGrpKey = null;
                List<String> phyInfosOfSameIdx = new ArrayList<>();
                List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
                phyInfosOfOneGrp.add(phyInfosOfSameIdx);

                int targetTblIndexInAutoBroadcastTblList = -1;
                if (containSingleTables) {
                    /**
                     * Find at least one single tables, so use the group key of the single first
                     */
                    int firstSingleIndex = singleTableTopologyIndexList.get(0);
                    targetTblIndexInAutoBroadcastTblList = firstSingleIndex;
                } else if (containReplicasTables) {
                    /**
                     * No found single table,
                     * but found at least one replicas tables, so use the group key of the replicas first
                     */
                    int firstReplicasIndex = replicasTableTopologyIndexList.get(0);
                    targetTblIndexInAutoBroadcastTblList = firstReplicasIndex;
                } else {
                    /**
                     * No found single tables and replicas tables,
                     * Here process the only broadcast, so use the group key of the broadcast first
                     *
                     */
                    int firstBroadcastIndex = broadcastTableTopologyIndexList.get(0);
                    targetTblIndexInAutoBroadcastTblList = firstBroadcastIndex;
                }
                targetGrpKey =
                    autoBroadcastTableTopologyList.get(targetTblIndexInAutoBroadcastTblList).keySet().stream()
                        .findFirst().get();

                if (!containSingleTables && containReplicasTables) {
                    if (pruningExtraInfo != null) {
                        String randomReadTargetGroupKey = pruningExtraInfo.getRandomReadTargetGroupKey();
                        if (!StringUtils.isEmpty(randomReadTargetGroupKey)) {
                            targetGrpKey = randomReadTargetGroupKey;
                        }
                    } else {
                        if (allInOneGroup) {
                            targetGrpKey = commonGroupKeyOutputOfAllPruningResults.iterator().next();
                        }
                    }
                }

                for (int i = 0; i < autoBroadcastTableTopologyList.size(); i++) {
                    Set<String> phyTbSetOfGrpOfLogTbl = autoBroadcastTableTopologyList.get(i).get(targetGrpKey);
                    if (phyTbSetOfGrpOfLogTbl == null || phyTbSetOfGrpOfLogTbl.isEmpty()) {
                        /**
                         * Do not have the same group, something is wrong.
                         */
                        targetTableInfo.setTargetTables(MapUtils.EMPTY_MAP);
                        return targetTableInfo;
                    }
                    String phyTable = phyTbSetOfGrpOfLogTbl.iterator().next();
                    phyInfosOfSameIdx.add(phyTable);
                }
                phyGrpInfoMap.put(targetGrpKey, phyInfosOfOneGrp);

//                // only broadcast
//                List<String> phyInfosOfSameIdx = new ArrayList<>();
//                List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
//                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
//                // TODO: broadcast table random access each group
//                String groupKey = autoBroadcastTopologyList.get(0).keySet().stream().findFirst().get();
//                for (int i = 0; i < autoBroadcastTopologyList.size(); i++) {
//                    String phyTable = autoBroadcastTopologyList.get(i).get(groupKey).iterator().next();
//                    phyInfosOfSameIdx.add(phyTable);
//                }
//                phyGrpInfoMap.put(groupKey, phyInfosOfOneGrp);

                return targetTableInfo;
            }
        }

        // not only broadcast
        for (Map.Entry<String, Map<Integer, List<String>>> phyInfosOfOneGrpItem : allPhyInfos.entrySet()) {
            String grpKey = phyInfosOfOneGrpItem.getKey();
            Map<Integer, List<String>> phyInfosOfOneGrpMap = phyInfosOfOneGrpItem.getValue();
            List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
            for (Map.Entry<Integer, List<String>> phyInfosOfSameIdxItem : phyInfosOfOneGrpMap.entrySet()) {
                List<String> phyInfosOfSameIdx = phyInfosOfSameIdxItem.getValue();
                for (int i = 0; i < autoBroadcastTableTopologyList.size(); i++) {
                    Set<String> phyTbSetOfGrpOfLogTbl = autoBroadcastTableTopologyList.get(i).get(grpKey);
                    if (phyTbSetOfGrpOfLogTbl == null || phyTbSetOfGrpOfLogTbl.isEmpty()) {
                        /**
                         * Do not have the same group, something is wrong.
                         */
                        targetTableInfo.setTargetTables(MapUtils.EMPTY_MAP);
                        return targetTableInfo;
                    }
                    String phyTable = phyTbSetOfGrpOfLogTbl.iterator().next();
                    phyInfosOfSameIdx.add(phyTable);
                }
                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
            }
            phyGrpInfoMap.put(grpKey, phyInfosOfOneGrp);
        }

        return targetTableInfo;
    }

    /**
     * Check if a partitions of partLevel are sorted on partCols
     * Notice:
     * if partKeyLevel == PartKeyLevel.SUBPARTITION_KEY,
     * return true means the subpartitions are sorted within only one partition
     */
    public static boolean checkPartitionsSortedByPartitionColumns(PartitionInfo partInfo,
                                                                  PartKeyLevel partKeyLevel,
                                                                  List<String> partColsOutput) {
        PartitionByDefinition partBy = partInfo.getPartitionBy();
        if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
            if (partBy.getSubPartitionBy() == null) {
                return false;
            }
            partBy = partInfo.getPartitionBy().getSubPartitionBy();
        }
        PartitionStrategy strategy = partBy.getStrategy();
        if (!strategy.isStrategyWithOrder()) {
            return false;
        }

        if (strategy == PartitionStrategy.RANGE) {
            PartitionIntFunction partFn = partBy.getPartIntFunc();
            if (partFn != null) {
                DataType partColDataType = partBy.getPartitionColumnTypeList().get(0);
                if (partFn.getMonotonicity(partColDataType) == Monotonicity.NON_MONOTONIC) {
                    return false;
                }
            }
        }
        if (partColsOutput != null) {
            partColsOutput.addAll(partBy.getPartitionColumnNameList());
        }
        return true;

    }

//    /*======= Methods for get PartitionIntFunction ========*/
//    public static PartitionIntFunction getPartitionIntFunction(SqlOperator sqlOperator,
//                                                               PartKeyLevel partLevel,
//                                                               PartitionInfo partInfo) {
//        PartitionByDefinition targetPartBy = partInfo.getPartitionBy();
//        if (partLevel == PartKeyLevel.SUBPARTITION_KEY
//            && partInfo.getPartitionBy().getSubPartitionBy() != null) {
//            targetPartBy = partInfo.getPartitionBy().getSubPartitionBy();
//        }
//        return targetPartBy.getPartIntFunc();
//    }

    /*======== Methods for covert to TargetDB from PartPrunedResult ========*/

    /**
     * Convert the list of topologyInfo to TargetDB， used by cdc only
     */
    public static List<TargetDB> buildTargetDbsByTopologyInfos(String logTbl,
                                                               Map<String, List<PhysicalPartitionInfo>> topologyInfo) {

        List<TargetDB> targetDbList = new ArrayList<>();
        Map<String, Map<String, Field>> targetDbInfo = new HashMap<>();

        final List<PhysicalPartitionInfo> allPhyPartInfo = new ArrayList<>();
        topologyInfo.entrySet().stream().forEach(e -> allPhyPartInfo.addAll(e.getValue()));
        for (int j = 0; j < allPhyPartInfo.size(); j++) {
            PhysicalPartitionInfo prunedPart = allPhyPartInfo.get(j);

            String grpKey = prunedPart.getGroupKey();
            String phyTb = prunedPart.getPhyTable();
            Map<String, Field> phyTables = targetDbInfo.get(grpKey);
            if (phyTables == null) {
                phyTables = new HashMap<>();
                targetDbInfo.put(grpKey, phyTables);
            }
            phyTables.put(phyTb, null);
        }
        targetDbInfo.forEach((k, v) -> {
            TargetDB targetDB = new TargetDB();
            targetDB.setDbIndex(k);
            targetDB.setTableNames(v);
            targetDB.setLogTblName(logTbl);
            targetDbList.add(targetDB);
        });
        return targetDbList;
    }

    /*======== Methods for building partition bitset ========*/

    public static BitSet buildPhyPartsBitSetByPhyPartPostSet(PartitionInfo partInfo, Set<Integer> postSet) {
        BitSet partBitSet = buildEmptyPhysicalPartitionsBitSet(partInfo);
        setPartBitSetForPartList(partBitSet, postSet, true);
        return partBitSet;
    }

    public static BitSet buildEmptyPartitionsBitSetByPartRouter(PartitionRouter router) {
        int allPartCount = router.getPartitionCount();
        BitSet allPartBitSet = new BitSet(allPartCount);
        return allPartBitSet;
    }

    public static BitSet buildFullScanPartitionsBitSetByPartRouter(PartitionRouter router) {
        int allPartCount = router.getPartitionCount();
        BitSet allPartBitSet = new BitSet(allPartCount);
        allPartBitSet.set(0, allPartCount, true);
        return allPartBitSet;
    }

    public static BitSet buildEmptyPhysicalPartitionsBitSet(PartitionInfo partInfo) {
        int allPartCount = partInfo.getPartitionBy().getPhysicalPartitions().size();
        BitSet partBitSet = new BitSet(allPartCount);
        return partBitSet;
    }

    public static BitSet buildFullPhysicalPartitionsBitSet(PartitionInfo partInfo) {
        int allPartCount = partInfo.getPartitionBy().getPhysicalPartitions().size();
        BitSet allPhyPartBitSet = new BitSet(allPartCount);
        allPhyPartBitSet.set(0, allPartCount, true);
        return allPhyPartBitSet;
    }

    /**
     * Set Partition BitSet
     * <pre>
     *
     * some def:
     *      ps: partition start position
     *      pe: partition end position
     *      sps: subpartition template start position of all partition
     *      spe: subpartition template end position of all partition
     *      spCnt: the subparition count of each patition
     *
     *  part-only:
     *      input:(start from 1)
     *          ps, pe
     *      BitSet Input: (start from 0)
     *          set ps-1, pe-1
     *
     *
     * part with sub-part:
     *      input:(start from 1)
     *          ps, pe
     *      BitSet Input: (start from 0)
     *          set (ps-1)*spCnt+0, (pe-1)*spCnt+(spCnt-1)
     *
     * sub-part:
     *      input:(start from 1)
     *          sps, spe
     *      BitSet Input: (start from 0)
     *          for each part p
     *              set (p-1)*spCnt+(sps-1), (p-1)*spCnt+(spe-1)
     *
     * sub-part with part:
     *      input:(start from 1)
     *          p, sps, spe
     *      BitSet Input: (start from 0)
     *          for each part p
     *              set (p-1)*spCnt+(sps-1), (p-1)*spCnt+(spe-1)
     *
     * </pre>
     */
    protected static BitSet setPartBitSetByStartEnd(BitSet partBitSet,
                                                    Integer startPartPosi,
                                                    Integer endPartPosi,
                                                    PartKeyLevel level,
                                                    Integer partCnt,
                                                    Integer subPartCntEachPart,
                                                    boolean bitSetVal) {

        int subPartCnt = subPartCntEachPart;

        int bitSetStart = 0;
        int bitSetEnd = 0;
        if (startPartPosi.equals(PartitionRouter.RouterResult.NO_FOUND_PARTITION_IDX) || endPartPosi
            .equals(PartitionRouter.RouterResult.NO_FOUND_PARTITION_IDX)) {
            return partBitSet;
        }
        if (level == PartKeyLevel.PARTITION_KEY) {
            if (subPartCnt <= 0) {
                bitSetStart = startPartPosi - 1;
                bitSetEnd = endPartPosi - 1;
            } else {
                bitSetStart = (startPartPosi - 1) * subPartCnt;
                bitSetEnd = (endPartPosi - 1) * subPartCnt + (subPartCnt - 1);
            }

            partBitSet.set(bitSetStart, bitSetEnd + 1, bitSetVal);
        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {

            for (int i = 1; i <= partCnt; i++) {
                bitSetStart = (i - 1) * subPartCnt + (startPartPosi - 1);
                bitSetEnd = (i - 1) * subPartCnt + (endPartPosi - 1);
                partBitSet.set(bitSetStart, bitSetEnd + 1, bitSetVal);
            }
        }

        return partBitSet;
    }

    public static BitSet setPartBitSetByStartEnd(BitSet partBitSet,
                                                 Integer startPartPosi,
                                                 Integer endPartPosi,
                                                 boolean bitSetVal) {

        int bitSetStart = 0;
        int bitSetEnd = 0;
        if (startPartPosi.equals(PartitionRouter.RouterResult.NO_FOUND_PARTITION_IDX) || endPartPosi
            .equals(PartitionRouter.RouterResult.NO_FOUND_PARTITION_IDX)) {
            return partBitSet;
        }
        bitSetStart = startPartPosi - 1;
        bitSetEnd = endPartPosi - 1;
        partBitSet.set(bitSetStart, bitSetEnd + 1, bitSetVal);
        return partBitSet;
    }

    /**
     * //--------------
     * <pre>
     * List:
     * part-only:
     *      input:(start from 1)
     *          list pl of part posi
     *      BitSet Input: (start from 0)
     *          for each element pp of pl
     *              set pp-1
     * part with sub-part:
     *      input:(start from 1)
     *          list pl of part posi
     *      BitSet Input: (start from 0)
     *          for each element p of pl
     *              set (p-1)*spCnt, (p-1)*spCnt+(spCnt-1)
     * sub-part:
     *      input:(start from 1)
     *          list spl of sub-part posi
     *      BitSet Input: (start from 0)
     *          for each part p
     *              for each element sp of spl
     *                  set (p-1)*spCnt+(sp-1)
     * sub-part with part:
     *      input:(start from 1)
     *          part posi p, list spl of sub-part posi
     *      BitSet Input: (start from 0)
     *          for each element sp of spl
     *              set (p-1)*spCnt+(sp-1)
     * </pre>
     */
    protected static BitSet setPartBitSetForPartList(BitSet partBitSet,
                                                     Set<Integer> partPostSet,
                                                     PartKeyLevel level,
                                                     Integer partCnt,
                                                     Integer subPartCntEachPart,
                                                     boolean bitSetVal) {

        int subPartCnt = subPartCntEachPart;
        int bitSetStart = 0;
        int bitSetEnd = 0;
        if (level == PartKeyLevel.PARTITION_KEY) {

            for (Integer posi : partPostSet) {
                if (subPartCnt > 0) {
                    bitSetStart = (posi - 1) * subPartCnt;
                    bitSetEnd = bitSetStart + (subPartCnt - 1);
                    partBitSet.set(bitSetStart, bitSetEnd, bitSetVal);
                } else {
                    partBitSet.set(posi - 1, bitSetVal);
                }
            }

        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {
            for (int k = 1; k <= partCnt; k++) {
                for (Integer posi : partPostSet) {
                    bitSetStart = (k - 1) * subPartCnt + posi;
                    partBitSet.set(bitSetStart, bitSetVal);
                }
            }
        }
        return partBitSet;
    }

    public static BitSet setPartBitSetForPartList(BitSet partBitSet,
                                                  Set<Integer> partPostSet,
                                                  boolean bitSetVal) {
        for (Integer posi : partPostSet) {
            partBitSet.set(posi - 1, bitSetVal);
        }
        return partBitSet;
    }

    public static Map<String, List<String>> getPartNameInfosFromBitSet(PartitionInfo partInfo, BitSet partBitSet) {

        Map<String, List<String>> partNameInfo = new HashMap<>();
        boolean hasSubPart = partInfo.containSubPartitions();
        List<PartitionSpec> partitions = partInfo.getPartitionBy().getPartitions();
        int partCnt = partitions.size();
        if (!hasSubPart) {
            for (int i = 0; i < partCnt; i++) {
                PartitionSpec ps = partitions.get(i);
                if (partBitSet.get(i)) {
                    partNameInfo.put(ps.getName(), new ArrayList<>());
                }
            }
        } else {
            PartitionSpec part0 = partitions.get(0);
            List<PartitionSpec> subpartitions = part0.getSubPartitions();
            int subPartCnt = subpartitions.size();
            for (int i = 0; i < partCnt; i++) {
                PartitionSpec ps = partitions.get(i);
                List<String> subPartNames = new ArrayList<>();
                for (int j = 0; j < subPartCnt; j++) {
                    int bsIndex = i * subPartCnt + j;
                    if (partBitSet.get(bsIndex)) {
                        PartitionSpec spec = subpartitions.get(j);
                        subPartNames.add(spec.getName());
                    }
                }
                if (subPartNames.size() > 0) {
                    partNameInfo.put(ps.getName(), subPartNames);
                }
            }
        }

        return partNameInfo;
    }

    /*======== Methods for eval expression of all partitioned table ddl and partitioned routing ========*/
    public static RexBuilder getRexBuilder() {
        return rexBuilder;
    }

    public static RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /**
     * This method is called when need to build partInfo from metadb/create table ast/alter partition ast
     */
    public static PartitionBoundVal getBoundValByRexExpr(RexNode oneBndExpr,
                                                         RelDataType bndColRelDataType,
                                                         PartFieldAccessType specifiedAccessType,
                                                         ExecutionContext context) {

        try {
            ExprContextProvider exprContextProvider = new ExprContextProvider();
            IExpression boundValExpr = RexUtils.getEvalFuncExec(oneBndExpr, exprContextProvider);

            RelDataType oneBndExprReturnType = oneBndExpr.getType();
            DataType tmpBndExprDataType = DataTypeUtil.calciteToDrdsType(oneBndExprReturnType);
            DataType bndExprDataType = tmpBndExprDataType;
            DataType bndColDataType = DataTypeUtil.calciteToDrdsType(bndColRelDataType);

            if (DataTypeUtil.isStringType(bndColDataType) && DataTypeUtil.isStringType(tmpBndExprDataType)) {
                String connCharset = context.getEncoding();
                String colCharset = oneBndExprReturnType.getCharset().name();
                if (connCharset != null && colCharset != null && !connCharset.equalsIgnoreCase(colCharset)) {
                    oneBndExprReturnType =
                        DataTypeUtil.getCharacterTypeWithCharsetAndCollation(oneBndExprReturnType, connCharset, null);
                    bndExprDataType = DataTypeUtil.calciteToDrdsType(oneBndExprReturnType);
                }
            }

            ConstExprEvalParams exprEvalParams = new ConstExprEvalParams();
            exprEvalParams.calcExpr = boundValExpr;
            exprEvalParams.needGetTypeFromDynamicExpr = false;
            exprEvalParams.exprReturnType = bndExprDataType;
            exprEvalParams.partColType = bndColDataType;
            exprEvalParams.executionContext = context;
            exprEvalParams.fldEndpoints = null;
            exprEvalParams.accessType =
                specifiedAccessType != null ? specifiedAccessType : PartFieldAccessType.DDL_EXECUTION;
            exprEvalParams.constExprId = null;
            exprEvalParams.needCacheEvalResult = false;
            PartitionField partField = evalExprValAndCache(exprEvalParams);
            PartitionBoundVal bndVal = PartitionBoundVal
                .createPartitionBoundVal(partField, PartitionBoundValueKind.DATUM_NORMAL_VALUE);
            return bndVal;
        } catch (InvalidTypeConversionException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, ex,
                String.format("Partition column values of incorrect data type conversion[%s] in DDL", ex.getStatus()));
        }
    }

    /**
     * Calc the predicate expr value by IExpression and convert the data type of result to the specified exprReturnType
     */
    protected static PartitionField evalExprValAndCache(ConstExprEvalParams evalParams) {

        IExpression calcExpr = evalParams.calcExpr;
        boolean needGetTypeFromDynamicExpr = evalParams.needGetTypeFromDynamicExpr;
        DataType exprReturnType = evalParams.exprReturnType;
        DataType partColType = evalParams.partColType;
        ColumnMeta partColMeta = evalParams.partColMeta;
        boolean[] fldEndpoints = evalParams.fldEndpoints;
        ExecutionContext executionContext = evalParams.executionContext;
        PartPruneStepPruningContext pruningCtx = evalParams.pruningCtx;
        PartFieldAccessType accessType = evalParams.accessType;
        Integer constExprId = evalParams.constExprId;
        Boolean needCacheEvalResult = evalParams.needCacheEvalResult;

        /**
         * When constExpr is null expr, its constExprId will be null
         */
        PartPruneStepPruningContext execContext = pruningCtx;
        Object evalValObj = null;
        boolean isNullVal = false;
        needCacheEvalResult &= execContext != null && constExprId != null;
        if (needCacheEvalResult) {
            PartPruneStepPruningContext.ExprEvalResult evalResult = execContext.getEvalResult(constExprId);
            if (evalResult != null) {
                evalValObj = evalResult.rawVal;
                isNullVal = evalResult.isNull;
            }
        }

        /**
         * If fail to fetch eval result from cache, then eval expr directly
         */
        if (evalValObj == null && !isNullVal) {
            // Eval expr and get result
            evalValObj = calcExpr.eval(null, executionContext);
        }

        /**
         * Try to fetch the data type from evalValObj
         */
        DataType exprDataType = exprReturnType;
        if (needGetTypeFromDynamicExpr && evalValObj != null) {
            exprDataType = DataTypeUtil.getTypeOfObject(evalValObj);
            Object[] newV = new Object[1];
            DataType[] newDt = new DataType[1];
            boolean fixedValue = DataTypeUtil.fixDynamicParamObjectIfNeed(evalValObj, exprDataType, newV, newDt);
            if (fixedValue) {
                evalValObj = newV[0];
                exprDataType = newDt[0];
            }
        }

        /**
         * Build the part field by the eval result
         */
        PartitionField partField =
            PartitionPrunerUtils
                .buildPartFieldForPartCol(evalValObj, exprDataType, partColType, partColMeta, fldEndpoints,
                    executionContext, accessType);

        if (needCacheEvalResult) {
            /**
             * If PartitionField exists data truncated, should not catch its result
             */
            if (partField.lastStatus() == TypeConversionStatus.TYPE_OK) {
                /**
                 * Try to cache eval result
                 */
                execContext
                    .putEvalResult(constExprId, new PartPruneStepPruningContext.ExprEvalResult(evalValObj, partField));
            }
        }
        return partField;
    }

    /**
     * Calc the partition int function value
     */
    protected static PartitionField evalPartFuncVal(PartitionField partField,
                                                    PartitionIntFunction partFunc,
                                                    PartitionStrategy partStrategy,
                                                    ExecutionContext context,
                                                    boolean[] endpoints,
                                                    PartFieldAccessType scenario) {

        Object evalObj = null;
        // get a session properties from context
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        DataType dataType = partFunc.getReturnType();
        PartitionField newPartField = null;
        boolean isBuildInFunc = PartitionFunctionBuilder.isBuildInPartFunc(partFunc);
        if (dataType == DataTypes.LongType && isBuildInFunc) {
            if (endpoints != null) {
                evalObj = partFunc.evalIntEndpoint(partField, sessionProperties, endpoints);
            } else {
                evalObj = partFunc.evalInt(partField, sessionProperties);
            }
            // evalObj must be Long because it is from partFunc.evalIntEndpoint or partFunc.evalInt
            newPartField = PartitionPrunerUtils.buildPartField(evalObj,
                DataTypes.LongType, partFunc.getReturnType(), endpoints, context, scenario);

        } else {
            List<PartitionField> fullPartColFlds = new ArrayList<>();
            fullPartColFlds.add(partField);
            List<PartitionField> fullParams = partFunc.getFullParamsByPartColFields(fullPartColFlds);
            evalObj = partFunc.evalEndpoint(fullParams, sessionProperties, endpoints);
            PartitionField partFunEvalField = PartitionPrunerUtils.buildPartField(evalObj,
                partFunc.getReturnType(), partFunc.getReturnType(), endpoints, context, scenario);

            /**
             * <pre>
             * For the partition definition as the following ( int_col1 and int_col2 are partition columns):
             *    CO_HASH( right(int_col1, 4), int_col2 ) or CO_HASH( right(int_col1, 4), right(int_col1, 4) )
             * , some substring of the eval result of right(int_col1) /left(int_col1) /substr(int_col1)
             *  maybe a number with zero-symbol-beginning, such as
             *      right(12000001) == "0001"
             *  , bue the original value of int_col2 is just 1,
             *  then the routing result of "0001" and "1" will be different if they are treated as varchar datatype objects.
             *  so we must convert these substring of the eval result into the dateype of partColMeta for number datatype,
             *  including tinyint/smallint/mediumint/int/bigint/decimal with scale=0
             * </pre>
             */
            newPartField =
                PartitionPrunerUtils.convertPartFuncEvalValueToPartColDataTypeIfNeed(partFunEvalField, partField,
                    partFunc, partStrategy, context, endpoints, scenario);
        }

        return newPartField;
    }

    /**
     * Calc the partition int function value by the partColFLd
     */
    public static PartitionField buildPartFieldByEvalPartFuncExpr(
        PartitionField partColField,
        PartitionByDefinition partBy,
        ExecutionContext context) {
        PartitionIntFunction partIntFunc = partBy.getPartIntFunc();
        if (partIntFunc == null) {
            return partColField;
        }
        PartitionStrategy partStrategy = partBy.getStrategy();
        PartitionField partFuncEvalResultFld =
            evalPartFuncVal(partColField, partIntFunc, partStrategy, context, null, PartFieldAccessType.DDL_EXECUTION);
        return partFuncEvalResultFld;
    }

    public static PartitionField buildPartField(Object predExprVal,
                                                DataType predExprDataType,
                                                DataType partFldDataType,
                                                boolean[] endpoints,
                                                ExecutionContext context,
                                                PartFieldAccessType accessType) {
        return buildPartFieldInner(predExprVal, predExprDataType, partFldDataType, 0, false, endpoints, context,
            accessType);
    }

    public static PartitionField buildPartFieldForPartCol(Object predExprVal,
                                                          DataType predExprDataType,
                                                          DataType partFldDataType,
                                                          ColumnMeta partColMeta,
                                                          boolean[] endpoints,
                                                          ExecutionContext context,
                                                          PartFieldAccessType accessType) {
        int binaryLengthDef = 0;
        boolean useVarbinary = false;
        if (partFldDataType instanceof BinaryType && partColMeta != null) {
            binaryLengthDef = partColMeta.getField().getPrecision();
            if (partColMeta.getField().getRelType().getSqlTypeName() == SqlTypeName.VARBINARY) {
                useVarbinary = true;
            }
        }
        return buildPartFieldInner(predExprVal, predExprDataType, partFldDataType, binaryLengthDef, useVarbinary,
            endpoints, context, accessType);
    }

    protected static PartitionField buildPartFieldInner(Object predExprVal,
                                                        DataType predExprDataType,
                                                        DataType partFldDataType,
                                                        int partFldBinaryTypeLen,
                                                        boolean useVarbinary,
                                                        boolean[] endpoints,
                                                        ExecutionContext context,
                                                        PartFieldAccessType accessType) {

        // make field of partition key by its data type definition
        PartitionField field = null;
        if (partFldDataType instanceof BinaryType) {
            if (useVarbinary) {
                field = PartitionFieldBuilder.createVarBinaryField(partFldBinaryTypeLen);
            } else {
                field = PartitionFieldBuilder.createBinaryField(partFldBinaryTypeLen);
            }
        } else {
            field = PartitionFieldBuilder.createField(partFldDataType);
        }

        SessionProperties sessionProperties;
        if (context == null) {
            if (endpoints == null) {
                // store value from query.
                field.store(predExprVal, predExprDataType);
            } else {
                // store value from query.
                field.store(predExprVal, predExprDataType, null, endpoints);
            }
        } else {
            // so we abstract a session properties:
            sessionProperties = SessionProperties.fromExecutionContext(context);
            if (accessType == PartFieldAccessType.DML_PRUNING || accessType == PartFieldAccessType.DDL_EXECUTION) {
                sessionProperties.setCheckLevel(FieldCheckLevel.CHECK_FIELD_WARN);
            }

            // store value from query.
            if (endpoints != null) {
                /**
                 * Store for Range Query of "<" or ">" or ">=" or "<="
                 */
                field.store(predExprVal, predExprDataType, sessionProperties, endpoints);
            } else {
                /**
                 * Store for Insert and Point Query of "="
                 */
                field.store(predExprVal, predExprDataType, sessionProperties);
            }
        }
        // Process the TypeConversionStatus for pruning
        processTypeConversionStatus(accessType, predExprDataType, field, endpoints, context);
        return field;
    }

    protected static void processTypeConversionStatus(PartFieldAccessType accessType,
                                                      DataType srcDataType, PartitionField storedField,
                                                      boolean[] endpoints,
                                                      ExecutionContext executionContext) {
        PartFieldTypeConversionProcessor.processTypeConversionStatus(accessType, srcDataType, storedField, endpoints,
            executionContext);
    }

    protected static SearchExprEvalResult evalExprValsAndBuildOneDatum(ExecutionContext context,
                                                                       PartPruneStepPruningContext pruningCtx,
                                                                       SearchExprInfo exprInfo) {
        PartClauseExprExec[] predExprExecArr = exprInfo.getExprExecArr();
        int partColNum = predExprExecArr.length;
        PartitionBoundVal[] searchValArr = new PartitionBoundVal[partColNum];

        boolean[] epInfo = null;
        ComparisonKind newCmpKind = null;
        epInfo = PartFuncMonotonicityUtil.buildIntervalEndPointInfo(exprInfo.getCmpKind());
        if (partColNum == 1) {
            searchValArr[0] =
                PartitionPrunerUtils.evalExecAndBuildBoundValue(context, pruningCtx, predExprExecArr[0], epInfo);
            newCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(epInfo);
        } else {
            int invalidTypeCastPartColInddex = -1;
            ComparisonKind exprInfoCmpKind = exprInfo.getCmpKind();
            for (int j = 0; j < partColNum; j++) {
                searchValArr[j] =
                    PartitionPrunerUtils.evalExecAndBuildBoundValue(context, pruningCtx, predExprExecArr[j], epInfo);
                if (searchValArr[j].isNormalValue() && !searchValArr[j].isNullValue()) {
                    TypeConversionStatus typeConvertStatus = searchValArr[j].getValue().lastStatus();
                    if (typeConvertStatus != TypeConversionStatus.TYPE_OK) {
                        /**
                         * typeConvertStatus must be a type-truncated-status
                         */
                        if (exprInfoCmpKind == ComparisonKind.EQUAL) {
                            /**
                             * <pre>
                             * For full-part-col equality predicate,
                             * just use the truncated partFld to finish routing
                             * </pre>
                             *
                             */
                            continue;
                        } else {
                            invalidTypeCastPartColInddex = j;
                            break;
                        }
                    }
                }
            }

            /**
             * When find a partition expr exists invalid type cast,
             * should auto ignore all partition expr after it and auto fill
             * max/min value to enlarge the range..
             */
            if (invalidTypeCastPartColInddex > -1) {
                PartitionBoundVal autoFillVal = null;
                if (exprInfoCmpKind == ComparisonKind.GREATER_THAN_OR_EQUAL
                    || exprInfoCmpKind == ComparisonKind.GREATER_THAN) {
                    autoFillVal = PartitionBoundVal.createMinValue();
                    newCmpKind = exprInfoCmpKind;
                } else if (exprInfoCmpKind == ComparisonKind.LESS_THAN_OR_EQUAL
                    || exprInfoCmpKind == ComparisonKind.LESS_THAN) {
                    autoFillVal = PartitionBoundVal.createMaxValue();
                    newCmpKind = exprInfoCmpKind;
                } else {
                    // exprInfoCmpKind == ComparisonKind.EQUAL
                    newCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(epInfo);
                }
                if (exprInfoCmpKind != ComparisonKind.EQUAL) {
                    for (int i = invalidTypeCastPartColInddex; i < partColNum; i++) {
                        searchValArr[i] = autoFillVal;
                    }
                } else {
                    /**
                     * Impossible come here
                     */
                }
            } else {
                newCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(epInfo);
            }
        }

        SearchDatumInfo searchDatumInfo = new SearchDatumInfo(searchValArr);
        SearchExprEvalResult exprEvalResult = new SearchExprEvalResult(searchDatumInfo, newCmpKind);
        return exprEvalResult;
    }

    protected static PartitionBoundVal evalExecAndBuildBoundValue(ExecutionContext context,
                                                                  PartPruneStepPruningContext pruningCtx,
                                                                  PartClauseExprExec exprExec,
                                                                  boolean[] fldEndpoints) {
        PartitionBoundValueKind valueKind = exprExec.getValueKind();
        PartitionField partField = null;
        if (exprExec.getValueKind() == PartitionBoundValueKind.DATUM_NORMAL_VALUE && !exprExec.isAlwaysNullValue()) {
            partField = exprExec.evalPredExprVal(context, pruningCtx, fldEndpoints);
        }
        PartitionBoundVal searchVal =
            PartitionBoundVal.createPartitionBoundVal(partField, valueKind);

        return searchVal;
    }

    /**
     * i.e. p=[1,9), if atVal=1, flag = -1, [1,2),[2,9)
     * if atval=3, flag = 0, [1,3),[3,4),[4,9)
     * if atVal=8, flag = 1, [1,8),[8,9)
     * if p=[1,2) atVal=1, flag=-2 not need to split any more
     */
    public static int getExtractPosition(PartitionSpec curSpec, PartitionSpec prevSpec, Long[] atVal) {
        PartitionBoundSpec boundSpec = curSpec.getBoundSpec();
        int flag = 0;
        // TODO: simplify
        // FIXME: support key partition
        SearchDatumInfo curSpecUpperBound = boundSpec.getSingleDatum();
        Long[] lowBoundHashCode = new Long[atVal.length];
        for (int i = 0; i < atVal.length; i++) {
            lowBoundHashCode[i] = Long.MIN_VALUE;
        }
        SearchDatumInfo prevSpecUpperBound = SearchDatumInfo.createFromHashCodes(lowBoundHashCode);
        if (prevSpec != null) {
            prevSpecUpperBound = prevSpec.getBoundSpec().getSingleDatum();
        }
        SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(atVal);
        int distWithPrevPart = compareDistance(prevSpecUpperBound, searchDatumInfo);
        int distWithCurPart = compareDistance(searchDatumInfo, curSpecUpperBound);
        if (distWithCurPart == 0 && distWithPrevPart == -1) {
            flag = -2;
        } else if (distWithPrevPart == -1) {
            flag = -1;
        } else if (distWithCurPart == 0) {
            flag = 1;
        } else {
            flag = 0;
        }
        return flag;
    }

    // 1: distance greater than 1
    // 0: distance equal to 1
    // -1: one == two
    private static int compareDistance(SearchDatumInfo one, SearchDatumInfo two) {
        assert one.getDatumInfo().length == two.getDatumInfo().length;
        int i = 0;
        SearchDatumInfo small = one;
        SearchDatumInfo big = two;
        boolean equal = true;
        do {
            long v1 = small.getDatumInfo()[i].getValue().longValue();
            long v2 = big.getDatumInfo()[i].getValue().longValue();
            if (v1 != v2) {
                if (i + 1 < one.getDatumInfo().length) {
                    return 1;
                } else {
                    boolean needSwitch = (v1 > v2);
                    if (needSwitch) {
                        long temp = v1;
                        v1 = v2;
                        v2 = temp;
                    }
                    if ((v1 + 1) == v2) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            } else if (i + 1 == one.getDatumInfo().length) {
                return -1;
            }
            i++;
        } while (i < one.getDatumInfo().length);
        //can't reach here
        throw new RuntimeException("compare SearchDatumInfo error");
    }

    public static boolean checkIfPointSelect(PartitionPruneStep step, ExecutionContext ec) {
        if (ec != null && ec.getParams() != null) {
            Map<Integer, ParameterContext> map = ec.getParams().getCurrentParameter();
            for (ParameterContext parameterContext : map.values()) {
                if (parameterContext.getValue() instanceof RawString) {
                    return false;
                }
            }
        }
        boolean onlyContainEqCond = PartitionPruneStepUtil.onlyContainEqualConditionInner(false, step);
        return onlyContainEqCond;
    }

    public static void logStepExplainInfo(ExecutionContext context,
                                          PartitionInfo partInfo,
                                          PartPruneStepPruningContext pruningContext) {
        if (!pruningContext.isEnableLogPruning()) {
            return;
        }
        try {
            String traceId = context.getTraceId();
            String dbName = partInfo.getTableSchema();
            String tblName = partInfo.getTableName();
            StringBuilder explainBuilder = new StringBuilder();
            explainBuilder.append("\nTraceId=").append(traceId);
            explainBuilder.append(",").append("Table=").append(dbName).append(".").append(tblName);
            if (!pruningContext.isPruningByTuple()) {
                logStepExplainInfoInner(null, pruningContext.getRootStep(), 2, pruningContext.getStepExplainInfo(),
                    explainBuilder);
            } else {
                PartitionTupleRouteInfo tupleRouteInfo = pruningContext.getRootTuple();
                List<PartTupleDispatchInfo> dispatchInfos = tupleRouteInfo.getTupleDispatchFuncInfos();
                for (int i = 0; i < dispatchInfos.size(); i++) {
                    logStepExplainInfoInner(null, dispatchInfos.get(i), 2, pruningContext.getStepExplainInfo(),
                        explainBuilder);
                }
            }

            explainBuilder.append("\n");
            PRUNER_LOG.warn(explainBuilder.toString());
        } catch (Throwable ex) {
            // ignore
            PRUNER_LOG.error(ex);
        }
    }

    private static void logStepExplainInfoInner(Integer parentSpecPosi,
                                                PartitionPruneBase current,
                                                int currentLevel,
                                                Map<Object, StepExplainItem> stepExplainInfo,
                                                StringBuilder explainBuilder) {

        boolean isTuple = current instanceof PartTupleDispatchInfo;

        StepExplainItem item = stepExplainInfo.get(current);
        if (item == null) {
            return;
        }
        explainBuilder.append("\n");
        for (int i = 0; i < currentLevel; i++) {
            explainBuilder.append(" ");
        }

        if (parentSpecPosi == null) {
            parentSpecPosi = -1;
        }

        PartPrunedResult result = item.partSpecPosiToPruneResultMap.get(parentSpecPosi);

        explainBuilder.append(isTuple ? "Tuple=" : "Step=");
        explainBuilder.append(item.stepDesc);
        explainBuilder.append(",");
        if (item.useSubPartByTemp) {
            explainBuilder.append("useSubPartTemp=true").append(",");
        }
        explainBuilder.append("PartSet={").append(result.toAllPhyPartBitString()).append("}");

        if (current instanceof PartitionPruneSubPartStepAnd) {
            List<PartitionPruneStep> steps = item.targetSubSteps;
            for (int i = 0; i < steps.size(); i++) {
                logStepExplainInfoInner(null, steps.get(i), currentLevel + 1, stepExplainInfo,
                    explainBuilder);
            }
        } else if (current instanceof PartitionPruneSubPartStepOr) {
            List<PartitionPruneStep> steps = item.targetSubSteps;
            PartitionPruneStep subPartStep = steps.get(0);
            List<Integer> posiList = item.prunePartSpecPosiList;

            if (!item.useSubPartByTemp) {
                for (int i = 0; i < posiList.size(); i++) {
                    logStepExplainInfoInner(posiList.get(i), subPartStep, currentLevel + 1, stepExplainInfo,
                        explainBuilder);
                }
            } else {
                logStepExplainInfoInner(posiList.get(0), subPartStep, currentLevel + 1, stepExplainInfo,
                    explainBuilder);
            }

        } else if (current instanceof PartitionPruneStepCombine) {
            PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) current;
            List<PartitionPruneStep> steps = stepCombine.getSubSteps();
            for (int i = 0; i < steps.size(); i++) {
                logStepExplainInfoInner(parentSpecPosi, steps.get(i), currentLevel + 1, stepExplainInfo,
                    explainBuilder);
            }
        }
    }

    public static void collateTupleRouteExplainInfo(PartTupleDispatchInfo tupleDispatchInfo,
                                                    ExecutionContext context,
                                                    PartPrunedResult result,
                                                    PartPruneStepPruningContext pruningContext) {
        if (!pruningContext.isEnableLogPruning()) {
            return;
        }
        try {
            StepExplainItem item = new StepExplainItem();
            item.prunedResult = result;
            item.stepDesc = tupleDispatchInfo.buildStepDigest(context);
            Map<Object, StepExplainItem> explainInfo = pruningContext.getStepExplainInfo();
            explainInfo.put(tupleDispatchInfo, item);

        } catch (Throwable ex) {
            // ignore all exception
            PRUNER_LOG.error(ex);
        }
    }

    public static StepExplainItem collateStepExplainInfo(PartitionPruneStep step,
                                                         ExecutionContext context,
                                                         PartPrunedResult result,
                                                         PartPruneStepPruningContext pruningContext) {
        if (!pruningContext.isEnableLogPruning()) {
            return null;
        }
        try {
            StepExplainItem item = new StepExplainItem();
            item.prunedResult = result;
            if (step instanceof PartitionPruneStepOp) {
                item.stepDesc = ((PartitionPruneStepOp) step).buildStepDigest(context);
            } else {
                item.stepDesc = ((PartitionPruneStepCombine) step).getCombineSymbol();
            }
            Integer parentSpecPosi = result.getParentSpecPosi();
            if (parentSpecPosi == null) {
                parentSpecPosi = -1;
            }
            item.partLevel = step.getPartLevel();
            item.parentSpecPosiList.add(parentSpecPosi);
            item.partSpecPosiToPruneResultMap.put(parentSpecPosi, result);

            Map<Object, StepExplainItem> explainInfo = pruningContext.getStepExplainInfo();
            StepExplainItem explainItem = explainInfo.get(step);
            if (explainItem == null) {
                explainInfo.put(step, item);
                return item;
            } else {
                if (!explainItem.partSpecPosiToPruneResultMap.containsKey(parentSpecPosi)) {
                    explainItem.parentSpecPosiList.add(parentSpecPosi);
                    explainItem.partSpecPosiToPruneResultMap.put(parentSpecPosi, result);
                }
                return explainItem;
            }
        } catch (Throwable ex) {
            // ignore all exception
            PRUNER_LOG.error(ex);
            return null;
        }
    }

    /**
     * <pre>
     *
     * Convert the computed result PartField of part function of substr/right/left to the PartFiled of PartCol
     * which datatype is int and decimal
     *
     * </pre>
     */
    private static PartitionField convertPartFuncEvalValueToPartColDataTypeIfNeed(PartitionField partFuncEvalValFld,
                                                                                  PartitionField partColInputFld,
                                                                                  PartitionIntFunction partFun,
                                                                                  PartitionStrategy partStrategy,
                                                                                  ExecutionContext context,
                                                                                  boolean[] endpoints,
                                                                                  PartFieldAccessType scenario) {
        /**
         * <pre>
         * For the partition definition as the following ( int_col1 and int_col2 are partition columns):
         *    CO_HASH( right(int_col1, 4), int_col2 ) or CO_HASH( right(int_col1, 4), right(int_col1, 4) )
         * , some substring of the eval result of right(int_col1) /left(int_col1) /substr(int_col1)
         *  maybe a number with zero-symbol-beginning, such as
         *      right(12000001) == "0001"
         *  , bue the original value of int_col2 is just 1,
         *  then the routing result of "0001" and "1" will be different if they are treated as varchar datatype objects.
         *  so we must convert these substring of the eval result into the dateype of partColMeta for number datatype,
         *  including tinyint/smallint/mediumint/int/bigint/decimal with scale=0
         * </pre>
         */

        if (partStrategy != PartitionStrategy.CO_HASH) {
            return partFuncEvalValFld;
        }

        if (!PartitionFunctionBuilder.isStringFamilyPartitionFunction(partFun.getSqlOperator().getName())) {
            return partFuncEvalValFld;
        }

        /**
         * The dataTYpe of partColMeta
         */
        DataType partColDataType = partColInputFld.dataType();
        if (!(DataTypeUtil.isUnderBigintUnsignedType(partColDataType) || DataTypeUtil.isDecimalType(partColDataType))) {
            return partFuncEvalValFld;
        }

        PartitionField newPartColFldReturn = PartitionFieldBuilder.createField(partColDataType);
        if (partFuncEvalValFld.isNull()) {
            newPartColFldReturn.setNull(true);
            return newPartColFldReturn;
        }
        String strVal = partFuncEvalValFld.stringValue().toStringUtf8();
        if (StringUtils.isEmpty(strVal)) {
            return newPartColFldReturn;
        }

        /**
         * The dataTYpe of partition Function return
         */
        DataType partFuncReturnDataType = partFuncEvalValFld.dataType();

        newPartColFldReturn.store(strVal, partFuncReturnDataType);
        processTypeConversionStatus(scenario, partFuncReturnDataType, newPartColFldReturn, endpoints, context);
        return newPartColFldReturn;
    }

    public static void filterPartitionsBySelectedPartition(PartPrunedResult partPrunedResult, SqlNode partitions) {
        if (partitions == null) {
            return;
        }

        PartitionInfo partInfo = partPrunedResult.getPartInfo();
        if (partInfo.getTableType() == PartitionTableType.PARTITION_TABLE
            || partInfo.getTableType() == PartitionTableType.GSI_TABLE
            || partInfo.getTableType() == PartitionTableType.COLUMNAR_TABLE) {
            SqlNodeList partNamesAst = (SqlNodeList) partitions;
            Set<Integer> selectedPartPostSet = new HashSet<>();
            for (SqlNode partNameAst : partNamesAst.getList()) {
                String partName = ((SqlIdentifier) partNameAst).getLastName();
                PartitionSpec pSpec = partInfo.getPartSpecSearcher().getPartSpecByPartName(partName);
                if (pSpec == null || (pSpec.getStatus() != null
                    && pSpec.getStatus() == TablePartitionRecord.PARTITION_STATUS_PARTITION_OFFLINE)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        String.format("Unknown partition '%s' in table '%s'", partName,
                            partInfo.getTableName()));
                }
                boolean isPhySpec = !pSpec.isLogical();
                if (isPhySpec) {
                    selectedPartPostSet.add(pSpec.getPhyPartPosition().intValue());
                } else {
                    if (pSpec.isSpecTemplate()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            String.format("Not allowed to select partition by using subpartition template '%s'",
                                pSpec.getTemplateName()));
                    }

                    List<PartitionSpec> subPartList = pSpec.getSubPartitions();
                    for (PartitionSpec subPart : subPartList) {
                        selectedPartPostSet.add(subPart.getPhyPartPosition().intValue());
                    }
                }

            }
            BitSet partSetSelected =
                PartitionPrunerUtils.buildPhyPartsBitSetByPhyPartPostSet(partInfo, selectedPartPostSet);
            partPrunedResult.getPartBitSet().and(partSetSelected);
        } else if (partInfo.getTableType() == PartitionTableType.BROADCAST_TABLE) {
            return;
        } else if (partInfo.getTableType() == PartitionTableType.REPLICAS_TABLE) {
            return;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "PARTITION () clause on non partitioned table");
        }
    }

    public static PartPruneStepPruningExtraInfo preparePruningExtraInfoIfNeed(ExecutionContext ec,
                                                                              String targetSchemaName,
                                                                              List<String> targetTableNames) {
        PartPruneStepPruningExtraInfo extraInfo = null;
        if (!DbInfoManager.getInstance().isNewPartitionDb(targetSchemaName)) {
            return extraInfo;
        }
        SchemaManager schemaManager = ec.getSchemaManager(targetSchemaName);
        boolean containAnyReplicasTables = false;
        for (int i = 0; i < targetTableNames.size(); i++) {
            TableMeta tm = schemaManager.getTable(targetTableNames.get(i));
            PartitionInfo partInfo = tm.getPartitionInfo();
            if (partInfo.isReplicasTable()) {
                containAnyReplicasTables = true;
                break;
            }
        }
        if (containAnyReplicasTables) {
            extraInfo = PartitionPrunerUtils.buildPruningExtraInfoByTableSchemaAndTableNames(ec, targetSchemaName,
                targetTableNames);
        }
        return extraInfo;
    }

    public static PartPruneStepPruningExtraInfo buildPruningExtraInfoByTableSchemaAndTableNames(
        ExecutionContext ec,
        String targetTableSchema,
        List<String> targetTableNames) {
        PartPruneStepPruningExtraInfo pruningExtraInfo = new PartPruneStepPruningExtraInfo();
        LogicalViewCommonGroupInfo commonGroupKeyInfo =
            PartitionPrunerUtils.fetchCommonGroupKeyInfoByTableSchemaAndTableNames(ec, targetTableSchema,
                targetTableNames);
        pruningExtraInfo.setCommonGroupKeyInfo(commonGroupKeyInfo);
        return pruningExtraInfo;
    }

    public static LogicalViewCommonGroupInfo fetchCommonGroupKeyInfoByTableSchemaAndTableNames(ExecutionContext ec,
                                                                                               String targetSchemaName,
                                                                                               List<String> targetTableNames) {
        LogicalViewCommonGroupInfo commonGroupKeyInfo = new LogicalViewCommonGroupInfo();
        Set<String> tmpGroupKeySet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (!DbInfoManager.getInstance().isNewPartitionDb(targetSchemaName)) {
            return commonGroupKeyInfo;
        }
        SchemaManager sm = null;
        if (ec != null) {
            sm = ec.getSchemaManager(targetSchemaName);
        } else {
            sm = OptimizerContext.getContext(targetSchemaName).getLatestSchemaManager();
        }
        int logTbNum = targetTableNames.size();

        DirectPlanCommonGroupInfo directPlanCommonGroupInfo = new DirectPlanCommonGroupInfo();
        for (int i = 0; i < logTbNum; i++) {
            String logTb = targetTableNames.get(i);
            directPlanCommonGroupInfo.updateCommonGroupKeyByTableName(ec, targetSchemaName, logTb);
        }

        /**
         * Check if contain any single/partitioned tables
         */
        boolean containAnySingledOrPartitionedTables = false;
        Long targetTgId = null;
        TableMeta targetTableMeta = null;
        for (int i = 0; i < logTbNum; i++) {
            String logTb = targetTableNames.get(i);
            TableMeta tableMeta = null;
            PartitionInfo partInfo = null;
            tableMeta = sm.getTable(logTb);
            partInfo = tableMeta.getPartitionInfo();
            boolean isPartitioned = partInfo.isGsiOrPartitionedTable();
            boolean isSingled = partInfo.isGsiSingleOrSingleTable();
            if (isPartitioned || isSingled) {
                containAnySingledOrPartitionedTables = true;
                targetTableMeta = tableMeta;
                targetTgId = partInfo.getTableGroupId();
                break;
            }
        }

        if (containAnySingledOrPartitionedTables) {
            PartitionInfo targetPartInfo = targetTableMeta.getPartitionInfo();
            Set<String> grpGrpKeyOfCurTbl = targetPartInfo.getPartSpecSearcher().getGroupKeySetOfAllPhyPartSpecs();
            commonGroupKeyInfo.getCommonGroupKeySet().addAll(grpGrpKeyOfCurTbl);
            commonGroupKeyInfo.setAllowRandomSelected(false);
            commonGroupKeyInfo.setForceMatchTgId(targetTgId);
            return commonGroupKeyInfo;
        }

        /**
         * Come to here, only contains replicas/broadcast tables
         */
        for (int i = 0; i < logTbNum; i++) {
            String logTb = targetTableNames.get(i);
            TableMeta tableMeta = null;
            PartitionInfo partInfo = null;
            tableMeta = sm.getTable(logTb);
            partInfo = tableMeta.getPartitionInfo();
            Set<String> grpGrpKeyOfCurTbl = partInfo.getPartSpecSearcher().getGroupKeySetOfAllPhyPartSpecs();
            boolean isBroadcast = partInfo.isGsiBroadcastOrBroadcast();
            boolean isReplicas = partInfo.isReplicasTable();
            if (isBroadcast || isReplicas) {
                if (tmpGroupKeySet.isEmpty() && i < 1) {
                    /**
                     * If it is the first table, just add all group keys
                     */
                    tmpGroupKeySet.addAll(grpGrpKeyOfCurTbl);
                } else {
                    tmpGroupKeySet.retainAll(grpGrpKeyOfCurTbl);
                }
            }
        }
        commonGroupKeyInfo.setCommonGroupKeySet(tmpGroupKeySet);
        commonGroupKeyInfo.setAllowRandomSelected(true);
        commonGroupKeyInfo.setForceMatchTgId(null);
        return commonGroupKeyInfo;
    }

    private static boolean calcCommonGroupKeySetFromAllPrunedResults(List<PartPrunedResult> prunedResults,
                                                                     ExecutionContext ec,
                                                                     DirectPlanCommonGroupInfo directPlanCommonGroupInfo) {

        boolean ret = false;
        for (int i = 0; i < prunedResults.size(); i++) {
            PartPrunedResult prunedResult = prunedResults.get(i);
            PartitionInfo partInfo = prunedResult.getPartInfo();
            PartitionTableType tableType = partInfo.getTableType();
            if (tableType == PartitionTableType.COLUMNAR_TABLE || tableType == PartitionTableType.OSS_TABLE) {
                return false;
            }
            directPlanCommonGroupInfo.updateCommonGroupKeyByTablePrunedResult(partInfo.getTableSchema(),
                partInfo.getTableName(), prunedResult);

        }
        Set<String> targetGrpKeySet = directPlanCommonGroupInfo.getCommonGroupKeySet();
        ret = targetGrpKeySet.size() == 1;
        return ret;
    }
}
