package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlColFuncExprInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import io.airlift.slice.Slice;
import lombok.Data;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class TtlPartitionUtil {

    @Data
    public static class CreateArcCciPartByDefCalcParams {

        protected TableMeta ttlTblMeta;
        protected TtlDefinitionInfo ttlInfo;
        protected String arcTableSchema;
        protected String arcTableName;
        protected Boolean buildForCci;
        protected PartKeyLevel targetPartLevel;
        protected String pivotPointValStr;
        protected String preBuildTargetBoundValStr;
        protected ExecutionContext ec;

        public CreateArcCciPartByDefCalcParams() {
        }
    }

    @Data
    public static class CreateArcCciPartByDefResult {
        protected CreateArcCciPartByDefCalcParams params;

        /**
         * Item of PartSpec:
         * key of pair: newPartSpecName
         * val of pair: newPartSpecBoundStr
         */
        protected List<Pair<String, String>> newCreatedPartSpecInfos = new ArrayList<>();

        public CreateArcCciPartByDefResult() {
        }

        public String generateCreateCciPartBySql() {
            TtlDefinitionInfo ttlDefinitionInfo = params.getTtlInfo();
            String arcTblSchema = params.getArcTableSchema();
            String arcTblName = params.getArcTableName();
            ExecutionContext executionContext = params.getEc();

            List<String> allPartNamesToBeAdded = new ArrayList<>();
            List<String> allPartBndValsToBeAdded = new ArrayList<>();
            for (int i = 0; i < newCreatedPartSpecInfos.size(); i++) {
                Pair<String, String> newSpec = newCreatedPartSpecInfos.get(i);
                allPartNamesToBeAdded.add(newSpec.getKey());
                allPartBndValsToBeAdded.add(newSpec.getValue());
            }

            String createCiSqlForArcTbl =
                TtlTaskSqlBuilder.buildCreateColumnarIndexSqlForArcTbl(ttlDefinitionInfo, arcTblSchema, arcTblName,
                    allPartBndValsToBeAdded,
                    allPartNamesToBeAdded, executionContext);

            return createCiSqlForArcTbl;
        }
    }

    @Data
    public static class BuildPrePartCalcParams {

        protected TtlDefinitionInfo ttlInfo;
        protected Boolean buildForCci;
        protected TableMeta tarTblMeta;
        protected PartKeyLevel targetPartLevel;
        protected String pivotPointValStr;
        protected String preBuildTargetBoundValStr;

        protected Integer preBuildNewPartCount;
        protected Integer postBuildNewPartCount;
        protected Boolean expiredByOverPartCount;
        protected ExecutionContext ec;
        protected TtlColValueCalcContext calcContext;
        protected Boolean enableSplitFromNearestPart = false;

        /**
         * The gap count (in units of arc-part-interval) beyond current time that identifies
         * user-reserved range partitions not managed by TTL. Default -1 means infinity.
         */
        protected Integer reservedPartGapCount = -1;

        public BuildPrePartCalcParams() {
        }

        public Boolean getBuildForCci() {
            return buildForCci;
        }

        public void setBuildForCci(Boolean buildForCci) {
            this.buildForCci = buildForCci;
        }

        public String getPreBuildTargetBoundValStr() {
            return preBuildTargetBoundValStr;
        }

        public void setPreBuildTargetBoundValStr(String preBuildTargetBoundValStr) {
            this.preBuildTargetBoundValStr = preBuildTargetBoundValStr;
        }

        public TtlColValueCalcContext getCalcContext() {
            return calcContext;
        }

        public void setCalcContext(
            TtlColValueCalcContext calcContext) {
            this.calcContext = calcContext;
        }

        public Boolean getEnableSplitFromNearestPart() {
            return enableSplitFromNearestPart;
        }

        public void setEnableSplitFromNearestPart(Boolean enableSplitFromNearestPart) {
            this.enableSplitFromNearestPart = enableSplitFromNearestPart;
        }

        public Integer getReservedPartGapCount() {
            return reservedPartGapCount;
        }

        public void setReservedPartGapCount(Integer reservedPartGapCount) {
            this.reservedPartGapCount = reservedPartGapCount;
        }
    }

    @Data
    public static class BuildPrePartCalcResult {
        protected BuildPrePartCalcParams params;
        protected Boolean needAddNewPartCount = false;
        protected Boolean foundMaxValPart = false;
        protected String maxValPartName = null;
        /**
         * Item of PartSpec:
         * key of pair: newPartSpecName
         * val of pair: newPartSpecBoundStr
         */
        protected List<Pair<String, String>> newAddPartSpecInfos = new ArrayList<>();

        /**
         * The selected single split source partition name for nearest split optimization.
         */
        protected String splitSourcePartName = null;

        /**
         * The original upper bound string of splitSourcePartName.
         * Used when split source is not a MAXVALUE partition.
         */
        protected String splitSourceUpperBound = null;

        /**
         * Whether the splitSourcePartName is a MAXVALUE partition.
         */
        protected Boolean splitSourceIsMaxVal = null;

        /**
         * The final new part spec list that should be split from splitSourcePartName.
         */
        protected List<Pair<String, String>> finalNewPartsForSplit = new ArrayList<>();

        public BuildPrePartCalcResult() {
        }

        public boolean needAddParts() {
            return !newAddPartSpecInfos.isEmpty();
        }

        public String getNewMaxBoundValueAfterAddingParts() {
            if (newAddPartSpecInfos.isEmpty()) {
                return null;
            }
            String newMaxBndValStr = newAddPartSpecInfos.get(newAddPartSpecInfos.size() - 1).getValue();
            return newMaxBndValStr;
        }

        public String generateAddPartsSql(String sqlHint) {
            if (Boolean.TRUE.equals(params.getEnableSplitFromNearestPart())
                && !StringUtils.isEmpty(splitSourcePartName)
                && !finalNewPartsForSplit.isEmpty()) {
                return buildSplitPartitionSql(splitSourcePartName, Boolean.TRUE.equals(splitSourceIsMaxVal),
                    splitSourceUpperBound, finalNewPartsForSplit, sqlHint);
            }
            return generateSingleAddPartsSql(sqlHint);
        }

        private String buildSplitPartitionSql(String srcPartName,
                                              boolean srcIsMaxVal,
                                              String srcUpperBound,
                                              List<Pair<String, String>> newParts,
                                              String sqlHint) {
            boolean isAlterPartFofCci = this.params.getBuildForCci();
            TtlDefinitionInfo ttlInfo = params.getTtlInfo();
            PartKeyLevel tarPartLevel = this.params.getTargetPartLevel();
            ExecutionContext ec = params.getEc();
            boolean useSubPart = tarPartLevel == PartKeyLevel.SUBPARTITION_KEY;

            List<String> newPartNameList = new ArrayList<>();
            List<String> newPartBndStrList = new ArrayList<>();
            for (int i = 0; i < newParts.size(); i++) {
                Pair<String, String> newPart = newParts.get(i);
                newPartNameList.add(newPart.getKey());
                newPartBndStrList.add(newPart.getValue());
            }

            String partBoundDefs = "";
            List<String> normalizedNewAddPartBoundList =
                normalizedRangePartBoundValueList(ttlInfo, newPartBndStrList, isAlterPartFofCci, tarPartLevel, ec);
            for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
                String bndStr = normalizedNewAddPartBoundList.get(i);
                String partNameStr = newPartNameList.get(i);
                String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
                String part = null;
                if (useSubPart) {
                    part = String.format("SUBPARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
                } else {
                    part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
                }
                if (!partBoundDefs.isEmpty()) {
                    partBoundDefs += ",\n";
                }
                partBoundDefs += part;
            }

            String alterContentPart = "";
            if (srcIsMaxVal) {
                if (useSubPart) {
                    alterContentPart = String.format(
                        "SPLIT SUBPARTITION `%s` INTO ( \n%s, SUBPARTITION `%s` VALUES LESS THAN (MAXVALUE) )",
                        srcPartName, partBoundDefs, srcPartName);
                } else {
                    alterContentPart =
                        String.format("SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (MAXVALUE) )",
                            srcPartName, partBoundDefs, srcPartName);
                }
            } else {
                /**
                 * Normalize the src-part upper bound the same way as the new-part bounds
                 * (add quotes for datetime, wrap with part-func if needed), otherwise a raw datetime
                 * would be mis-parsed (e.g. VALUES LESS THAN (2098-01-01 00:00:00)).
                 */
                String normalizedSrcUpperBound = srcUpperBound;
                if (srcUpperBound != null) {
                    List<String> normalizedSrcList = normalizedRangePartBoundValueList(ttlInfo,
                        java.util.Collections.singletonList(srcUpperBound), isAlterPartFofCci, tarPartLevel, ec);
                    if (normalizedSrcList != null && !normalizedSrcList.isEmpty()) {
                        normalizedSrcUpperBound = normalizedSrcList.get(0);
                    }
                }
                if (useSubPart) {
                    alterContentPart = String.format(
                        "SPLIT SUBPARTITION `%s` INTO ( \n%s, SUBPARTITION `%s` VALUES LESS THAN (%s) )",
                        srcPartName, partBoundDefs, srcPartName, normalizedSrcUpperBound);
                } else {
                    alterContentPart = String.format(
                        "SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (%s) )",
                        srcPartName, partBoundDefs, srcPartName, normalizedSrcUpperBound);
                }
            }

            return wrapAlterPartStmt(alterContentPart, sqlHint);
        }

        private String generateSingleAddPartsSql(String sqlHint) {
            boolean isAlterPartFofCci = this.params.getBuildForCci();
            TtlDefinitionInfo ttlInfo = params.getTtlInfo();
            PartKeyLevel tarPartLevel = this.params.getTargetPartLevel();
            ExecutionContext ec = params.getEc();
            boolean useSubPart = tarPartLevel == PartKeyLevel.SUBPARTITION_KEY;

            List<String> newPartNameList = new ArrayList<>();
            List<String> newPartBndStrList = new ArrayList<>();
            for (int i = 0; i < newAddPartSpecInfos.size(); i++) {
                Pair<String, String> newPart = newAddPartSpecInfos.get(i);
                String newPartName = newPart.getKey();
                String newPartBndStr = newPart.getValue();

                newPartNameList.add(newPartName);
                newPartBndStrList.add(newPartBndStr);
            }

            String partBoundDefs = "";
            List<String> normalizedNewAddPartBoundList =
                normalizedRangePartBoundValueList(ttlInfo, newPartBndStrList, isAlterPartFofCci, tarPartLevel, ec);
            for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
                String bndStr = normalizedNewAddPartBoundList.get(i);
                String partNameStr = newPartNameList.get(i);
                String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
                String part = null;
                if (useSubPart) {
                    part = String.format("SUBPARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
                } else {
                    part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
                }
                if (!partBoundDefs.isEmpty()) {
                    partBoundDefs += ",\n";
                }
                partBoundDefs += part;
            }

            String alterContentPart = "";

            if (foundMaxValPart) {
                if (useSubPart) {
                    alterContentPart = String.format(
                        "SPLIT SUBPARTITION `%s` INTO ( \n%s, SUBPARTITION `%s` VALUES LESS THAN (MAXVALUE) )",
                        maxValPartName, partBoundDefs, maxValPartName);
                } else {
                    alterContentPart =
                        String.format("SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (MAXVALUE) )",
                            maxValPartName, partBoundDefs, maxValPartName);
                }

            } else {
                if (useSubPart) {
                    alterContentPart = String.format("ADD SUBPARTITION ( %s )", partBoundDefs);
                } else {
                    alterContentPart = String.format("ADD PARTITION ( %s )", partBoundDefs);
                }
            }

            return wrapAlterPartStmt(alterContentPart, sqlHint);
        }

        private String wrapAlterPartStmt(String alterContentPart, String sqlHint) {
            boolean isAlterPartFofCci = this.params.getBuildForCci();
            TtlDefinitionInfo ttlInfo = params.getTtlInfo();
            String primTableSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String primTblName = ttlInfo.getTtlInfoRecord().getTableName();
            String cciName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
            ExecutionContext ec = params.getEc();

            String skipDdlTasks =
                ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_CCI_SKIP_DDL_TASKS);
            String queryHint = sqlHint;
            if (!StringUtils.isEmpty(skipDdlTasks)) {
                queryHint =
                    TtlTaskSqlBuilder.addNewParamsIntoExtraCmdHint(queryHint,
                        String.format("SKIP_DDL_TASKS=\"%s\"", skipDdlTasks));
            }

            String alterPartStmt = "";
            if (!isAlterPartFofCci) {
                alterPartStmt = String.format("%s ALTER TABLE `%s`.`%s` %s", queryHint, primTableSchema, primTblName,
                    alterContentPart);
            } else {
                alterPartStmt =
                    String.format("%s ALTER INDEX `%s` ON TABLE `%s`.`%s` %s", queryHint, cciName, primTableSchema,
                        primTblName, alterContentPart);
            }

            return alterPartStmt;
        }
    }

    @Data
    public static class CleanupPostPartsCalcParams {
        protected TtlDefinitionInfo ttlInfo;
        protected TtlColValueCalcContext calcContext;
        protected boolean cleanupPostPastForCci;
        protected PartKeyLevel targetPartLevel;
        protected String cleanupUpperBoundDatetimeStr;
        protected Integer newAddPartsCount = 0;
        protected ExecutionContext ec;

        public CleanupPostPartsCalcParams() {
        }
    }

    @Data
    public static class CleanupPostPartsCalcResult {
        protected CleanupPostPartsCalcParams params;
        protected List<String> partNamesToBeDropped = new ArrayList<>();

        public CleanupPostPartsCalcResult() {
        }

        public boolean needDropParts() {
            return !partNamesToBeDropped.isEmpty();
        }

        public String generateDropPartsStmtSql(String queryHint) {

            if (partNamesToBeDropped.isEmpty()) {
                return "";
            }

            if (queryHint == null) {
                queryHint = "";
            }

            String partNameListStr = "";
            for (int i = 0; i < partNamesToBeDropped.size(); i++) {
                if (i > 0) {
                    partNameListStr += ",";
                }
                partNameListStr += String.format("`%s`", partNamesToBeDropped.get(i));
            }

            String dropPartSpecListSql = "";
            PartKeyLevel partKeyLevel = params.getTargetPartLevel();
            if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                dropPartSpecListSql = String.format("DROP SUBPARTITION %s", partNameListStr);
            } else {
                dropPartSpecListSql = String.format("DROP PARTITION %s", partNameListStr);
            }

            String alterTblDropStmt = "";
            TtlDefinitionInfo ttlInfo = params.getTtlInfo();
            boolean isDropPartsForCci = params.isCleanupPostPastForCci();
            String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            String ttlTblCciName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
            if (isDropPartsForCci) {
                alterTblDropStmt =
                    String.format("%s ALTER INDEX %s ON TABLE `%s`.`%s` %s", queryHint, ttlTblCciName, ttlTblSchema,
                        ttlTblName, dropPartSpecListSql);
            } else {
                alterTblDropStmt = String.format("%s ALTER TABLE `%s`.`%s` %s", queryHint, ttlTblSchema, ttlTblName,
                    dropPartSpecListSql);
            }

            return alterTblDropStmt;
        }
    }

    public static BuildPrePartCalcResult calcBuildPrePartSpecs(BuildPrePartCalcParams params) {
        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        TableMeta tarTblMeta = params.getTarTblMeta();
        PartKeyLevel partLevel = params.getTargetPartLevel();
        boolean calcPrePartSpecsForCci = params.getBuildForCci();

        ExecutionContext ec = params.getEc();
        TtlColValueCalcContext calcContext = params.getCalcContext();
        String pivotPointValStr = params.getPivotPointValStr();
        String preBuildTargetBoundValStr = params.getPreBuildTargetBoundValStr();
        String ttlTimeZone = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        Integer arcPartInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartTimeUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit());
        Boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();

        Integer preBuildPartCnt = params.getPreBuildNewPartCount();
        Integer postBuildPartCnt = params.getPostBuildNewPartCount();
        List<Pair<String, String>> newAddPartSpecInfosOutputResult = new ArrayList<>();

        /**
         * Calc the pivotPoint which is used to calculate new parts
         */
        PivotPointResult pivotPointResult =
            calcAndBuildPivotPointResult(pivotPointValStr, preBuildTargetBoundValStr, calcPrePartSpecsForCci, ttlInfo,
                ec, calcContext);

        /**
         * Compute the user-reserved partition set for CCI nearest split.
         * Only effective when nearest-split is enabled and gapCount >= 0.
         * A partition whose bound is beyond (pivot + gapCount * arcPartInterval) is treated as
         * user-reserved (transparent to TTL, only usable as split target). gapCount = -1 means
         * infinity (empty reserved set, fully backward-compatible).
         */
        Set<String> reservedPartNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Integer reservedGapCnt = params.getReservedPartGapCount();
        if (Boolean.TRUE.equals(params.getEnableSplitFromNearestPart())
            && reservedGapCnt != null && reservedGapCnt >= 0) {
            PartitionByDefinition partByForReserved = TtlPartitionUtil.getTargetPartBy(tarTblMeta, partLevel);
            reservedPartNames = computeReservedPartNames(partByForReserved, ttlInfo,
                pivotPointResult.getPreBuildPivotPointStr(), reservedGapCnt,
                arcPartInterval, arcPartTimeUnit, calcContext, ec);
        }

        CalcNewPartBoundsByPivotBoundValParams calcParams = new CalcNewPartBoundsByPivotBoundValParams();
        calcParams.setTtlInfo(ttlInfo);
        calcParams.setCalcContext(calcContext);
        calcParams.setPartKeyLevel(partLevel);
        calcParams.setPivotBoundValue(pivotPointResult.getPreBuildPivotPointStr());
        calcParams.setTtlTimeZone(ttlTimeZone);
        calcParams.setPreBuildCnt(preBuildPartCnt);
        calcParams.setPostBuildCnt(postBuildPartCnt);
        calcParams.setArcPartUnit(arcPartTimeUnit);
        calcParams.setArcPartInterval(Long.valueOf(arcPartInterval));
        calcParams.setPreBuildTargetBoundValue(pivotPointResult.getPreBuildTargetBoundValStr());
        calcParams.setIgnoreRoutingCheck(false);
        calcParams.setUseExpireOver(useExpireOver);
        calcParams.setEc(ec);
        calcParams.setNewAddPartSpecInfosOutput(newAddPartSpecInfosOutputResult);
        calcParams.setReservedPartNames(reservedPartNames);

        CalcNewPartBoundsByPivotBoundValWithTblMetaParams buildParams =
            new CalcNewPartBoundsByPivotBoundValWithTblMetaParams();
        buildParams.setCalcParams(calcParams);
        buildParams.setTarTblMeta(tarTblMeta);
        buildParams.setTarPartKeyLevel(partLevel);
        /**
         * Calc the new pre-parts by pivot bound
         */
        calcNewPartBoundsByPivotBoundValWithTblMeta(buildParams);

        PartitionByDefinition partBy = TtlPartitionUtil.getTargetPartBy(tarTblMeta, partLevel);

        /**
         * Find the maxvalue partition and the last TTL-managed non-max-val partition.
         * Reserved partitions (bound beyond threshold) are skipped so that the "last non-max-val"
         * baseline reflects the last managed partition instead of a far-future reserved one.
         */
        String maxValPartName = null;
        List<PartitionSpec> tarPartSpecList = partBy.getPartitions();
        PartitionSpec lastNonMaxValPart = null;
        for (PartitionSpec ps : tarPartSpecList) {
            if (ps.getBoundSpec().containMaxValues()) {
                maxValPartName = ps.getName();
            } else if (!isReservedPart(reservedPartNames, ps.getName())) {
                lastNonMaxValPart = ps;
            }
        }
        boolean foundMaxValPart = !StringUtils.isEmpty(maxValPartName);

        /**
         * Mark sure that the bound values of
         * all new parts are more than the bound value of the all the non-max-val part
         */
        Map<String, TtlColBoundValue> newPartBndValMappings =
            buildParams.getCalcParams().getNewAddPartSpecMappingsOutput();
        List<Pair<String, String>> finalNewAddPartSpecInfos = new ArrayList<>();
        if (lastNonMaxValPart != null) {
            SearchDatumInfo bndValOfLastNonMaxValPart = lastNonMaxValPart.getBoundSpec().getSingleDatum();
            PartitionBoundValueKind bndValKind = bndValOfLastNonMaxValPart.getSingletonValue().getValueKind();
            if (bndValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                PartitionField fldOfBndValOfLastNonMaxValPart =
                    bndValOfLastNonMaxValPart.getSingletonValue().getValue();

//                DataType partColDt = partBy.getPartitionColumnTypeList().get(0);
                PartitionIntFunction partIntFunc = partBy.getPartIntFunc();
                ExecutionContext newEc = ec.copy();
                InternalTimeZone tz = TimeZoneUtils.convertFromMySqlTZ(ttlTimeZone);
                newEc.setTimeZone(tz);
                for (int i = 0; i < newAddPartSpecInfosOutputResult.size(); i++) {
                    Pair<String, String> newPart = newAddPartSpecInfosOutputResult.get(i);
                    String newPartName = newPart.getKey();
//                    String newPartBndStr = newPart.getValue();
                    TtlColBoundValue newPartBndVal = newPartBndValMappings.get(newPartName);

                    PartitionField fldOfNewPartBnd = newPartBndVal.getPartColValFld();
                    PartitionField partFuncFldOfNewPartBnd = null;
                    if (partIntFunc != null) {
                        partFuncFldOfNewPartBnd =
                            PartitionPrunerUtils.buildPartFieldByEvalPartFuncExpr(fldOfNewPartBnd, partBy, newEc);
                    } else {
                        partFuncFldOfNewPartBnd = fldOfNewPartBnd;
                    }

                    /**
                     * Mark sure that the bound values of
                     * all new parts are more than the bound value of the all the non-max-val part
                     */
                    if (partFuncFldOfNewPartBnd.compareTo(fldOfBndValOfLastNonMaxValPart) > 0) {
                        finalNewAddPartSpecInfos.add(newPart);
                    }
                }
            }
        }

        BuildPrePartCalcResult result = new BuildPrePartCalcResult();
        result.setParams(params);
        result.setMaxValPartName(maxValPartName);
        result.setFoundMaxValPart(!StringUtils.isEmpty(maxValPartName));
        result.setNewAddPartSpecInfos(finalNewAddPartSpecInfos);

        /**
         * Try to find a nearest non-maxval partition as the single split source.
         * This is only enabled when the switch is on and there are new partitions to add.
         */
        if (Boolean.TRUE.equals(params.getEnableSplitFromNearestPart()) && !finalNewAddPartSpecInfos.isEmpty()) {
            fillNearestSplitSource(result, tarPartSpecList, newPartBndValMappings, partBy, ec);
        }

        return result;
    }

    /**
     * Find the nearest existing range partition that can contain all new parts as the single split source.
     * If no suitable non-maxval partition found, fallback to the maxval partition.
     */
    private static void fillNearestSplitSource(BuildPrePartCalcResult result,
                                               List<PartitionSpec> tarPartSpecList,
                                               Map<String, TtlColBoundValue> newPartBndValMappings,
                                               PartitionByDefinition partBy,
                                               ExecutionContext ec) {
        List<PartitionSpec> nonMaxValParts = new ArrayList<>();
        PartitionSpec maxValPart = null;
        for (PartitionSpec spec : tarPartSpecList) {
            if (spec.getBoundSpec().containMaxValues()) {
                maxValPart = spec;
            } else {
                nonMaxValParts.add(spec);
            }
        }

        List<Pair<String, String>> finalNewAddPartSpecInfos = result.getNewAddPartSpecInfos();
        Pair<String, String> firstNewPart = finalNewAddPartSpecInfos.get(0);
        TtlColBoundValue firstBndVal = newPartBndValMappings.get(firstNewPart.getKey());
        if (firstBndVal == null) {
            return;
        }

        PartitionIntFunction partIntFunc = partBy.getPartIntFunc();
        ExecutionContext newEc = ec.copy();
        InternalTimeZone tz =
            TimeZoneUtils.convertFromMySqlTZ(result.getParams().getTtlInfo().getTtlInfoRecord().getTtlTimezone());
        newEc.setTimeZone(tz);

        PartitionField firstPartFuncFld =
            buildPartFuncField(firstBndVal.getPartColValFld(), partBy, partIntFunc, newEc);
        if (firstPartFuncFld == null) {
            return;
        }

        PartitionSpec srcPart = findFirstSrcWhoseUpperBoundGreaterThan(firstPartFuncFld, nonMaxValParts);
        boolean useMaxValAsSrc = (srcPart == null);
        if (useMaxValAsSrc) {
            srcPart = maxValPart;
        }

        if (srcPart != null && !useMaxValAsSrc) {
            PartitionField srcUpper = getPartFuncUpperBound(srcPart, partBy, partIntFunc, newEc);
            if (srcUpper == null) {
                srcPart = maxValPart;
                useMaxValAsSrc = true;
            } else {
                for (Pair<String, String> newPart : finalNewAddPartSpecInfos) {
                    TtlColBoundValue bndVal = newPartBndValMappings.get(newPart.getKey());
                    if (bndVal == null) {
                        continue;
                    }
                    PartitionField partFuncFld =
                        buildPartFuncField(bndVal.getPartColValFld(), partBy, partIntFunc, newEc);
                    if (partFuncFld == null) {
                        continue;
                    }
                    if (partFuncFld.compareTo(srcUpper) >= 0) {
                        srcPart = maxValPart;
                        useMaxValAsSrc = true;
                        break;
                    }
                }
            }
        }

        if (srcPart != null) {
            result.setSplitSourcePartName(srcPart.getName());
            result.setSplitSourceIsMaxVal(useMaxValAsSrc);
            if (!useMaxValAsSrc) {
                TtlDefinitionInfo ttlInfo = result.getParams().getTtlInfo();
                TtlColValueCalcContext calcContext = result.getParams().getCalcContext();
                if (calcContext == null && ttlInfo != null && ttlInfo.isTtlColUseFuncExpr()) {
                    calcContext = TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);
                }
                BuildPartFieldStringParams buildParams =
                    BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo, calcContext);
                result.setSplitSourceUpperBound(convertPartSpecUpperBoundToString(srcPart, buildParams));
            }
            result.setFinalNewPartsForSplit(finalNewAddPartSpecInfos);
        }
    }

    private static PartitionSpec findFirstSrcWhoseUpperBoundGreaterThan(PartitionField targetBndFld,
                                                                        List<PartitionSpec> nonMaxValParts) {
        for (PartitionSpec spec : nonMaxValParts) {
            SearchDatumInfo bndValInfo = spec.getBoundSpec().getSingleDatum();
            if (bndValInfo == null || bndValInfo.getSingletonValue() == null) {
                continue;
            }
            PartitionBoundValueKind bndValKind = bndValInfo.getSingletonValue().getValueKind();
            if (bndValKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                continue;
            }
            PartitionField upperFld = bndValInfo.getSingletonValue().getValue();
            if (upperFld == null) {
                continue;
            }
            if (upperFld.compareTo(targetBndFld) > 0) {
                return spec;
            }
        }
        return null;
    }

    /**
     * Whether the given partition is classified as a user-reserved partition.
     * A null/empty reserved set means no reserved partitions (fully backward-compatible).
     */
    private static boolean isReservedPart(Set<String> reservedPartNames, String partName) {
        return reservedPartNames != null && reservedPartNames.contains(partName);
    }

    /**
     * Compute the set of existing range partitions that are classified as user-reserved.
     * A non-maxvalue partition whose (part-func) upper bound is beyond
     * threshold = pivot + gapCount * arcPartInterval is treated as user-reserved.
     * Returns an empty set on any failure so that the caller falls back to the original behavior.
     */
    private static Set<String> computeReservedPartNames(PartitionByDefinition partBy,
                                                        TtlDefinitionInfo ttlInfo,
                                                        String pivotValStr,
                                                        int gapCount,
                                                        Integer arcPartInterval,
                                                        TtlTimeUnit arcPartUnit,
                                                        TtlColValueCalcContext calcContext,
                                                        ExecutionContext ec) {
        Set<String> reserved = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        try {
            ColumnMeta partColMeta = partBy.getPartitionFieldList().get(0);
            PartKeyLevel partKeyLevel = partBy.getPartLevel();
            boolean usePartFunc = partBy.getPartIntFunc() != null;
            TtlColValueCalcContext ctx = calcContext;
            if (ctx == null) {
                ctx = TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);
            }
            TtlColBoundValue pivotVal =
                TtlColBoundValue.buildPartBoundValue(pivotValStr, partColMeta, partKeyLevel, usePartFunc, ttlInfo, ctx);
            TtlColBoundValue thresholdVal =
                pivotVal.plusInterval((long) gapCount * arcPartInterval, arcPartUnit, ctx);

            PartitionIntFunction partIntFunc = partBy.getPartIntFunc();
            ExecutionContext newEc = ec.copy();
            InternalTimeZone tz = TimeZoneUtils.convertFromMySqlTZ(ttlInfo.getTtlInfoRecord().getTtlTimezone());
            newEc.setTimeZone(tz);
            PartitionField thresholdFld =
                buildPartFuncField(thresholdVal.getPartColValFld(), partBy, partIntFunc, newEc);
            if (thresholdFld == null) {
                return reserved;
            }
            for (PartitionSpec spec : partBy.getPartitions()) {
                if (spec.getBoundSpec().containMaxValues()) {
                    continue;
                }
                PartitionField upperFld = getPartFuncUpperBound(spec, partBy, partIntFunc, newEc);
                if (upperFld == null) {
                    continue;
                }
                if (upperFld.compareTo(thresholdFld) > 0) {
                    reserved.add(spec.getName());
                }
            }
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn("failed to compute reserved part names for cci nearest split", ex);
            reserved.clear();
        }
        return reserved;
    }

    private static PartitionField buildPartFuncField(PartitionField partFld,
                                                     PartitionByDefinition partBy,
                                                     PartitionIntFunction partIntFunc,
                                                     ExecutionContext newEc) {
        if (partFld == null) {
            return null;
        }
        if (partIntFunc != null) {
            return PartitionPrunerUtils.buildPartFieldByEvalPartFuncExpr(partFld, partBy, newEc);
        }
        return partFld;
    }

    private static PartitionField getPartFuncUpperBound(PartitionSpec spec,
                                                        PartitionByDefinition partBy,
                                                        PartitionIntFunction partIntFunc,
                                                        ExecutionContext newEc) {
        SearchDatumInfo bndValInfo = spec.getBoundSpec().getSingleDatum();
        if (bndValInfo == null || bndValInfo.getSingletonValue() == null) {
            return null;
        }
        PartitionBoundValueKind bndValKind = bndValInfo.getSingletonValue().getValueKind();
        if (bndValKind != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
            return null;
        }
        PartitionField upperFld = bndValInfo.getSingletonValue().getValue();
        if (upperFld == null) {
            return null;
        }
        if (partIntFunc != null) {
            return PartitionPrunerUtils.buildPartFieldByEvalPartFuncExpr(upperFld, partBy, newEc);
        }
        return upperFld;
    }

    private static String convertPartSpecUpperBoundToString(PartitionSpec spec,
                                                            BuildPartFieldStringParams params) {
        SearchDatumInfo bndValInfo = spec.getBoundSpec().getSingleDatum();
        if (bndValInfo == null || bndValInfo.getSingletonValue() == null) {
            return null;
        }
        return convertPartSpecBoundValToString(bndValInfo.getSingletonValue(), params);
    }

    protected static @NotNull PivotPointResult calcAndBuildPivotPointResult(String pivotPointValStr,
                                                                            String specifiedPreBuildTargetBoundValStr,
                                                                            boolean calcPrePartSpecsForCci,
                                                                            TtlDefinitionInfo ttlInfo,
                                                                            ExecutionContext ec,
                                                                            TtlColValueCalcContext calcContext) {
        Boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();
        Boolean archiveByPartition = ttlInfo.performArchiveByPartitionOrSubPartition();

        String preBuildTargetBoundValStr = null;
        /**
         * when using expireOver policy, the input of pivotPointValStr will be null
         */
        String preBuildPivotPointStr = pivotPointValStr;
        if (calcPrePartSpecsForCci && useExpireOver && archiveByPartition) {
            String ttlTblSchemaName = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            TableMeta primTableMeta = ec.getSchemaManager(ttlTblSchemaName).getTable(ttlTblName);

            /**
             * Fetch the partInfo for primTbl
             */
            PartitionInfo primPartInfo = primTableMeta.getPartitionInfo();
            PartKeyLevel primTtlPartLevel = PartKeyLevel.PARTITION_KEY;
            if (ttlInfo.performArchiveBySubPartition()) {
                primTtlPartLevel = PartKeyLevel.SUBPARTITION_KEY;
            }

            /**
             * Fetch the minBndVal and maxBndVal from primTtlPartInfo (ignore max-val part)
             */
            Pair<String, SearchDatumInfo> minBndValDatumInfoIgnoreMaxValBndOfPrimTbl =
                TtlPartitionUtil.findMinPartBoundValAndPartNameFromNonMaxValParts(primPartInfo, primTtlPartLevel);
            Pair<String, SearchDatumInfo> maxBndValDatumInfoIgnoreMaxValBndOfPrimTbl =
                TtlPartitionUtil.findMaxPartBoundValAndPartNameFromNonMaxValParts(primPartInfo, primTtlPartLevel);

            /**
             * The addPartSql of cci on expireOver policy must include all the bound range
             * from both
             * minBndVal of primTtlPartInfo (preBuildPivotPointStr)
             * and
             * maxBndVal of primTtlPartInfo (preBuildTargetBoundValStr)
             */
            BuildPartFieldStringParams params =
                BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo, calcContext);
            preBuildPivotPointStr = convertPartSpecBoundValToString(
                minBndValDatumInfoIgnoreMaxValBndOfPrimTbl.getValue().getSingletonValue(), params);
            preBuildTargetBoundValStr = convertPartSpecBoundValToString(
                maxBndValDatumInfoIgnoreMaxValBndOfPrimTbl.getValue().getSingletonValue(), params);
            if (specifiedPreBuildTargetBoundValStr != null) {
                preBuildTargetBoundValStr = specifiedPreBuildTargetBoundValStr;
            }
        }
        String formatedPreBuildPivotPointStr = null;
        if (useExpireOver) {
            /**
             * When using expireOver policy,
             * the preBuildPivotPointStr from the first part bound of ttlTblPart,
             * maybe a non-formated bound value,
             * so here do the formatting.
             */
            formatedPreBuildPivotPointStr =
                TtlJobUtil.formatPivotPointStringByTtlUnitIfNeed(ec, ttlInfo, preBuildPivotPointStr);
        } else {
            formatedPreBuildPivotPointStr = preBuildPivotPointStr;
        }

        PivotPointResult result = new PivotPointResult(formatedPreBuildPivotPointStr, preBuildTargetBoundValStr);
        return result;
    }

    /**
     * Convert bound value to string,
     * if the bound is from int-datatype ttl_col which use func expr encoding,
     * then convert it into iso-formated datetime string
     * (decode ttl_col if need)
     */
    private static String convertPartSpecBoundValToString(PartitionBoundVal boundVal,
                                                          BuildPartFieldStringParams params) {
        return convertPartFieldToIsoFormatedDatetimeString(boundVal.getValue(), params);
    }

    /**
     * Convert the PartitionField of bound value of its original datatype of ttl_col into
     * the iso-formated datatype string if need
     * (decoding ttl_col)
     */
    private static String convertPartFieldToIsoFormatedDatetimeString(PartitionField partBndFld,
                                                                      BuildPartFieldStringParams params) {
        if (partBndFld.isNull()) {
            return null;
        }

        String partFldStr = partBndFld.stringValue().toStringUtf8();
        boolean isDateType = DataTypeUtil.isDateType(partBndFld.dataType());
        TtlColValueCalcContext calcContext = params.getCalcContext();
        if (params != null) {
            if (params.isForceReturnPartFldString()) {
                if (isDateType) {
                    partFldStr = partBndFld.datetimeValue().toDatetimeString(0);
                }
                return partFldStr;
            }

            /**
             * Perform ttl_col decoding
             */
            boolean needConvert = params.isNeedConvertNumberToTimestampOrDate();
            if (needConvert) {
                DataType dt = partBndFld.dataType();
                if (!DataTypeUtil.isNumberSqlType(dt)) {
                    return partFldStr;
                }
//                boolean needConvertToDate = params.isNeedConvertNumberToDate();
//                if (needConvertToDate) {
//                    //...
//                    throw new UnsupportedOperationException();
//                }
//
//                long uxtsNumLongVal = partBndFld.longValue();
//                boolean treatNumAsTsMillSec = params.isTreatAsUnixTimestampMillsSec();
//                if (treatNumAsTsMillSec) {
//                    uxtsNumLongVal = uxtsNumLongVal / 1000;
//                }
//                String targetTz = params.getTargetTimeZone();
//                String tsStr = convertUnixTimestampToZonedDateTimeString(uxtsNumLongVal, targetTz);
//                partFldStr = tsStr;

                String isoDateTimeFormatedString = null;
                try {
                    PartitionField rs = TtlColBoundValue.decodeTtlCol(partBndFld, calcContext);
                    isoDateTimeFormatedString = rs.stringValue().toStringUtf8();
                    partFldStr = isoDateTimeFormatedString;
                } catch (Throwable ex) {
                    String errMsg = String.format("Failed to perform decoding ttl col by using decoder expr");
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(errMsg, ex);
                    throw new TtlJobRuntimeException(ex, errMsg);
                }
            }
        }
        return partFldStr;
    }

    public static String convertUnixTimestampToZonedDateTimeString(long unixTimestampNum,
                                                                   String tarTimeZone) {
        long timestamp = unixTimestampNum;
        Instant instant = Instant.ofEpochSecond(timestamp);
        ZoneId targetTz = ZoneId.of(tarTimeZone);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, targetTz);
        DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
        String formattedDateTime = dateTime.format(formatter);
        return formattedDateTime;
    }

    public static long convertZonedDateTimeStringToUnixTimestamp(
        String dateTimeString,
        String tarTimeZone
    ) {
        String timeZone = tarTimeZone; // e.g：'+08:00'
        DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
        ZonedDateTime zonedDateTime = ZonedDateTime.of(
            java.time.LocalDateTime.parse(dateTimeString, formatter),
            ZoneId.of(timeZone)
        );
        long unixTimestamp = zonedDateTime.toEpochSecond();
        return unixTimestamp;
    }

    private static class PivotPointResult {
        /**
         * The start boundValue point for calculating new parts
         */
        public final String preBuildPivotPointStr;

        /**
         * The end boundValue point for calculating new parts
         */
        public final String preBuildTargetBoundValStr;

        public PivotPointResult(String preBuildPivotPointStr, String preBuildTargetBoundValStr) {
            this.preBuildPivotPointStr = preBuildPivotPointStr;
            this.preBuildTargetBoundValStr = preBuildTargetBoundValStr;
        }

        public String getPreBuildPivotPointStr() {
            return preBuildPivotPointStr;
        }

        public String getPreBuildTargetBoundValStr() {
            return preBuildTargetBoundValStr;
        }
    }

    public static CreateArcCciPartByDefResult calcCreateArcCciPartByDef(CreateArcCciPartByDefCalcParams params) {

        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        PartKeyLevel cciPartLevel = PartKeyLevel.PARTITION_KEY;

        ExecutionContext ec = params.getEc();
        TtlColValueCalcContext calcContext = TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);
        TableMeta ttlTblMeta = params.getTtlTblMeta();
        String pivotPointValStr = params.getPivotPointValStr();
        String ttlTimeZone = ttlInfo.getTtlInfoRecord().getTtlTimezone();

        Integer ttlInterval = ttlInfo.getTtlInfoRecord().getTtlInterval();
        TtlTimeUnit ttlUnitVal = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getTtlUnit());
        Integer arcPartInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartTimeUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit());
        boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();
        boolean preBuildForCci = params.getBuildForCci();

        int preBuiltPartCntForFuture = ttlInfo.getTtlInfoRecord().getArcPrePartCnt();
        int postBuiltPartCntForPast = ttlInfo.getTtlInfoRecord().getArcPostPartCnt();

        List<Pair<String, String>> newAddPartSpecInfosOutputResult = new ArrayList<>();
        int finalPreBuildPartCnt =
            TtlJobUtil.decidePreBuiltPartCnt(preBuiltPartCntForFuture, ttlInterval, ttlUnitVal, arcPartInterval,
                arcPartTimeUnit);

        PivotPointResult pivotPointResult =
            calcAndBuildPivotPointResult(pivotPointValStr, null, preBuildForCci, ttlInfo, ec, calcContext);

        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        ColumnMeta ttlColMeta = ttlTblMeta.getColumn(ttlColName);

        /**
         * Here need check if partCol use part func
         */
        boolean usePartFunc = false;

        CalcNewPartBoundsByPivotBoundValParams buildParams = new CalcNewPartBoundsByPivotBoundValParams();

        buildParams.setTtlInfo(ttlInfo);
        buildParams.setCalcContext(calcContext);
        buildParams.setPivotBoundValue(pivotPointResult.getPreBuildPivotPointStr());
        buildParams.setTtlTimeZone(ttlTimeZone);
        buildParams.setPreBuildCnt(finalPreBuildPartCnt);
        buildParams.setPostBuildCnt(postBuiltPartCntForPast);
        buildParams.setPreBuildTargetBoundValue(pivotPointResult.getPreBuildTargetBoundValStr());
        buildParams.setArcPartUnit(arcPartTimeUnit);
        buildParams.setArcPartInterval(Long.valueOf(arcPartInterval));
        buildParams.setUseExpireOver(useExpireOver);
        buildParams.setIgnoreRoutingCheck(true);
        buildParams.setTtlPartitionRouter(null);
        buildParams.setPartColMeta(ttlColMeta);
        buildParams.setPartKeyLevel(cciPartLevel);
        buildParams.setUsePartFunc(usePartFunc);
        buildParams.setEc(ec);
        buildParams.setNewAddPartSpecInfosOutput(newAddPartSpecInfosOutputResult);
        calcNewPartBoundsByPivotBoundVal(buildParams);

        if (ec != null) {
            Boolean needAutoAddMaxValPart =
                ec.getParamManager().getBoolean(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING);
            if (needAutoAddMaxValPart) {
                Pair<String, String> maxValPartNameAndBndVal =
                    new Pair<>(TtlTaskSqlBuilder.ARC_TBL_MAXVAL_PART_NAME, TtlTaskSqlBuilder.ARC_TBL_MAXVALUE_BOUND);
                newAddPartSpecInfosOutputResult.add(maxValPartNameAndBndVal);
            }
        }

        CreateArcCciPartByDefResult result = new CreateArcCciPartByDefResult();
        result.setParams(params);
        result.setNewCreatedPartSpecInfos(newAddPartSpecInfosOutputResult);

        return result;
    }

    public static CleanupPostPartsCalcResult calcCleanupPostParts(CleanupPostPartsCalcParams params) {
        CleanupPostPartsCalcResult result = new CleanupPostPartsCalcResult();
        result.setParams(params);

        boolean cleanupForCci = params.isCleanupPostPastForCci();
        String pivotDatetimeStr = params.getCleanupUpperBoundDatetimeStr();
        Integer newAddPartsCount = params.getNewAddPartsCount();
        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        TtlColValueCalcContext calcContext = params.getCalcContext();
        PartKeyLevel partKeyLevel = params.getTargetPartLevel();
        ExecutionContext ec = params.getEc();

        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ttlTimeZone = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        boolean expireByOver = ttlInfo.useExpireOverPartitionsPolicy();

        TableMeta tarTblMeta = null;
        String tarTblSchema = null;
        String tarTblName = null;
        if (cleanupForCci) {
            tarTblSchema = ttlInfo.getArchiveTableSchema();
            tarTblName = ttlInfo.getTmpTableName();
            tarTblMeta = TtlJobUtil.getArchiveCciTableMeta(ttlTblSchema, ttlTblName, ec);
        } else {
            tarTblSchema = ttlTblSchema;
            tarTblName = ttlTblName;
            tarTblMeta = ec.getSchemaManager(ttlTblSchema).getTableWithNull(ttlTblName);
        }

        if (tarTblMeta == null) {
            // throw ex
            throw new TtlJobRuntimeException(
                String.format("No found any tableMeta for `%s`.`%s`", tarTblSchema, tarTblName));
        }

        List<String> partListToBeDropped = null;
        if (expireByOver) {
            partListToBeDropped =
                calcExpiredPartsByExpiredOverPolicy(ec, calcContext, tarTblMeta, partKeyLevel, newAddPartsCount);
        } else {
            partListToBeDropped =
                calcExpiredPartsByExpiredAfterPolicy(ec, calcContext, tarTblMeta, ttlInfo, partKeyLevel, ttlTimeZone,
                    pivotDatetimeStr);
        }

        result.setPartNamesToBeDropped(partListToBeDropped);
        return result;
    }

    private static @NotNull List<String> calcExpiredPartsByExpiredAfterPolicy(ExecutionContext ec,
                                                                              TtlColValueCalcContext calcContext,
                                                                              TableMeta tarTblMeta,
                                                                              TtlDefinitionInfo primTtlInfo,
                                                                              PartKeyLevel partKeyLevel,
                                                                              String ttlTimeZone,
                                                                              String cleanupUpperBoundDatetimeStr) {
        /**
         * Route the pivotDatetimeStr to target partitions
         */
        String tarPartName = TtlPartitionUtil.findTargetNonMaxValPartByRoutingTtlColVal(ec, tarTblMeta,
            primTtlInfo,
            partKeyLevel,
            ttlTimeZone,
            cleanupUpperBoundDatetimeStr, calcContext);

        List<String> partListToBeDropped = new ArrayList<>();
        if (tarPartName != null) {
            PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, partKeyLevel);

            List<PartitionSpec> allPartSpecs = tarPartBy.getPartitions();
            boolean findTarPartName = false;
            int tarPartIndex = -1;
            for (int i = 0; i < allPartSpecs.size(); i++) {
                PartitionSpec part = allPartSpecs.get(i);
                String partName = part.getName();
                if (partName.equalsIgnoreCase(tarPartName)) {
                    findTarPartName = true;
                    tarPartIndex = i;
                    break;
                }
            }

            /**
             * All the parts from first part to the part before tarPartName,
             * can be dropped
             */

            if (findTarPartName) {
                for (int i = 0; i < tarPartIndex; i++) {
                    partListToBeDropped.add(allPartSpecs.get(i).getName());
                }
            }
        }
        return partListToBeDropped;
    }

    private static @NotNull List<String> calcExpiredPartsByExpiredOverPolicy(ExecutionContext ec,
                                                                             TtlColValueCalcContext calcContext,
                                                                             TableMeta tarTblMeta,
                                                                             PartKeyLevel partKeyLevel,
                                                                             Integer newAddPartsCount) {
        List<String> partListToBeDropped = new ArrayList<>();
        PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, partKeyLevel);
        int partCnt = tarPartBy.getPartitions().size();
        TtlDefinitionInfo ttlInfo = tarTblMeta.getTtlDefinitionInfo();
        int expireOverCnt = ttlInfo.getTtlInfoRecord().getArcPrePartCnt();

        int partCntToBeDropped =
            TtlPartitionUtil.decideDropPartsCountForTtlTblWithExpiredOverPolicy(partCnt, newAddPartsCount,
                expireOverCnt);

        List<PartitionSpec> partitionSpecs = tarPartBy.getPartitions();
        for (int i = 0; i < partCntToBeDropped; i++) {
            PartitionSpec p = partitionSpecs.get(i);
            partListToBeDropped.add(p.getName());
        }
        return partListToBeDropped;
    }

    /**
     * calculate the new partitions by Pivot TimePoint
     *
     * <pre>
     *  case1: there some Covered partitions between old parts and new parts
     *
     *     Time --------|------|------|-----...-----|---------------->
     *              [ p1Bnd, p2Bnd, p3Bnd, ... , pnBnd ( exclude pmax ) )
     *
     *                                  [ newP1Bnd, newP2Bnd, newP3Bnd, ... , newPmBnd )
     *     Time -----------------------------|---------|---------|------...------|--------------->
     *
     *  so, add the following new parts (because newP1Bnd < pnBnd < newP2Bnd ):
     *          newP2Bnd, newP3Bnd, ... , newPmBnd
     *
     *  case2: there no Covered partitions between old parts and new parts
     *
     *     Time --------|------|------|-----...-----|---------------->
     *              [ p1Bnd, p2Bnd, p3Bnd, ... , pnBnd ( exclude pmax ) )
     *
     *                                                  [ newP1Bnd, newP2Bnd, newP3Bnd, ... , newPmBnd )
     *     Time --------------------------------------------|---------|---------|-------...-----|--------------->
     *
     *  so, add the following new parts (because pnBnd < newP1Bnd ):
     *          newP1Bnd, newP3Bnd, ... , newPmBnd
     *
     *  case3: there all Covered partitions between old parts and new parts
     *
     *     Time --------|------|------|----...------|---------------->
     *              [ p1Bnd, p2Bnd, p3Bnd, ... , pnBnd ( exclude pmax ) )
     *
     *         [ newP1Bnd, newP2Bnd, newP3Bnd, ... , newPmBnd )
     *     Time ----|---------|---------|------...-----|--------------->
     *
     *  so, add the following new parts (because pnBnd < newPmBnd ):
     *          PmBnd
     *
     * </pre>
     */
//    protected static void calcNewPartBoundsByPivotTimePoint(String pivotDatetime,
//                                                            String ttlTimeZone,
//                                                            Integer preBuildCnt,
//                                                            Integer postBuildCnt,
//                                                            TtlTimeUnit arcPartTimeUnit,
//                                                            Integer arcPartInterval,
//                                                            boolean ignoreRoutingCheck,
//                                                            TableMeta targetTblMeta,
//                                                            PartKeyLevel partKeyLevel,
//                                                            ExecutionContext ec,
//                                                            List<Pair<String, String>> newAddPartSpecInfosOutput) {
//
//        try {
//            DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
//            LocalDateTime pivotTimePoint = LocalDateTime.parse(pivotDatetime, formatter);
//
//            List<LocalDateTime> newPostPartBoundValList = new ArrayList<>();
//            if (postBuildCnt != null && postBuildCnt > 0) {
//                LocalDateTime newPostBound = pivotTimePoint;
//                for (int i = 0; i < postBuildCnt; i++) {
//                    newPostBound = TtlJobUtil.plusDeltaIntervals(newPostBound, arcPartTimeUnit, -1 * arcPartInterval);
//                    String newPostPartBndValStr = newPostBound.format(formatter);
//
//                    String tarPartName = null;
//                    if (!ignoreRoutingCheck) {
//                        tarPartName = findTargetNonMaxValPartByRoutingTtlColVal(ec, targetTblMeta,
//                            partKeyLevel,
//                            ttlTimeZone,
//                            newPostPartBndValStr);
//                        if (tarPartName == null) {
//                            /**
//                             * No found any non-max-value parts
//                             */
//                            newPostPartBoundValList.add(newPostBound);
//                        }
//                    } else {
//                        newPostPartBoundValList.add(newPostBound);
//                    }
//                }
//            }
//
//            List<LocalDateTime> newPrePartBoundValList = new ArrayList<>();
//            newPrePartBoundValList.add(pivotTimePoint);
//            LocalDateTime newBound = pivotTimePoint;
//            for (int i = 0; i < preBuildCnt; i++) {
//                newBound = TtlJobUtil.plusDeltaIntervals(newBound, arcPartTimeUnit, arcPartInterval);
//                String newPartBndValStr = newBound.format(formatter);
//                if (!ignoreRoutingCheck) {
//                    String tarPartName = findTargetNonMaxValPartByRoutingTtlColVal(ec, targetTblMeta,
//                        partKeyLevel,
//                        ttlTimeZone,
//                        newPartBndValStr);
//                    if (tarPartName == null) {
//                        /**
//                         * No found any non-max-value parts
//                         */
//                        newPrePartBoundValList.add(newBound);
//                    }
//                } else {
//                    newPrePartBoundValList.add(newBound);
//                }
//            }
//
//            List<LocalDateTime> finalNewPartInfos = new ArrayList<>();
//
//            for (int i = newPostPartBoundValList.size() - 1; i > -1; --i) {
//                finalNewPartInfos.add(newPostPartBoundValList.get(i));
//            }
//            finalNewPartInfos.addAll(newPrePartBoundValList);
//
//            if (newAddPartSpecInfosOutput != null) {
//                if (!finalNewPartInfos.isEmpty()) {
//                    boolean buildForSubPartTempName = partKeyLevel == PartKeyLevel.SUBPARTITION_KEY;
//                    for (int i = 0; i < finalNewPartInfos.size(); i++) {
//                        LocalDateTime newBndVal = finalNewPartInfos.get(i);
//                        String newBndValStr = newBndVal.format(formatter);
////                        LocalDateTime bndValForPartName =
////                            TtlJobUtil.plusDeltaIntervals(newBndVal, arcPartTimeUnit, -1 * rngInterval);
//                        String bndValForPartNameStr =
//                            buildPartNameByBoundValue(newBndVal, arcPartTimeUnit, buildForSubPartTempName);
//                        Pair<String, String> newPartNameAndBndVal = new Pair<>(bndValForPartNameStr, newBndValStr);
//                        newAddPartSpecInfosOutput.add(newPartNameAndBndVal);
//                    }
//                }
//            }
//        } catch (Throwable ex) {
//            throw new TtlJobRuntimeException(ex);
//        }
//    }

    /**
     * The Context for calc new bound value of ttl_tbl or arc_tbl
     */
    @Data
    public static class TtlColValueCalcContext {

        protected TtlDefinitionInfo ttlInfo = null;
        protected boolean useTtlEncoding = false;
        protected boolean useTtlColFunc = false;
        protected SQLExpr ttlColEncoderExpr = null;
        protected SQLExpr ttlColDecoderExpr = null;
        protected ExecutionContext ec;

        protected TtlColValueCalcContext() {
        }

        protected void init() {
            try {

                boolean needTtlEncoding = false;
                String ttlColEncoder = null;
                String ttlColDecoder = null;
                TtlColFuncExprInfo ttlColFuncExprInfo = ttlInfo.getTtlColFuncExprInfo();
                boolean useTtlColFunc = false;
                if (ttlColFuncExprInfo != null) {
                    ttlColEncoder = ttlInfo.getTtlColFuncExprInfo().getTtlColFullEncoderStr();
                    ttlColDecoder = ttlInfo.getTtlColFuncExprInfo().getTtlColFullDecoderStr();
                    needTtlEncoding = true;
                    useTtlColFunc = true;
                }
                this.useTtlEncoding = needTtlEncoding;
                this.useTtlColFunc = useTtlColFunc;
                if (!useTtlEncoding) {
                    return;
                }

//                if (useTtlColFunc) {
//                    if (ttlColFuncExprInfo.isTreatTtlColAsUnixTimestampSeconds()) {
//                        ttlColEncoder = "UNIX_TIMESTAMP(?)";
//                        ttlColDecoder = "FROM_UNIXTIME(?)";
//                    } else if (ttlColFuncExprInfo.isTreatTtlColAsUnixTimestampMillSeconds()) {
//                        ttlColEncoder = "UNIX_TIMESTAMP(?) * 1000";
//                        ttlColDecoder = "FROM_UNIXTIME(? / 1000)";
//                    } else if (ttlColFuncExprInfo.isTreatTtlColAsToDaysNumber()) {
//                        ttlColEncoder = "TO_DAYS(?)";
//                        ttlColDecoder = "FROM_DAYS(?)";
//                    }
//                }

                SQLExpr ttlColEncoderExprVal = TtlUtil.parseExprString(ttlColEncoder);
                this.ttlColEncoderExpr = ttlColEncoderExprVal;

                SQLExpr ttlColDecoderExprVal = TtlUtil.parseExprString(ttlColDecoder);
                this.ttlColDecoderExpr = ttlColDecoderExprVal;
            } catch (Throwable ex) {
                throw new TtlJobRuntimeException(ex);
            }
        }

        public String buildTtlEncoderExprSql(String datetimeStr) {
            SQLExpr ttlEncoderExpr = this.ttlColEncoderExpr;
            String sqlContent = null;
            if (ttlEncoderExpr != null) {
                String datetimeStrWrappedQuotes = String.format("'%s'", datetimeStr);
                String ttlColEncoderStrVal =
                    TtlUtil.replaceParamsAndBuildExprSql(datetimeStrWrappedQuotes, ttlEncoderExpr);
                sqlContent = ttlColEncoderStrVal;
            }
            return sqlContent;
        }

        public String buildTtlDecoderExprSql(String intStr) {
            SQLExpr ttlDecoderExpr = this.ttlColDecoderExpr;
            String sqlContent = null;
            if (ttlDecoderExpr != null) {
                String ttlColEncoderStrVal = TtlUtil.replaceParamsAndBuildExprSql(intStr, ttlDecoderExpr);
                sqlContent = ttlColEncoderStrVal;
            }
            return sqlContent;
        }

        public static TtlColValueCalcContext buildBoundValueCalcContext(TtlDefinitionInfo ttlInfo,
                                                                        ExecutionContext ec) {
            TtlColValueCalcContext ctx = new TtlColValueCalcContext();
            ctx.setTtlInfo(ttlInfo);
            ctx.setEc(ec);
            ctx.init();
            return ctx;
        }

        public ExecutionContext getEc() {
            return ec;
        }

        public void setEc(ExecutionContext ec) {
            this.ec = ec;
        }
    }

    /**
     * A middle bound value which is used for shielding different datatype (like date/datetime/integer) of ttl_col
     */
    public static class TtlColBoundValue {
        protected TtlDefinitionInfo ttlInfo;

        /**
         * The flag label if ttl_col is a normal int type (contains datetime information),
         * such using expire over policy
         */
        protected boolean isNumberType = false;

        /**
         * he flag label if the datatype of ttl_col is a mysql date type, like 'date/datetime'
         */
        protected boolean isMySqlDateType = false;
        /**
         * The ColumnMeta of ttl_col
         */
        protected ColumnMeta partColMeta = null;
        /**
         * The partLevel of ttl_col
         */
        protected PartKeyLevel partKeyLevel;
        protected boolean isForSubPart = false;

        /**
         * The flag that label if ttl_col use partFunc,like TO_DAYS/YEAR/TO_MONTHS/UNIX_TIMESTAMP, and so on
         */
        protected boolean usePartFunc = false;
        /**
         * The flag that label if the ttl_col in ttl_expr use func expr to decode from timestamp int value to datetime value
         */
        protected boolean useTtlColFuncExpr = false;

        /**
         * The original DataType of ttl_col
         */
        protected DataType partColValDataType = null;
        /**
         * The original PartitionField of ttl_col
         */
        protected PartitionField partColValFld;

        /**
         * The middle rel DataType(maybe datatype/timestamp) which is using for calculating new bounds
         */
        protected DataType ttlColDataTypeForCalcNewBounds;
        /**
         * The middle DataType(maybe datatype/timestamp) which is using for calculating new bounds
         */
        protected RelDataType ttlColRelDataTypeForCalcNewBounds;
        /**
         * The partField of middle DataType(maybe datatype/timestamp) which is using for calculating new bounds
         */
        protected PartitionField ttlColValFldForCalcNewBounds;

        /**
         * The final query expr after replacing dynamic params by inputVal for ttl_col_encoder
         */
        protected String ttlColEncoderExprStr = null;
        /**
         * The partField(String) of the final query val fo the final query expr of ttl_col_encoder
         */
        protected PartitionField ttlColEncoderExprFld = null;

        /**
         * The final query expr after replacing dynamic params by inputVal for ttl_col_decoder
         */
        protected String ttlColDecoderExprStr = null;
        /**
         * The partField(String) of the final query val fo the final query expr of ttl_col_decoder
         */
        protected PartitionField ttlColDecoderExprFld = null;

        protected TtlColValueCalcContext calcContext = null;

        private TtlColBoundValue(String bndValStr,
                                 ColumnMeta partColMeta,
                                 PartKeyLevel partKeyLevel,
                                 boolean usePartFunc,
                                 TtlDefinitionInfo ttlInfo,
                                 TtlColValueCalcContext calcContext) {
            init(bndValStr, partColMeta, partKeyLevel, usePartFunc, ttlInfo, calcContext);
        }

        private void init(String bndValStr,
                          ColumnMeta partColMeta,
                          PartKeyLevel partKeyLevel,
                          boolean usePartFunc,
                          TtlDefinitionInfo ttlInfo,
                          TtlColValueCalcContext calcContext) {
            this.ttlInfo = ttlInfo;
            this.calcContext = calcContext;

            if (this.ttlInfo != null) {
                this.useTtlColFuncExpr = this.ttlInfo.isTtlColUseFuncExpr();
            }

            this.usePartFunc = usePartFunc;
            this.partColMeta = partColMeta;
            this.partColValDataType = partColMeta.getDataType();

            this.ttlColRelDataTypeForCalcNewBounds = getTtlColRelDataTypeForCalcNewBounds(ttlInfo, partColMeta);
            this.ttlColDataTypeForCalcNewBounds =
                getTtlColDataTypeForCalcNewBounds(ttlInfo, partColMeta, ttlColRelDataTypeForCalcNewBounds);

            this.isNumberType =
                DataTypeUtil.isUnderBigintUnsignedType(ttlColDataTypeForCalcNewBounds) || DataTypeUtil.isDecimalType(
                    ttlColDataTypeForCalcNewBounds);
            this.partKeyLevel = partKeyLevel;
            if (this.isNumberType) {
                /**
                 *  if part col of range is a number type,
                 *  that means the datatype of bound value is the same as the datetype of part col
                 */
            } else {
                /**
                 *  if part col of range is a datetime type,
                 *  then
                 *  the datatype of bound value is different of the datetype of part col
                 *  if using partFuc, such partition by range(unix_timestamp(ts_col))
                 */
                if (this.partColValDataType.getSqlType() == DataTypes.DateType.getSqlType()) {
                    isMySqlDateType = true;
                }
            }
            this.isForSubPart = partKeyLevel == PartKeyLevel.SUBPARTITION_KEY;

            if (!useTtlColFuncExpr) {
                this.partColValFld = PartitionFieldBuilder.createField(partColValDataType);
                this.partColValFld.store(bndValStr, DataTypes.StringType);
                this.ttlColValFldForCalcNewBounds = this.partColValFld;
            } else {
                this.ttlColValFldForCalcNewBounds = PartitionFieldBuilder.createField(ttlColDataTypeForCalcNewBounds);
                this.ttlColValFldForCalcNewBounds.store(bndValStr, DataTypes.StringType);

//                // Here Convert the datetime string into timestamp int_col
//                // (encode ttl_col)
//                String datetimeStr = this.ttlColValFldForCalcNewBounds.stringValue().toStringUtf8();
//                Long unixTimestampLongVal =
//                    convertZonedDateTimeStringToUnixTimestamp(datetimeStr, ttlInfo.getTtlInfoRecord().getTtlTimezone());
//                this.partColValFld = PartitionFieldBuilder.createField(partColValDataType);
//                if (ttlInfo.getTtlColFuncExprInfo().isTreatTtlColAsUnixTimestampMillSeconds()) {
//                    unixTimestampLongVal *= 1000;
//                }
//                this.partColValFld.store(unixTimestampLongVal, DataTypes.LongType);

                try {
                    this.partColValFld =
                        encodeTtlCol(this.ttlColValFldForCalcNewBounds, this.partColValDataType, calcContext);
                } catch (Throwable ex) {
                    String errMsg = String.format("Failed to perform encoding ttl col by using encoder expr");
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(errMsg, ex);
                    throw new TtlJobRuntimeException(ex, errMsg);
                }
            }
        }

        /**
         * Encode the boundVal Fields used by calc new bounds
         * into the bound Fields of Original Datatype
         */
        protected static PartitionField encodeTtlCol(
            PartitionField ttlColValFldForCalcBounds,
            DataType ttlColValDataType,
            TtlColValueCalcContext calcContext) {

            String dtStr = ttlColValFldForCalcBounds.stringValue().toStringUtf8();
            String encoderExprSql = calcContext.buildTtlEncoderExprSql(dtStr);
            ExecutionContext ec = calcContext.getEc();
            TtlDefinitionInfo ttlInfo = calcContext.getTtlInfo();

            String queryEncoderExprSql = String.format("select %s as encoder_ttl_col", encoderExprSql);
            List<Map<String, Object>> rs = TtlJobUtil.execQueryExprSqlAndGetResult(ec, ttlInfo, queryEncoderExprSql);
            Object rsVal = rs.get(0).get("encoder_ttl_col");

            PartitionField encodedTtlColFld = PartitionFieldBuilder.createField(ttlColValDataType);

            if (rsVal instanceof Slice) {
                encodedTtlColFld.store(rsVal, DataTypes.VarcharType);
            } else if (rsVal instanceof String) {
                encodedTtlColFld.store(rsVal, DataTypes.StringType);
            } else {
                String rsValStr = String.valueOf(rsVal);
                encodedTtlColFld.store(rsValStr, DataTypes.StringType);
            }
            return encodedTtlColFld;
        }

        /**
         * Decode the bound Fields of Original Datatype
         * into
         * the boundVal Fields used by calc new bounds
         */
        protected static PartitionField decodeTtlCol(PartitionField ttlColValFldOfOriginalDatatype,
                                                     TtlColValueCalcContext calcContext) {

            String rawTtlColIntValStr = ttlColValFldOfOriginalDatatype.stringValue().toStringUtf8();
            String decoderExprSql = calcContext.buildTtlDecoderExprSql(rawTtlColIntValStr);
            ExecutionContext ec = calcContext.getEc();
            TtlDefinitionInfo ttlInfo = calcContext.getTtlInfo();

            String queryDecoderExprSql = String.format("select %s as decoder_ttl_col", decoderExprSql);
            List<Map<String, Object>> rs = TtlJobUtil.execQueryExprSqlAndGetResult(ec, ttlInfo, queryDecoderExprSql);
            Object rsVal = rs.get(0).get("decoder_ttl_col");

            PartitionField decodedTtlColFld = PartitionFieldBuilder.createField(DataTypes.VarcharType);
            decodedTtlColFld.store(rsVal, DataTypes.VarcharType);
//            System.out.println(decodedTtlColFld.stringValue().toStringUtf8());
            return decodedTtlColFld;
        }

        /**
         * Convert a boundString of PartField of ttl col to the middle-type boundValue
         * if the bound of PartField is an iso-formated datetime string and the ttl_col is the int value,
         * then convert iso-formated datetime string the int-type bound of PartField
         * (encode ttl_col)
         */
        public static TtlColBoundValue buildPartBoundValue(String bndValStr,
                                                           ColumnMeta partColMeta,
                                                           PartKeyLevel partKeyLevel,
                                                           boolean usePartFunc,
                                                           TtlDefinitionInfo ttlInfo,
                                                           TtlColValueCalcContext calcContext) {
            TtlColBoundValue boundValue = new TtlColBoundValue(
                bndValStr,
                partColMeta,
                partKeyLevel,
                usePartFunc,
                ttlInfo,
                calcContext);
            return boundValue;
        }

        /**
         * According to the datetype and partField of ttl_col,
         * convert to the value of partField into iso-datetime-formated string value
         * which is using for calculating new bounds
         * (decoding ttl_col)
         */
        public String getPartBoundValueStringForCalcNewBounds() {
            BuildPartFieldStringParams params =
                BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo, calcContext);
            params.setForceReturnPartFldString(true);
            String rsString = convertPartFieldToIsoFormatedDatetimeString(this.ttlColValFldForCalcNewBounds, params);
            return rsString;
        }

        /**
         * Change the middle-type boundValue (int-type value) of its Original int-DataType PartCol
         * into the string value of iso-formated datetime string
         * (encode ttl_col)
         */
        public String getPartBoundValueStringByOriginalPartColDataType() {
            BuildPartFieldStringParams params =
                BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo, calcContext);
            params.setForceReturnPartFldString(true);
            String bndValStr = convertPartFieldToIsoFormatedDatetimeString(this.partColValFld, params);
            return bndValStr;
        }

        private String getDateTimePartBoundValueFormatOnPartName(TtlTimeUnit ttlUnit) {
            DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
            String bndValStr = getPartBoundValueStringForCalcNewBounds();
            LocalDateTime localDateTimeObj = LocalDateTime.parse(bndValStr, formatter);
            String partNameFormatterPattern = TtlTaskSqlBuilder.getPartNameFormatterPatternByTtlUnit(TtlTimeUnit.DAY);
            DateTimeFormatter partNameDatetimeFormatter = DateTimeFormatter.ofPattern(partNameFormatterPattern);
            String formatedResult = localDateTimeObj.format(partNameDatetimeFormatter);
            return formatedResult;
        }

        private String getNumberPartBoundValueFormatOnPartName(TtlTimeUnit ttlUnit) {
            String bndValStr = getPartBoundValueStringForCalcNewBounds();
            return bndValStr;
        }

        /**
         * Build the partName by new middle-type boundValue and the timeUnit
         */
        public String buildPartNameByPartUnit(TtlTimeUnit ttlUnit) {
            String partNamePrefix = "p";
            if (isForSubPart) {
                partNamePrefix = "sp";
            }
            String partName = null;
            if (!isNumberType) {
                partName = partNamePrefix + getDateTimePartBoundValueFormatOnPartName(ttlUnit);
            } else {
                partName = partNamePrefix + getNumberPartBoundValueFormatOnPartName(ttlUnit);
            }
            return partName;

        }

        /**
         * The specified relDataType for calculating new bound: DATATIME(0)
         */
        private RelDataType getTtlColRelDataTypeForCalcNewBounds(TtlDefinitionInfo ttlInfo,
                                                                 ColumnMeta partColMeta) {
            boolean useTtlFuncExpr = ttlInfo.isTtlColUseFuncExpr();//use isTtlColUseExprEncoding
            if (!useTtlFuncExpr) {
                return partColMeta.getField().getRelType();
            }
            /**
             * All the value of date/datetime/timestamp should treat as datetime with scale=0 relDatetype
             */
            RelDataType calciteDataType = DataTypeUtil.jdbcTypeToRelDataType(0,
                "DATETIME", 0, 0, 0, true);
            return calciteDataType;
        }

        private DataType getTtlColDataTypeForCalcNewBounds(TtlDefinitionInfo ttlInfo,
                                                           ColumnMeta partColMeta,
                                                           RelDataType ttlColRelDataTypeForCalcNewBounds) {
            boolean useTtlFuncExpr = ttlInfo.isTtlColUseFuncExpr();//use isTtlColUseExprEncoding
            if (!useTtlFuncExpr) {
                return partColMeta.getDataType();
            }
            /**
             * All the value of date/datetime/timestamp should treat as datetime string
             */
            DataType dataTypeResult = DataTypeUtil.calciteToDrdsType(ttlColRelDataTypeForCalcNewBounds);
            return dataTypeResult;
        }

        public TtlColBoundValue plusInterval(Long partInterval,
                                             TtlTimeUnit partIntervalUnit,
                                             TtlColValueCalcContext calcContext) {

            String newBndValStr = null;
//            PartitionByDefinition tarPartBy = this.getTartPartBy();
            if (!isNumberType) {
                String pivotPointStr = getPartBoundValueStringForCalcNewBounds();
                DateTimeFormatter dateTimeIsoFormatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
                LocalDateTime pivotTimePointObj = LocalDateTime.parse(pivotPointStr, dateTimeIsoFormatter);
                LocalDateTime nextBoundObj =
                    TtlJobUtil.plusDeltaIntervals(pivotTimePointObj, partIntervalUnit, partInterval.intValue());
                newBndValStr = nextBoundObj.format(dateTimeIsoFormatter);
            } else {
                String utf8StrVal = getPartBoundValueStringForCalcNewBounds();
                if (DataTypeUtil.isUnderBigintUnsignedType(ttlColDataTypeForCalcNewBounds)) {
                    BigInteger tmpBigIntVal = new BigInteger(utf8StrVal);
                    BigInteger tmpInterval = new BigInteger(String.valueOf(partInterval));
                    tmpBigIntVal = tmpBigIntVal.add(tmpInterval);
                    newBndValStr = String.valueOf(tmpBigIntVal);
                } else if (DataTypeUtil.isDecimalType(ttlColDataTypeForCalcNewBounds)) {
                    BigDecimal tmpBigDeciVal = new BigDecimal(utf8StrVal);
                    BigDecimal tmpInterval = new BigDecimal(String.valueOf(partInterval));
                    tmpBigDeciVal = tmpBigDeciVal.add(tmpInterval);
                    newBndValStr = String.valueOf(tmpBigDeciVal);
                }
            }
//            TtlColBoundValue newBondVal =
//                new TtlColBoundValue(newBndValStr, this.partColMeta, this.partKeyLevel, this.usePartFunc, this.ttlInfo);
            TtlColBoundValue newBondVal =
                TtlColBoundValue.buildPartBoundValue(newBndValStr, this.partColMeta, this.partKeyLevel,
                    this.usePartFunc, this.ttlInfo, calcContext);
            return newBondVal;
        }

        /**
         * currBndVal > otherPartBndVal , return 1;
         * currBndVal = otherPartBndVal , return 0;
         * currBndVal < otherPartBndVal , return -1
         */
        public int compare(TtlColBoundValue otherPartBndVal) {

            SearchDatumComparator partColSpaceComp = null;
            if (!useTtlColFuncExpr) {
                List<ColumnMeta> partColMetas = new ArrayList<>();
                partColMetas.add(partColMeta);
                partColSpaceComp = PartitionInfoUtil.buildPartColumnComparator(partColMetas);
            } else {
                List<RelDataType> ttlColRelDataTypes = new ArrayList<>();
                List<DataType> ttlColDataTypes = new ArrayList<>();
                ttlColRelDataTypes.add(ttlColRelDataTypeForCalcNewBounds);
                ttlColDataTypes.add(ttlColDataTypeForCalcNewBounds);
                partColSpaceComp =
                    PartitionInfoUtil.buildQuerySpaceComparatorBySpecifyDataTypes(ttlColRelDataTypes, ttlColDataTypes);
            }

            PartitionField otherPartBndFld = otherPartBndVal.getPartColValFld();
            SearchDatumInfo otherPartBndDatum = SearchDatumInfo.createFromField(otherPartBndFld);
            SearchDatumInfo currBndValDatum = SearchDatumInfo.createFromField(partColValFld);
            int comRs = partColSpaceComp.compare(currBndValDatum, otherPartBndDatum);
            return comRs;
        }

        @Override
        public String toString() {
            if (partColValFld != null) {
                if (partColValFld.isNull()) {
                    return null;
                }
                return getPartBoundValueStringForCalcNewBounds();
            }
            return null;
        }

        public boolean isNumberType() {
            return isNumberType;
        }

        public DataType getPartColValDataType() {
            return partColValDataType;
        }

        public PartitionField getPartColValFld() {
            return partColValFld;
        }

        public PartKeyLevel getPartKeyLevel() {
            return partKeyLevel;
        }

        public boolean isUsePartFunc() {
            return usePartFunc;
        }

        public boolean isForSubPart() {
            return isForSubPart;
        }
    }

    public static PartitionByDefinition getTargetPartBy(TableMeta tarTblMeta, PartKeyLevel partKeyLevel) {
        PartitionInfo partInfo = tarTblMeta.getPartitionInfo();
        return getTargetPartByFromPartInfo(partInfo, partKeyLevel);
    }

    public static PartitionByDefinition getTargetPartByFromPartInfo(PartitionInfo partInfo, PartKeyLevel partKeyLevel) {
        PartitionByDefinition partBy = partInfo.getPartitionBy();
        if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partBy = partBy.getSubPartitionBy();
        }
        return partBy;
    }

    protected static class TtlPartitionRouter {

        protected TableMeta tarTblMeta;
        protected PartKeyLevel partLevel;
        protected String ttlTimeZone;

        public TtlPartitionRouter(TableMeta tarTblMeta,
                                  PartKeyLevel partLevel,
                                  String ttlTimeZone
        ) {
            this.tarTblMeta = tarTblMeta;
            this.partLevel = partLevel;
            this.ttlTimeZone = ttlTimeZone;
        }

        /**
         * Found the target non-maxvalue part by routing one boundValue of ttl_col
         */
        protected String findTargetNonMaxValPartByRoutingTtlColVal(ExecutionContext ec, TtlColBoundValue ttlColVal) {
            List<String> phyParts = fetchTargetPartNamesByRoutingTtlColValAndPartLevel(ec,
                tarTblMeta,
                partLevel,
                ttlTimeZone,
                ttlColVal);
            if (phyParts.isEmpty()) {
                return null;
            }
            String partNameRs = phyParts.get(0);
            PartitionInfo tarTblPartInfo = tarTblMeta.getPartitionInfo();
            PartitionSpec partSpecRs = null;
            if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
                partSpecRs = tarTblPartInfo.getPartSpecSearcher().getSubPartTempSpecNameBySubPartTempName(partNameRs);
            } else {
                partSpecRs = tarTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(partNameRs);
            }

            if (partSpecRs != null && partSpecRs.getBoundSpec().containMaxValues()) {
                return null;
            }
            return partNameRs;
        }

        /**
         * Auto fix the invalid partitions which is to be added newly
         * <pre>
         *    Some auto-gen bound value will be out of range of date type
         *    (1) auto remove the duplicated partitions (partition names are the same or bound value are the same);
         *    (2) auto generate new part name the the new add part whose name are already exists in curr partitions but
         *        bound value is different.
         *
         * </pre>
         */
        public boolean fixInvalidPartitionsWithPartInfo(List<Pair<String, String>> newAddPartSpecInfosOutput,
                                                        List<Pair<String, String>> fixedPartSpecInfosOutput,
                                                        List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput) {
            TtlPartitionUtil.fixInvalidPartitions(newAddPartSpecInfosOutput, tarTblMeta, partLevel,
                fixedPartSpecInfosOutput, fixedPartSpecIndexMappingsOutput);
            return true;
        }
    }

    /**
     *
     */
    protected static void calcNewPartBoundsByPivotBoundValWithTblMeta(
        CalcNewPartBoundsByPivotBoundValWithTblMetaParams params) {

        TableMeta targetTblMeta = params.getTarTblMeta();
        PartKeyLevel partKeyLevel = params.getTarPartKeyLevel();
        CalcNewPartBoundsByPivotBoundValParams inputParams = params.getCalcParams();
        ExecutionContext ec = inputParams.getEc();

        String ttlTimeZone = inputParams.getTtlTimeZone();
        PartitionByDefinition tarPartBy = TtlPartitionUtil.getTargetPartBy(targetTblMeta, partKeyLevel);
        TtlPartitionRouter ttlPartitionRouter = new TtlPartitionRouter(targetTblMeta, partKeyLevel, ttlTimeZone);
        TtlDefinitionInfo ttlInfo = inputParams.getTtlInfo();
        TtlColValueCalcContext calcContext = TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, ec);

        CalcNewPartBoundsByPivotBoundValParams buildParams = new CalcNewPartBoundsByPivotBoundValParams();
        buildParams.setTtlInfo(inputParams.getTtlInfo());
        buildParams.setCalcContext(calcContext);
        buildParams.setPivotBoundValue(inputParams.getPivotBoundValue());
        buildParams.setTtlTimeZone(inputParams.getTtlTimeZone());
        buildParams.setPreBuildCnt(inputParams.getPreBuildCnt());
        buildParams.setPostBuildCnt(inputParams.getPostBuildCnt());
        buildParams.setArcPartUnit(inputParams.getArcPartUnit());
        buildParams.setArcPartInterval(Long.valueOf(inputParams.getArcPartInterval()));
        buildParams.setPreBuildTargetBoundValue(inputParams.getPreBuildTargetBoundValue());
        buildParams.setIgnoreRoutingCheck(inputParams.isIgnoreRoutingCheck());
        buildParams.setUseExpireOver(inputParams.isUseExpireOver());
        buildParams.setEc(inputParams.getEc());
        buildParams.setNewAddPartSpecInfosOutput(inputParams.getNewAddPartSpecInfosOutput());
        buildParams.setNewAddPartSpecMappingsOutput(inputParams.getNewAddPartSpecMappingsOutput());
        buildParams.setReservedPartNames(inputParams.getReservedPartNames());

        buildParams.setTtlPartitionRouter(ttlPartitionRouter);
        buildParams.setPartColMeta(tarPartBy.getPartitionFieldList().get(0));
        buildParams.setPartKeyLevel(tarPartBy.getPartLevel());
        buildParams.setUsePartFunc(tarPartBy.getPartIntFunc() != null);

        calcNewPartBoundsByPivotBoundVal(buildParams);
    }

    /**
     * <pre>
     *     Calc both new postBuild parts and new preBuild parts,
     *     it process is :
     *     (1) generate all postParts by postPartCnt if need;
     *     (2) generate all preParts by prePartCnt or targetBndValue;
     *     (3) put postParts and preParts into an allPartList;
     *     (4) skip some invalid parts for the allPartList;
     *     (5) prepare output result.
     * </pre>
     */
    public static void calcNewPartBoundsByPivotBoundVal(CalcNewPartBoundsByPivotBoundValParams params) {

        String pivotBoundValue = params.getPivotBoundValue();
        Integer preBuildCnt = params.getPreBuildCnt();
        Integer postBuildCnt = params.getPostBuildCnt();
        String preBuildTargetBoundValue = params.getPreBuildTargetBoundValue();
        TtlTimeUnit arcPartUnit = params.getArcPartUnit();
        Long arcPartInterval = params.getArcPartInterval();
        boolean ignoreRoutingCheck = params.isIgnoreRoutingCheck();
        TtlPartitionUtil.TtlPartitionRouter ttlPartitionRouter = params.getTtlPartitionRouter();
        ColumnMeta partColMeta = params.getPartColMeta();
        PartKeyLevel partKeyLevel = params.getPartKeyLevel();
        boolean usePartFunc = params.isUsePartFunc();
        ExecutionContext ec = params.getEc();
        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        TtlColValueCalcContext calcContext = params.getCalcContext();
        List<Pair<String, String>> newAddPartSpecInfosOutput = params.getNewAddPartSpecInfosOutput();
        Map<String, TtlColBoundValue> newAddPartSpecMappingsOutput = params.getNewAddPartSpecMappingsOutput();
        Set<String> reservedPartNames = params.getReservedPartNames();
        try {
            /**
             * Change the pivotBoundValue into the middle-type boundValue which is using for calculating new bounds
             */
            String pivotValStr = pivotBoundValue;
            TtlColBoundValue pivotValuePoint =
                TtlColBoundValue.buildPartBoundValue(pivotValStr, partColMeta, partKeyLevel, usePartFunc, ttlInfo,
                    calcContext);

            /**
             * Calc the range parts according to postBuildCnt for postBuild of arc_tbl if need
             */
            List<TtlColBoundValue> newPostPartBoundValList = new ArrayList<>();
            if (postBuildCnt != null && postBuildCnt > 0) {

                TtlColBoundValue newPostBound = pivotValuePoint;
                for (int i = 0; i < postBuildCnt; i++) {
                    /**
                     * Calculating new bound by plus an interval
                     */
                    newPostBound = newPostBound.plusInterval(-1L * arcPartInterval, arcPartUnit, calcContext);
                    String tarPartName = null;
                    if (!ignoreRoutingCheck) {
                        if (ttlPartitionRouter != null) {
                            /**
                             * Find the max bound which is not a non-maxvalue partition
                             */
                            tarPartName =
                                ttlPartitionRouter.findTargetNonMaxValPartByRoutingTtlColVal(ec, newPostBound);
                            if (tarPartName == null) {
                                /**
                                 * No found any non-max-value parts
                                 */
                                newPostPartBoundValList.add(newPostBound);
                            }
                        }
                    } else {
                        newPostPartBoundValList.add(newPostBound);
                    }
                }
            }

            /**
             * Calc the range parts according to preBuildCnt and preBuildTargetBndVal for preBuild of ttl_tbl/arc_tbl
             */
            List<TtlColBoundValue> newPrePartBoundValList = new ArrayList<>();
            newPrePartBoundValList.add(pivotValuePoint);

            /**
             * Here check if need building new pre-build parts by using interval  [pivotValuePoint, preBuildTargetBndVal)
             */
            String preBuildTarBoundValueStr = preBuildTargetBoundValue;
            TtlColBoundValue preBuildTargetBndVal = null;
            boolean needPreBuildToTargetBndVal = false;
            if (preBuildTarBoundValueStr != null) {
                /**
                 * In the partition generations of creating cci(arc_tbl) for ttl-tbl using archive by partition/subpartition,
                 * because it has not any partInfo of cci,
                 * so it need fetch the startBoundValue(pivotValuePoint) and endBoundValue(preBuildTargetBndVal)
                 * from ttl_tbl partInfo,
                 * and treat the [pivotValuePoint, preBuildTargetBndVal) as the whole ranges of cci.
                 */
                preBuildTargetBndVal =
                    TtlColBoundValue.buildPartBoundValue(preBuildTarBoundValueStr, partColMeta, partKeyLevel,
                        usePartFunc, ttlInfo, calcContext);
                needPreBuildToTargetBndVal = true;
            }

            /**
             * <pre>
             *     Here generate all new pre-build parts by two policy :
             *          policy 1: generate parts by the [pivotValuePoint, preBuildTargetBndVal)
             *          policy 2: generate parts by the prePartCount, [pivotValuePoint, pivotValuePoint + n * pare_interval)
             * </pre>
             *
             */
            TtlColBoundValue newBound = pivotValuePoint;
            if (needPreBuildToTargetBndVal) {
                /**
                 * Come here :  generate parts by the [pivotValuePoint, preBuildTargetBndVal)
                 */
                while (true) {
                    /**
                     * Calculating new bound for prebuildPart by plus an interval
                     */
                    newBound = newBound.plusInterval(arcPartInterval, arcPartUnit, calcContext);
                    if (!ignoreRoutingCheck) {

                        // Here make sure that all new partBounds should route empty non-maxvalue parts

                        if (ttlPartitionRouter != null) {
                            String tarPartName =
                                ttlPartitionRouter.findTargetNonMaxValPartByRoutingTtlColVal(ec, newBound);
                            if (tarPartName == null
                                || isReservedPart(reservedPartNames, tarPartName)) {
                                /**
                                 * No found any managed non-max-value parts (routed to maxval or reserved part)
                                 */
                                newPrePartBoundValList.add(newBound);
                            }
                        }
                    } else {
                        newPrePartBoundValList.add(newBound);
                    }
                    if (newBound.compare(preBuildTargetBndVal) >= 0) {
                        break;
                    }
                }
            } else {
                /**
                 * Come here :
                 * generate parts by the prePartCount, [pivotValuePoint, pivotValuePoint + n * pare_interval)
                 * and skip each new-generated bound value which can route and find an already-exists non-maxvalue part
                 */
                for (int i = 0; i < preBuildCnt; i++) {
                    newBound = newBound.plusInterval(arcPartInterval, arcPartUnit, calcContext);
                    if (!ignoreRoutingCheck) {
                        if (ttlPartitionRouter != null) {

                            // Here make sure that all new partBounds should route empty non-maxvalue parts

                            String tarPartName =
                                ttlPartitionRouter.findTargetNonMaxValPartByRoutingTtlColVal(ec, newBound);
                            if (tarPartName == null
                                || isReservedPart(reservedPartNames, tarPartName)) {
                                /**
                                 * No found any managed non-max-value parts (routed to maxval or reserved part)
                                 */
                                newPrePartBoundValList.add(newBound);
                            }
                        }
                    } else {
                        newPrePartBoundValList.add(newBound);
                    }
                }
            }

            // finalNewPartInfos includes all new generated parts, included postParts and preBuild parts
            List<TtlColBoundValue> finalNewPartInfos = new ArrayList<>();

            // because new postParts are build by desc order,
            // so here need to reverse scan to make finalNewPartInfos because asc order parts
            for (int i = newPostPartBoundValList.size() - 1; i > -1; --i) {
                finalNewPartInfos.add(newPostPartBoundValList.get(i));
            }
            finalNewPartInfos.addAll(newPrePartBoundValList);

            /**
             * Here prepare the output info of new generated parts
             */
            List<Pair<String, String>> newAddPartSpecInfosBeforeFixingOutput = new ArrayList<>();
            if (newAddPartSpecInfosOutput != null) {
                if (!finalNewPartInfos.isEmpty()) {
                    for (int i = 0; i < finalNewPartInfos.size(); i++) {
                        TtlColBoundValue newBndVal = finalNewPartInfos.get(i);
                        /**
                         * Change the middle-type boundValue to its Original PartCol DataType
                         */
                        String newBndValStr = newBndVal.getPartBoundValueStringByOriginalPartColDataType();
                        /**
                         * Build the partName by the middle-type bound value
                         */
                        String bndValForPartNameStr = newBndVal.buildPartNameByPartUnit(arcPartUnit);
                        Pair<String, String> newPartNameAndBndVal = new Pair<>(bndValForPartNameStr, newBndValStr);
                        newAddPartSpecInfosBeforeFixingOutput.add(newPartNameAndBndVal);
                    }
                }

                List<Pair<String, String>> newAddPartSpecInfosAfterFixingOutput = new ArrayList<>();
                List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput = new ArrayList<>();
                if (ttlPartitionRouter != null) {
                    // if ttlPartitionRouter exists, will check all routing for all bound values
                    /**
                     * Auto fix some wrong or invalid parts like:
                     * 1.  duplicated bound values
                     * 2.  duplicated part names
                     * 3.  too-long part names fix
                     * 4.  invalid bound values
                     */
                    ttlPartitionRouter.fixInvalidPartitionsWithPartInfo(newAddPartSpecInfosBeforeFixingOutput,
                        newAddPartSpecInfosAfterFixingOutput, fixedPartSpecIndexMappingsOutput);
                    newAddPartSpecInfosOutput.clear();
                    newAddPartSpecInfosOutput.addAll(newAddPartSpecInfosAfterFixingOutput);
                } else {
                    // if ttlPartitionRouter does NOT exist, then no check routing,
                    // such create cci or new create ttl tbl
                    /**
                     * Auto fix some wrong parts like:
                     * 1.  duplicated bound values
                     * 2.  duplicated parts names
                     * 3.  invalid bound values
                     */
                    TtlPartitionUtil.fixInvalidPartitionsWithoutPartInfo(newAddPartSpecInfosBeforeFixingOutput,
                        newAddPartSpecInfosAfterFixingOutput, fixedPartSpecIndexMappingsOutput);
                    newAddPartSpecInfosOutput.clear();
                    newAddPartSpecInfosOutput.addAll(newAddPartSpecInfosAfterFixingOutput);
                }

                if (newAddPartSpecMappingsOutput != null) {
                    for (int i = 0; i < fixedPartSpecIndexMappingsOutput.size(); i++) {
                        Pair<Integer, Integer> specIndexMapping = fixedPartSpecIndexMappingsOutput.get(i);
                        String newPartName = newAddPartSpecInfosAfterFixingOutput.get(i).getKey();

                        Integer originIdx = specIndexMapping.getKey();
                        TtlColBoundValue partBndVal = finalNewPartInfos.get(originIdx);
                        newAddPartSpecMappingsOutput.put(newPartName, partBndVal);
                    }
                }
            }
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
    }

    public static String findTargetNonMaxValPartByRoutingTtlColVal(ExecutionContext ec,
                                                                   TableMeta tarTblMeta,
                                                                   TtlDefinitionInfo primTblTtlInfo,
                                                                   PartKeyLevel partLevel,
                                                                   String ttlTimeZone,
                                                                   String ttlColVal,
                                                                   TtlColValueCalcContext calcContext) {

        ColumnMeta ttlColMeta = primTblTtlInfo.getTtlColMeta(ec);
        PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, partLevel);
        TtlColBoundValue ttlColBoundVal = TtlColBoundValue.buildPartBoundValue(ttlColVal, ttlColMeta, partLevel,
            tarPartBy.getPartIntFunc() != null, primTblTtlInfo, calcContext);
        List<String> phyParts = fetchTargetPartNamesByRoutingTtlColValAndPartLevel(ec,
            tarTblMeta,
            partLevel,
            ttlTimeZone,
            ttlColBoundVal);
        if (phyParts.isEmpty()) {
            return null;
        }
        String partNameRs = phyParts.get(0);
        PartitionInfo tarTblPartInfo = tarTblMeta.getPartitionInfo();
        PartitionSpec partSpecRs = null;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partSpecRs = tarTblPartInfo.getPartSpecSearcher().getSubPartTempSpecNameBySubPartTempName(partNameRs);
        } else {
            partSpecRs = tarTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(partNameRs);
        }

        if (partSpecRs != null && partSpecRs.getBoundSpec().containMaxValues()) {
            return null;
        }
        return partNameRs;
    }

    /**
     * Find the target partNames by routing new ttl_col value and part_level
     */
    protected static List<String> fetchTargetPartNamesByRoutingTtlColValAndPartLevel(ExecutionContext ec,
                                                                                     TableMeta tarTblMeta,
                                                                                     PartKeyLevel targetPartLevel,
                                                                                     String ttlTimezone,
                                                                                     TtlColBoundValue tarTtlColValToRoute) {
        PartitionInfo arcTmpTblPartInfo = tarTblMeta.getPartitionInfo();
        InternalTimeZone internalTimeZone = TimeZoneUtils.convertFromMySqlTZ(ttlTimezone);
        ec.setTimeZone(internalTimeZone);
        List<Object> pointValue = new ArrayList<>();
        String ttlColValStr = tarTtlColValToRoute.getPartBoundValueStringByOriginalPartColDataType();
        pointValue.add(ttlColValStr);

        List<DataType> pointValueOpTypes = new ArrayList<>();
        pointValueOpTypes.add(DataTypes.StringType);
        ExecutionContext[] newEcOutput = new ExecutionContext[1];
        RelDataTypeFactory type = PartitionPrunerUtils.getTypeFactory();
        RelDataType tbRelRowType = tarTblMeta.getPhysicalRowType(type);

        PartitionPruneStep pruneStep = PartitionPruneStepBuilder.genPointSelectPruneStepInfoForTtlRouting(pointValue,
            pointValueOpTypes, ec, newEcOutput, arcTmpTblPartInfo, targetPartLevel, tbRelRowType);

        PartPrunedResult prunedResult = PartitionPruner.doPruningByStepInfo(pruneStep, newEcOutput[0]);
        List<String> phyPartNameList = prunedResult.getPrunedPartitionNamesOfPartLevel(targetPartLevel, true);

        return phyPartNameList;
    }

//    protected static String buildPartNameByBoundValue(LocalDateTime partBoundVal,
//                                                      TtlTimeUnit ttlUnit,
//                                                      Boolean buildForSubPartTempName) {
//        String partNameFormatterPattern = TtlTaskSqlBuilder.getPartNameFormatterPatternByTtlUnit(ttlUnit);
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(partNameFormatterPattern);
//        String partNamePrefix = "p";
//        if (buildForSubPartTempName) {
//            partNamePrefix = "sp";
//        }
//        String partName = partNamePrefix + partBoundVal.format(formatter);
//        return partName;
//    }
//    protected static String buildAddPartitionsSqlTemplate(TtlDefinitionInfo ttlInfo,
//                                                       List<String> newAddRangeBounds,
//                                                       List<String> newAddRangePartNames,
//                                                       String maxValPartName,
//                                                       String queryHint,
//                                                       ExecutionContext executionContext) {
//
//        String partBoundDefs = "";
//        List<String> normalizedNewAddPartBoundList =
//            normalizedRangePartBoundValueList(ttlInfo, newAddRangeBounds, executionContext);
//        for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
//            String bndStr = normalizedNewAddPartBoundList.get(i);
//            String partNameStr = newAddRangePartNames.get(i);
//            String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
//            String part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
//            if (!partBoundDefs.isEmpty()) {
//                partBoundDefs += ",\n";
//            }
//            partBoundDefs += part;
//        }
//
//        String addPartSpecSql = "";
//        if (StringUtils.isEmpty(maxValPartName)) {
//            addPartSpecSql = String.format("ADD PARTITION ( \n%s );",
//                partBoundDefs);
//        } else {
//            addPartSpecSql = String.format("SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (MAXVALUE) );",
//                maxValPartName, partBoundDefs, maxValPartName);
//        }
//
//        String skipDdlTasks =
//            executionContext.getParamManager().getString(ConnectionParams.TTL_DEBUG_CCI_SKIP_DDL_TASKS);
//        if (!StringUtils.isEmpty(skipDdlTasks)) {
//            queryHint = TtlTaskSqlBuilder.addCciHint(queryHint, String.format("SKIP_DDL_TASKS=\"%s\"", skipDdlTasks));
//        }
//
//        String addPartSqlTemp = queryHint + " ALTER INDEX %s ON TABLE %s " + addPartSpecSql;
//
//        return addPartSqlTemp;
//    }

    protected static List<String> normalizedRangePartBoundValueList(TtlDefinitionInfo ttlInfo,
                                                                    List<String> rangeBounds,
                                                                    boolean buildPartForCci,
                                                                    PartKeyLevel tarPartLevel,
                                                                    ExecutionContext executionContext) {
        String primTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String primTblName = ttlInfo.getTtlInfoRecord().getTableName();
        TableMeta primTblMeta = executionContext.getSchemaManager(primTblSchema).getTable(primTblName);

        TableMeta tarTblMeta = null;
        PartitionInfo tarTblPartInfo = null;
        PartitionByDefinition tarPartBy = null;
        String tarTblSchema = null;
        String tarTblName = null;
        if (buildPartForCci) {
            tarTblSchema = ttlInfo.getTmpTableSchema();
            tarTblName = ttlInfo.getTmpTableName();
            tarTblMeta = TtlJobUtil.getArchiveCciTableMeta(primTblSchema, primTblName, executionContext);
            tarTblPartInfo = tarTblMeta.getPartitionInfo();
        } else {
            tarTblSchema = primTblSchema;
            tarTblName = primTblName;
            tarTblMeta = primTblMeta;
            tarTblPartInfo = tarTblMeta.getPartitionInfo();
        }

        PartitionTableType tblType = tarTblPartInfo.getTableType();
        if (PartitionTableType.PARTITIONED_TABLE.contains(tblType)) {
            tarPartBy = tarTblPartInfo.getPartitionBy();
            if (tarPartLevel == PartKeyLevel.SUBPARTITION_KEY) {
                tarPartBy = tarPartBy.getSubPartitionBy();
            }
        }

        String tarPartFuncName = "";
        PartitionIntFunction partFunc = tarPartBy.getPartIntFunc();
        DataType partColType = tarPartBy.getPartitionColumnTypeList().get(0);
        boolean isNumberType = DataTypeUtil.isNumberSqlType(partColType);
        if (partFunc != null) {
            String partFuncName = partFunc.getSqlOperator().getName();
            if (partFunc.getMonotonicity(partColType) != Monotonicity.NON_MONOTONIC
                && PartitionFunctionBuilder.isTimeBasedFamilyPartitionFunction(partFuncName)) {
                tarPartFuncName = partFunc.getSqlOperator().getName();
            } else {
                throw new TtlJobRuntimeException(String.format(
                    "Found invalid partition function `%s` on the partition columns of %s level  of `%s`.`%s`",
                    partFuncName, tarPartLevel.name(), tarTblSchema, tarTblName));
            }
        }

        List<String> result = new ArrayList<>();
        for (int i = 0; i < rangeBounds.size(); i++) {
            String bndStr = rangeBounds.get(i);
            boolean isMaxValBnd = bndStr.toUpperCase().contains(TtlTaskSqlBuilder.ARC_TBL_MAXVALUE_BOUND);
            boolean partBndNeedUseFuncExpr = !StringUtils.isEmpty(tarPartFuncName);
            String normalizedPartBndVal = "";
            if (!isMaxValBnd) {
//                if (!partBndNeedUseFuncExpr) {
//                    normalizedPartBndVal = String.format("'%s'", bndStr);
//                } else {
//                    normalizedPartBndVal = convertToUnixTimestampLongStr(bndStr, ttlInfo);
//                }
                if (!isNumberType) {
                    if (partBndNeedUseFuncExpr) {
                        normalizedPartBndVal = String.format("%s('%s')", tarPartFuncName, bndStr);
                    } else {
                        normalizedPartBndVal = String.format("'%s'", bndStr);
                    }
                } else {
                    normalizedPartBndVal = String.format("%s", bndStr);
                }

            } else {
                normalizedPartBndVal = bndStr;
            }
            result.add(normalizedPartBndVal);
        }
        return result;
    }

    public static String convertToUnixTimestampLongStr(String bndStr,
                                                       TtlDefinitionInfo ttlInfo) {
        String timezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        ZoneId zoneId = ZoneId.of(timezoneStr);
        LocalDateTime localDateTime = LocalDateTime.parse(bndStr, TtlTaskSqlBuilder.ISO_DATETIME_FORMATTER);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);
        Long unixTs = zonedDateTime.toEpochSecond();
        String unixTsStr = String.valueOf(unixTs);
        return unixTsStr;
    }

    /**
     * Find the min boundValue partition and partition names from all-non-maxvalue partitions
     */
    public static Pair<String, SearchDatumInfo> findMinPartBoundValAndPartNameFromNonMaxValParts(
        PartitionInfo tarPartInfo,
        PartKeyLevel partLevel) {
        PartitionByDefinition partBy = TtlPartitionUtil.getTargetPartByFromPartInfo(tarPartInfo, partLevel);
        List<PartitionSpec> parts = partBy.getPartitions();

        PartitionSpec firstPart = parts.get(0);
        if (firstPart.getBoundSpec().containMaxValues()) {
            return null;
        }

        Pair<String, SearchDatumInfo> result =
            new Pair<String, SearchDatumInfo>(firstPart.getName(), firstPart.getBoundSpec().getSingleDatum());
        return result;
    }

    /**
     * Find the max boundValue partition and partition names from all-non-maxvalue partitions
     */
    public static Pair<String, SearchDatumInfo> findMaxPartBoundValAndPartNameFromNonMaxValParts(
        PartitionInfo tarPartInfo,
        PartKeyLevel partLevel) {
        PartitionByDefinition partBy = TtlPartitionUtil.getTargetPartByFromPartInfo(tarPartInfo, partLevel);
        List<PartitionSpec> parts = partBy.getPartitions();
        int partCnt = parts.size();
        PartitionSpec lastNonMaxValPartSpec = parts.get(partCnt - 1);
        if (lastNonMaxValPartSpec.getBoundSpec().containMaxValues()) {
            if (partCnt - 2 >= 0) {
                lastNonMaxValPartSpec = parts.get(partCnt - 2);
            } else {
                lastNonMaxValPartSpec = null;
            }
        }
        if (lastNonMaxValPartSpec == null) {
            return null;
        }

        SearchDatumInfo bndVal = lastNonMaxValPartSpec.getBoundSpec().getSingleDatum();
        Pair<String, SearchDatumInfo> result =
            new Pair<String, SearchDatumInfo>(lastNonMaxValPartSpec.getName(), bndVal);
        return result;
    }

    public static String findMaxPartBoundValStrFromNonMaxValParts(PartitionInfo tarPartInfo,
                                                                  PartKeyLevel partLevel,
                                                                  TtlDefinitionInfo ttlInfo,
                                                                  TtlColValueCalcContext calcContext) {
        Pair<String, SearchDatumInfo> partNameAndDatum =
            findMaxPartBoundValAndPartNameFromNonMaxValParts(tarPartInfo, partLevel);
        SearchDatumInfo bndValDatum = partNameAndDatum.getValue();
        // decode ttl_col if need
        BuildPartFieldStringParams params =
            BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo, calcContext);
        return convertPartSpecBoundValToString(bndValDatum.getDatumInfo()[0], params);
    }

    public static boolean fixInvalidPartitionsWithoutPartInfo(
        List<Pair<String, String>> newAddPartSpecInfosOutput,
        List<Pair<String, String>> fixedPartSpecInfosOutput,
        List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput) {
        TtlPartitionUtil.fixInvalidPartitions(newAddPartSpecInfosOutput, null, null, fixedPartSpecInfosOutput,
            fixedPartSpecIndexMappingsOutput);
        return true;
    }

    /**
     * Auto fix the invalid partitions which is to be added newly
     * <pre>
     *    Some auto-gen bound value will be out of range of date type
     *    (1) auto remove the duplicated partitions (partition names are the same or bound value are the same);
     *    (2) auto generate new part name the the new add part whose name are already exists in curr partitions but
     *        bound value is different.
     *
     * </pre>
     */
    protected static boolean fixInvalidPartitions(
        List<Pair<String, String>> newAddPartSpecInfosOutput,
        TableMeta tarTblMeta,
        PartKeyLevel tarTblPartLevel,
        List<Pair<String, String>> fixedPartSpecInfosOutput,
        List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput) {

        List<Pair<String, String>> newAddPartSpecInfosOutputAfterFixing = new ArrayList<>();
        List<Pair<Integer, Integer>> newAddPartSpecIndexMappingsOutputAfterFixing = new ArrayList<>();

        boolean specifyTblMeta = tarTblMeta != null;
        Map<String, String> allPartBndInfoMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        PartitionInfo tartPartInfo = null;
        PartSpecSearcher partSpecSearcher = null;
        if (specifyTblMeta) {
            PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, tarTblPartLevel);
            tartPartInfo = tarTblMeta.getPartitionInfo();
            partSpecSearcher = tartPartInfo.getPartSpecSearcher();
            List<PartitionSpec> currPartList = tarPartBy.getPartitions();
            for (int i = 0; i < currPartList.size(); i++) {
                PartitionSpec pspec = currPartList.get(i);
                String partName = pspec.getName();
                String bndValStr = pspec.getBoundSpec().getSingleDatum().toStringWithNoBracket();
                allPartBndInfoMapping.put(bndValStr, partName);
            }
        }

        int beginIdx = -1;
        int endIdx = -1;

        String lastBndValStr = null;
        for (int i = 0; i < newAddPartSpecInfosOutput.size(); i++) {
            Pair<String, String> partVal = newAddPartSpecInfosOutput.get(i);
            String bndStr = partVal.getValue();
            if (lastBndValStr == null) {
                lastBndValStr = bndStr;
                beginIdx = i;
                continue;
            }
            if (bndStr.equalsIgnoreCase(lastBndValStr)) {
                beginIdx = i;
            } else {
                break;
            }
        }

        lastBndValStr = null;
        for (int i = newAddPartSpecInfosOutput.size() - 1; i > -1; --i) {
            Pair<String, String> partVal = newAddPartSpecInfosOutput.get(i);
            String bndStr = partVal.getValue();
            if (lastBndValStr == null) {
                lastBndValStr = bndStr;
                endIdx = i;
                continue;
            }
            if (bndStr.equalsIgnoreCase(lastBndValStr)) {
                endIdx = i;
            } else {
                break;
            }
        }

        for (int i = beginIdx; i <= endIdx; i++) {
            Pair<String, String> partSpec = newAddPartSpecInfosOutput.get(i);
            String partNameOfNewPart = partSpec.getKey();
            String bndValStrOfNewPart = partSpec.getValue();

            if (specifyTblMeta && allPartBndInfoMapping.containsKey(bndValStrOfNewPart)) {
                // ignore
                continue;
            }

            String fixedPartNameOfNewPart = null;
            String fixedBndValOfNewPart = null;

            if (specifyTblMeta) {
                PartitionSpec tartPart = partSpecSearcher.getPartSpecByPartName(partNameOfNewPart);
                if (tartPart == null && tarTblPartLevel == PartKeyLevel.SUBPARTITION_KEY) {
                    tartPart = partSpecSearcher.getSubPartTempSpecNameBySubPartTempName(partNameOfNewPart);
                }
                if (tartPart != null) {
                    /**
                     * Found already-exist part by using partNameOfNewPart
                     */
                    fixedPartNameOfNewPart = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partNameOfNewPart);
                } else {
                    fixedPartNameOfNewPart = partNameOfNewPart;
                }
            } else {
                fixedPartNameOfNewPart = partNameOfNewPart;
            }

            fixedBndValOfNewPart = bndValStrOfNewPart;

            Pair<String, String> fixedNewPartInfo = new Pair<>(fixedPartNameOfNewPart, fixedBndValOfNewPart);
            newAddPartSpecInfosOutputAfterFixing.add(fixedNewPartInfo);

            Pair<Integer, Integer> fixedNewPartIndexMapping =
                new Pair<>(i, newAddPartSpecInfosOutputAfterFixing.size() - 1);
            newAddPartSpecIndexMappingsOutputAfterFixing.add(fixedNewPartIndexMapping);
        }

        if (fixedPartSpecInfosOutput != null) {
            fixedPartSpecInfosOutput.addAll(newAddPartSpecInfosOutputAfterFixing);
        }

        if (fixedPartSpecIndexMappingsOutput != null) {
            fixedPartSpecIndexMappingsOutput.addAll(newAddPartSpecIndexMappingsOutputAfterFixing);
        }

        return true;

    }

    protected static int decideAddPartsCountForTtlTblWithExpiredOverPolicy(int curPartCnt,
                                                                           int expireOverCnt) {
        int finalPreBuildPartCnt = 0;
        if (curPartCnt < expireOverCnt) {
            /**
             * When currPartCount is less than expireOverCount,
             * just add parts and no parts to be dropped (if ttl_cleanup=on)
             */
            finalPreBuildPartCnt = expireOverCnt - curPartCnt;
        } else if (curPartCnt == expireOverCnt) {
            /**
             * When currPartCount is equal to expireOverCount,
             * just add one part, and then the first part will to be dropped (if ttl_cleanup=on)
             */
            finalPreBuildPartCnt = 1;
        } else {
            /**
             * When currPartCount is more than the expireOverCount,
             * then no parts will be added, and then top parts will to be dropped (if ttl_cleanup=on)
             */
            finalPreBuildPartCnt = 0;
        }
        return finalPreBuildPartCnt;
    }

    protected static int decideDropPartsCountForTtlTblWithExpiredOverPolicy(int currPartCnt,
                                                                            int newAddPartsCnt,
                                                                            int expireOverCnt) {
        int partCntToBeDropped = 0;
        int currAllPartCnt = currPartCnt + newAddPartsCnt;
        if (currAllPartCnt <= expireOverCnt) {
            return partCntToBeDropped;
        }
        partCntToBeDropped = currAllPartCnt - expireOverCnt;
        return partCntToBeDropped;
    }
}
