package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlPartitionUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.ttl.TtlInfoAccessor;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.gms.util.TtlEventLogUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.StringUtil;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDatetimeNormalizer;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;

import java.sql.Connection;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * check if ttl table which need auto parts has add parts successfully as excepted.
 * if not, should do some warning!!
 */

/**
 * @author chenghui.lch
 */
public class TtlAddPartsWarningScanner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TtlAddPartsWarningScanner.class);

    public TtlAddPartsWarningScanner() {
    }

    @Override
    public void run() {
        try {
            if (!DynamicConfig.getInstance().isTtlEnableScanAddPartsWarning()) {
                return;
            }
            if (!ExecUtils.hasLeadership(null)) {
                return;
            }
            checkTtlTableAutoAddParts(null);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
        }
    }

    public void doWarningForCheckedResults(Map<String, String> warningTableInfoMap) {

        if (warningTableInfoMap.isEmpty()) {
            return;
        }
        List<String> warningTableNameListForAddParts = new ArrayList<>();
        List<String> warningTableNameListForInvalidMeta = new ArrayList<>();

        for (Map.Entry<String, String> msgItem : warningTableInfoMap.entrySet()) {
            String dbTbName = msgItem.getKey();
            String warnMsg = msgItem.getValue();
            boolean isInvalidMeta = warnMsg.contains("ttl_meta_invalid");
            if (isInvalidMeta) {
                warningTableNameListForInvalidMeta.add(dbTbName);
            } else {
                warningTableNameListForAddParts.add(dbTbName);
            }
        }

        Long ts = System.currentTimeMillis();
        for (int i = 0; i < warningTableNameListForAddParts.size(); i++) {
            String tbl = warningTableNameListForAddParts.get(i);
            String warningMsg = warningTableInfoMap.get(tbl);
            TtlLoggerUtil.logAddPartsWarningMsg(ts, tbl, warningMsg);
        }
        if (!warningTableNameListForAddParts.isEmpty()) {
            TtlEventLogUtil.logAutoAddApartStoopedEvent(warningTableNameListForAddParts);
        }

        for (int i = 0; i < warningTableNameListForInvalidMeta.size(); i++) {
            String tbl = warningTableNameListForInvalidMeta.get(i);
            String warningMsg = warningTableInfoMap.get(tbl);
            TtlLoggerUtil.logInvalidTtlMetaMsg(ts, tbl, warningMsg);
        }
        if (!warningTableNameListForInvalidMeta.isEmpty()) {
            TtlEventLogUtil.logInvalidTtlMetaInfoEvent(warningTableNameListForInvalidMeta);
        }
    }

    /**
     * do the warning check for ttl tables.
     *
     * @param ec if ec is null ,then ec will be created inner method
     */
    public Map<String, String> checkTtlTableAutoAddParts(ExecutionContext ec) {
        List<TtlInfoRecord> ttlInfoRecs = new ArrayList<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            TtlInfoAccessor ttlInfoAccessor = new TtlInfoAccessor();
            ttlInfoAccessor.setConnection(metaDbConn);
            ttlInfoRecs = ttlInfoAccessor.queryAllTtlInfoList();
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex);
        }
        if (ec == null) {
            ec = new ExecutionContext();
        }
        Map<String, String> warningTableInfoMap = checkAndFindTablesNeedWarnings(ttlInfoRecs, ec);
        doWarningForCheckedResults(warningTableInfoMap);
        return warningTableInfoMap;
    }

    public Map<String, String> checkAndFindTablesNeedWarnings(List<TtlInfoRecord> ttlInfoRecs,
                                                              ExecutionContext ec) {

        List<String> warningTableInfoList = new ArrayList<>();
        Map<String, String> warningTableMsgInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        String debugCurrentDataTime = ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_CURRENT_DATETIME);
        String debugTargetSchema =
            ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_WARNING_SCAN_TARGET_SCHEMA);
        String debugTargetTable = ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_WARNING_SCAN_TARGET_TABLE);

        for (int i = 0; i < ttlInfoRecs.size(); i++) {

            TtlInfoRecord ttlInfoRec = ttlInfoRecs.get(i);
            String dbName = ttlInfoRec.getTableSchema();
            String tblName = ttlInfoRec.getTableName();

            if (!StringUtils.isEmpty(debugTargetSchema)) {
                if (!dbName.equalsIgnoreCase(debugTargetSchema)) {
                    continue;
                }
            }

            if (!StringUtils.isEmpty(debugTargetTable)) {
                if (!tblName.equalsIgnoreCase(debugTargetTable)) {
                    continue;
                }
            }

            String ttlTimeZoneStr = ttlInfoRec.getTtlTimezone();
            ZoneId ttlZoneId = ZoneId.of(ttlTimeZoneStr);
            String currentDateTimeStr = TtlDatetimeNormalizer.fetchCurrentDateTimeWithTimeZone(ttlZoneId);
            if (!StringUtil.isEmpty(debugCurrentDataTime)) {
                currentDateTimeStr = debugCurrentDataTime;
            }
            SchemaManager sc = null;
            try {
                sc = ec.getSchemaManager(dbName);
            } catch (Throwable ex) {
                logger.warn(String.format("Failed to fetch schema manager of db[%s] in ttl warning scanning", dbName),
                    ex);
            }
            if (sc == null) {
                continue;
            }
            TableMeta ttlPrimTblMeta = sc.getTableWithNull(tblName);
            if (ttlPrimTblMeta == null) {
                continue;
            }

            TtlDefinitionInfo primTtlInfo = ttlPrimTblMeta.getTtlDefinitionInfo();
            PartitionInfo primPartInfo = ttlPrimTblMeta.getPartitionInfo();

            /**
             * Check if the ttl table need ignore warning checks
             */
            if (checkIfNeedIgnoreWarning(ec, primPartInfo, primTtlInfo, ttlZoneId, currentDateTimeStr)) {
                continue;
            }

            boolean ttlMetaInvalid = false;
            String warningMsg = "";
            boolean arcByRow = primTtlInfo.performArchiveByRow();
            boolean boundArcTbl = primTtlInfo.needPerformExpiredDataArchiving();

            TableMeta targetTblMeta = ttlPrimTblMeta;
            PartitionInfo targetPartInfo = ttlPrimTblMeta.getPartitionInfo();
            PartitionByDefinition targetPartBy = targetPartInfo.getPartitionBy();
            boolean checkAddPartsForArcCci = false;
            TableMeta arcCciTblMeta = null;
            boolean invalidTtlMeta = false;
            String[] invalidTtlMetaWarningMsgOutput = new String[1];

            if (arcByRow) {
                if (boundArcTbl) {
                    arcCciTblMeta = fetchAndCheckCciTableMeta(ec, ttlPrimTblMeta, invalidTtlMetaWarningMsgOutput);
                    if (arcCciTblMeta != null) {
                        targetPartInfo = arcCciTblMeta.getPartitionInfo();
                        targetPartBy = targetPartInfo.getPartitionBy();
                        targetTblMeta = arcCciTblMeta;
                        checkAddPartsForArcCci = true;
                    } else {
                        invalidTtlMeta = true;
                    }
                }
            } else {
                if (ttlInfoRec.getArcKind() == TtlInfoRecord.ARCHIVE_KIND_SUBPARTITION) {
                    targetPartBy = targetPartBy.getSubPartitionBy();
                }
                if (boundArcTbl) {
                    arcCciTblMeta = fetchAndCheckCciTableMeta(ec, ttlPrimTblMeta, invalidTtlMetaWarningMsgOutput);
                    if (arcCciTblMeta == null) {
                        invalidTtlMeta = true;
                    }
                }
            }

            if (invalidTtlMeta) {
                warningMsg = invalidTtlMetaWarningMsgOutput[0];
                String tblInfo = String.format("`%s`.`%s`", dbName, tblName);
                warningTableMsgInfoMap.put(tblInfo, warningMsg);
                continue;
            }

            List<PartitionSpec> psList = targetPartBy.getPartitions();
            int partCnt = psList.size();
            boolean usingMaxValParts = targetPartBy.containsMaxValuePartition();
            Long posiOfLastNonMaxValPart = Long.valueOf(partCnt);
            if (usingMaxValParts && partCnt > 1) {
                posiOfLastNonMaxValPart = partCnt - 2L;
            }

            boolean onlyWaringForLastPart =
                ec.getParamManager().getBoolean(ConnectionParams.TTL_ONLY_WARNING_FOR_THE_LAST_PART);

            /**
             * Route the pivotDatetimeStr to target partitions
             */
            PartKeyLevel partKeyLevel = targetPartBy.getPartLevel();
            PartSpecSearcher specSearcher = targetPartInfo.getPartSpecSearcher();
            TtlPartitionUtil.TtlColValueCalcContext calcContext =
                TtlPartitionUtil.TtlColValueCalcContext.buildBoundValueCalcContext(primTtlInfo, ec);
            String tarPartName = TtlPartitionUtil.findTargetNonMaxValPartByRoutingTtlColVal(
                ec,
                targetTblMeta,
                primTtlInfo,
                partKeyLevel,
                ttlTimeZoneStr,
                currentDateTimeStr,
                calcContext);

            if (tarPartName == null) {
                /**
                 * tarPartName is maxvalue partition
                 */

                if (!usingMaxValParts) {
                    /**
                     * No found any partitions for current datetime !!!!
                     */
                    String tblInfo = String.format("`%s`.`%s`", dbName, tblName);
                    warningTableInfoList.add(tblInfo);
                    warningMsg =
                        String.format("No found any parts for current time: [%s]", currentDateTimeStr);
                    if (checkAddPartsForArcCci) {
                        warningMsg += ", checkForArcCci=true";
                    }
                    warningTableMsgInfoMap.put(tblInfo, warningMsg);
                } else {

                    /**
                     * Using maxvalue partition,
                     * and current datetime is routing to the last maxvalue partition
                     */

                    /**
                     * Current Datetime is routing to the last maxvalue partition
                     */
                    String tblInfo = String.format("`%s`.`%s`", dbName, tblName);
                    warningTableInfoList.add(tblInfo);
                    warningMsg =
                        String.format("The last maxvalue part is using for current_time[%s]", currentDateTimeStr);
                    if (checkAddPartsForArcCci) {
                        warningMsg += ", checkForArcCci=true";
                    }
                    warningTableMsgInfoMap.put(tblInfo, warningMsg);
                }

            } else {
                /**
                 * tarPartName must be a non-maxvalue partition
                 */
                PartitionSpec tarPartSpec = specSearcher.getPartSpecByPartName(tarPartName);
                Long partPosi = tarPartSpec.getPosition();

                if (partPosi < posiOfLastNonMaxValPart) {
                    /**
                     * Current Datetime is NOT route to the last non-maxvalue partition,
                     */

                    if (onlyWaringForLastPart) {
                        /**
                         * Only warning for the last part, so ignore
                         */
                        continue;
                    }

                    Long deltaPartCntFromCurrToLastNonMaxValPart = posiOfLastNonMaxValPart - partPosi;
                    Integer arcTimeUnit = primTtlInfo.getTtlInfoRecord().getArcPartUnit();
                    int arcPrePartCntValOfTtl = primTtlInfo.getTtlInfoRecord().getArcPrePartCnt();

                    /**
                     * Calc the final warning prePartCntVal
                     * by defaultWarningPrePartCntValOfTtl and arcPrePartCntValOfTtl
                     * , because arcPrePartCntValOfTtl defined by user may be a small value
                     */
                    int warningPartCntVaCalcByArcPrePartCnt = arcPrePartCntValOfTtl / 4;
                    Integer defaultWarningPrePartCntValOfTtl =
                        TtlConfigUtil.getTtlTimeUnitToPrePartCntWarning().get(arcTimeUnit);
                    int finalWarningPartCnt =
                        Math.max(warningPartCntVaCalcByArcPrePartCnt, defaultWarningPrePartCntValOfTtl);
                    if (deltaPartCntFromCurrToLastNonMaxValPart >= finalWarningPartCnt) {
                        continue;
                    }

                    warningMsg =
                        String.format("Only [%s] parts left for current_time[%s], warnings count is [%s]",
                            defaultWarningPrePartCntValOfTtl, currentDateTimeStr, finalWarningPartCnt);
                } else {
                    /**
                     * Current Datetime is routing to the last non-maxvalue partition
                     */
                    warningMsg = String.format("The last parts for current_time[%s]", currentDateTimeStr);
                }

                if (checkAddPartsForArcCci) {
                    warningMsg += ", checkForArcCci=true";
                }

                /**
                 * Current Datetime is routing to the last maxvalue partition,
                 * so need do some warning
                 */
                String tblInfo = String.format("`%s`.`%s`", dbName, tblName);
                warningTableInfoList.add(tblInfo);
                warningTableMsgInfoMap.put(tblInfo, warningMsg);
            }
        }
        return warningTableMsgInfoMap;
    }

    private static TableMeta fetchAndCheckCciTableMeta(ExecutionContext ec,
                                                       TableMeta ttlPrimTblMeta,
                                                       String[] warningMsgOutput) {
        boolean ttlMetaInvalid = false;
        TableMeta arcCciTblMeta = null;
        String warningMsg = "";

        TtlDefinitionInfo targetTtlInfo = ttlPrimTblMeta.getTtlDefinitionInfo();
        String ttlSchema = targetTtlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = targetTtlInfo.getTtlInfoRecord().getTableName();
        String ttlArcCciName = targetTtlInfo.getTmpTableName();
        String tblNameOfArcCci = TtlJobUtil.getActualTableNameAndCheckCciTypeForArcCci(ttlPrimTblMeta, ec);

        if (StringUtils.isEmpty(tblNameOfArcCci)) {
            ttlMetaInvalid = true;
            warningMsg = String.format(
                "The arcCci[%s] using by ttl table [%s.%s] is not a archive columnar index, ttl_meta_invalid=true",
                ttlArcCciName, ttlSchema, ttlTblName);
        } else {
            arcCciTblMeta = TtlJobUtil.getArchiveCciTableMeta(ttlSchema, ttlTblName, ec);
            if (arcCciTblMeta == null) {
                ttlMetaInvalid = true;
                warningMsg = String.format(
                    "The arcCci[%s] using by ttl table [%s.%s] cannot found the table meta, ttl_meta_invalid=true",
                    ttlArcCciName, ttlSchema, ttlTblName);
            }
        }
        if (ttlMetaInvalid) {
            if (warningMsgOutput != null) {
                warningMsgOutput[0] = warningMsg;
            }
        }
        return arcCciTblMeta;
    }

    /**
     * Check if the ttl table need ignore warning checks by using ExecutionContext
     */
    private boolean checkIfNeedIgnoreWarning(ExecutionContext ec,
                                             PartitionInfo partInfo,
                                             TtlDefinitionInfo ttlInfo,
                                             ZoneId ttlTimeZoneId,
                                             String currentDateTimeStr) {
        if (!partInfo.isPartitionedTable() && !partInfo.isSingleTable()) {
            return true;
        }

        if (ttlInfo.useExpireOverPartitionsPolicy()) {
            /**
             * Ignore using expire over policy
             */
            return true;
        }
        if (!ttlInfo.isEnableTtlSchedule()) {
            /**
             * Ignore the ttl tables with ttl schedule disabled
             */
            return true;
        }

        if (ttlInfo.performArchiveByRow() && !ttlInfo.needPerformExpiredDataArchiving()) {
            /**
             * Ignore use row-level cleanup and has not any archive tables
             */
            return true;
        }

        Date ttlDefModifiedTime = ttlInfo.getTtlInfoRecord().getGmtModified();
        ZoneId defaultTz = TtlDatetimeNormalizer.TTL_DEFAULT_TIME_ZONE_ID;
        String ttlDefModifiedTimeStr = TtlDatetimeNormalizer.isoFormatDateObj(ttlDefModifiedTime);
        String debugTttModifiedTimeStr =
            ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_WARNING_SCAN_TTLINFO_MODIFIED_TIME);
        if (!StringUtils.isEmpty(debugTttModifiedTimeStr)) {
            ttlDefModifiedTimeStr = debugTttModifiedTimeStr;
        }

        Long ttlDefCreatedTimeToUnixTimeSec =
            TtlDatetimeNormalizer.datetimeStrToUnixTime(ttlDefModifiedTimeStr, defaultTz);
        Long currDateTimeToUnixTimeSec = TtlDatetimeNormalizer.datetimeStrToUnixTime(currentDateTimeStr, ttlTimeZoneId);
        Long deltaTimeSec = currDateTimeToUnixTimeSec - ttlDefCreatedTimeToUnixTimeSec;
        Long delayWarningSec =
            ec.getParamManager().getLong(ConnectionParams.TTL_WARNING_DELAY_SECONDS_AFTER_TTL_MODIFIED);

        if (deltaTimeSec < delayWarningSec) {
            /**
             * Ignore ttl table which created time is later than current datetime
             */
            return true;
        }

        /**
         * Need do the warning for  ttl table
         */
        return false;
    }
}