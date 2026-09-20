package com.alibaba.polardbx.optimizer.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author fangwu
 */
public class ExecutionContextPropertiesLifeCycle {
    private static final Logger logger = LoggerFactory.getLogger(ExecutionContextPropertiesLifeCycle.class);

    /**
     * lifecycle for execution properties
     */
    public enum PROPERTIES_LIFE {
        STMT {
            @Override
            public void clean(ExecutionContext ec) {
                if (ec == null) {
                    return;
                }
                ec.cleanAfterStmt();
            }
        }, TRANS {
            @Override
            public void clean(ExecutionContext ec) {
                if (ec == null) {
                    return;
                }
                ec.setTransaction(null);
            }
        }, SESSION {
            @Override
            public void clean(ExecutionContext ec) {
                if (ec == null) {
                    return;
                }
                ec.setPartitionHint(null);
            }
        };

        public abstract void clean(ExecutionContext ec);
    }

    public static Set<Field> ignoreFields = new HashSet<>();

    public static Map<Field, PROPERTIES_LIFE> propertiesLife = new HashMap<>();

    static {
        try {
            propertiesLife.put(ExecutionContext.class.getDeclaredField("grayWorkload"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("sqlTemplateId"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("pruningTime"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("traceId"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("tracer"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("originSql"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("params"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("parallelStatisticExecutor"),
                PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("hllExecutor"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("isWarmup"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("htapTrace"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("routingType"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("ttlQueryType"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("filesSchemaManager"), PROPERTIES_LIFE.STMT);

            propertiesLife.put(ExecutionContext.class.getDeclaredField("versionStorageStatistics"),
                PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("columnarScanMetrics"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("extColStats"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("dmlWriteContext"), PROPERTIES_LIFE.STMT);

            propertiesLife.put(ExecutionContext.class.getDeclaredField("partitionHint"), PROPERTIES_LIFE.SESSION);

            propertiesLife.put(ExecutionContext.class.getDeclaredField("transaction"), PROPERTIES_LIFE.TRANS);

            ignoreFields.add(ExecutionContext.class.getDeclaredField("schemaManagers"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("forbidBuildLocalIndexLater"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("currentSchemaManager"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("autoCommit"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("txIsolation"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("txId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("needAutoSavepoint"));

            ignoreFields.add(ExecutionContext.class.getDeclaredField("extraCmds"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("defaultExtraCmds"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("hintCmds"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("paramManager"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("parameterNlsStrings"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("concurrentService"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("groupHint"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("autoGeneratedKeys"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("columnIndexes"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("columnNames"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("resultSetType"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("resultSetConcurrency"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("localInFileInputStream"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sqlMode"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sqlModeFlags"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sql"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("encoding"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sessionCharset"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("appName"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("schemaName"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("physicalRecorder"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("recorder"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("enableTrace"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("enableDdlTrace"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("enableFeedBackWorkload"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("stressTestValid"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("socketTimeout"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("fkModifyCascade"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("isPrivilegeMode"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("modifySelect"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("modifySelectParallel"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("correlateRowMap"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("correlateFieldInViewMap"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("scalarSubqueryCtxMap"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("explain"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sqlType"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("planProperties"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("asiConf"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("hasScanWholeTable"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("hasUnpushedJoin"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("hasTempTable"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("privilegeVerifyItems"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("onlyUseTmpTblPool"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("internalSystemSql"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("runtimeStatistics"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("usingPhySqlCache"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("memoryPoolHolder"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("doingBatchInsertBySpliter"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("internalSubExecution"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("isApplyingSubquery"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("subqueryId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("cacheRefs"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("cacheRelNodeIds"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("finalPlan"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("unOptimizedPlan"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("phySqlId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sqlId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("cluster"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("startTime"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("logicalSqlStartTimeInMs"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("logicalSqlStartTime"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("executeMode"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("workloadType"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("mdcConnString"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("recordRowCnt"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("distinctKeyCnt"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("asyncDDLContext"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("ddlContext"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("phyDdlExecutionRecord"));
            propertiesLife.put(ExecutionContext.class.getDeclaredField("ddlInitialJobId"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("exclusiveResources"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("sharedResources"), PROPERTIES_LIFE.STMT);
            propertiesLife.put(ExecutionContext.class.getDeclaredField("hasDdlInitialJob"), PROPERTIES_LIFE.STMT);
            // Cleared by cleanAfterStmt() via set(false): the AtomicReference itself must
            // keep being shared with DdlContext, so it can not be nulled out per statement.
            ignoreFields.add(ExecutionContext.class.getDeclaredField("ddlClientConnectionReset"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("enableTwoPhaseDdl"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("multiDdlContext"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("runOnNewDdlEngine"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("tableInfoManager"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("dmlRelScaleOutWriteFlagMap"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("hasScaleOutWrite"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("isOriginSqlPushdownOrRoute"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("privilegeContext"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("flashbackArea"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("asOfCrossDdl"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("testMode"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("useHint"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("planSource"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("loadDataContext"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("cclContext"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("rescheduled"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("querySpillSpaceMonitor"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("shareReadView"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("groupParallelism"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("point"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("constantValues"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("returning"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("backfillReturning"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("returningAll"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("returningFlagForCdc"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("optimizedWithReturning"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("backfillId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("clientFoundRows"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("xplanStat"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("blockBuilderCapacity"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("enableOssDelayMaterializationOnExchange"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("columnarMaxShard"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("planType"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("columnarPlanCache"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("executingPreparedStmt"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("preparedStmtCache"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("calcitePlanOptimizerTrace"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("visitDBBuildIn"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("ignoredGsiSet"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("driverStatistics"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("readOrcFiles"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("readDeltaFiles"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("fcManager"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("scManager"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("storageInfoSupplier"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("userSql"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("resultSetHoldability"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("taskId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("estimatedBackfillBatchRows"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("overrideDdlParams"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("connection"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("columnarTracer"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("extraDatas"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("stats"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("serverVariables"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("userDefVariables"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("extraServerVariables"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("timeZone"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("connId"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("clientIp"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("readOnly"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("partitionHint"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("firstSwitchoverWaitTime"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("switchoverPerfCollections"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("checkSwitchoverWhenGetConnection"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("sqlParameterized"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("checkSupportsReturningAll"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("checkIsAllDnUseXDataSource"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("deepPageCacheMap"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("rescheduleStmtDone"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("rescheduleStmtFuture"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("multiStmtHasMore"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("containsBlockChainTable"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("blockChainSchema"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("blockChainTable"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("port"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("user"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("innerConnection"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("versionStorageStatistics"));
            ignoreFields.add(ExecutionContext.class.getDeclaredField("columnarScanMetrics"));
        } catch (NoSuchFieldException e) {
            logger.error("loading execution context properties error when init lifecycle", e);
        }
    }

}
