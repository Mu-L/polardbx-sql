package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.ConcurrentHashSet;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.ccl.DnCclRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.node.CCLDetectUtils.DubiousItem;

/**
 * @author liugaoji
 */
public class CCLDetectManager extends AbstractLifecycle {

    protected static final Logger logger = LoggerFactory.getLogger(CCLDetectManager.class);
    private static final CCLDetectManager instance = new CCLDetectManager();
    public static final String DEFAULT_ROOT_COLUMN = "DEFAULT_ROOT_COLUMN";
    public static final String UNDETERMINED_COLUMN = "UNDETERMINED_COLUMN";
    private static final long CCL_DETECT_INTERVAL = DynamicConfig.getInstance().getCclDetectInterval();
    private final ConcurrentHashSet<StorageInstHaContext> dubiousInsts = new ConcurrentHashSet<>();
    // pxc-id;CCL;templateId -> {generated concurrency, id}
    private final ConcurrentHashMap<String, Pair<Long, Long>> globalGenKeyDict = new ConcurrentHashMap<>();
    // DN Accesser
    public CCLDetectDnActor cclDetectDnActor = new CCLDetectDnActor();
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledExecutorService dubiousInstHandleExecutor;
    private ScheduledExecutorService dnCclRuleExpireExecutor;

    public static CCLDetectManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    public synchronized void clearGlobalGenKey() {
        this.dubiousInsts.clear();
        this.globalGenKeyDict.clear();
    }

    public synchronized void initGlobalGenKey() {
        this.clearGlobalGenKey();
        try {
            Iterator<StorageInstHaContext> iterator = getDnMasterIterator();
            while (iterator.hasNext()) {
                StorageInstHaContext instHaContext = iterator.next();
                List<DnCclRecord> records = CCLDetectDnActor.getDnCclRules(() ->
                        DbTopologyManager.getConnectionForStorage(instHaContext)
                    , instHaContext.getInstId(), instHaContext.getStorageInstId());
                if (records != null) {
                    records.stream().
                        filter(e -> e.keywords.contains("CCL"))
                        .forEach(record -> {
                            globalGenKeyDict.put(genGlobakKey(instHaContext, record.keywords),
                                Pair.of(Long.valueOf(record.concurrencyCount), Long.valueOf(record.id)));
                        });
                }
            }

            // log
            StringBuilder sb = new StringBuilder();
            globalGenKeyDict.forEach((k, v) ->
                sb.append(k).append("->[concurrency=").append(v.getKey())
                    .append(", id=").append(v.getValue()).append("]; "));
            logger.warn(String.format("CCL_DETECT initGlobalGenKey SUCCESS %s", sb));
        } catch (Throwable t) {
            logger.error("CCL_DETECT initGlobalGenKey ERROR", t);
        }

    }

    public static Iterator<StorageInstHaContext> getDnMasterIterator() {
        //storageStatusMap only storageIds of master && htap-learner.
        Map<String, StorageInstHaContext> storageStatusMap =
            StorageHaManager.getInstance().getStorageHaCtxCache();

        return storageStatusMap.values()
            .stream()
            .filter(StorageInstHaContext::isDNMaster)
            .iterator();
    }

    @Override
    protected void doInit() {
        if (ConfigDataMode.isMasterMode()) {
            // Start thread in any Role, check isMasterNode later to prevent leader crash
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("CCL-Detect-Factory", true));

            scheduledExecutorService
                .scheduleWithFixedDelay(new CCLDetectTask(), 0L, CCL_DETECT_INTERVAL,
                    TimeUnit.SECONDS);

            dubiousInstHandleExecutor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("CCL-DubiousInst-Handle-Factory", true));

            dnCclRuleExpireExecutor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("CCL-DN-CCLRule-Expire-Factory", true));

            initGlobalGenKey();
        }
    }

    public class CCLDetectTask implements Runnable {
        private CCLDetectConfig config;

        // for test
        public CCLDetectTask(CCLDetectConfig config) {
            this.config = config;
        }

        public CCLDetectTask() {
        }

        @Override
        public void run() {
            try {
                config = CCLDetectConfig.getInstance();
                config.refresh();
                if (!config.isCclDetectEnable() || !ConfigDataMode.isMasterMode() || !LeaderStatusBridge.getInstance()
                    .hasLeadership()) {
                    return;
                }
                //storageStatusMap only storageIds of master && htap-learner.
                Iterator<StorageInstHaContext> iterator = getDnMasterIterator();
                while (iterator.hasNext()) {
                    StorageInstHaContext instHaContext = iterator.next();

                    // already in kill Queue or metadb
                    if (instHaContext == null || dubiousInsts.contains(instHaContext) || instHaContext.isMetaDb()) {
                        continue;
                    }
                    long activeNum = cclDetectDnActor.getActiveSessionNum(() ->
                            DbTopologyManager.getConnectionForStorage(instHaContext)
                        , instHaContext.getInstId(), instHaContext.getStorageInstId());

                    // add to queue
                    if (activeNum > config.getConnectionLimit()) {

                        boolean passDdlChecker = cclDetectDnActor.hasDdlChecker(() ->
                                DbTopologyManager.getConnectionForStorage(instHaContext)
                            , instHaContext.getStorageInstId());

                        EventLogger.log(EventType.CCL_DETECT, String.format(
                            "CCL_DETECT added inst %s with ActiveSession %s PassDdlChecker %s to DubiousInst",
                            instHaContext.toBriefString(), activeNum, passDdlChecker));

                        if (passDdlChecker && !dubiousInsts.contains(instHaContext)) {
                            // dry run is stateless
                            if (!config.isDryRun()) {
                                dubiousInsts.add(instHaContext);
                            }
                            handleDubiousInstWithLoop(instHaContext, 0);
                        }

                    }
                }

            } catch (Throwable t) {
                logger.error("CCL_DETECT check slave delay error!", t);
            }
        }

        // recursively generate CCLRule: every getCclDetectDnDelayInterval we kill and generate a batch CCL Rule
        public void handleDubiousInstWithLoop(StorageInstHaContext instHaContext, int depth) {

            long delayInterval = config.getDnDelayInterval();
            dubiousInstHandleExecutor.schedule(() -> {
                try {

                    // recheck
                    long curActiveNum = cclDetectDnActor.getActiveSessionNum(() ->
                            DbTopologyManager.getConnectionForStorage(instHaContext)
                        , instHaContext.getInstId(), instHaContext.getStorageInstId());

                    if (curActiveNum > config.getConnectionLimit()) {

                        // generateCCL rules and kill
                        List<DubiousItem> infos = cclDetectDnActor.getActiveSession(
                            () -> DbTopologyManager.getConnectionForStorage(instHaContext),
                            instHaContext.getStorageInstId());

                        int ruleNum = -1;
                        if (infos == null || infos.isEmpty()) {
                            logger.error("CCL_DETECT DN is abnormal but info is empty");
                        } else {

                            EventLogger.log(EventType.CCL_DETECT, String.format(
                                "CCL_DETECT DubiousItemSize %d DubiousItem[0] %s",
                                infos.size(),
                                (infos.get(0).toString().length() > 150
                                    ? infos.get(0).toString().substring(0, 150) + "..."
                                    : infos.get(0).toString())
                            ));

                            ruleNum = generateCclRulesAndKill(
                                () -> DbTopologyManager.getConnectionForStorage(instHaContext),
                                infos, instHaContext, depth, config);
                        }

                        EventLogger.log(EventType.CCL_DETECT, String.format(
                            "CCL_DETECT Dubious inst %s generateCCLRule %s at batch %s with sessionNum %s",
                            instHaContext.toBriefString(), ruleNum, depth, curActiveNum));

                        if (ruleNum > 0 && !config.isDryRun()) {
                            handleDubiousInstWithLoop(instHaContext, depth + 1);
                        } else {
                            dubiousInsts.remove(instHaContext);
                            EventLogger.log(EventType.CCL_DETECT, String.format(
                                "CCL_DETECT Dubious Inst %s not dive deeper depth %s ruleNum %s",
                                instHaContext.toBriefString(),
                                depth, ruleNum));
                        }

                    } else {
                        dubiousInsts.remove(instHaContext);
                        EventLogger.log(EventType.CCL_DETECT, String.format(
                            "CCL_DETECT Dubious Inst %s has recover after Batch %s", instHaContext.toBriefString(),
                            depth));
                    }
                } catch (Throwable t) {
                    logger.error("CCL_DETECT handleDubiousInstWithLoop error!", t);
                }

            }, delayInterval, TimeUnit.SECONDS);
        }
    }

    // depth start from 0
    public int generateCclRulesAndKill(Supplier<Connection> connectionSupplier,
                                       List<DubiousItem> infos, StorageInstHaContext instHaContext, int depth,
                                       CCLDetectConfig config) {

        // (templateId;RootColumn) -> {totalTime, concurrency}
        Map<String, Pair<Long, Integer>> interceptTime = new HashMap<>();
        // (templateId;RootColumn) -> List<Item>
        Map<String, List<DubiousItem>> interceptInfos = new HashMap<>();

        boolean isTimeInMillis = cclDetectDnActor.isTimeInMillis(connectionSupplier);
        boolean hasOutTrxSql = CCLDetectUtils.hasOutTrx(infos, interceptTime, interceptInfos, isTimeInMillis);
        if (!hasOutTrxSql) {
            EventLogger.log(EventType.CCL_DETECT,
                String.format("CCL_DETECT Depth %s No OutTrxSQL AvgExecTime larger than CclDetectSlowThreshold %d",
                    depth, config.getSlowThreshold()));
            return 0;
        }

        // print first key as log
        String firstKey = interceptTime.keySet().stream().findFirst().orElse(null);
        Pair<Long, Integer> firstInterceptTimeEntry = firstKey != null ? interceptTime.get(firstKey) : Pair.of(0L, 0);
        List<DubiousItem> firstInterceptInfoList = firstKey != null ? interceptInfos.get(firstKey) : Arrays.asList();

        logger.warn(
            String.format("CCL_DETECT GenerateCCLRulesAndKill Depth %s InterceptTime Size=%d InterceptInfos size=%d " +
                    "FirstKey: %s, FirstInterceptTime: %s, FirstInterceptInfoList: %s "
                    + "KillBatch %s",
                depth, interceptTime.size(), interceptInfos.size(), firstKey, firstInterceptTimeEntry,
                firstInterceptInfoList,
                config.getKillBatch()));

        // expire key and id
        HashMap<String, Long> cclRuleKeyAndId = new HashMap<>();

        interceptTime.entrySet()
            .stream()
            // sort avgExecTime as desc
            .sorted(Map.Entry.<String, Pair<Long, Integer>>comparingByValue((a, b) -> {
                double ratio1 = a.getKey().doubleValue() / a.getValue().doubleValue();
                double ratio2 = b.getKey().doubleValue() / b.getValue().doubleValue();
                return Double.compare(ratio2, ratio1);
            }))
            // concurrency > threshold
            .filter((entry) -> entry.getValue().getValue() > config.getKillMinConcurrency())
            // avgExecTime > cclDetectSlowThreshold (slowThreshold is in seconds; convert to ms only when DN time unit is ms)
            .filter(
                (entry) -> entry.getValue().getKey()
                    > (long) config.getSlowThreshold() * (isTimeInMillis ? 1000L : 1L) * entry.getValue().getValue())
            // avgExecTime < cclDetectMaxThreshold (maxThreshold is in seconds; convert to ms only when DN time unit is ms)
            .filter(
                (entry) -> entry.getValue().getKey()
                    < (long) config.getMaxThreshold() * (isTimeInMillis ? 1000L : 1L) * entry.getValue().getValue())
            .limit(Math.min(config.isDryRun() ? config.getKillBatch() : 1L << depth, config.getKillBatch()))
            .forEach((entry) -> {

                    // templateId;userId
                    String key = entry.getKey();

                    // kill query
                    List<DubiousItem> toKill = interceptInfos.get(key);
                    for (int i = 0; i < toKill.size(); i++) {
                        logger.warn(
                            String.format(
                                "CCL_DETECT Send Kill Inst %s Depth %s Index %s ExecTime %d Id %d TotalNum %d OriginSql %s",
                                instHaContext.toBriefString(),
                                depth,
                                i,
                                toKill.get(i).time,
                                toKill.get(i).id,
                                toKill.size(),
                                toKill.get(i).info));
                    }

                    long concurrency =
                        cclDetectDnActor.getCclConcurrency(entry.getValue().getValue(), depth);

                    // delete previous key firstly if previous concurrency is larger then current
                    String globalKey = genGlobakKey(instHaContext, key);
                    CclRuleDecision decision = decideCclRuleAction(config, globalKey, concurrency);
                    long shouldDelId = decision.shouldDelId;
                    boolean shouldGen = decision.shouldGen;
                    boolean shouldDelete = decision.shouldDelete;
                    concurrency = decision.concurrency;

                    // DN_CONCURRENCY start from curCon / 2, then curCon / 4 .... until 1
                    String genSql = cclDetectDnActor.getGenSql(connectionSupplier, concurrency, key);

                    Long id = -1L;
                    if (config.isDryRun()) {
                        EventLogger.log(EventType.CCL_DETECT, String.format(
                            "CCL_DETECT Depth %s DRY_RUN inst %s GenerateSQL %s TotalTime %s Concurrency %s",
                            depth, instHaContext.toBriefString(), genSql, entry.getValue().getKey(),
                            entry.getValue().getValue()));
                    } else {
                        // 1. log
                        EventLogger.log(EventType.CCL_DETECT,
                            String.format(
                                "CCL_DETECT Gen DN_CCL_RULE Inst %s Depth %s AvgTime %.2f Stmt %s ShouldDelete %s",
                                instHaContext.toBriefString(), depth,
                                entry.getValue().getKey().doubleValue() / entry.getValue()
                                    .getValue()
                                    .doubleValue()
                                , genSql,
                                shouldDelete));

                        try {
                            id = cclDetectDnActor.killAndGenerateCcl(connectionSupplier, toKill, shouldGen, genSql);
                        } catch (SQLException e) {
                            logger.error("CCL_DETECT Generate DN CCL RULE error" + e.getMessage());
                        }

                        if (shouldDelete) {
                            logger.warn("CCL_DETECT DELETE DN CCL RULE " + key + " ID " + shouldDelId);
                            //2. delete
                            cclDetectDnActor.deleteDnCCLWithKill(connectionSupplier,
                                Pair.of(key, shouldDelId));
                        }

                    }
                    cclRuleKeyAndId.put(key, id);
                    globalGenKeyDict.put(globalKey, Pair.of(concurrency, id));
                }
            );

        // delete DN cclRules after cclDetectDnRuleExpireTime
        dnCclRuleExpireExecutor.schedule(() -> {
            try {
                if (!config.isDryRun()) {
                    cclDetectDnActor.deleteDnCCL(connectionSupplier, cclRuleKeyAndId);
                }
                for (Map.Entry<String, Long> entry : cclRuleKeyAndId.entrySet()) {
                    globalGenKeyDict.remove(genGlobakKey(instHaContext, entry.getKey()));
                }
            } catch (Throwable t) {
                logger.error("CCL_DETECT DELETE DN CCL RULE error" + t.getMessage());
            }
        }, config.getDnRuleExpireTime(), TimeUnit.SECONDS);
        return cclRuleKeyAndId.size();
    }

    public ConcurrentHashMap<String, Pair<Long, Long>> getGlobalGenKeyDict() {
        return globalGenKeyDict;
    }

    private String genGlobakKey(StorageInstHaContext instHaContext, String key) {
        return instHaContext.getStorageInstId() + ";" + key;
    }

    /**
     * CCL规则决策结果
     */
    public static class CclRuleDecision {
        long shouldDelId;
        boolean shouldDelete;
        boolean shouldGen;
        long concurrency;

        public CclRuleDecision(long shouldDelId, boolean shouldDelete, boolean shouldGen, long concurrency) {
            this.shouldDelId = shouldDelId;
            this.shouldDelete = shouldDelete;
            this.shouldGen = shouldGen;
            this.concurrency = concurrency;
        }
    }

    /**
     * 决定CCL规则的操作（生成/删除/只kill）
     *
     * @param config CCL检测配置
     * @param globalKey 全局key
     * @param concurrency 当前并发数
     * @return CCL规则决策结果
     */
    public CclRuleDecision decideCclRuleAction(CCLDetectConfig config, String globalKey, long concurrency) {
        long shouldDelId = -1;
        boolean shouldGen = true;
        boolean shouldDelete = false;

        if (!config.isDryRun()) {
            String content = globalGenKeyDict.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
            logger.warn(
                String.format("CCL_DETECT DEBUG GlobalKey %s Contains %s GlobalKeySize %s Content %s",
                    globalKey,
                    globalGenKeyDict.containsKey(globalKey), globalGenKeyDict.size(), content));

            if (globalGenKeyDict.containsKey(globalKey)) {
                Pair<Long, Long> prev = globalGenKeyDict.get(globalKey);
                long prevCon = prev.getKey();
                if (prevCon > concurrency) {
                    shouldDelId = prev.getValue();
                    shouldDelete = true;
                } else {
                    // only kill
                    shouldGen = false;
                    concurrency = prevCon;
                }
            }
        }

        return new CclRuleDecision(shouldDelId, shouldDelete, shouldGen, concurrency);
    }

}