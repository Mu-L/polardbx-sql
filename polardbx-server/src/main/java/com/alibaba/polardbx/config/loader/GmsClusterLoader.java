package com.alibaba.polardbx.config.loader;

import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.common.secret.CredentialResolver;
import com.alibaba.polardbx.optimizer.external.catalog.ExternalSchemaReclaimer;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.ClusterSyncManager;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.PolarQuarantineManager;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.pl.UdfUtils;
import com.alibaba.polardbx.gms.config.InstConfigReceiver;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionMetaRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.InstLockAccessor;
import com.alibaba.polardbx.gms.topology.InstLockRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.ServerInstSubManager;
import com.alibaba.polardbx.gms.topology.SubInstConfigAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author chenghui.lch
 */
public class GmsClusterLoader extends ClusterLoader {

    private final SystemConfig systemConfig;
    private String instanceId;
    /**
     * A thread executor for async warming up new created db
     */
    private ThreadPoolExecutor newCreatedDbWarmingUpExecutor;

    protected static class InstInfoListener implements ConfigListener {

        protected final GmsClusterLoader gmsClusterLoader;

        public InstInfoListener(GmsClusterLoader gmsClusterLoader) {
            this.gmsClusterLoader = gmsClusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {

            // reload all read-only server inst ids
            ServerInstIdManager.getInstance().reload();

            // Reload all nodes including master and read-only.
            this.gmsClusterLoader.loadNodeInfos();

            // Init group configs for all read-only storage inst
            DbTopologyManager.initGroupConfigsForReadOnlyInstIfNeed();
        }
    }

    protected static class ServerInfoListener implements ConfigListener {
        protected GmsClusterLoader gmsClusterLoader;

        public ServerInfoListener(GmsClusterLoader clusterLoader) {
            this.gmsClusterLoader = clusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            // reload all read-only server inst ids
            ServerInstIdManager.getInstance().loadAllHtapInstIds();
            //here register the new storageIds listener after load the new learner InstId.
            ServerInstIdManager.getInstance().registerLearnerStorageInstId();
            this.gmsClusterLoader.loadNodeInfos();
        }
    }

    protected static class ServerSubClusterInfoListener implements ConfigListener {
        protected GmsClusterLoader gmsClusterLoader;

        public ServerSubClusterInfoListener(GmsClusterLoader clusterLoader) {
            this.gmsClusterLoader = clusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            ServerInstSubManager.getInstance().loadNodeSubCluster();
        }
    }

    protected static class ServerLoadWeightInfoListener implements ConfigListener {
        protected GmsClusterLoader gmsClusterLoader;

        public ServerLoadWeightInfoListener(GmsClusterLoader clusterLoader) {
            this.gmsClusterLoader = clusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            ServerInstSubManager.getInstance().loadNodeLoadWeight();
        }
    }

    protected static class InstLockConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            String instId = InstIdUtil.getInstId();
            processInstLockByGms(instId);
        }
    }

    protected static class InstPropertiesConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            MetaDbInstConfigManager.getInstance().reloadInstConfig();
        }
    }

    protected static class DbInfoConfigListener implements ConfigListener {

        protected GmsClusterLoader gmsClusterLoader;

        public DbInfoConfigListener(GmsClusterLoader gmsClusterLoader) {
            this.gmsClusterLoader = gmsClusterLoader;
        }

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            this.gmsClusterLoader.reloadDbInfoFromMetaDB();
        }
    }

    protected static class CdcSystemConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {

        }
    }

    protected static class WarmupOneSchemaTask implements Runnable {

        private SchemaConfig schema;

        public WarmupOneSchemaTask(SchemaConfig schemaConfig) {
            this.schema = schemaConfig;
        }

        @Override
        public void run() {

            if (schema == null) {
                return;
            }

            if (schema.isDropped()) {
                return;
            }

            if (TStringUtil.equalsIgnoreCase(schema.getName(), SystemDbHelper.CDC_DB_NAME)) {
                return;
            }

            final TDataSource ds = schema.getDataSource();
            if (ds == null) {
                return;
            }
            long startTime = System.nanoTime();
            Throwable ex = TDataSourceInitUtils.initDataSource(ds);
            if (ex == null) {

                logger.info("Init schema '{}' costs {} secs", schema.getName(),
                    (System.nanoTime() - startTime) / 1e9);

                try {
                    // Before init the next schema,
                    // wait for this schema finish some init task,
                    // e.g. RotateTrxLogTask, StatisticsTask, etc.
                    long sleepSeconds = DynamicConfig.getInstance().getWarmUpDbInterval();
                    if (sleepSeconds > 0 && 0 == DynamicConfig.getInstance().getTrxLogMethod()) {
                        Thread.sleep(sleepSeconds * 1000);
                    }
                } catch (InterruptedException e) {
                    logger.info("LogicalDb-Warming-Up-Thread is interrupted unexpectedly");
                    return;
                }
            } else {
                logger.warn(
                    "Failed to init schema " + schema.getName() + ", cause is " + ex.getMessage(), ex);
            }
        }
    }

    public GmsClusterLoader(SystemConfig systemConfig) {
        super(systemConfig.getClusterName(), systemConfig.getUnitName());
        this.systemConfig = systemConfig;
    }

    @Override
    public void doInit() {
        initAsyncDbWarmingUpExecutor();
        this.appLoader = new GmsAppLoader(this.cluster, this.unitName);
        if (StringUtils.isNotEmpty(cluster)) {
            this.appLoader.init();
        }
        this.loadCluster();
        this.loadLock();
    }

    protected void initDbInfoListener() {
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getDbInfoDataId(), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getDbInfoDataId(), new DbInfoConfigListener(this));
    }

    protected void loadLock() {

        if (ConfigDataMode.isPolarDbX()) {
            String instId = InstIdUtil.getInstId();
            processInstLockByGms(instId);
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getInstLockDataId(instId), null);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getInstLockDataId(instId),
                new InstLockConfigListener());
        } else {
            //ignore
        }
    }

    protected static void processInstLockByGms(String instId) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            InstLockAccessor instLockAccessor = new InstLockAccessor();
            instLockAccessor.setConnection(metaDbConn);
            InstLockRecord instLockRecord =
                instLockAccessor.getInstLockByInstId(instId);
            if (instLockRecord != null && instLockRecord.locked == InstLockRecord.INST_LOCKED) {
                CobarServer.getInstance().getConfig().getClusterLoader().isLock = true;
            } else {
                CobarServer.getInstance().getConfig().getClusterLoader().isLock = false;
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    @Override
    public void loadCluster() {
        logger.info("loadCluster:" + cluster + ",ConfigDataMode=" + ConfigDataMode.getMode());
        try {
            if (!ConfigDataMode.isPolarDbX()) {
                //ignore
            } else {
                loadPolarDbXCluster();
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    protected void loadPolarDbXCluster() {

        // Set Sync Manager used by GMS.
        GmsSyncManagerHelper.setSyncManager(new ClusterSyncManager());

        // load node info from server_info in metaDb
        loadServerNodeInfos();

        loadServerLoadWeightInfos();
        loadServerSubClusterInfos();

        // load properties from inst_config in metaDb
        this.loadProperties(instanceId);

        // register listener for db_info
        initDbInfoListener();

        // load dbInfo/userPriv/dbPriv from metaDb
        initClusterAppInfo();

        // register stored function
        registerStoredFunction();

        try {
            // init java function manager
            JavaFunctionManager.getInstance();
        } catch (Throwable ex) {
            logger.error("init java function manager failed, caused by " + ex.getMessage());
        }

        //init ccl
        CclManager.getService();

        // start some background threads.
        initBackgroundTasks();
    }

    private void initBackgroundTasks() {
    }

    private void registerStoredFunction() {
        try (Connection connection = MetaDbUtil.getConnection();) {
            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            List<FunctionMetaRecord> records = accessor.loadFunctionMetas();
            for (FunctionMetaRecord record : records) {
                UdfUtils.registerSqlUdf(record.routineMeta, record.canPush);
            }
        } catch (Throwable ex) {
            logger.error("Load function failed: " + ex.getCause());
        }
    }

    protected void loadServerInstIdInfos() {

        // register the listener to server_info
        String instInfoDataId = MetaDbDataIdBuilder.getInstInfoDataId();
        MetaDbConfigManager.getInstance()
            .register(instInfoDataId, null);
        MetaDbConfigManager.getInstance()
            .bindListener(instInfoDataId, new InstInfoListener(this));
    }

    protected void loadServerNodeInfos() {

        // load all server instId info (included server master inst and server read-only inst)
        // and their change  from metaDb
        loadServerInstIdInfos();

        // Load server/nodes info.
        loadNodeInfos();

        // register the listener to server_info
        String serverInfoDataId = MetaDbDataIdBuilder.getServerInfoDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance()
            .register(serverInfoDataId, null);
        MetaDbConfigManager.getInstance()
            .bindListener(serverInfoDataId, new ServerInfoListener(this));

        // load instId
        this.instanceId = CobarServer.getInstance().getConfig().getInstanceId();

        // load inst version
        String instanceVersion = Optional.ofNullable(System.getProperty(InstanceVersion.systemVersion)).orElse("5");
        InstanceVersion.reloadVersion(instanceVersion);
    }

    protected void loadServerSubClusterInfos() {
        String subClusterInfoDataId = MetaDbDataIdBuilder.getServerSubClusterDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance()
            .register(subClusterInfoDataId, null);
        MetaDbConfigManager.getInstance()
            .bindListener(subClusterInfoDataId, new ServerSubClusterInfoListener(this));
    }

    protected void loadServerLoadWeightInfos() {
        String loadWeightInfoDataId = MetaDbDataIdBuilder.getServerLoadWeightDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance()
            .register(loadWeightInfoDataId, null);
        MetaDbConfigManager.getInstance()
            .bindListener(loadWeightInfoDataId, new ServerLoadWeightInfoListener(this));
    }

    protected void loadNodeInfos() {
        synchronized (this) {
            GmsNodeManager.getInstance().reloadNodes(systemConfig.getServerPort());
            resetIdGenerator();
        }
    }

    protected void resetIdGenerator() {
        // IdGenerator must be reset after NodeId is allocated.
        IdGenerator.remove(TrxIdGenerator.getInstance().getIdGenerator());
        TrxIdGenerator.getInstance().setIdGenerator(IdGenerator.getIdGenerator());

        // Rebind new NodeId to all IdGenerators
        IdGenerator.rebindAll();
    }

    protected void initClusterAppInfo() {
        if (StringUtils.isEmpty(instanceId)) {
            return;
        }

        // To be load by MetaDB
        if (!appLoader.isInited()) {
            appLoader.init();
        }

        CobarServer.getInstance().getConfig().setInstanceId(this.instanceId);
        CobarServer.getInstance().getConfig().getSystem().setInstanceId(this.instanceId);

        // init external catalog
        ExternalCatalogManager.getInstance().loadFromGms();
        SecretManager.getInstance().loadFromGms();
        CredentialResolver.setResolver(SecretManager.getInstance()::resolve);
        ExternalSchemaReclaimer.getInstance().init();

        // init instance-level quarantine config
        PolarQuarantineManager.getInstance().init();

        // CobarServer should set instance id before initializing
        // this so that we can get correct instance id in subsequent
        // MatrixConfigHolder.doInit().
        ((GmsAppLoader) appLoader).initDbUserPrivsInfo(this.instanceId);

        warmingLogicalDb();
        RoutingRuleManager.getInstance().init();

        // open the server port and accept query now!
        CobarServer.getInstance().online();
    }

    protected void warmingLogicalDb() {
        if (ConfigDataMode.isPolarDbX() && systemConfig.getEnableLogicalDbWarmmingUp()) {
            // Auto load all schemas here
            ConcurrentLinkedDeque<SchemaConfig> schemas = new ConcurrentLinkedDeque<>(appLoader.getSchemas().values());
//            final Runnable warmupLogicalDbTask = () -> {
//                SchemaConfig schema;
//                while (null != (schema = schemas.poll())) {
//                    if (schema.isDropped()) {
//                        continue;
//                    }
//                    final TDataSource ds = schema.getDataSource();
//                    long startTime = System.nanoTime();
//                    Throwable ex = TDataSourceInitUtils.initDataSource(ds);
//                    if (ex == null) {
//                        logger.info("Init schema '{}' costs {} secs", schema.getName(),
//                            (System.nanoTime() - startTime) / 1e9);
//                        try {
//                            // Before init the next schema,
//                            // wait for this schema finish some init task,
//                            // e.g. RotateTrxLogTask, StatisticsTask, etc.
//                            long sleepSeconds = DynamicConfig.getInstance().getWarmUpDbInterval();
//                            if (sleepSeconds > 0 && 0 == DynamicConfig.getInstance().getTrxLogMethod()) {
//                                Thread.sleep(sleepSeconds * 1000);
//                            }
//                        } catch (InterruptedException e) {
//                            logger.info("LogicalDb-Warming-Up-Thread is interrupted unexpectedly");
//                            return;
//                        }
//                    } else {
//                        logger.warn(
//                            "Failed to init schema " + schema.getName() + ", cause is " + ex.getMessage(), ex);
//                    }
//                }
//            };
            final Runnable warmupLogicalDbTask = () -> {
                SchemaConfig schema;
                while (null != (schema = schemas.poll())) {
                    WarmupOneSchemaTask warmupOneSchemaTask = new WarmupOneSchemaTask(schema);
                    warmupOneSchemaTask.run();
                }
            };

            int parallelism = DynamicConfig.getInstance().getWarmUpDbParallelism();
            parallelism = Integer.min(parallelism, schemas.size());
            for (int i = 0; i < parallelism; i++) {
                (new Thread(warmupLogicalDbTask, "LogicalDb-Warming-Up-Thread-" + i)).start();
            }
        }
    }

    public void loadProperties(String instId) {
        if (!ConfigDataMode.isPolarDbX()) {
            //ignore
            return;
        }
        MetaDbInstConfigManager.getInstance().registerInstReceiver(new InstConfigReceiver() {
            @Override
            public void apply(Properties props) {
                applyProperties(props);
            }
        });
        String dataId = MetaDbDataIdBuilder.getInstConfigDataId(InstIdUtil.getInstId());
        MetaDbConfigManager.getInstance().register(dataId, null);
        MetaDbConfigManager.getInstance().bindListener(dataId, new InstPropertiesConfigListener());

        // Register sub-instance configuration if sub-instance ID exists
        String subInstId = InstIdUtil.getSubInstId();
        if (subInstId != null) {
            String subInstDataId = MetaDbDataIdBuilder.getSubInstConfigDataId(subInstId);
            MetaDbConfigManager.getInstance().register(subInstDataId, null);
            MetaDbConfigManager.getInstance()
                .bindListener(subInstDataId, new SubInstConfigAccessor.SubInstPropertiesConfigListener());
        }

        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getVariableConfigDataId(instId), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getVariableConfigDataId(instId),
                new MetaDbVariableConfigManager.MetaDbVariableConfigListener());
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getCdcSystemConfigDataId(), null);
        MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getCdcSystemConfigDataId(),
            new MetaDbVariableConfigManager.CdcSystemConfigListener());
    }

    protected synchronized void reloadDbInfoFromMetaDB() {

        // Fetch all SchemaConfigs that are load in memory
        Map<String, SchemaConfig> allSchemaConfigMap = CobarServer.getInstance().getConfig().getSchemas();

        // Get dbInfos from metaDB
        Map<String, DbInfoRecord> newAddedDbInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, DbInfoRecord> newRemovedDbInfoMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        DbInfoManager.getInstance().loadDbInfoFromMetaDb(newAddedDbInfoMap, newRemovedDbInfoMap);

        // reload DbGroupManager
        DbGroupInfoManager.getInstance().onDbInfoChange(newAddedDbInfoMap, newRemovedDbInfoMap);

        // Find all db that are added
        Map<String, DbInfoRecord> dbInfoToBeLoadMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, DbInfoRecord> dbAppItem : newAddedDbInfoMap.entrySet()) {
            String dbName = dbAppItem.getKey();
            if (!allSchemaConfigMap.containsKey(dbName)) {
                dbInfoToBeLoadMap.putIfAbsent(dbName, dbAppItem.getValue());
            }
        }

        // Find all db that are removed
        Map<String, DbInfoRecord> dbInfoToUnLoadMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, DbInfoRecord> dbAppItem : newRemovedDbInfoMap.entrySet()) {
            String dbName = dbAppItem.getKey();
            if (allSchemaConfigMap.containsKey(dbName)) {
                dbInfoToUnLoadMap.putIfAbsent(dbName, dbAppItem.getValue());
            }
        }
        if (!ConfigDataMode.isMasterMode()) {
            // Find all db that are removed and is realdy load in schemaConfig
            for (String schemaName : allSchemaConfigMap.keySet()) {
                if (!newAddedDbInfoMap.containsKey(schemaName)) {
                    dbInfoToUnLoadMap.putIfAbsent(schemaName, null);
                }
            }
        } else {
            // In master mode, check for schemas that have been physically deleted from MetaDB
            // (not just DROPPING) but still reside in memory, caused by a missed DROP DATABASE sync.
            for (String schemaName : allSchemaConfigMap.keySet()) {
                if (!newAddedDbInfoMap.containsKey(schemaName)
                    && !dbInfoToUnLoadMap.containsKey(schemaName)
                    && !SystemDbHelper.isDBBuildIn(schemaName)) {
                    if (DynamicConfig.getInstance().isEnableFixStaleSchemaConfig()) {
                        // Fix mode: schedule the stale schema for release.
                        logger.warn(String.format(
                            "reloadDbInfoFromMetaDB: schema '%s' exists in memory but absent from MetaDB "
                                + "(isMasterMode=true), scheduling release to fix stale SchemaConfig",
                            schemaName));
                        dbInfoToUnLoadMap.putIfAbsent(schemaName, null);
                    } else {
                        // Diagnostic mode: log only, do not release.
                        logger.warn(String.format(
                            "reloadDbInfoFromMetaDB: schema '%s' exists in memory but absent from MetaDB, "
                                + "isMasterMode=true, skipped release (ENABLE_FIX_STALE_SCHEMA_CONFIG=false)",
                            schemaName));
                    }
                }
            }
        }

        // Reload priv info
        PolarPrivManager.getInstance().reloadPriv();

        // alloc resource for new created db
        for (Map.Entry<String, DbInfoRecord> dbAppItem : dbInfoToBeLoadMap.entrySet()) {
            allocResourceForLogicalDb(dbAppItem.getKey());
        }

        // clean resource and configs for removed db
        for (Map.Entry<String, DbInfoRecord> dbAppItem : dbInfoToUnLoadMap.entrySet()) {
            releaseResourceForLogicalDb(dbAppItem.getKey());
        }

    }

    protected void allocResourceForLogicalDb(String dbName) {
        Map<String, SchemaConfig> currentSchemas = CobarServer.getInstance().getConfig().getSchemas();
        SchemaConfig staleSchema = currentSchemas.get(dbName);
        if (staleSchema != null && !staleSchema.isDropped()) {
            // A stale SchemaConfig (dropped=false) exists for a db being newly allocated.
            // This indicates a missed DROP DATABASE sync on this CN node (e.g. network timeout).
            if (DynamicConfig.getInstance().isEnableFixStaleSchemaConfig()) {
                // Fix mode: force unload the stale SchemaConfig before loading the new one.
                logger.warn(String.format(
                    "allocResourceForLogicalDb: schema '%s' has stale SchemaConfig in memory (dropped=false), "
                        + "force releasing before reload (ENABLE_FIX_STALE_SCHEMA_CONFIG=true)",
                    dbName));
                releaseResourceForLogicalDb(dbName);
            } else {
                // Diagnostic mode: log only, do not release.
                logger.warn(String.format(
                    "allocResourceForLogicalDb: schema '%s' has stale SchemaConfig in memory (dropped=false), "
                        + "skipped release (ENABLE_FIX_STALE_SCHEMA_CONFIG=false)",
                    dbName));
            }
        }
        this.appLoader.loadApp(dbName);
        this.submitWarmingUpOneSchemaTask(dbName);
    }

    protected void releaseResourceForLogicalDb(String dbName) {
        this.appLoader.unLoadApp(dbName);

        // ---- clear db config receiver
        MetaDbInstConfigManager.getInstance().unRegisterDbReceiver(dbName);

        // ---- clear all group HaSwitcher for db
        StorageHaManager.getInstance().clearHaSwitcher(dbName);
    }

    protected void initAsyncDbWarmingUpExecutor() {
        ThreadPoolExecutor newCreatedDbWarmingUpExecutor =
            ExecutorUtil.createBufferedExecutor("NewCreatedDbWarmingUpTaskExecutor", 1,
                1024);
        this.newCreatedDbWarmingUpExecutor = newCreatedDbWarmingUpExecutor;
    }

    protected void submitWarmingUpOneSchemaTask(String dbName) {
        try {
            SchemaConfig schemaConfig = this.appLoader.getSchemas().get(dbName);
            if (schemaConfig == null) {
                return;
            }
            if (schemaConfig.isDropped()) {
                return;
            }
            if (schemaConfig.getDataSource() == null) {
                return;
            }
            if (schemaConfig.getDataSource().isInited()) {
                return;
            }
            WarmupOneSchemaTask warmupOneSchemaTask = new WarmupOneSchemaTask(schemaConfig);
            this.newCreatedDbWarmingUpExecutor.submit(warmupOneSchemaTask);
        } catch (Throwable ex) {
            logger.warn(ex.getMessage(), ex);
        }
    }
}
