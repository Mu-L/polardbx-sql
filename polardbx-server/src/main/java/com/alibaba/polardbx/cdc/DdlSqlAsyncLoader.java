package com.alibaba.polardbx.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.cdc.entity.DdlLoadStatusInfo;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.DdlTableMetaInfoAccessor;
import com.alibaba.polardbx.gms.metadb.table.DdlTableMetaInfoRecord;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.server.conn.InnerConnection;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.common.cdc.CdcConstants.DDL_LOAD_STATUS_RUNNING;
import static com.alibaba.polardbx.common.cdc.CdcConstants.DDL_LOAD_STATUS_STOPPED;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.GDN_DUPLICATE_DDL_LOAD_MSG_PREFIX;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_INJECT_DUPLICATE_TROUBLE_ENABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_AUTO_INIT_CHECKPOINT_ENABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_BATCH_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_CONNECTION_INIT_SQLS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_ENABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_INTERVAL_MS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_STATUS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_WAIT_ALIGN_TIMEOUT_SECONDS;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_ID;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

@Slf4j
public class DdlSqlAsyncLoader implements Runnable {
    private static final String WAIT_ALIGN_CHECK_SQL_1 =
        "/!TDDL:scan()*/select count(id) from __cdc__.__cdc_ddl_record__ where id = %s";
    private static final String WAIT_ALIGN_CHECK_SQL_2 =
        "/!TDDL:node(%s)*/select id from %s.%s where id = %s";

    private static final DdlSqlAsyncLoader INSTANCE = new DdlSqlAsyncLoader();

    @Setter
    private int serverPort;

    private DdlSqlAsyncLoader() {
    }

    public static DdlSqlAsyncLoader getInstance() {
        return INSTANCE;
    }

    @Setter
    @Getter
    private ParamManager paramManager;

    private volatile boolean suspend;

    private String errorInfo;

    @Override
    public void run() {
        while (true) {
            try {
                if (Thread.interrupted()) {
                    log.warn("ddl sql async loader thread is interrupted !");
                    break;
                }

                if (!ExecUtils.hasLeadership(null)) {
                    sleep();
                    continue;
                }

                if (paramManager.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_ENABLE)) {
                    if (DDL_LOAD_STATUS_STOPPED.equals(
                        paramManager.getString(ASYNC_LOAD_GDN_DDL_SQL_STATUS))) {
                        suspend = true;
                        sleep();
                        continue;
                    } else {
                        suspend = false;
                    }

                    Long checkPoint = MetaDbUtil.queryDdlLoadCheckPoint();
                    if (checkPoint == null && paramManager.getBoolean(
                        ASYNC_LOAD_GDN_DDL_SQL_AUTO_INIT_CHECKPOINT_ENABLE)) {
                        initCheckPoint();
                        checkPoint = MetaDbUtil.queryDdlLoadCheckPoint();
                    }

                    if (checkPoint == null) {
                        log.warn("checkPoint is empty for loading gdn ddl sql !!");
                        sleep();
                        continue;
                    }

                    int affect = load(checkPoint);
                    errorInfo = "";
                    if (affect == 0) {
                        sleep();
                    }
                } else {
                    suspend = true;
                    sleep();
                }
            } catch (Throwable t) {
                errorInfo = t.getMessage();
                log.error("async load gdn ddl sql error!!", t);
                try {
                    sleep();
                } catch (InterruptedException e) {
                }
            }
        }
    }

    @SneakyThrows
    public DdlLoadStatusInfo getDdlLoadStatusInfo() {
        DdlLoadStatusInfo ddlLoadStatusInfo = new DdlLoadStatusInfo();
        CdcDdlRecord cdcDdlRecordMax = CdcTableUtil.getInstance().getMaxIdCdcDdlRecord();
        Long checkpoint = MetaDbUtil.queryDdlLoadCheckPoint();

        ddlLoadStatusInfo.setMaxDdlId(cdcDdlRecordMax == null ? 0 : cdcDdlRecordMax.getId());
        ddlLoadStatusInfo.setExecDdlId(checkpoint == null ? 0 : checkpoint);
        ddlLoadStatusInfo.setStatus(suspend ? DDL_LOAD_STATUS_STOPPED : DDL_LOAD_STATUS_RUNNING);
        ddlLoadStatusInfo.setErrorInfo(errorInfo);
        ddlLoadStatusInfo.setDelayCount(ddlLoadStatusInfo.getMaxDdlId() - ddlLoadStatusInfo.getExecDdlId());

        if (ddlLoadStatusInfo.getDelayCount() > 0) {
            CdcDdlRecord cdcDdlRecordExec;
            if (ddlLoadStatusInfo.getExecDdlId() <= 0) {
                cdcDdlRecordExec = CdcTableUtil.getInstance().getMinIdCdcDdlRecord();
            } else {
                try (InnerConnection connection = new InnerConnection(CDC_DB_NAME)) {
                    cdcDdlRecordExec = CdcTableUtil.getInstance().queryDdlRecordById(
                        connection, ddlLoadStatusInfo.getExecDdlId());
                }
            }

            long delay = (System.currentTimeMillis() - cdcDdlRecordExec.getGmtCreated().getTime()) / 1000;
            ddlLoadStatusInfo.setDelayTime(delay);
        }
        return ddlLoadStatusInfo;
    }

    public boolean enableDdlLoad() {
        return paramManager.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_ENABLE);
    }

    protected static boolean checkIfRunningStrongly() throws SQLException {
        InstConfigRecord instConfigRecord = MetaDbUtil.getGlobal(ConnectionProperties.ASYNC_LOAD_GDN_DDL_SQL_STATUS);
        return instConfigRecord != null && DDL_LOAD_STATUS_RUNNING.equals(instConfigRecord.paramVal);
    }

    public void init() {
        if (paramManager == null) {
            SchemaConfig schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get("__cdc__");
            paramManager = new ParamManager(schemaConfig.getDataSource().getConnectionProperties());
        }
    }

    protected int load(Long checkPoint) throws SQLException, InterruptedException, TimeoutException {
        if (checkPoint == 0L && !checkIfRunningStrongly()) {
            log.warn("check point is zero, and ddl sql async loader is not running, skip ddl load at this round.");
            return 0;
        }

        List<CdcDdlRecord> records;
        try (InnerConnection connection = new InnerConnection(CDC_DB_NAME)) {
            records = CdcTableUtil.getInstance().queryDdlRecordLargeThanId(connection, checkPoint,
                paramManager.getInt(ASYNC_LOAD_GDN_DDL_SQL_BATCH_SIZE));
        }

        for (CdcDdlRecord record : records) {
            waitAlign(record.getId());
            executeDdlSql(record);
            tryInjectDuplicateTrouble(record);
            MetaDbUtil.upsertDdlLoadCheckPoint(record.getId());
        }

        return records.size();
    }

    void waitAlign(long id) throws SQLException, TimeoutException, InterruptedException {
        long timeout = paramManager.getInt(ASYNC_LOAD_GDN_DDL_SQL_WAIT_ALIGN_TIMEOUT_SECONDS) * 1000L;
        long start = System.currentTimeMillis();
        List<DbGroupInfoRecord> dbGroupInfo = getAlignExpectedGroups();
        String phyTableName = getCdcPhyTableName();

        try (InnerConnection conn = new InnerConnection(CDC_DB_NAME)) {
            while (true) {
                if (System.currentTimeMillis() - start > timeout) {
                    throw new TimeoutException("wait align timeout for cdc ddl record id " + id);
                }

                boolean align = true;
                for (DbGroupInfoRecord record : dbGroupInfo) {
                    try (Statement stmt = conn.createStatement()) {
                        String sql = String.format(WAIT_ALIGN_CHECK_SQL_2, record.groupName,
                            record.phyDbName, phyTableName, id);
                        ResultSet resultSet = stmt.executeQuery(sql);
                        if (!resultSet.next()) {
                            align = false;
                            break;
                        }
                    }
                }

                if (align) {
                    break;
                }

                Thread.sleep(1000);
            }
        }
    }

    void executeDdlSql(CdcDdlRecord record) throws SQLException {
        log.warn("prepare to load ddl sql with record id " + record.getId());
        if (canSkipDdlBecauseOfDropDatabase(record)) {
            skipDdl(record);
            return;
        }

        String hints = String.format("/*+TDDL:cmd_extra(DRY_RUN_PHYSICAL_DDL=true,%s=%s)*/",
            ASYNC_LOAD_GDN_DDL_SQL_ID, record.getId());
        Map<String, String> storageMapping = CdcTableUtil.getInstance().buildStorageMapping();

        String sql = record.getDdlSql();
        if (StringUtils.isNotBlank(record.getExt())) {
            DDLExtInfo ddlExtInfo = JSONObject.parseObject(record.getExt(), DDLExtInfo.class);
            if (StringUtils.isNotBlank(ddlExtInfo.getOriginalDdl())) {
                sql = ddlExtInfo.getOriginalDdl();
            }
        }

        for (Map.Entry<String, String> entry : storageMapping.entrySet()) {
            if (StringUtils.contains(sql, entry.getKey())) {
                sql = StringUtils.replace(sql, entry.getKey(), entry.getValue());
            }
        }

        try (Connection conn = prepareConnection(record); Statement statement = conn.createStatement()) {
            if (StringUtils.equals(SqlKind.CREATE_JAVA_FUNCTION.name(), record.getSqlKind())) {
                statement.setEscapeProcessing(false);
            }
            statement.executeUpdate(tryRewriteDdlSql(record, sql, hints));
        } catch (SQLException e) {
            processException(record, e);
        }
    }

    protected int processException(CdcDdlRecord record, SQLException e) throws SQLException {
        if (canSkipDdlBecauseOfDropDatabase(record)) {
            skipDdl(record);
            return 1;
        } else if (StringUtils.contains(e.getMessage(), GDN_DUPLICATE_DDL_LOAD_MSG_PREFIX)) {
            log.warn("meet duplicate ddl sql load, record id {}, error msg {}", record.getId(), e.getMessage());
            return 2;
        } else if (isDuplicateRevokeRole(record, e)) {
            log.warn("meet duplicate revoke role ddl sql, record id {}, error msg {}", record.getId(), e.getMessage());
            return 3;
        } else {
            throw e;
        }
    }

    protected boolean isDuplicateRevokeRole(CdcDdlRecord record, SQLException e) {
        return StringUtils.equals("REVOKE_ROLE", record.getSqlKind()) &&
            StringUtils.containsIgnoreCase(e.getMessage(), "ERR_ROLE_NOT_GRANTED");
    }

    private boolean canSkipDdlBecauseOfDropDatabase(CdcDdlRecord record) {
        return !StringUtils.equals("CREATE_DATABASE", record.getSqlKind()) &&
            !StringUtils.equals("DROP_DATABASE", record.getSqlKind()) &&
            CdcTableUtil.getInstance().checkIfExistsDropDatabaseAfterId(record.getId(), record.getSchemaName());
    }

    private void skipDdl(CdcDdlRecord record) {
        log.warn("skip ddl sql load, ddl record id is " + record.getId() + ", because of Unknown database!");
        recordSkipInfo(record);
    }

    private void recordSkipInfo(CdcDdlRecord record) {
        if (!paramManager.getBoolean(ASYNC_LOAD_GDN_DDL_DEBUG_DUMP_TABLE_META)) {
            return;
        }

        if (!StringUtils.containsAny(record.getSqlKind(),
            "CREATE_TABLE", "ALTER_TABLE", "DROP_TABLE", "CREATE_INDEX", "DROP_INDEX")) {
            return;
        }

        DdlTableMetaInfoAccessor ddlTableMetaInfoAccessor = new DdlTableMetaInfoAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            ddlTableMetaInfoAccessor.setConnection(connection);
            Long sourceJobId = record.getDdlExtInfo().getRootJobId() != null ?
                record.getDdlExtInfo().getRootJobId() : record.getJobId();

            if (ddlTableMetaInfoAccessor.queryBySourceJobId(sourceJobId).size() > 0) {
                return;
            }

            DdlTableMetaInfoRecord ddlTableMetaInfoRecord =
                new DdlTableMetaInfoRecord(record.getSchemaName(), record.getTableName(), record.ddlSql,
                    record.getSqlKind(), DdlJobManager.ID_GENERATOR.nextId(), sourceJobId, "SKIP");
            ddlTableMetaInfoAccessor.insert(ddlTableMetaInfoRecord);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "failed to dump table meta for skip ddl load :" + e);
        }
    }

    void tryInjectDuplicateTrouble(CdcDdlRecord record) throws SQLException {
        boolean injectTrouble = paramManager.getBoolean(ASYNC_LOAD_GDN_DDL_INJECT_DUPLICATE_TROUBLE_ENABLE);
        if (injectTrouble && record.jobId > 0) {
            boolean flag1 = RandomUtils.nextBoolean();
            boolean flag2 = RandomUtils.nextBoolean();
            if (flag1 && flag2) {
                executeDdlSql(record);
            }
        }
    }

    @SneakyThrows
    private Connection newSocketConnection(String dbName) {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection(String.format("jdbc:mysql://127.0.0.1:%s/%s",
            serverPort, dbName), "polardbx_root", "");
    }

    private Connection prepareConnection(CdcDdlRecord record) throws SQLException {
        // 使用InnerConnection会绕过 FrontendConnection -> ServerQueryHander -> ServerConnection的执行流程
        // 对于某些ddl sql是没有问题的，但某些ddl则不能绕过，比如 create user，直接使用InnerConnection会报错
        Connection connection = null;
        if (SqlKind.CREATE_DATABASE.name().equals(record.getSqlKind()) ||
            SqlKind.DROP_DATABASE.name().equals(record.getSqlKind())) {
            connection = newSocketConnection(CDC_DB_NAME);
        } else {
            String schemaName = record.getSchemaName();
            if (paramManager.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)) {
                schemaName = schemaName + "_gdn_shadow";
            }
            connection = newSocketConnection(schemaName);
        }

        String[] initSqls = getInitSqls();
        if (initSqls.length > 0) {
            for (String initSql : initSqls) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(initSql);
                }
            }
        }

        if (record.getDdlExtInfo().isPushDownAutoIncrement()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET ENABLE_PUSH_DOWN_AUTO_INCREMENT = true");
            }
        }
        return connection;
    }

    protected String tryRewriteDdlSql(CdcDdlRecord cdcDdlRecord, String sql, String hints) {
        if (paramManager.getBoolean(ASYNC_LOAD_GDN_DDL_SQL_IN_SHADOW_MODE_ENABLE)) {
            String newSchemaName = cdcDdlRecord.getSchemaName() + "_gdn_shadow";
            sql = StringUtils.replaceIgnoreCase(sql, cdcDdlRecord.getSchemaName(), newSchemaName);
        }

        if (SqlKind.CREATE_DATABASE.name().equalsIgnoreCase(cdcDdlRecord.getSqlKind())) {
            SQLCreateDatabaseStatement createDatabaseStatement = (SQLCreateDatabaseStatement) SQLHelper.parseSql(sql);
            createDatabaseStatement.setDryrun(true);
            if (!createDatabaseStatement.isIfNotExists()) {
                createDatabaseStatement.setIfNotExists(true);
            }
            sql = createDatabaseStatement.toString();
        } else if (SqlKind.DROP_DATABASE.name().equalsIgnoreCase(cdcDdlRecord.getSqlKind())) {
            SQLDropDatabaseStatement dropDatabaseStatement = (SQLDropDatabaseStatement) SQLHelper.parseSql(sql);
            dropDatabaseStatement.setDryrun(true);
            if (!dropDatabaseStatement.isIfExists()) {
                dropDatabaseStatement.setIfExists(true);
            }
            sql = dropDatabaseStatement.toString();
        } else if (SqlKind.CREATE_USER.name().equalsIgnoreCase(cdcDdlRecord.getSqlKind())) {
            MySqlCreateUserStatement createUserStatement = (MySqlCreateUserStatement) SQLHelper.parseSql(sql);
            if (!createUserStatement.isIfNotExists()) {
                createUserStatement.setIfNotExists(true);
            }
            sql = createUserStatement.toString();
        } else if (SqlKind.CREATE_ROLE.name().equalsIgnoreCase(cdcDdlRecord.getSqlKind())) {
            MySqlCreateRoleStatement createRoleStatement = (MySqlCreateRoleStatement) SQLHelper.parseSql(sql);
            if (!createRoleStatement.isIfNotExists()) {
                createRoleStatement.setIfNotExists(true);
            }
            sql = createRoleStatement.toString();
        }

        boolean enableHints = true;
        String[] sqlKinds = getWithoutHintsSqlKinds();
        if (sqlKinds.length > 0 && StringUtils.equalsAny(cdcDdlRecord.getSqlKind(), sqlKinds)) {
            enableHints = false;
        }

        return enableHints ? hints + sql : sql;
    }

    private String[] getWithoutHintsSqlKinds() {
        String withoutHintsSqlKinds = paramManager.getString(ASYNC_LOAD_GDN_DDL_SQL_WITHOUT_HINTS_SQL_KINDS);
        if (StringUtils.isBlank(withoutHintsSqlKinds)) {
            return new String[0];
        }
        return withoutHintsSqlKinds.split(",");
    }

    private String[] getInitSqls() {
        String initSqls = paramManager.getString(ASYNC_LOAD_GDN_DDL_SQL_CONNECTION_INIT_SQLS);
        if (StringUtils.isBlank(initSqls)) {
            return new String[0];
        }
        return initSqls.split(";");
    }

    List<DbGroupInfoRecord> getAlignExpectedGroups() throws SQLException {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);
            return dbGroupInfoAccessor.queryDbGroupByDbName(CDC_DB_NAME);
        }
    }

    private String getCdcPhyTableName() throws SQLException {
        try (InnerConnection con = new InnerConnection(CDC_DB_NAME)) {
            try (Statement statement = con.createStatement();
                ResultSet resultSet = statement.executeQuery("show topology from __cdc_ddl_record__")) {
                if (resultSet.next()) {
                    return resultSet.getString("TABLE_NAME");
                }
            }
        }
        throw new RuntimeException("get cdc phy table name failed");
    }

    private void initCheckPoint() throws SQLException {
        CdcDdlRecord cdcDdlRecord = CdcTableUtil.getInstance().getMaxIdCdcDdlRecord();
        long maxId = cdcDdlRecord == null ? 0 : cdcDdlRecord.getId();
        MetaDbUtil.upsertDdlLoadCheckPoint(maxId);
    }

    private void sleep() throws InterruptedException {
        int sleepTimeMs = paramManager.getInt(ASYNC_LOAD_GDN_DDL_SQL_INTERVAL_MS);
        Thread.sleep(sleepTimeMs);
    }

    public boolean isSuspend() {
        return suspend;
    }
}
