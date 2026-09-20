package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.MergedStorageInfo;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.GLOBAL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.MDL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.NO_DEADLOCKS_DETECTED;

/**
 * @author chenmo.cm
 */
public class StorageInfoManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(StorageInfoManager.class);

    private final Map<String, StorageInfo> storageInfos;
    private final TopologyHandler topologyHandler;
    @Setter
    @Getter
    private static volatile boolean supportGetCidx = false;

    @Getter
    private volatile MergedStorageInfo mergedStorageInfo;

    /**
     * Record the latest global deadlock log and global MDL deadlock log
     */
    private static final ConcurrentMap<String, String> deadlockLogMap;

    static {
        deadlockLogMap = new ConcurrentHashMap<>(2);
        deadlockLogMap.put(GLOBAL_DEADLOCK, NO_DEADLOCKS_DETECTED);
        deadlockLogMap.put(MDL_DEADLOCK, NO_DEADLOCKS_DETECTED);
    }

    public StorageInfoManager(TopologyHandler topologyHandler) {
        storageInfos = new ConcurrentHashMap<>();

        Preconditions.checkNotNull(topologyHandler);
        this.topologyHandler = topologyHandler;
    }

    /**
     * Get deadlock information
     */
    public static String getDeadlockInfo() {
        return deadlockLogMap.get(GLOBAL_DEADLOCK);
    }

    /**
     * Get MDL deadlock information
     */
    public static String getMdlDeadlockInfo() {
        return deadlockLogMap.get(MDL_DEADLOCK);
    }

    /**
     * Update deadlock information
     */
    public static void updateDeadlockInfo(String newDeadlockInfo) {
        deadlockLogMap.put(GLOBAL_DEADLOCK, newDeadlockInfo);
    }

    /**
     * Update MDL deadlock information
     */
    public static void updateMdlDeadlockInfo(String newMdlDeadlockInfo) {
        deadlockLogMap.put(MDL_DEADLOCK, newMdlDeadlockInfo);
    }

    public static String getMySqlVersion(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT @@version")) {
            boolean hasNext = rs.next();
            assert hasNext;
            return rs.getString(1);
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to get MySQL version: " + ex.getMessage());
        }
    }

    public static boolean checkSupportTso(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_commit_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check TSO support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportPurgeTso(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_purge_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check TSO support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportTsoHeartbeat(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_heartbeat_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check TSO support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportCtsTransaction(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_cts_transaction'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check innodb_cts_transaction support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportLizard1PCTransaction(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_current_snapshot_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check innodb_cts_transaction support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportAsyncCommit(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'polarx_distributed_trx_id'");
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check async commit support: " + ex.getMessage());
        }
    }

    public static boolean checkIsXEngine(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'xengine_datadir'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex, "Failed to check xengine: " + ex.getMessage());
        }
    }

    public static boolean checkSupportPerformanceSchema(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'performance_schema'")) {
            boolean hasNext = rs.next();
            return hasNext && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check performance_schema support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportSharedReadView(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_transaction_group'")) {
            boolean hasNext = rs.next();
            // 该变量只需要存在就支持，默认为OFF
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check shared read view support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportChangeSet(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'enable_changeset'")) {
            boolean hasNext = rs.next();
            // 该变量只需要存在就支持，默认为OFF
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check changeset support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportChangeSetBackPressure(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_admin.show_native_procedure()")) {
            boolean supportChangeSetBackPressure = false;
            while (rs.next()) {
                final String schemaName = rs.getString(1);
                final String procName = rs.getString(2);
                supportChangeSetBackPressure =
                    "polarx".equalsIgnoreCase(schemaName) && "changeset_backpressure".equalsIgnoreCase(procName);
                if (supportChangeSetBackPressure) {
                    break;
                }
            }
            return supportChangeSetBackPressure;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check changeset backpressure support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportReturning(DataSource dataSource) {
        if (!ConfigDataMode.isPolarDbX() || XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_admin.show_native_procedure()")) {
            boolean supportReturning = false;
            while (rs.next()) {
                final String schemaName = rs.getString(1);
                final String procName = rs.getString(2);
                supportReturning |= "dbms_trans".equalsIgnoreCase(schemaName) && "returning".equalsIgnoreCase(procName);
                if (supportReturning) {
                    break;
                }
            }
            return supportReturning;
        } catch (SQLException ex) {
            final boolean ER_SP_DOES_NOT_EXIST =
                "42000".equalsIgnoreCase(ex.getSQLState()) && 1305 == ex.getErrorCode() && ex.getMessage()
                    .contains("does not exist");
            if (ER_SP_DOES_NOT_EXIST) {
                logger.warn("PROCEDURE dbms_admin.show_native_procedure does not exist");
                return false;
            }

            final boolean ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 3130 == ex.getErrorCode() && ex.getMessage()
                    .contains("Command not supported by pluggable protocols");
            if (ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED) {
                logger.warn("Do not support call dbms_amdin procedures within XPotocol");
                return false;
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check returning support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportReturningAll(DataSource dataSource) {
        if (!ConfigDataMode.isPolarDbX() || XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_admin.show_native_procedure()")) {
            boolean supportReturningAll = false;
            while (rs.next()) {
                final String schemaName = rs.getString(1);
                final String procName = rs.getString(2);
                supportReturningAll |=
                    "dbms_trans".equalsIgnoreCase(schemaName) && "returning_all".equalsIgnoreCase(procName);
                if (supportReturningAll) {
                    break;
                }
            }
            return supportReturningAll;
        } catch (SQLException ex) {
            final boolean ER_SP_DOES_NOT_EXIST =
                "42000".equalsIgnoreCase(ex.getSQLState()) && 1305 == ex.getErrorCode() && ex.getMessage()
                    .contains("does not exist");
            if (ER_SP_DOES_NOT_EXIST) {
                logger.warn("PROCEDURE dbms_admin.show_native_procedure does not exist");
                return false;
            }

            final boolean ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 3130 == ex.getErrorCode() && ex.getMessage()
                    .contains("Command not supported by pluggable protocols");
            if (ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED) {
                logger.warn("Do not support call dbms_admin procedures within XPotocol");
                return false;
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check returning support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportBackfillReturning(DataSource dataSource) {
        if (!ConfigDataMode.isPolarDbX() || XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_admin.show_native_procedure()")) {
            boolean supportReturning = false;
            while (rs.next()) {
                final String schemaName = rs.getString(1);
                final String procName = rs.getString(2);
                supportReturning |= "dbms_trans".equalsIgnoreCase(schemaName) && "backfill".equalsIgnoreCase(procName);
                if (supportReturning) {
                    break;
                }
            }
            return supportReturning;
        } catch (SQLException ex) {
            final boolean ER_SP_DOES_NOT_EXIST =
                "42000".equalsIgnoreCase(ex.getSQLState()) && 1305 == ex.getErrorCode() && ex.getMessage()
                    .contains("does not exist");
            if (ER_SP_DOES_NOT_EXIST) {
                logger.warn("PROCEDURE dbms_admin.show_native_procedure does not exist");
                return false;
            }

            final boolean ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 3130 == ex.getErrorCode() && ex.getMessage()
                    .contains("Command not supported by pluggable protocols");
            if (ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED) {
                logger.warn("Do not support call dbms_amdin procedures within XPotocol");
                return false;
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check returning support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportAlterType(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select alter_type(1)")) {
            return true;
        } catch (SQLException ex) {
            final boolean INCORRECT_ARGS =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 1210 == ex.getErrorCode() && ex.getMessage()
                    .contains("Incorrect arguments to alter_type");
            if (INCORRECT_ARGS) {
                return true;
            }
            return false;
        }
    }

    public static boolean checkSupportModifyOnUpdate(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT MODIFY_ON_UPDATE(NOW())")) {
            return true;
        } catch (SQLException ex) {
            return false;
        }
    }

    public static boolean checkRDS80(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select version()")) {
            if (rs.next() && rs.getString(1).startsWith("8.0")) {
                return true;
            }
        } catch (SQLException ex) {
        }
        return false;
    }

    public static boolean checkMetaDataLocksSelectPrivilege(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT\n"
                + "  `pt`.`PROCESSLIST_ID` AS `waiting`,\n"
                + "  `gt`.`PROCESSLIST_ID` AS `blocking` \n"
                + "FROM\n"
                + "  (((((\n"
                + "            `performance_schema`.`metadata_locks` `g`\n"
                + "            JOIN `performance_schema`.`metadata_locks` `p` ON (((\n"
                + "                  `g`.`OBJECT_TYPE` = `p`.`OBJECT_TYPE` \n"
                + "                  ) \n"
                + "                AND ( `g`.`OBJECT_SCHEMA` = `p`.`OBJECT_SCHEMA` ) \n"
                + "                AND ( `g`.`OBJECT_NAME` = `p`.`OBJECT_NAME` ) \n"
                + "                AND ( `g`.`LOCK_STATUS` = 'GRANTED' ) \n"
                + "              AND ( `p`.`LOCK_STATUS` = 'PENDING' ))))\n"
                + "          JOIN `performance_schema`.`threads` `gt` ON ((\n"
                + "              `g`.`OWNER_THREAD_ID` = `gt`.`THREAD_ID` \n"
                + "            )))\n"
                + "        JOIN `performance_schema`.`threads` `pt` ON ((\n"
                + "            `p`.`OWNER_THREAD_ID` = `pt`.`THREAD_ID` \n"
                + "          )))\n"
                + "      LEFT JOIN `performance_schema`.`events_statements_current` `gs` ON ((\n"
                + "          `g`.`OWNER_THREAD_ID` = `gs`.`THREAD_ID` \n"
                + "        )))\n"
                + "    LEFT JOIN `performance_schema`.`events_statements_current` `ps` ON ((\n"
                + "        `p`.`OWNER_THREAD_ID` = `ps`.`THREAD_ID` \n"
                + "      ))) \n"
                + "WHERE\n"
                + "  ( `g`.`OBJECT_TYPE` = 'TABLE' ) \n"
                + "  AND `pt`.`PROCESSLIST_ID` != `gt`.`PROCESSLIST_ID`"
                + "  AND FALSE"
            );
            return true;
        } catch (SQLException ex) {
            logger.error("Failed to check performance_schema select privilege: " + ex.getMessage());
            return false;
        }
    }

    public static boolean checkMetaDataLocksEnable(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select enabled,timed from performance_schema.setup_instruments "
                    + "WHERE NAME = 'wait/lock/metadata/sql/mdl' ")) {
            boolean hasNext = rs.next();
            return hasNext && StringUtils.equalsIgnoreCase(rs.getString(1), "YES");
        } catch (SQLException ex) {
            logger.error("Failed to check performance_schema.metadata_locks: " + ex.getMessage());
            return false;
        }
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/server-status-variables.html#statvar_Rsa_public_key">Rsa_public_key</a>
     */
    public static boolean checkSupportOpenSSL(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Rsa_public_key'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check openssl support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXxHash(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'udf_bloomfilter_xxhash'")) {
            return rs.next() && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check  support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXOptForAutoSp(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'auto_savepoint_opt'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check x-protocol optimized for auto savepoint support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXRpc(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'new_rpc'")) {
            return rs.next() && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check x-rpc support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportMarkDistributed(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_mark_distributed'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check innodb_mark_distributed support: " + ex.getMessage());
        }
    }

    protected static boolean checkSupportSyncPoint(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'enable_polarx_sync_point'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check sync point support: " + ex.getMessage());
        }
    }

    protected static boolean checkSupportFlashbackArea(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'opt_flashback_area'")) {
            return rs.next();
        } catch (SQLException ex) {
            return false;
        }
    }

    protected static boolean checkSupportAsOfCrossDdl(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'as_of_cross_ddl_enabled'")) {
            return rs.next();
        } catch (SQLException ex) {
            return false;
        }
    }

    private static boolean checkSupportTsoOpt(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call DBMS_XA.FIND_BY_XID('0', '0', 1)")) {
            return rs.next();
        } catch (SQLException ex) {
            if (ex.getMessage().contains("PROCEDURE dbms_xa.FIND_BY_XID does not exist")) {
                return false;
            }
            logger.error("Failed to check tso opt support.", ex);
            return false;
        }
    }

    protected static boolean checkSupportGetCidx(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_consensus.get_cidx()")) {
            return rs.next();
        } catch (SQLException ex) {
            if (ex.getMessage().contains("PROCEDURE dbms_consensus.get_cidx does not exist")) {
                return false;
            }
            logger.error("Failed to check tso opt support.", ex);
            return false;
        }
    }

    protected static boolean checkSupportsVectorIndexes(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT 1 FROM information_schema.tables "
                    + "WHERE table_schema = 'information_schema' AND table_name = 'VECTOR_INDEXES' LIMIT 1")) {
            return rs.next();
        } catch (Exception ex) {
            logger.warn("Failed to check information_schema.VECTOR_INDEXES support.", ex);
            return false;
        }
    }

    private static boolean checkSupportAsyncCommit8032(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            try {
                stmt.execute("call DBMS_XA.AC_PREPARE()");
            } catch (SQLException e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(),
                    "Incorrect number of arguments for PROCEDURE dbms_xa.ac_prepare; expected 6, got 0")) {
                    return true;
                }
            }
            return false;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check tso opt support: " + ex.getMessage());
        }
    }

    public static int getLowerCaseTableNames(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT @@global.lower_case_table_names;")) {
            boolean hasNext = rs.next();
            assert hasNext;
            return rs.getInt(1);
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to get variable lower_case_table_names: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXOptForPhysicalBackfill(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'physical_backfill_opt'")) {
            return rs.next() && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check x-protocol for physical backfill support: " + ex.getMessage());
        }
    }

    @Override
    protected void doInit() {
        refresh(storageInfos);
    }

    public void reload() {
        if (isInited()) {
            Map<String, StorageInfo> tmpStorageInfos = new ConcurrentHashMap<>();
            refresh(tmpStorageInfos);
            //不直接调用storageInfos.clear，防止：DML调用的时候storageInfos为空，getDnVersion返回不正确
            storageInfos.putAll(tmpStorageInfos);
            Iterator<Map.Entry<String, StorageInfo>> it = storageInfos.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, StorageInfo> entry = it.next();
                String key = entry.getKey();
                if (!tmpStorageInfos.containsKey(key)) {
                    it.remove();
                }
            }
        } else {
            init();
        }
    }

    private void refresh(Map<String, StorageInfo> storageInfos) {
        boolean tmpSupportXA = true;
        boolean tmpSupportTso = true;
        boolean tmpSupportPurgeTso = true;
        boolean tmpSupportTsoHeartbeat = true;
        boolean tmpSupportDeadlockDetection = true;
        boolean tmpSupportMdlDeadlockDetection = true;
        boolean tmpSupportsBloomFilter = true;
        boolean tmpSupportsReturning = true;
        boolean tmpSupportsReturningAll = true;
        boolean tmpSupportsBackfillReturning = true;
        boolean tmpSupportsAlterType = true;
        boolean tmpLowerCaseTableNames = true;
        boolean tmpSupportOpenSSL = true;
        boolean tmpSupportSharedReadView = true;
        boolean tmpSupportCtsTransaction = true;
        boolean tmpSupportAsyncCommit = true;
        boolean tmpSupportLizard1PCTransaction = true;
        boolean tmpSupportHyperLogLog = true;
        boolean tmpSupportXxHash = true;
        boolean lessMysql56 = false;
        boolean tmpSupportFastChecker = true;
        boolean tmpRDS80 = true;
        boolean tmpSupportChangeSet = true;
        boolean tmpSupportChangeSetBackPressure = true;
        boolean tmpSupportXOptForAutoSp = true;
        boolean tmpSupportXRpc = true;
        boolean tmpSupportXOptForPhysicalBackfill = true;
        boolean tmpSupportMarkDistributed = true;
        boolean tmpSupportTsoOpt = true;
        boolean tmpSupportAsyncCommit8032 = true;
        boolean tmpSupportsModifyOnUpdate = true;
        boolean tmpSupportGetCidx = true;
        boolean tmpSupportsVectorIndexes = true;

        boolean storageInfoEmpty = true;
        boolean tmpSupportSyncPoint = true;
        boolean tmpSupportFlashbackArea = true;
        boolean tmpSupportAsOfCrossDdl = true;
        for (Group group : topologyHandler.getMatrix().getGroups()) {
            if (group.getType() != GroupType.MYSQL_JDBC || !DbGroupInfoManager.isNormalGroup(group)) {
                continue;
            }

            IGroupExecutor groupExecutor = topologyHandler.get(group.getName());

            final StorageInfo storageInfo = initStorageInfo(group, groupExecutor.getDataSource(), storageInfos);
            if (storageInfo != null) {
                storageInfoEmpty = false;
                tmpSupportXA &= supportXA(storageInfo);
                lessMysql56 = lessMysql56 || lessMysql56Version(storageInfo);
                tmpSupportTso &= storageInfo.supportTso;
                tmpSupportPurgeTso &= storageInfo.supportPurgeTso;
                tmpSupportTsoHeartbeat &= storageInfo.supportTsoHeartbeat;
                tmpSupportCtsTransaction &= storageInfo.supportCtsTransaction;
                tmpSupportAsyncCommit &= storageInfo.supportAsyncCommit;
                tmpSupportLizard1PCTransaction &= storageInfo.supportLizard1PCTransaction;
                tmpSupportDeadlockDetection &= supportDeadlockDetection(storageInfo);
                tmpSupportMdlDeadlockDetection &=
                    ConfigDataMode.isPolarDbX() && supportMdlDeadlockDetection(storageInfo);
                tmpSupportsBloomFilter &= storageInfo.supportsBloomFilter;
                tmpSupportOpenSSL &= storageInfo.supportOpenSSL;
                tmpSupportHyperLogLog &= storageInfo.supportHyperLogLog;
                tmpSupportsReturning &= storageInfo.supportsReturning;
                tmpSupportsReturningAll &= storageInfo.supportsReturningAll;
                tmpSupportsBackfillReturning &= storageInfo.supportsBackfillReturning;
                tmpSupportsAlterType &= storageInfo.supportsAlterType;
                tmpLowerCaseTableNames &= enableLowerCaseTableNames(storageInfo);
                tmpSupportSharedReadView &= storageInfo.supportSharedReadView;
                tmpSupportFastChecker &= storageInfo.supportFastChecker;
                tmpRDS80 &= isRDS80(storageInfo);
                tmpSupportXxHash &= storageInfo.supportXxHash;
                tmpSupportChangeSet &= storageInfo.supportChangeSet;
                tmpSupportChangeSetBackPressure &= storageInfo.supportChangeSetBackPressure;
                tmpSupportXOptForAutoSp &= storageInfo.supportXOptForAutoSp;
                tmpSupportXRpc &= storageInfo.supportXRpc;
                tmpSupportXOptForPhysicalBackfill &= storageInfo.supportXOptForPhysicalBackfill;
                tmpSupportMarkDistributed &= storageInfo.supportMarkDistributed;
                tmpSupportSyncPoint &= storageInfo.supportSyncpoint;
                tmpSupportFlashbackArea &= storageInfo.supportFlashbackArea;
                tmpSupportAsOfCrossDdl &= storageInfo.supportAsOfCrossDdl;
                tmpSupportTsoOpt &= storageInfo.supportTsoOpt;
                tmpSupportAsyncCommit8032 &= storageInfo.supportAsyncCommit8032;
                tmpSupportsModifyOnUpdate &= storageInfo.supportsModifyOnUpdate;
                tmpSupportGetCidx &= storageInfo.supportGetCidx;
                tmpSupportsVectorIndexes &= storageInfo.supportsVectorIndexes;
            }

        }

        boolean readOnly = !ConfigDataMode.needInitMasterModeResource() && !ConfigDataMode.isFastMock();

        // Do not enable XA transaction in read-only instance
        boolean supportXA = tmpSupportXA && !readOnly;
        boolean supportsBloomFilter = tmpSupportsBloomFilter;
        boolean supportsReturning = tmpSupportsReturning;
        boolean supportsReturningAll = tmpSupportsReturningAll;
        boolean supportsBackfillReturning = tmpSupportsBackfillReturning;
        boolean supportsAlterType = tmpSupportsAlterType;
        boolean supportTso = tmpSupportTso && (metaDbUsesXProtocol() || tmpRDS80);
        boolean supportPurgeTso = tmpSupportPurgeTso && metaDbUsesXProtocol();
        boolean supportTsoHeartbeat = tmpSupportTsoHeartbeat && metaDbUsesXProtocol();
        boolean supportCtsTransaction = tmpSupportCtsTransaction;
        boolean supportAsyncCommit = tmpSupportAsyncCommit;
        boolean supportLizard1PCTransaction = tmpSupportLizard1PCTransaction;
        boolean supportSharedReadView = tmpSupportSharedReadView;
        boolean supportDeadlockDetection = tmpSupportDeadlockDetection;
        boolean supportMdlDeadlockDetection = tmpSupportMdlDeadlockDetection;
        boolean supportOpenSSL = tmpSupportOpenSSL;
        boolean lowerCaseTableNames = tmpLowerCaseTableNames;
        boolean supportHyperLogLog = tmpSupportHyperLogLog;
        boolean lessMy56Version = lessMysql56;
        boolean supportFastChecker = tmpSupportFastChecker;
        boolean supportXxHash = tmpSupportXxHash;
        boolean isMysql80 = tmpRDS80;
        boolean supportChangeSet = tmpSupportChangeSet;
        boolean supportChangeSetBackPressure = tmpSupportChangeSetBackPressure;
        boolean supportXOptForAutoSp = tmpSupportXOptForAutoSp && tmpSupportXRpc;
        boolean supportXRpc = tmpSupportXRpc;
        boolean supportXOptForPhysicalBackfill = tmpSupportXOptForPhysicalBackfill && tmpSupportXRpc;
        boolean supportMarkDistributed = tmpSupportMarkDistributed;
        boolean supportSyncPoint = tmpSupportSyncPoint;
        boolean supportFlashbackArea = tmpSupportFlashbackArea;
        boolean supportAsOfCrossDdl = tmpSupportAsOfCrossDdl;
        boolean supportTsoOpt = tmpRDS80 && tmpSupportTsoOpt;
        boolean supportAsyncCommit8032 = tmpRDS80 && tmpSupportAsyncCommit8032;
        boolean supportsModifyOnUpdate = tmpSupportsModifyOnUpdate;
        boolean supportGetCidx = tmpRDS80 && tmpSupportGetCidx;
        boolean supportsVectorIndexes = !storageInfoEmpty && tmpSupportsVectorIndexes;

        this.mergedStorageInfo = new MergedStorageInfo(
            supportXA,
            supportTso,
            supportPurgeTso,
            supportTsoHeartbeat,
            supportCtsTransaction,
            supportAsyncCommit,
            supportLizard1PCTransaction,
            supportDeadlockDetection,
            supportMdlDeadlockDetection,
            supportsBloomFilter,
            supportOpenSSL,
            supportSharedReadView,
            supportsReturning,
            supportsReturningAll,
            supportsBackfillReturning,
            supportsAlterType,
            readOnly,
            lowerCaseTableNames,
            supportHyperLogLog,
            lessMy56Version,
            supportXxHash,
            isMysql80,
            supportFastChecker,
            supportChangeSet,
            supportChangeSetBackPressure,
            supportXOptForAutoSp,
            supportXRpc,
            supportMarkDistributed,
            supportXOptForPhysicalBackfill,
            supportSyncPoint,
            supportFlashbackArea,
            supportAsOfCrossDdl,
            supportTsoOpt,
            supportAsyncCommit8032,
            supportsModifyOnUpdate,
            supportGetCidx,
            supportsVectorIndexes
        );

        if (!storageInfoEmpty) {
            InstanceVersion.setMYSQL80(isMysql80);
            InstanceVersion.setSupportsVectorIndexes(supportsVectorIndexes);
            setSupportGetCidx(supportGetCidx);
        }
    }

    private boolean metaDbUsesXProtocol() {
        if (!ConfigDataMode.isPolarDbX()) {
            return false;
        }
        try {
            return MetaDbDataSource.getInstance().getDataSource().isWrapperFor(XDataSource.class);
        } catch (SQLException ex) {
            return false;
        }
    }

    private boolean isRDS80(StorageInfo storageInfo) {
        return storageInfo.version.startsWith("8.0");
    }

    private boolean supportXA(StorageInfo storageInfo) {
        return null == storageInfo
            || (!storageInfo.version.startsWith("5.6") && !storageInfo.version.startsWith("5.5")
            && !storageInfo.isXEngine);
    }

    private boolean lessMysql56Version(StorageInfo storageInfo) {
        return null != storageInfo
            && (storageInfo.version.startsWith("5.6") || storageInfo.version.startsWith("5.5"));
    }

    private boolean supportDeadlockDetection(StorageInfo storageInfo) {
        return null == storageInfo || storageInfo.version.startsWith("5.");
    }

    private boolean supportMdlDeadlockDetection(StorageInfo storageInfo) {
        return null == storageInfo ||
            storageInfo.supportPerformanceSchema
                && storageInfo.hasMetaDataLocksSelectPrivilege
                && storageInfo.isMetaDataLocksEnable;
    }

    private boolean enableLowerCaseTableNames(StorageInfo storageInfo) {
        return null == storageInfo || storageInfo.lowerCaseTableNames != 0;
    }

    @Override
    protected void doDestroy() {
        storageInfos.clear();
    }

    private StorageInfo initStorageInfo(Group group, IDataSource dataSource, Map<String, StorageInfo> storageInfos) {

        if (!ConfigDataMode.needDNResource() && !SystemDbHelper.isDBBuildInExceptCdc(group.getSchemaName())) {
            return null;
        }

        if (group.getType() != GroupType.MYSQL_JDBC) {
            return null;
        }
        StorageInfo storageInfo = StorageInfo.create(dataSource);
        storageInfos.put(group.getName(), storageInfo);

        return storageInfo;
    }

    public boolean supportXA() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportXA();
    }

    public boolean supportTso() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportTso();
    }

    public boolean supportPurgeTso() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportPurgeTso();
    }

    public boolean isLessMy56Version() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isLessMy56Version();
    }

    public boolean isMysql80() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isMysql80();
    }

    public String getDnVersion() {
        if (!isInited()) {
            init();
        }
        String version = null;
        Iterator<StorageInfo> iterator = storageInfos.values().iterator();
        if (iterator.hasNext()) {
            StorageInfo storageInfo = iterator.next();
            version = storageInfo.version;
        }
        return version;
    }

    public boolean supportTsoHeartbeat() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportTsoHeartbeat();
    }

    public boolean supportCtsTransaction() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportCtsTransaction();
    }

    public boolean supportAsyncCommit57() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportAsyncCommit57();
    }

    public boolean supportLizard1PCTransaction() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportLizard1PCTransaction();
    }

    public boolean supportDeadlockDetection() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportDeadlockDetection();
    }

    public boolean supportMdlDeadlockDetection() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportMdlDeadlockDetection();
    }

    public boolean supportsBloomFilter() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportsBloomFilter();
    }

    public boolean supportSharedReadView() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportSharedReadView();
    }

    public boolean supportOpenSSL() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportOpenSSL();
    }

    public boolean supportsHyperLogLog() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportHyperLogLog();
    }

    public boolean supportsXxHash() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportXxHash();
    }

    public boolean supportsReturning() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportsReturning();
    }

    public boolean supportsReturningAll() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportsReturningAll();
    }

    public boolean supportsBackfillReturning() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportsBackfillReturning();
    }

    public boolean supportsAlterType() {
        if (!isInited()) {
            init();
        }

        return this.mergedStorageInfo.isSupportsAlterType();
    }

    public boolean isReadOnly() {
        return this.mergedStorageInfo.isReadOnly();
    }

    public boolean isLowerCaseTableNames() {
        return this.mergedStorageInfo.isLowerCaseTableNames();
    }

    public boolean supportFastChecker() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportFastChecker();
    }

    public boolean supportChangeSet() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportChangeSet();
    }

    public boolean supportChangeSetBackPressure() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportChangeSetBackPressure();
    }

    public boolean supportXOptForAutoSp() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportXOptForAutoSp();
    }

    public boolean supportXRpc() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportXRpc();
    }

    public boolean isSupportMarkDistributed() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportMarkDistributed();
    }

    public boolean supportXOptForPhysicalBackfill() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportXOptForPhysicalBackfill();
    }

    public boolean isSupportSyncPoint() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportSyncPoint();
    }

    public boolean isSupportFlashbackArea() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportFlashbackArea();
    }

    public boolean isSupportTsoOpt() {
        if (!isInited()) {
            init();
        }
        return InstanceVersion.isMYSQL80() && this.mergedStorageInfo.isSupportTsoOpt();
    }

    public boolean isSupportAsyncCommit8032() {
        if (!isInited()) {
            init();
        }
        return InstanceVersion.isMYSQL80() && this.mergedStorageInfo.isSupportAsyncCommit8032();
    }

    public boolean supportsModifyOnUpdate() {
        if (!isInited()) {
            init();
        }
        return this.mergedStorageInfo.isSupportsModifyOnUpdate();
    }

    public static class StorageInfo {

        public final String version;
        public final boolean supportTso;
        public final boolean supportPurgeTso;
        private volatile boolean supportTsoHeartbeat;
        public final boolean supportCtsTransaction;
        public final boolean supportAsyncCommit;
        public final boolean supportSyncpoint;
        public final boolean supportLizard1PCTransaction;
        public final boolean supportsBloomFilter;
        public final boolean supportsReturning;
        public final boolean supportsReturningAll;
        public final boolean supportsBackfillReturning;
        public final boolean supportsAlterType;
        public final int lowerCaseTableNames;
        public final boolean supportPerformanceSchema;
        public final boolean isXEngine;
        public final boolean supportSharedReadView;
        boolean hasMetaDataLocksSelectPrivilege;
        boolean isMetaDataLocksEnable;
        public final boolean supportOpenSSL;
        boolean supportHyperLogLog;
        boolean supportFastChecker;
        boolean supportXxHash;
        boolean supportChangeSet;
        boolean supportChangeSetBackPressure;
        boolean supportXOptForAutoSp;
        boolean supportXRpc;
        boolean supportXOptForPhysicalBackfill;
        boolean supportMarkDistributed;
        boolean supportFlashbackArea;
        boolean supportAsOfCrossDdl;
        boolean supportTsoOpt;
        boolean supportAsyncCommit8032;
        boolean supportsModifyOnUpdate;
        boolean supportGetCidx;
        boolean supportsVectorIndexes;

        public StorageInfo(
            String version,
            boolean supportTso,
            boolean supportPurgeTso,
            boolean supportTsoHeartbeat,
            boolean supportCtsTransaction,
            boolean supportAsyncCommit,
            boolean supportLizard1PCTransaction,
            boolean supportsBloomFilter,
            boolean supportsReturning,
            boolean supportsReturningAll,
            boolean supportsBackfillReturning,
            boolean supportsAlterType,
            int lowerCaseTableNames,
            boolean supportPerformanceSchema,
            boolean isXEngine,
            boolean supportSharedReadView,
            boolean hasMetaDataLocksSelectPrivilege,
            boolean isMetaDataLocksEnable,
            boolean supportHyperLogLog,
            boolean supportOpenSSL,
            boolean supportFastChecker,
            boolean supportXxHash,
            boolean supportChangeSet,
            boolean supportChangeSetBackPressure,
            boolean supportXOptForAutoSp,
            boolean supportXRpc,
            boolean supportXOptForPhysicalBackfill,
            boolean supportMarkDistributed,
            boolean supportSyncPoint,
            boolean supportFlashbackArea,
            boolean supportAsOfCrossDdl,
            boolean supportTsoOpt,
            boolean supportAsyncCommit8032,
            boolean supportsModifyOnUpdate,
            boolean supportGetCidx,
            boolean supportsVectorIndexes
        ) {
            this.version = version;
            this.supportTso = supportTso;
            this.supportPurgeTso = supportPurgeTso;
            this.supportTsoHeartbeat = supportTsoHeartbeat;
            this.supportCtsTransaction = supportCtsTransaction;
            this.supportAsyncCommit = supportAsyncCommit;
            this.supportLizard1PCTransaction = supportLizard1PCTransaction;
            this.supportsBloomFilter = supportsBloomFilter;
            this.supportsReturning = supportsReturning;
            this.supportsReturningAll = supportsReturningAll;
            this.supportsBackfillReturning = supportsBackfillReturning;
            this.supportsAlterType = supportsAlterType;
            this.lowerCaseTableNames = lowerCaseTableNames;
            this.supportPerformanceSchema = supportPerformanceSchema;
            this.isXEngine = isXEngine;
            this.supportSharedReadView = supportSharedReadView;
            this.hasMetaDataLocksSelectPrivilege = hasMetaDataLocksSelectPrivilege;
            this.isMetaDataLocksEnable = isMetaDataLocksEnable;
            this.supportOpenSSL = supportOpenSSL;
            this.supportHyperLogLog = supportHyperLogLog;
            this.supportFastChecker = supportFastChecker;
            this.supportXxHash = supportXxHash;
            this.supportChangeSet = supportChangeSet;
            this.supportChangeSetBackPressure = supportChangeSetBackPressure;
            this.supportXOptForAutoSp = supportXOptForAutoSp;
            this.supportXRpc = supportXRpc;
            this.supportXOptForPhysicalBackfill = supportXOptForPhysicalBackfill && supportXRpc;
            this.supportMarkDistributed = supportMarkDistributed;
            this.supportSyncpoint = supportSyncPoint;
            this.supportFlashbackArea = supportFlashbackArea;
            this.supportAsOfCrossDdl = supportAsOfCrossDdl;
            this.supportTsoOpt = supportTsoOpt;
            this.supportAsyncCommit8032 = supportAsyncCommit8032;
            this.supportsModifyOnUpdate = supportsModifyOnUpdate;
            this.supportGetCidx = supportGetCidx;
            this.supportsVectorIndexes = supportsVectorIndexes;
        }

        public static StorageInfo create(IDataSource dataSource) {
            // mock storage version 5.7
            if (ConfigDataMode.isFastMock()) {
                return new StorageInfo(
                    "5.7",
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    1,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false
                );
            }

            String version = getMySqlVersion(dataSource);
            boolean supportTso = checkSupportTso(dataSource);
            boolean supportPurgeTso = checkSupportPurgeTso(dataSource);
            boolean supportTsoHeartbeat = checkSupportTsoHeartbeat(dataSource);
            boolean supportPerformanceSchema = checkSupportPerformanceSchema(dataSource);
            boolean isXEngine = checkIsXEngine(dataSource);

            Optional<PolarxUDFInfo> polarxUDFInfo = PolarxUDFInfo.build(dataSource);
            boolean supportsBloomFilter = polarxUDFInfo.map(PolarxUDFInfo::supportsBloomFilter).orElse(false);
            boolean supportsReturning = checkSupportReturning(dataSource);
            boolean supportsReturningAll = checkSupportReturningAll(dataSource);
            boolean supportsBackfillReturning = checkSupportBackfillReturning(dataSource);
            boolean supportChangeSet = checkSupportChangeSet(dataSource);
            boolean supportChangeSetBackPressure = checkSupportChangeSetBackPressure(dataSource);
            boolean supportsAlterType = checkSupportAlterType(dataSource);
            boolean supportCtsTransaction = checkSupportCtsTransaction(dataSource);
            boolean supportAsyncCommit = checkSupportAsyncCommit(dataSource);
            boolean supportLizard1PCTransaction = checkSupportLizard1PCTransaction(dataSource);
            final int lowerCaseTableNames = getLowerCaseTableNames(dataSource);
            final boolean supportSharedReadView = checkSupportSharedReadView(dataSource);

            boolean hasMetaDataLocksSelectPrivilege = checkMetaDataLocksSelectPrivilege(dataSource);
            boolean isMetaDataLocksEnable = checkMetaDataLocksEnable(dataSource);
            boolean supportHyperLogLog = polarxUDFInfo.map(PolarxUDFInfo::supportsHyperLogLog).orElse(false);
            boolean supportOpenSSL = checkSupportOpenSSL(dataSource);
            boolean supportFastChecker = polarxUDFInfo.map(PolarxUDFInfo::supportFastChecker).orElse(false);
            boolean supportXxHash = checkSupportXxHash(dataSource);
            boolean supportXOptForAutoSp = checkSupportXOptForAutoSp(dataSource);
            boolean supportXRpc = checkSupportXRpc(dataSource);
            boolean supportXoptForPhysicalBackfill = checkSupportXOptForPhysicalBackfill(dataSource);
            boolean supportMarkDistributed = checkSupportMarkDistributed(dataSource);
            boolean supportSyncPoint = checkSupportSyncPoint(dataSource);
            boolean supportFlashbackArea = checkSupportFlashbackArea(dataSource);
            boolean supportAsOfCrossDdl = checkSupportAsOfCrossDdl(dataSource);
            boolean supportTsoOpt = checkSupportTsoOpt(dataSource);
            boolean supportAsyncCommit8032 = checkSupportAsyncCommit8032(dataSource);
            boolean supportsModifyOnUpdate = checkSupportModifyOnUpdate(dataSource);
            boolean supportGetCidx = checkSupportGetCidx(dataSource);
            boolean supportsVectorIndexes = checkSupportsVectorIndexes(dataSource);

            return new StorageInfo(
                version,
                supportTso,
                supportPurgeTso,
                supportTsoHeartbeat,
                supportCtsTransaction,
                supportAsyncCommit,
                supportLizard1PCTransaction,
                supportsBloomFilter,
                supportsReturning,
                supportsReturningAll,
                supportsBackfillReturning,
                supportsAlterType,
                lowerCaseTableNames,
                supportPerformanceSchema,
                isXEngine,
                supportSharedReadView,
                hasMetaDataLocksSelectPrivilege,
                isMetaDataLocksEnable,
                supportHyperLogLog,
                supportOpenSSL,
                supportFastChecker,
                supportXxHash,
                supportChangeSet,
                supportChangeSetBackPressure,
                supportXOptForAutoSp,
                supportXRpc,
                supportXoptForPhysicalBackfill,
                supportMarkDistributed,
                supportSyncPoint,
                supportFlashbackArea,
                supportAsOfCrossDdl,
                supportTsoOpt,
                supportAsyncCommit8032,
                supportsModifyOnUpdate,
                supportGetCidx,
                supportsVectorIndexes
            );
        }
    }

    public static class PolarxUDFInfo {
        private static final String STATUS_ACTIVE = "ACTIVE";

        private static final String PLUGIN_NAME = "polarx_udf";
        private static final String VAR_FUNCTION_LIST = "polarx_udf_function_list";
        private static final String UDF_BLOOM_FILTER = "bloomfilter";
        private static final String UDF_HYPERLOGLOG = "hyperloglog";
        private static final String UDF_HASHCHECK = "hashcheck";

        private final int majorVersion;
        private final int minorVersion;
        private final String status;
        private final Set<String> functions;

        private PolarxUDFInfo(int majorVersion, int minorVersion, String status,
                              Set<String> functions) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
            this.status = status;
            this.functions = functions;
        }

        public static Optional<PolarxUDFInfo> build(DataSource dataSource) {
            try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
                String pluginSql = "select `PLUGIN_VERSION`, `PLUGIN_STATUS` "
                    + " from information_schema.plugins "
                    + " where `PLUGIN_NAME` = '" + PLUGIN_NAME + "';";

                int[] versionParts = new int[2];
                String status;
                Set<String> udfFunctions = new HashSet<>();
                try (ResultSet rs = stmt.executeQuery(pluginSql)) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }

                    if (!parseVersion(rs.getString(1), versionParts)) {
                        return Optional.empty();
                    }
                    status = rs.getString(2);
                }
                String functionListSql = "SHOW VARIABLES LIKE '" + VAR_FUNCTION_LIST + "';";
                try (ResultSet rs = stmt.executeQuery(functionListSql)) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }

                    String functionListString = rs.getString(2);
                    udfFunctions.addAll(Arrays.stream(functionListString.split(",")).collect(Collectors.toList()));
                }

                return Optional.of(new PolarxUDFInfo(versionParts[0], versionParts[1], status, udfFunctions));
            } catch (Exception ex) {
                logger.warn("Failed to check polar udf info", ex);
                return Optional.empty();
            }
        }

        private static boolean parseVersion(String versionString, int[] version) {
            String[] parts = versionString.split("\\.");
            if (parts.length < 2) {
                return false;
            }

            try {
                version[0] = Integer.parseInt(parts[0]);
                version[1] = Integer.parseInt(parts[1]);
                return true;
            } catch (Exception e) {
                logger.warn("Failed to parse polarx udf version string: " + versionString);
                return false;
            }
        }

        public boolean supportsBloomFilter() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_BLOOM_FILTER);
        }

        public boolean supportsHyperLogLog() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_HYPERLOGLOG);
        }

        public boolean supportFastChecker() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_HASHCHECK);
        }
    }
}
