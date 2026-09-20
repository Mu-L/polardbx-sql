package com.alibaba.polardbx.server;
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


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.cdc.CdcDumpStreamObserver;
import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.audit.ConnectionInfo;
import com.alibaba.polardbx.common.cdc.BinlogDumpMetrics;
import com.alibaba.polardbx.common.cdc.BinlogDumpMetricsManager;
import com.alibaba.polardbx.common.constants.CpuStatAttribute;
import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.constants.ServerVariables;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ShareReadViewPolicy;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.statementsummary.StatementSummaryManager;
import com.alibaba.polardbx.common.statementsummary.model.ExecInfo;
import com.alibaba.polardbx.common.utils.ExceptionUtils;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.MergeHashMap;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ai.AgentSession;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.GsiStatisticsManager;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbUserPrivilegeManager;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskAlgo;
import com.alibaba.polardbx.gms.metadb.encdb.mask.EncdbMaskType;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.sqlaudit.SqlAuditInterceptor;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.TPreparedStatement;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.net.ClusterAcceptIdGenerator;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.FrontendAuthenticator;
import com.alibaba.polardbx.net.handler.FrontendAuthorityAuthenticator;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.ccl.common.CclMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;
import com.alibaba.polardbx.optimizer.ccl.exception.CclRescheduleException;
import com.alibaba.polardbx.optimizer.ccl.service.Reschedulable;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContextPropertiesLifeCycle;
import com.alibaba.polardbx.optimizer.context.LoadDataContext;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.MetaConverter;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.deepage.DeepPageCache;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.PreStmtMetaData;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.StatementMap;
import com.alibaba.polardbx.optimizer.secret.SecretMaskUtils;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.statis.XplanStat;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.DumpRequest;
import com.alibaba.polardbx.rpc.cdc.DumpStream;
import com.alibaba.polardbx.server.conn.ResultSetCachedObj;
import com.alibaba.polardbx.server.encdb.EncdbResultSet;
import com.alibaba.polardbx.server.encdb.EncdbRuleSpreader;
import com.alibaba.polardbx.server.encdb.EncdbServer;
import com.alibaba.polardbx.server.encdb.EncdbSessionState;
import com.alibaba.polardbx.server.executor.utils.BinaryResultSetUtil;
import com.alibaba.polardbx.server.executor.utils.MysqlDefs;
import com.alibaba.polardbx.server.executor.utils.ResultSetUtil;
import com.alibaba.polardbx.server.handler.ServerLoadDataHandler;
import com.alibaba.polardbx.server.lock.LockingFunctionManager;
import com.alibaba.polardbx.server.mock.MockExecutor;
import com.alibaba.polardbx.server.response.Ping;
import com.alibaba.polardbx.server.session.ServerSession;
import com.alibaba.polardbx.server.ugly.hint.EagleeyeTestHintParser;
import com.alibaba.polardbx.server.util.AuditUtil;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.server.util.MockUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.transaction.trx.ReadOnlyTsoTransaction;
import com.alibaba.polardbx.transaction.trx.XATsoTransaction;
import com.google.common.base.Preconditions;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSystemVar;
import org.apache.calcite.sql.SqlUserDefVar;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_HANDLE_DATA;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_DEADLOCK;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_FATAL_CANNOT_CONTINUE;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TRANS_ROLLBACK_STATEMENT_FAIL;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.common.utils.ExceptionUtils.isMySQLIntegrityConstraintViolationException;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.vendorErrorIs;

/**
 * @author xianmao.hexm 2011-4-21 上午11:22:57
 */
public final class ServerConnection extends FrontendConnection implements Reschedulable {

    protected static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);
    private static final Logger cdcLogger = LoggerFactory.getLogger(ServerConnection.class);
    private static final ErrorPacket shutDownError = PacketUtil.getShutdown();
    private static final long AUTH_TIMEOUT = 15 * 1000L;

    /**
     * sql是否正在执行
     */
    private final AtomicBoolean statementExecuting = new AtomicBoolean(false);

    /**
     * Lightweight cancellation flag for NL2SQL agent queries.
     * Set by cancelQuery() unconditionally; checked by isQueryCancelled().
     * Unlike futureCancelErrorCode, this does NOT trigger conn.kill() or closeByTraceId().
     */
    private volatile boolean queryCancelled = false;
    private final Object mdlContextLock = new Object();
    /**
     * <pre>
     * Id of physical sql group, example:
     *
     * create table t1 (id int, name varchar(64), primary key(id)) dbpartition by hash(name);
     * insert into t1 (id, name) value(1, "alic"), (2, "bob"), (3, "alc");
     * update t1 set name = "alice" where name in ("alic", "alc");
     *
     * update will be converted to select + delete + insert like
     *
     * select id, name from t1 where name in ("alic", "alc") for update;
     * delete from t1 where id in (1,3);
     * insert into t1 (id, name) value (1, "alice"), (3, "alice");
     *
     * For cdc, the information "delete executed before insert" is important,
     * so we use a successive physical sql group id to represent which goes first.
     *
     * </pre>
     */
    private Long phySqlId = 0L;
    private Long sqlId = 0L;
    private Long txId = 0L;
    private String sqlSample = null;
    private InternalTimeZone timeZone = null;
    private volatile int txIsolation = -1;
    private volatile int stmtTxIsolation = -1;
    private volatile int socketTimeout = -1;
    private volatile boolean autocommit = true;
    private ITransactionPolicy trxPolicy = null;
    private volatile SchemaConfig schemaConfig = null;
    private volatile Map<String, TbPriv> tablePrivCache = null;
    private volatile Map<String, DbPriv> schemaPrivCache = null;
    // default sql mode
    private volatile String sqlMode = null;
    private ServerSession session;
    private volatile TConnection conn;

    final private AtomicInteger stmtId = new AtomicInteger(0);

    /**
     * smForQuery for COM_QUERY and smForPrepare for COM_STMT_PREPARE, they are
     * independent since COM_QUERY need set @param = xxx, so another param map
     * is needed, COM_STMT_PREPARE no need the param map since it always carry
     * the param on one COM_STMT_EXECUTE command.
     */
    private StatementMap smForQuery;
    private StatementMap smForPrepare;

    /**
     * Statement ID -> Cached result set
     * Since it is used in rare cases (only used in cursor-fetch mode),
     * we initialize it in {@link #getResultSetMap}.
     */
    final private ConcurrentMap<Integer, ResultSetCachedObj> resultSetMap = new ConcurrentHashMap<>();
    private MemoryPool cursorFetchMemoryPool = null;

    private boolean beginTransaction = false;
    private MatrixStatistics stats;
    // 下推到下层的系统变量，全部小写
    final private Map<String, Object> serverVariables = new HashMap<>();
    // 全局的系统变量
    final private Map<String, Object> globalServerVariables = new HashMap<>();
    // 用户定义的变量，全部小写
    final private Map<String, Object> userDefVariables = new HashMap<>();
    // 特殊处理的系统变量以及自定义的系统变量，全部小写
    final private Map<String, Object> extraServerVariables = new HashMap<>();
    // ConnectionParams 中支持的其他系统变量，全部大写
    final private Map<String, Object> connectionVariables = new HashMap();
    private String traceId = null;
    private boolean readOnly = false;
    private boolean txReadOnly = false;
    private boolean enableANSIQuotes = false;
    private long lastSqlStartTime = 0;
    private boolean sqlMock = false;
    private MockExecutor mockExecutor;
    private MdlContext mdlContext;
    private RuntimeStatistics lastSqlRunTimeStat = null;
    private volatile RescheduleParam rescheduleParam;
    private volatile RescheduleTask rescheduleTask;
    private ExecInfo execInfo;
    private String partitionHint;

    private String jdbcUrl;

    // print log even if ExecutionContext is null
    private boolean isLoginAction = false;
    /**
     * Session's active roles.
     *
     * @see ActiveRoles
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">Set Role</a>
     */
    private volatile ActiveRoles activeRoles = ActiveRoles.defaultValue();
    private ShareReadViewPolicy shareReadView = ShareReadViewPolicy.DEFAULT;

    /**
     * the intra group parallelism ,
     * when it is not null means user manually set the variable by "SET GROUP_PARALLELISM=xxx"
     */
    private Long groupParallelism = null;

    /**
     * data encryption for encdb
     * dek and nonce is session-level
     */
    private EncdbSessionState encdbSessionState;

    private Map<PlanCache.CacheKey, DeepPageCache> deepPageCacheMap = new HashMap<>(1);

    /**
     * NL2SQL Agent session state, lazily initialized.
     */
    private volatile AgentSession agentSession;

    public ServerConnection(SocketChannel channel) {

        super(channel);

        smForQuery = new StatementMap(false);
        smForPrepare = new StatementMap(true);

        // JDBC会改成STRICT_TRANS_TABLES，为与MySQL兼容，需要改成global的设置
        serverVariables.put("sql_mode", "default");
        serverVariables.put("net_write_timeout", (long) (8 * 60 * 60));

        instanceId = CobarServer.getInstance().getConfig().getSystem().getInstanceId();

        /**
         * For each front connection mdl context should be create and remove only once.
         * Cause some other component will call TConnection close during ds.init()
         * which will remove mdl context.
         * So we can only create MdlContext as a field of ServerConnection
         */
        mdlContext = MdlManager.addContext(this.id);
        //ExtraServerVariables 初始化
        initExtraServerVariables();

        if (!ConfigDataMode.isMasterMode()) {
            readOnly = true;
        }
    }

    public static int toFlag(ColumnMeta cm) {
        int flags = 0;
        if (cm.isNullable()) {
            flags |= 0001;
        }

        if (cm.isAutoIncrement()) {
            flags |= 0200;
        }

        return flags;
    }

    public Map<Integer, ResultSetCachedObj> getResultSetMap() {
        return resultSetMap;
    }

    public Map<String, Object> getServerVariables() {
        return serverVariables;
    }

    public Map<String, Object> getGlobalServerVariables() {
        return globalServerVariables;
    }

    public Map<String, Object> getUserDefVariables() {
        return userDefVariables;
    }

    public Map<String, Object> getConnectionVariables() {
        return connectionVariables;
    }

    private void initExtraServerVariables() {
        if (extraServerVariables.isEmpty()) {
            //init
            extraServerVariables.put("sockettimeout", this.socketTimeout);
            extraServerVariables.put("pure_async_ddl_mode", false);
            extraServerVariables.put("transaction policy", 3);
            extraServerVariables.put("trans.policy", "ALLOW_READ");
            extraServerVariables.put("drds_transaction_policy", "ALLOW_READ");
            extraServerVariables.put("read", "WRITE");
            extraServerVariables.put("batch_insert_policy", "SPLIT");
            extraServerVariables.put("sql_mock", this.sqlMock);
        }
    }

    public Map<String, Object> getExtraServerVariables() {
        return extraServerVariables;
    }

    public boolean isCharacterSetResultsNullOrBinary() {
        final Object characterSetResults = extraServerVariables.get("character_set_results");
        return characterSetResults != null
            && (characterSetResults.toString().isEmpty()
            || "binary".equalsIgnoreCase(characterSetResults.toString()));
    }

    public Object getSysVarValue(SqlSystemVar var) {
        Object value = null;
        VariableManager variableManager = OptimizerContext.getContext(schema).getVariableManager();
        String lowerCaseKey = var.getName().toLowerCase();

        if (extraServerVariables.containsKey(lowerCaseKey)) {
            value = extraServerVariables.get(lowerCaseKey);
        } else if (serverVariables.containsKey(lowerCaseKey)) {
            value = serverVariables.get(lowerCaseKey);
        } else {
            switch (var.getScope()) {
            case GLOBAL:
                value = variableManager.getGlobalVariable(lowerCaseKey);
                break;
            case SESSION:
                switch (lowerCaseKey) {
                case "last_insert_id":
                    value = getLastInsertId();
                    break;
                case "tx_isolation":
                case "transaction_isolation":
                    // Note: It's hard to get global isolation level here...
                    IsolationLevel isolation = IsolationLevel.fromInt(txIsolation);
                    value = (isolation != null ? isolation.nameWithHyphen() : null);
                    break;
                case "read_only":
                    if (ConfigDataMode.isMasterMode()) {
                        value = 0;
                    } else if (ConfigDataMode.isReadOnlyMode()) {
                        value = 1;
                    }
                    break;
                default:
                    value = variableManager.getSessionVariable(lowerCaseKey);
                    break;
                }
                break;
            default:
                value = null;
            }
        }
        return value;
    }

    public Object getVarValueBySqlNode(SqlNode oriValue) {
        Object value = null;
        if (oriValue instanceof SqlUserDefVar) {
            String lowerCaseKey = ((SqlUserDefVar) oriValue).getName().toLowerCase();
            value = userDefVariables.get(lowerCaseKey);
        } else if (oriValue instanceof SqlSystemVar) {
            SqlSystemVar var = (SqlSystemVar) oriValue;
            if (!ServerVariables.contains(var.getName()) && !ServerVariables.isExtra(var.getName())) {
                return null;
            }
            value = getSysVarValue(var);
        }
        return value;
    }

    public String getVarStringValue(SqlNode oriValue) {
        String value;
        if (oriValue instanceof SqlUserDefVar || oriValue instanceof SqlSystemVar) {
            value = String.valueOf(this.getVarValueBySqlNode(oriValue));
        } else {
            value = RelUtils.stringValue(oriValue);
        }
        return value;
    }

    public Integer getVarIntegerValue(SqlNode oriValue) {
        int value;
        if (oriValue instanceof SqlUserDefVar || oriValue instanceof SqlSystemVar) {
            value = Integer.parseInt(String.valueOf(this.getVarValueBySqlNode(oriValue)));
        } else {
            value = RelUtils.integerValue((SqlLiteral) oriValue);
        }
        return value;
    }

    public Long getVarLongValue(SqlNode oriValue) {
        long value;
        if (oriValue instanceof SqlUserDefVar || oriValue instanceof SqlSystemVar) {
            value = Long.parseLong(String.valueOf(this.getVarValueBySqlNode(oriValue)));
        } else {
            value = RelUtils.longValue(oriValue);
        }
        return value;
    }

    public Boolean getVarBooleanValue(SqlNode oriValue) {
        Object value = getVarValueBySqlNode(oriValue);
        if (value == null) {
            return null;
        }
        String strValue = String.valueOf(value);
        Boolean res = false;
        if (value instanceof Boolean) {
            res = (Boolean) value;
        } else if (value instanceof Integer) {
            res = (Integer) value != 0;
        } else if ("ON".equalsIgnoreCase(strValue)) {
            res = true;
        } else if ("OFF".equalsIgnoreCase(strValue)) {
            res = false;
        } else {
            return null;
        }
        return res;
    }

    @Override
    public boolean checkConnectionCount() {
        return CobarServer.getInstance().getConnectionCount() <= DynamicConfig.getInstance().getMaxConnections();
    }

    @Override
    public boolean checkUserConnectionCount(String user, String host, boolean ignoreHost) {
        int maxUserConnections = InstConfUtil.getInt(ConnectionParams.MAX_USER_CONNECTIONS);
        if (maxUserConnections < 0) {
            return true;
        }
        if (PolarPrivUtil.isPolarxRootUser(user)) {
            return true;
        }
        int count = 0;
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection c : p.getFrontends().values()) {
                if (user.equalsIgnoreCase(c.getUser())) {
                    if (ignoreHost || host.equals(c.getHost())) {
                        count++;
                    }
                }
            }
        }
        return count < maxUserConnections;
    }

    @Override
    protected long genConnId() {
        return ClusterAcceptIdGenerator.getInstance().nextId();
    }

    @Override
    public FrontendAuthenticator createFrontendAuthenticator(FrontendConnection conn) {
        if (isPrivilegeMode()) {
            return new FrontendAuthorityAuthenticator(conn);
        } else {
            return new FrontendAuthenticator(conn);
        }
    }

    @Override
    public void initDB(byte[] data) {
        super.initDB(data);
        switchDb(this.schema);
    }

    protected void switchDb(String schema) {
        if (DynamicConfig.getInstance().isEnableSameDbSwitchNoop()
            && schemaConfig != null && !schemaConfig.isDropped() && schema != null
            && schema.equalsIgnoreCase(schemaConfig.getName())) {
            return;
        }

        if (schemaConfig != null && schemaConfig.isDropped()) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.warn("error when close", e);
                } finally {
                    conn = null;
                    releaseLockAndRemoveMdlContext();
                }
            } else {
                releaseLockAndRemoveMdlContext();
            }
        } else {
            TConnection oldConn = this.conn;
            if (null != oldConn) {
                /**
                 * In mysql, its transaction can be commit
                 *  on different database after using 'use xxx_db',
                 * such as:
                 * <pre>
                 *      use d1;
                 *      begin;
                 *      delete from t1;--trx1
                 *      use d2;
                 *      commit;
                 * the transaction trx1 can be commit or rollback after using database d2.
                 * </pre>
                 *
                 * But in PolarDB-X, after use a new database by using `use xxx_db`;
                 * the trx of the last db will be auto rollback and closed.
                 */
                try {
                    oldConn.close();
                    this.conn = null;
                } catch (SQLException e) {
                    logger.warn("error when close trx conn for last db", e);
                } finally {
                    //
                }
            }
        }
        schemaConfig = null;
        tablePrivCache = null;
        schemaPrivCache = null;

        if (ConfigDataMode.isFastMock() && ExecutorContext.getContext(schema) == null) {
            schemaConfig = MockUtil.mockSchema(schema);
            return;
        }

        if (schema != null) {
            // first try cluster and apploader,then mock
            schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get(schema);

            // -- load new created db   --
            if (schemaConfig == null) {

                try {
                    CobarServer.getInstance().getConfig().getClusterLoader().getAppLoader().loadApp(schema);
                } catch (Throwable ex) {
                    if (ex.getMessage().contains("Unknown database")) {
                        return;
                    } else {
                        throw ex;
                    }
                }
                schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get(schema);
            }
        }

        if (schemaConfig == null) {
            this.stats = MatrixStatistics.EMPTY;
        } else {
            MatrixStatistics oldStats = stats;
            TDataSource ds = schemaConfig.getDataSource();
            this.stats = ds.getStatistics();
            if (oldStats != stats) {
                if (oldStats != null) {
                    oldStats.activeConnection.decrementAndGet();
                }
                if (this.stats != null) {
                    this.stats.activeConnection.incrementAndGet();
                }
            }

            warmUpDb(ds);
        }
    }

    private void warmUpDb(TDataSource ds) {
        if (CobarServer.getInstance().getConfig().getSystem().getEnableLogicalDbWarmmingUp()) {
            Throwable ex = null;
            try {
                ex = TDataSourceInitUtils.initDataSource(ds);
                if (ex != null) {
                    logger.warn("Failed to init schema " + ds.getSchemaName() + " during using db, the cause is "
                        + ex.getMessage(), ex);
                }
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    @Override
    public boolean isIdleTimeout() {
        if (isAuthenticated) {
            return super.isIdleTimeout();
        } else {
            return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + AUTH_TIMEOUT;
        }
    }

    public int getTxIsolation() {
        if (stmtTxIsolation != -1) {
            return stmtTxIsolation;
        }
        return txIsolation;
    }

    private void recoverTxIsolation() {
        if (stmtTxIsolation != -1) {
            stmtTxIsolation = -1;
            setConnTxIsolation(txIsolation);
        }
    }

    private void setConnTxIsolation(int txIsolation) {
        if (this.conn != null) {
            try {
                this.conn.setTransactionIsolation(txIsolation);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public void setStmtTxIsolation(int txIsolation) {
        if (this.beginTransaction) {
            throw new TddlRuntimeException(ErrorCode.ERR_CANT_CHANGE_TX_ISOLATION);
        }
        this.stmtTxIsolation = txIsolation;
        setConnTxIsolation(txIsolation);
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
        if (!ShareReadViewPolicy.supportTxIsolation(txIsolation)) {
            this.shareReadView = ShareReadViewPolicy.OFF;
        }
        setConnTxIsolation(txIsolation);
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int milliseconds) {
        this.socketTimeout = milliseconds;
        if (this.conn != null) {
            try {
                this.conn.setNetworkTimeout(null, milliseconds);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        setAutocommit(autocommit, false);
    }

    public synchronized void setAutocommit(boolean autocommit, boolean isBegin) {
        if (isTrxFatalAndShouldClose()) {
            close();
        }

        checkTrxCanCommit();

        // 新事务开始，清掉txId, 保留 BEGIN/START TRANSACTION 的 txId
        if (this.autocommit != autocommit && !isBegin) {
            this.txId = null;
        }

        // 自动提交, 清理事务参数
        if (autocommit) {
            this.trxPolicy = null;
            this.shareReadView = ShareReadViewPolicy.DEFAULT;
        }

        this.autocommit = autocommit;
        if (this.conn != null) {
            try {
                this.conn.setAutoCommit(autocommit);
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public InternalTimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZoneId) {
        if ("SYSTEM".equalsIgnoreCase(timeZoneId)) {
            this.conn.resetTimeZone();
            this.timeZone = this.conn.getTimeZone();
            this.extraServerVariables.put("time_zone", timeZoneId);
        } else {
            InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneId);
            if (timeZone == null) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_UNKNOWN_TZ,
                    timeZoneId);
            }
            this.timeZone = timeZone;
            this.extraServerVariables.put("time_zone", timeZoneId);
            if (this.conn != null) {
                this.conn.setTimeZone(timeZone);
            }
        }
    }

    public long getLastInsertId() {
        if (this.conn != null) {
            return this.conn.getLastInsertId();
        } else {
            return 0L;
        }
    }

    public ServerSession getSession() {
        return session;
    }

    public void setSession(ServerSession session) {
        this.session = session;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;

        if (this.conn != null) {
            this.conn.setSqlMode(sqlMode);
        }
    }

    @Override
    public boolean setCharsetIndex(int ci) {
        boolean result = super.setCharsetIndex(ci);
        if (result) {
            if (this.conn != null) {
                this.conn.setEncoding(resultSetCharset);
            }
        }
        return result;
    }

    @Override
    public boolean setCharset(String charset) {
        boolean result = super.setCharset(charset);
        if (result) {
            if (this.conn != null) {
                this.conn.setEncoding(resultSetCharset);
            }
        }

        return result;
    }

    @Override
    public synchronized void ping() {
        Ping.response(this);
    }

    private CdcRpcClient.CdcRpcStreamingProxy proxy = null;

    @Override
    public void binlogDump(byte[] data) {
        try {
            updatePrivilegeContext();
            this.conn.getExecutionContext().setPrivilegeMode(this.isPrivilegeMode());
            PolarPrivilegeUtils.checkInstancePrivilege(PrivilegePoint.REPLICATION_SLAVE,
                this.conn.getExecutionContext());
        } catch (Exception e) {
            EventLogger.log(EventType.CDC_WARN, String.format("No replication slave privilege for binlog dump! "
                + "user %s, host %s ", getUser(), getHost()));
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                PrivilegePoint.REPLICATION_SLAVE.name(), getUser(), getHost());
        }

        CountDownLatch countDownLatch = new CountDownLatch(1);
        MySQLMessage mm = new MySQLMessage(data);
        FrontendConnection connection = this;

        // Change context:
        // - Before: binlog-pos was parsed with MySQLMessage.readInt() (signed int). COM_BINLOG_DUMP
        //   defines binlog-pos as uint32, so positions above 2^31 were sign-extended to negative
        //   values and forwarded to the CDC Dumper, which silently rewinds such positions to the file
        //   start, replaying already-applied transactions (Duplicate entry 1062 on downstream slaves).
        // - Path impact: only the COM_BINLOG_DUMP request parsing path is affected; downstream
        //   DumpRequest.position is proto int64 and carries [0, 2^32) losslessly. COM_REGISTER_SLAVE
        //   parses no position and COM_BINLOG_DUMP_GTID is unimplemented, so no sibling path changes.
        // - Capability regression: None; for positions <= 2^31-1 readUB4 returns the identical
        //   non-negative value readInt produced, so existing dumps behave exactly as before.
        mm.position(5);
        long position = mm.readUB4();
        mm.position(11);
        int serverId = mm.readInt();
        String fileName = "";
        if (data.length > 15) {
            mm.position(15);
            fileName = mm.readString().trim();
        }
        if (cdcLogger.isInfoEnabled()) {
            cdcLogger.info(
                "Slave serverId=" + serverId + " will dump from " + fileName + "@" + position + " with params:"
                    + getUserDefVariables());
        }
        String streamName = "";
        if (fileName.contains("_")) {
            int idx = fileName.lastIndexOf('_');
            streamName = fileName.substring(0, idx);
        }
        proxy = new CdcRpcClient.CdcRpcStreamingProxy(streamName);
        BinlogDumpMetrics dumpMetrics = new BinlogDumpMetrics(traceId);
        BinlogDumpMetricsManager.getInstance().register(id, dumpMetrics);
        dumpMetrics.markDumpStart();
        StreamObserver<DumpStream> observer = new CdcDumpStreamObserver(connection, countDownLatch, dumpMetrics);

        Map<String, Object> ext = prepareBinlogDumpExtConfig();

        // 在获取下一个dumper target之前，先要等待上一个获取到的dumper target 发送完第一个包以更新dumper的统计信息
        cdcLogger.info("get lock for GET_DUMPER_TARGET");
        proxy.dump(
            DumpRequest.newBuilder().setFileName(fileName).setPosition(position)
                .setRegistered(registerSlave)
                .setExt(JSON.toJSONString(ext)).setStreamName(streamName).build(),
            observer);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            cdcLogger.warn("binlog dump countDownLatch.await fail ", e);
        }
    }

    public Map<String, Object> prepareBinlogDumpExtConfig() {
        Map<String, Object> ext = new HashMap<>(getUserDefVariables());
        ext.put("trace_id", traceId);
        ext.put("id", id);
        ext.put("user", user);
        ext.put("inst_id", instanceId);

        // 获取用户关于过滤的配置
        Map<String, Map<String, String>> binlogFilterUserConfigMap = new HashMap<>();
        String binlogFilterUserConfigJson =
            InstConfUtil.getValNoDefault(ConnectionParams.BINLOG_DUMP_FILTER_USER_CONFIG);
        if (binlogFilterUserConfigJson != null) {
            binlogFilterUserConfigMap = JSON.parseObject(binlogFilterUserConfigJson, Map.class);
        }
        Map<String, String> binlogFilterUserConfig = binlogFilterUserConfigMap.getOrDefault(user, new HashMap<>());

        // 将keys转为大写
        binlogFilterUserConfig = binlogFilterUserConfig.entrySet().stream()
            .collect(Collectors.toMap(
                e -> e.getKey().toUpperCase(),
                Map.Entry::getValue,
                (existing, replacement) -> existing));

        if ("polardbx_root".equalsIgnoreCase(user)) {
            binlogFilterUserConfig.putIfAbsent(ConnectionParams.BINLOG_DUMP_ROWS_QUERY_IGNORE_ENABLED.getName(),
                "false");
        }

        // 是否过滤归档表删除事件
        Boolean archiveFilterEnabled =
            getBinlogDumpExtConfigBool(ConnectionParams.BINLOG_DUMP_ARCHIVE_IGNORE_ENABLED, binlogFilterUserConfig);
        // 是否过滤rows query event
        Boolean rowsQueryFilterEnabled =
            getBinlogDumpExtConfigBool(ConnectionParams.BINLOG_DUMP_ROWS_QUERY_IGNORE_ENABLED, binlogFilterUserConfig);
        Boolean filterBySetFlag =
            getBinlogDumpExtConfigBool(ConnectionParams.BINLOG_DUMP_IGNORE_BY_SET_FLAG, binlogFilterUserConfig);

        if (archiveFilterEnabled != null) {
            ext.put("archive_ignore", archiveFilterEnabled);
        }
        if (rowsQueryFilterEnabled != null) {
            ext.put("rows_query_ignore", rowsQueryFilterEnabled);
        }
        if (filterBySetFlag != null) {
            ext.put("ignore_by_flag", filterBySetFlag);
        }

        // 识别黑白名单
        String binlogDumpDoTables =
            getBinlogDumpExtConfigStr(ConnectionParams.BINLOG_DUMP_DO_TABLE, binlogFilterUserConfig);
        String binlogDumpIgnoreTables =
            getBinlogDumpExtConfigStr(ConnectionParams.BINLOG_DUMP_IGNORE_TABLE, binlogFilterUserConfig);

        if (!StringUtil.isEmpty(binlogDumpIgnoreTables)) {
            // 统一转小写
            binlogDumpIgnoreTables = binlogDumpIgnoreTables.toLowerCase();
            ext.put("table_ignore", binlogDumpIgnoreTables);
        } else if (!StringUtil.isEmpty(binlogDumpDoTables)) {
            //  统一转小写
            binlogDumpDoTables = binlogDumpDoTables.toLowerCase();
            ext.put("table_allow", binlogDumpDoTables);
        }

        return ext;
    }

    /**
     * session config > global user config > global config > cdc global config
     */
    private Boolean getBinlogDumpExtConfigBool(BooleanConfigParam param, Map<String, String> config) {
        boolean res;
        Object sessionConfig = getConnectionVariables().get(param.getName());
        if (sessionConfig != null) {
            res = Boolean.parseBoolean((String) sessionConfig);
        } else {
            String userConfig = config.get(param.getName());
            if (userConfig != null) {
                res = Boolean.parseBoolean(userConfig);
            } else {
                String globalConfig = InstConfUtil.getValNoDefault(param);
                if (globalConfig != null) {
                    res = Boolean.parseBoolean(globalConfig);
                } else {
                    return null;
                }
            }
        }
        return res;
    }

    /**
     * session config > global user config > global config > cdc global config
     */
    private String getBinlogDumpExtConfigStr(ConfigParam param, Map<String, String> config) {
        Object sessionConfig = getConnectionVariables().get(param.getName());
        if (sessionConfig != null) {
            return (String) sessionConfig;
        } else {
            String globalUserConfig = config.get(param.getName());
            if (globalUserConfig != null) {
                return globalUserConfig;
            }
        }
        return InstConfUtil.getValNoDefault(param);
    }

    public boolean execute(String sql, boolean hasMore) {
        return execute(ByteString.from(sql), hasMore);
    }

    public boolean execute(ByteString sql, boolean hasMore) {
        return execute(sql, hasMore, false, null, null, null);
    }

    public boolean execute(ByteString sql, List<Pair<Integer, ParameterContext>> params, boolean hasMore,
                           QueryResultHandler handler) {
        return execute(sql, hasMore, false, params, null, handler);
    }

    public boolean execute(ByteString sql, boolean hasMore, boolean prepare,
                           final List<Pair<Integer, ParameterContext>> params, PreparedStmtCache preparedStmtCache,
                           QueryResultHandler handler) {
        try {
            final Future<Boolean> future =
                execute(sql, hasMore, prepare, params, -1, MySQLPacket.CURSOR_TYPE_NO_CURSOR, preparedStmtCache,
                    handler);
            if (future.isDone()) {
                return future.get();
            }
            return true;
        } catch (Exception e) {
            throw new TddlRuntimeException(ERR_SERVER, e);
        }
    }

    public synchronized boolean execute(ByteString sql, final List<Pair<Integer, ParameterContext>> params,
                                        int statementId, byte flags, PreparedStmtCache preparedStmtCache) {
        setPreparedStmt(preparedStmtCache);
        try {
            final Future<Boolean> future =
                execute(sql, false, true, params, statementId, flags, preparedStmtCache, null);
            if (future.isDone()) {
                return future.get();
            }
            return true;
        } catch (Exception e) {
            throw new TddlRuntimeException(ERR_SERVER, e);
        }
    }

    public synchronized Future<Boolean> executeFuture(ByteString sql, boolean hasMore) {
        return execute(sql, hasMore, false, null, -1, MySQLPacket.CURSOR_TYPE_NO_CURSOR, null, null);
    }

    public Future<Boolean> execute(ByteString sql, boolean hasMore, boolean prepare,
                                   final List<Pair<Integer, ParameterContext>> params, int statementId, byte flags,
                                   PreparedStmtCache preparedStmtCache, QueryResultHandler handler) {
        if (this.schema == null) {
            if (!ConfigDataMode.isPolarDbX()) {
                writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return CompletableFuture.completedFuture(false);
            }
        }

        int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
        sqlSample = SecretMaskUtils.mask(sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length())));

        if (prepare) {
            // Prepare Execute 的查询结果
            handler = (null == handler) ?
                new ServerPreparedResultHandler(hasMore, statementId, traceId, flags, preparedStmtCache) : handler;
        } else {
            clearPreparedStmt();
            handler = (null == handler) ? new ServerResultHandler(hasMore, statementId, flags) : handler;
        }

        final ExecutionContext ec = getExecutionContext();
        if (ec != null) {
            ec.setMultiStmtHasMore(hasMore);
        }
        return innerExecute(sql, params, handler, null);
    }

    /**
     * 放进Map的同时缓存返回包的元信息
     */
    public synchronized void savePrepareStmtCache(PreparedStmtCache preparedStmtCache, boolean isServerPrepare) {
        if (isClosed()) {
            logger.warn("connection has been closed");
            return;
        }
        if (isServerPrepare) {
            // 缓存回包元信息
            String javaCharset = CharsetUtil.getJavaCharset(resultSetCharset);
            preparedStmtCache.setJavaCharset(javaCharset);
            preparedStmtCache.setCharsetIndex(getResultSetCharsetIndex());
            preparedStmtCache.setCatalog(StringUtil.encode_0(FieldPacket.DEFAULT_CATALOG_STR, javaCharset));

            smForPrepare.put(preparedStmtCache.getStmt().getStmtId(), preparedStmtCache);
        } else {
            smForQuery.put(preparedStmtCache.getStmt().getStmtId(), preparedStmtCache);
        }
    }

    public void savePrepareStmtCache(String stmtId, PreparedStmtCache preparedStmtCache, boolean isServerPrepare) {
        if (isServerPrepare) {
            smForPrepare.put(stmtId, preparedStmtCache);
        } else {
            smForQuery.put(stmtId, preparedStmtCache);
        }
    }

    public SQLStatement parsePrepareSqlTableNode(String sql) throws SQLException {
        prepareConnection();

        /*
         * send back the packet method 1) since the prepare response is complex
         * which contains the column metadata, so the command must send to real
         * db first, and we then hook the response and replace the stmt_id by
         * ours. method 2) directly compose COM_STMT_PREPARE response on tddl5
         * according to rule and schema.
         */
        OptimizerContext.setContext(getTddlConnection().getDs().getConfigHolder().getOptimizerContext());

        /*
         * Must set executorContext & optimizerContext before get
         * sqlParseManager & do ast build.
         */
        List<SQLStatement> sqlStatementList = FastsqlUtils.parseSql(sql);

        if (sqlStatementList.size() > 1) {
            throw new IllegalArgumentException("prepare not support multi-sql");
        }

        return sqlStatementList.get(0);
    }

    private void prepareConnection() throws SQLException {
        if (this.getTddlConnection() != null) {
            return;
        }

        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }

        // 拿一下链接
        getConnection(schema);
    }

    /**
     * Prepare模式下更新权限上下文
     */
    public void updatePrivilegeContextForPrepare(PreparedStmtCache preparedStmtCache) {
        updatePrivilegeContext();
        if (this.conn != null) {
            preparedStmtCache.setPrivilegeContext(this.conn.getExecutionContext().getPrivilegeContext());
        }
    }

    /**
     * 将权限添加到SQL上下文中
     */
    public void updatePrivilegeContext() {
        String user = getUser();
        String host = getHost();
        String schema = getSchema();

        if (this.conn == null) {
            initTddlConnection();
        }
        PrivilegeContext privilegeContext = new PrivilegeContext();
        privilegeContext.setUser(getUser());
        privilegeContext.setHost(getHost());
        privilegeContext.setSchema(getSchema());
        privilegeContext.setPrivileges(privileges);
        privilegeContext.setTrustLogin(isTrustLogin());
        privilegeContext.setManaged(isManaged());

            PolarPrivileges polarPrivileges = (PolarPrivileges) privileges;
            PolarAccountInfo polarUserInfo = polarPrivileges.checkAndGetMatchUser(user, host);
            this.setMatchPolarUserInfo(polarUserInfo);
            privilegeContext.setPolarUserInfo(polarUserInfo);
            privilegeContext.setActiveRoles(activeRoles);
        this.conn.getExecutionContext().setPrivilegeContext(privilegeContext);
    }

    public void genTraceId() {

        IdGenerator traceIdGen = TrxIdGenerator.getInstance().getIdGenerator();

        StringBuilder sb = new StringBuilder();

        if (this.autocommit) {
            this.txId = traceIdGen.nextId();
            this.sqlId = 0L;
            sb.append(Long.toHexString(txId));
        } else {
            if (this.txId == null) {
                this.txId = traceIdGen.nextId();
                this.sqlId = 0L;
            }

            this.sqlId++;
            sb.append(Long.toHexString(txId)).append("-").append(this.sqlId);
        }

        this.phySqlId = 0L;
        this.traceId = sb.toString();
    }

    /**
     * 简单做同步，避免erlang/nodejs等异步驱动的模式，提交多个sql后，等返回结果，容易串包，加同步，保证串行处理
     * 在重新调度(reschedule)的时候也有用到
     */
    public synchronized Future<Boolean> innerExecute(ByteString sql, List<Pair<Integer, ParameterContext>> params,
                                                     QueryResultHandler handler, LoadDataContext dataContext) {
        ByteString realSql = dataContext != null ? ByteString.from(dataContext.getLoadDataSql()) : sql;
        long statExecCpuNano = 0;
        if (MetricLevel.isSQLMetricEnabled(RuntimeStat.getMetricLevel())) {
            statExecCpuNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }

        // 针对非事务的请求进行链接中断
        if (!CobarServer.getInstance().isOnline() && isAutocommit()) {
            shutDownError.write(PacketOutputProxyFactory.getInstance().createProxy(this));
            return CompletableFuture.completedFuture(false);
        }

        if (isClosed()) {
            return CompletableFuture.completedFuture(false);
        }

        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + this.schema + "'");
            return CompletableFuture.completedFuture(false);
        }

        if (isTrxFatalAndShouldClose()) {
            close();
        }

        statementExecuting.set(true);

        // process sql mock
        if (sqlMock) {
            processMock(sql);
            statementExecuting.set(false);
            return CompletableFuture.completedFuture(true);
        }

        if (ConfigDataMode.isFastMock()) {
            if (sql.startsWith("drop database ")) {
                String dbName = sql.toString().split(" ")[2];
                MockUtil.destroySchema(dbName);
                handler.sendUpdateResult(1);
                handler.sendPacketEnd(false);
                this.sqlSample = null;
                return CompletableFuture.completedFuture(true);
            }
        }

        setLastSqlStartTime(System.nanoTime());

        final AtomicLong rowCount = new AtomicLong();
        ResultSet rs = null;
        Throwable exception = null;
        TPreparedStatement stmt = null;
        ExecutionContext ec = null;
        boolean needRescheduled = false;
        boolean cursorFetchMode = false;
        int errorCode = -1;
        try {
            getConnection(schema);

            if (conn == null || conn.isClosed()) {
                logger.warn("connection has been closed");
                return CompletableFuture.completedFuture(false);
            }

            CobarServer.getInstance().getServerExecutor().initTraceStats(traceId);
            prepareExecutionContext(conn.getExecutionContext());
            if (!DynamicConfig.getInstance().enableExtremePerformance()) {
                conn.getExecutionContext().setTestMode(EagleeyeTestHintParser.parseHint(sql));
            } else {
                conn.getExecutionContext().setTestMode(false);
            }

            conn.getExecutionContext().setTestMode(EagleeyeTestHintParser.parseHint(sql));
            stmt = (TPreparedStatement) conn.prepareStatement(sql);

            ec = this.conn.getExecutionContext();
            // 将权限添加到SQL上下文中从prepareExecutionContext拆出来，依赖于conn
            if (!ec.isExecutingPreparedStmt()) {
                updatePrivilegeContext();
            } else {
                ec.setPrivilegeContext(ec.getPreparedStmtCache().getPrivilegeContext());
            }
            ec.setLoadDataContext(dataContext);
            ec.setUserSql(true);
            beforeExecution();

            // Set cursor-fetch mode.
            if (isCursorFetchMode(handler)) {
                cursorFetchMode = true;
            }

            // set params to prepare statement if exist
            fillParams(stmt, params, ec.isExecutingPreparedStmt());

            // 可能里边会执行多个sql，有多个返回结果
            boolean existMoreResults;

            // 第一个sql操作是query则返回true, 不是或没有返回结果就返回false
            boolean isQuery = stmt.execute();

            // 检查第一个查询结果
            if (!isQuery) {
                int affectRows = stmt.getUpdateCount();
                existMoreResults = affectRows >= 0;
            } else {
                existMoreResults = true;
            }

            while (existMoreResults) {
                /**
                 Stores the result of ROW_COUNT() function.

                 ROW_COUNT() function is a MySQL extention, but we try to keep it
                 similar to ROW_COUNT member of the GET DIAGNOSTICS stack of the SQL
                 standard (see SQL99, part 2, search for ROW_COUNT). It's value is
                 implementation defined for anything except INSERT, DELETE, UPDATE.

                 ROW_COUNT is assigned according to the following rules:

                 - In my_ok():
                 - for DML statements: to the number of affected rows;
                 - for DDL statements: to 0.

                 - In my_eof(): to -1 to indicate that there was a result set.

                 We derive this semantics from the JDBC specification, where int
                 java.sql.Statement.getUpdateCount() is defined to (sic) "return the
                 current result as an update count; if the result is a ResultSet
                 object or there are no more results, -1 is returned".

                 - In my_error(): to -1 to be compatible with the MySQL C API and
                 MySQL ODBC driver.

                 - For SIGNAL statements: to 0 per WL#2110 specification (see also
                 sql_signal.cc comment). Zero is used since that's the "default"
                 value of ROW_COUNT in the Diagnostics Area.
                 */
                if (!isQuery) {
                    int affectRows = stmt.getUpdateCount();
                    handler.sendUpdateResult(affectRows);
                    rowCount.set(affectRows);
                    conn.setFoundRows(0);
                    conn.setAffectedRows(affectRows);
                    EncdbRuleSpreader.maySpreadEncRules(ec);//encdb
                } else {
                    rs = stmt.getResultSet();
                    handler.sendSelectResult(rs, rowCount,
                        ec.getParamManager().getLong(ConnectionParams.SQL_SELECT_LIMIT));
                    conn.setFoundRows(rowCount.get());
                    conn.setAffectedRows(-1);
                }

                // 检查后边是否还有其它的查询结果, 同时会暗中关闭上一个resultSet
                isQuery = stmt.getMoreResults();
                if (!isQuery) {
                    int affectRows = stmt.getUpdateCount();
                    existMoreResults = affectRows >= 0;
                } else {
                    existMoreResults = true;
                }

                if (existMoreResults) {
                    handler.sendPacketEnd(true);
                }
            }
        } catch (Throwable e) {
            // In some cases, the current thread is interrupted due to some reason.
            // The actual reason is indicated by futureCancelErrorCode, so we should
            // first set the actual exception here.
            exception = getRealException(e, futureCancelErrorCode);

            // And then, ensure the error code is valid.
            if (!ErrorCode.match(exception.getMessage())) {
                errorCode = ERR_SERVER.getCode();
                exception = new TddlRuntimeException(ERR_SERVER, exception, exception.getMessage());
            } else {
                errorCode = getErrorCode(exception);
            }

        } finally {
            try {
                if (exception instanceof CclRescheduleException) {
                    ((CclRescheduleException) exception).getRescheduleCallback().apply(this);
                    needRescheduled = true;
                }
                if (rescheduled) {
                    // May CCL reschedule or switchover reschedule.
                    needRescheduled =
                        needRescheduled && ec != null && ((ec.getCclContext() != null && ec.getCclContext()
                            .isReschedule()) || (rescheduleTask != null && rescheduleTask.isSwitchoverReschedule()));
                    if (needRescheduled) {
                        rescheduleParam =
                            RescheduleParam.builder().sql(sql).params(params).handler(handler).dataContext(dataContext)
                                .build();
                    }
                } else {
                    CclManager.getService().end(ec);
                }
            } catch (Throwable e) {
                logger.error("Failed to call ccl end.", e);
            }

            try {
                if (rs != null) {
                    // 异常需要带入结果集游标，比如用于MPP的任务的早停
                    if (exception != null && rs instanceof TResultSet) {
                        ((TResultSet) rs).close(exception);
                    } else {
                        // For cursor-fetch mode, do not close the result set if it is successfully executed.
                        // This is because the result set will be cached, and closed when the statement is closed
                        // or the frontend connection is closed.
                        if (!(cursorFetchMode && exception == null)) {
                            rs.close();
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("Failed to close ResultSet", e);
            }

            if (stmt != null) {
                try {
                    if (cursorFetchMode) {
                        // For cursor-fetch mode, do not close the result set.
                        stmt.close(true, false);
                    } else {
                        stmt.close();
                    }
                } catch (Exception e) {
                    logger.error("Failed to close TStatement", e);
                }
            }
        }

        if (null != ec && null != conn.getTrx() && ec.isFlashbackArea()) {
            // Clear session variables here.
            conn.getTrx().clearFlashbackArea();
        }

        if (null != ec && null != conn.getTrx() && ec.isAsOfCrossDdl()) {
            // Clear session variables here.
            conn.getTrx().clearAsOfCrossDdl();
        }

        if (null != ec && null != conn.getTrx()) {
            conn.getTrx().updateStatisticsWhenStatementFinished(rowCount);
        }

        if (autocommit && null != conn) {
            if (exception == null) {
                try {
                    conn.commit();
                    recoverTxIsolation();
                } catch (Throwable ex) {
                    exception = ex;
                }
            } else if (conn != null && conn.getTrx() instanceof ReadOnlyTsoTransaction) {
                try {
                    conn.commit();
                } catch (Throwable ex) {
                    //ignore
                }
            }
        }

        synchronized (mdlContextLock) {
            if (mdlContext != null) {
                if (autocommit && exception != null) {
                    // Release mdl on autocommit transaction with execution err
                    mdlContext.releaseTransactionalLocks(txId);
                }

                if (autocommit && exception != null && null != ec && ec.getTransaction() != null) {
                    // Release mdl on autocommit transaction with execution
                    // err
                    mdlContext.releaseTransactionalLocks(ec.getTransaction().getId());
                }
            }
        }

        try {
            CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);
            CobarServer.getInstance().getServerExecutor().waitByTraceId(traceId);
        } catch (Throwable ex) {
            logger.error("Interrupted unexpectedly for " + (ec != null ? ec.getTraceId() : "null"), ex);
        }

        // For non-autocommit, if the current statement is a DDL, we reset the trxId
        // in case that the next statement reuses the same trxId of DDL
        if (!autocommit && conn != null && conn.isDdlStatement()) {
            this.txId = null;

            // since a DDL implicitly commits the last transaction,
            // we reset the transaction state if it is a begin-commit transaction
            if (this.beginTransaction) {
                this.beginTransaction = false;
                this.setAutocommit(true);
                this.setReadOnly(false);
                this.recoverTxIsolation();
            }
        }

        try {
            conn.tryClose();
            if (isClosed()) {
                conn.close();
            }
        } catch (Throwable e) {
            logger.error("Failed to close TConnection", e);
        }

        try {
            if (conn.getExecutionContext() != null) {
                try {
                    afterExecution(conn.getExecutionContext(), realSql, params, schema,
                        exception != null ? -1 : rowCount.get(), getTrxPolicyForLogging(), statExecCpuNano, exception,
                        errorCode);
                } catch (Throwable ex) {
                    logger.error("Exception happens after execution", ex);
                }
            }
        } catch (Throwable e) {
            logger.error("After execution error", e);
        }

        statementExecuting.set(false);

        if (needRescheduled) {
            return null == ec ? CompletableFuture.completedFuture(false) :
                (null == ec.getRescheduleStmtFuture() ?
                    CompletableFuture.completedFuture(false) : ec.getRescheduleStmtFuture());
        }
        if (exception != null) {
            try {
                // Rollback trx if deadlock occurs.
                // Rollback to savepoint if needed.
                // Release savepoint set before.
                handleErrorForTrx(exception, ec);
            } catch (Throwable ex) {
                logger.error("Failed to handle trx error", ex);
            }
            try {
                // Here must close the connection when rowCount > 0 and exception is not null.
                final boolean fatal = rowCount.get() > 0;
                handler.handleError(exception, realSql, ConfigDataMode.isFastMock() ? false : fatal);
            } catch (Throwable ex) {
                logger.error("Failed to send error message", ex);
            }
            return CompletableFuture.completedFuture(false);
        } else {
            handler.sendPacketEnd(false);
            // Release all savepoints after packet is sent.
            releaseAutoSavepoint();
            return CompletableFuture.completedFuture(true);
        }
    }

    static int getErrorCode(Throwable exception) {
        int errorCode;
        if (ErrorCode.extract(exception.getMessage()) == ErrorCode.ERR_SQL_EXCEED_CCL_EXECUTION_TIME.getCode()) {
            // getErrorCode() return vendor code in default, however we want catch CCL code here
            errorCode = ErrorCode.ERR_SQL_EXCEED_CCL_EXECUTION_TIME.getCode();
        } else if (exception instanceof TddlRuntimeException) {
            errorCode = ((TddlRuntimeException) exception).getErrorCode();
        } else {
            // This path should not being reached.
            errorCode = ErrorCode.extract(exception.getMessage());
        }
        return errorCode;
    }

    static Throwable getRealException(Throwable e, ErrorCode futureCancelErrorCode) {
        Throwable exception;
        if (futureCancelErrorCode == ER_LOCK_DEADLOCK) {
            exception = new TddlRuntimeException(futureCancelErrorCode);
            ((TddlRuntimeException) exception).setSQLState("40001");
        } else if (null != futureCancelErrorCode
            && !(e instanceof CclRescheduleException)) {
            exception = new TddlRuntimeException(futureCancelErrorCode);
        } else {
            exception = e;
        }
        return exception;
    }

    private boolean isTrxFatalAndShouldClose() {
        return null != this.conn && null != this.conn.getExecutionContext()
            && this.conn.getExecutionContext().isEnableCloseConnectionWhenTrxFatal()
            && null != this.conn.getTrx() && null != this.conn.getTrx().getCrucialError();
    }

    private void checkTrxCanCommit() {
        if (null != conn && null != this.conn.getExecutionContext()
            && this.conn.getExecutionContext().isEnableTrxFatalOnAnyError()
            && null != this.conn.getTrx() && null != this.conn.getTrx().getCrucialError()) {
            this.conn.getTrx().checkCanContinue();
        }
    }

    private ResultSet mayMatchEncRule(ResultSet rs, ExecutionContext ec) throws SQLException {
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_ENCDB)) {
            return rs;
        }

        try {

            ExecutionPlan executionPlan = ec.getFinalPlan();

            if (executionPlan.isExplain()) {
                return rs;
            }

            if (executionPlan.getTableSet() != null &&
                !EncdbRuleManager.getInstance().getRuleMatchTree().fastMatch(executionPlan.getTableSet(), schema)) {
                return rs;
            }

            if (!executionPlan.getAst().isA(SqlKind.QUERY)) {
                return rs;
            }

            if (executionPlan.getOriginColumnNames() == null) {
                executionPlan.setOriginColumnNames(CalciteUtils.buildOriginColumnNames(executionPlan.getPlan()));
            }

            if (executionPlan.getOriginColumnNames() == null) {
                return rs;
            }

            PolarAccountInfo accountInfo = ec.getPrivilegeContext().getPolarUserInfo();

            //polardbx_root不受此加密规则的限制
            if (accountInfo.getAccountType().isGod()) {
                return rs;
            }
            PolarAccount account = accountInfo.getAccount();

            EncdbUserPrivilegeManager userPrivilegeManager =
                EncdbRuleManager.getInstance().getUserPrivilegeManager();
            boolean isGlobalRestrictedAccess = false;
            if (userPrivilegeManager != null) {
                if (userPrivilegeManager.isFullAccess(account)) {
                    //全局明文权限，直接返回明文
                    return rs;
                } else if (userPrivilegeManager.isRestrictedAccess(account)) {
                    //全局密文权限，返回密文
                    isGlobalRestrictedAccess = true;
                } else if (InstConfUtil.getOriginVal(ConnectionParams.ENCDB_DEFAULT_ROLE_PRIVILEGE)
                    .equalsIgnoreCase(MsgKeyConstants.FULL_ACCESS)) {
                    //没有全局权限，且默认权限为明文权限，直接返回明文
                    return rs;
                }

            }

            //检查是否有对当前用户匹配的脱敏列
            boolean[] colMaskBitmap = EncdbRuleManager.getInstance().getRuleMatchTree()
                .getColumnMaskBitmap(executionPlan.getOriginColumnNames(), account, isGlobalRestrictedAccess);
            if (colMaskBitmap != null && colMaskBitmap.length != rs.getMetaData().getColumnCount()) {
                logger.warn("result set is inconsistent with column masking bitmap, then give up masking");
                return rs;
            }
            EncdbMaskAlgo[] colMaskAlgoList = null;
            if (colMaskBitmap != null) {
                //找到脱敏列对应的脱敏算法
                colMaskAlgoList = new EncdbMaskAlgo[colMaskBitmap.length];
                for (int i = 0; i < colMaskBitmap.length; i++) {
                    if (colMaskBitmap[i]) {
                        Set<RelColumnOrigin> columnOrigins = executionPlan.getPlan().getCluster().getMetadataQuery()
                            .getColumnOrigins(executionPlan.getPlan(), i);
                        if (columnOrigins != null && columnOrigins.size() == 1) {
                            RelColumnOrigin columnOrigin = columnOrigins.iterator().next();
                            if (columnOrigin != null && !columnOrigin.isDerived()) {
                                String[] colName = executionPlan.getOriginColumnNames().get(i).get(0);
                                EncdbRule rule = EncdbRuleManager.getInstance().getRuleMatchTree()
                                    .matchRule(colName[0], colName[1], colName[2]);
                                if (rule == null || rule.getMaskAlgo() == null) {
                                    colMaskAlgoList[i] =
                                        new EncdbMaskAlgo(EncdbMaskType.MASK_DATA_TYPE, new Object[0]);
                                } else {
                                    colMaskAlgoList[i] = rule.getMaskAlgo();
                                }
                            } else {
                                colMaskAlgoList[i] = new EncdbMaskAlgo(EncdbMaskType.MASK_DATA_TYPE, new Object[0]);
                            }
                        } else {
                            colMaskAlgoList[i] = new EncdbMaskAlgo(EncdbMaskType.MASK_DATA_TYPE, new Object[0]);
                        }
                    }
                }
            }

            //检查是否有对当前用户匹配的加密列
            boolean[] colEncBitmap = EncdbRuleManager.getInstance().getRuleMatchTree()
                .getColumnEncBitmap(executionPlan.getOriginColumnNames(), account, isGlobalRestrictedAccess);
            if (colEncBitmap != null && colEncBitmap.length != rs.getMetaData().getColumnCount()) {
                logger.warn("result set is inconsistent with column encryption bitmap, then give up encryption");
                return rs;
            }

            if (colEncBitmap != null || colMaskAlgoList != null) {
                return new EncdbResultSet(rs, colEncBitmap, colMaskAlgoList, ec,
                    colEncBitmap == null ? null : getEncdbSessionState());
            }
        } catch (Throwable e) {
            logger.error("[ENCDB] Failed to match enc rule", e);
            EventLogger.log(EventType.ENCDB_ERROR, "Failed to match enc rule");
        }

        return rs;
    }

    private void releaseAutoSavepoint() {
        if (null != conn && null != conn.getTrx()) {
            conn.getTrx().releaseAutoSavepoint(traceId);
        }
    }

    private Integer getStatementId(QueryResultHandler handler) {
        return handler instanceof ServerResultHandler ? ((ServerResultHandler) handler).getStatementId() : null;
    }

    private boolean isCursorFetchMode(QueryResultHandler handler) {
        return handler instanceof ServerResultHandler && ((ServerResultHandler) handler).isCursorFetch();
    }

    private String getTrxPolicyForLogging() {
        if (conn.getTrxPolicyForLogging() != null) {
            return conn.getTrxPolicyForLogging().toString();
        } else if (trxPolicy != null) {
            return trxPolicy.toString();
        }
        return null;
    }

    public synchronized PreStmtMetaData getPreparedMetaData(PreparedStmtCache preparedStmtCache, List<?> params)
        throws SQLException {
        ExecutionContext ec = null;
        try {
            ByteString sql = preparedStmtCache.getStmt().getRawSql();
            SchemaConfig schema = getSchemaConfig();
            if (schema == null) {
                throw new SQLException("Unknown database '" + this.schema + "'");
            }
            getConnection(schema);
            TConnection tddlConnection = this.getTddlConnection();
            if (tddlConnection == null) {
                boolean ret = initTddlConnection();
                if (!ret) {
                    return null;
                }
                tddlConnection = this.getTddlConnection();
                if (tddlConnection == null) {
                    throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR,
                        "prepare refuse connection");
                }
            }
            ec = tddlConnection.getExecutionContext();
            prepareExecutionContext(ec);
            tddlConnection.initExecutionContextForPrepare(sql);
            /**
             * 将权限添加到SQL上下文中 因为prepare不走innerExecute,所以针对prepare需要获得内部的目标sql并检查是否有权限执行，
             * 而且有了prepare的检查就不需要检查execute了。
             */
            updatePrivilegeContextForPrepare(preparedStmtCache);
            ec.setPrivilegeMode(this.isPrivilegeMode());

            ec.setExtraCmds(new MergeHashMap<>(tddlConnection.getDs().getConnectionProperties()));
            return MetaConverter.getMetaData(preparedStmtCache, params, ec);
        } catch (RuntimeException ex) {
            logger.error(ex);
            throw ex;
        } finally {
            // Make sure memory pool is released after query
            if (ec != null) {
                try {
                    ec.clearAllMemoryPool();
                } catch (Throwable e) {
                    logger.warn("Failed to release memory of current request", e);
                }
            }
        }
    }

    private void prepareExecutionContext(ExecutionContext ec) {
        ec.setClientIp(host);
        ec.setPort(port);
        ec.setConnId(id);
        ec.setUser(user);
        ec.setRescheduled(rescheduled);
        ec.setTxId(txId);
        ec.setTraceId(traceId);
        ec.setPhySqlId(phySqlId);
        ec.setSchemaName(schema);
        ec.setClientFoundRows(clientFoundRows());
        ec.setTxIsolation(this.txIsolation);
        ec.setPartitionHint(partitionHint);
        if (StringUtils.isNotEmpty(partitionHint)) {
            ec.setUseHint(true);
        }

        buildMDC();

        ec.renewMemoryPoolHolder();
    }

    private boolean clientFoundRows() {
        return (clientFlags & Capabilities.CLIENT_FOUND_ROWS) > 0;
    }

    private void beforeExecution() {

        if (this.conn == null) {
            return;
        }
        ExecutionContext ec = this.conn.getExecutionContext();
        if (ec != null) {
            /* enable profile for stmt */
            ec.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, RuntimeStat.getMetricLevel());

            ec.setOnlyUseTmpTblPool(false);
            /*
             * all the sql from ServerConn must NOT be internal
             * system sql of drds
             */
            ec.setInternalSystemSql(false);
            ec.setUsingPhySqlCache(true);
            ec.setStartTime(System.nanoTime());
            ec.setPrivilegeMode(isPrivilegeMode());
            ec.setMdcConnString(buildMDCCache);

            ec.setLogicalSqlStartTimeInMs(getSqlBeginTimestamp());
            ec.setLogicalSqlStartTime(getLastActiveTime());
            ec.setSqlId(sqlId);

            //深翻页优化
            ec.setDeepPageCacheMap(deepPageCacheMap);

        }
    }

    @Override
    public void sqlAuditLoginSuccess(String user) {
        if (this.user == null) {
            this.user = user;
        }
        if (this.schema == null) {
            this.schema = "polardbx";
        }
        this.updateMDC();
        // recordSql should handle loginAction specially
        this.isLoginAction = true;
        LogUtils.recordSql(this, ByteString.from("Login Success"), true);
        this.isLoginAction = false;
    }

    public boolean isLoginAction() {
        return this.isLoginAction;
    }

    @Override
    public void sqlAuditLoginFail(String user) {
        if (this.user == null) {
            this.user = user;
        }
        if (this.schema == null) {
            this.schema = "polardbx";
        }
        this.updateMDC();
        this.isLoginAction = true;
        LogUtils.recordSql(this, ByteString.from("Login Failed"), true);
        this.isLoginAction = false;
    }

    @Override
    public void sqlAuditLogout() {
        this.updateMDC();
        this.isLoginAction = true;
        long logoutStartTimeNano = System.nanoTime();
        LogUtils.recordSql(this, ByteString.from("Logout"), true, 0, logoutStartTimeNano);
        this.isLoginAction = false;
    }

    void afterExecution(ExecutionContext ec, ByteString sql, List<Pair<Integer, ParameterContext>> params,
                        SchemaConfig schema, long lastAffectedRows, String trxPolicy, long startExecTimeNano,
                        Throwable exception, int errorCode) {
        //do not profile and log the query when the query will be rescheduled.
        if (exception instanceof CclRescheduleException) {
            return;
        }

        if (DynamicConfig.getInstance().enableExtremePerformance()) {
            try {
                this.conn.feedBackPlan(exception);
            } catch (Throwable t) {
                logger.warn("feedBackPlan error !", t);
            }
            this.sqlSample = null;
            getStatistics().request++;
            this.conn.newExecutionContext();
            return;
        }
        RelOptCost cost = CBOUtil.getCost(ec);
        WorkloadType workloadType = WorkloadUtil.getWorkloadType(ec);
        ExplainResult explainResult = ec.getExplain();
        if (explainResult != null && !explainResult.explainMode.isAnalyze()) {
            workloadType = WorkloadType.TP;
        }

        long finishFetchSqlRsNano = System.nanoTime();
        double executeTimeMs = (finishFetchSqlRsNano - this.conn.getLastExecutionBeginNano()) / 1e6;

        Integer baselineId = null;
        Integer planId = null;

        // Do plan evolution
        if (ec.getParamManager().getBoolean(ConnectionParams.ENABLE_SPM)) {
            Pair<Integer, Integer> baselineIdAndPlanId =
                this.conn.updatePlanManagementInfo(
                    this.conn.getLastExecutionBeginUnixTime(),
                    executeTimeMs,
                    ec,
                    exception);
            if (baselineIdAndPlanId != null) {
                baselineId = baselineIdAndPlanId.getKey();
                planId = baselineIdAndPlanId.getValue();
            }
        }

        // always record planId for columnar plan
        planId = (planId == null && ec.isUseColumnar()) ? PlanInfo.genPlanId(ec.getFinalPlan()) : planId;

        //create plan id for deep page optimizer
        planId = (planId == null && ec.getPlanSource() == PlanManager.PLAN_SOURCE.DEEP_PAGE_OPTIMIZER) ?
            PlanInfo.genPlanId(ec.getFinalPlan()) : planId;

        boolean sqlMetricEnabled = ExecUtils.isSQLMetricEnabled(ec);
        RuntimeStatistics runtimeStat = (RuntimeStatistics) ec.getRuntimeStatistics();
        LogUtils.QueryMetrics metrics =
            getQueryMetrics(ec, startExecTimeNano, sqlMetricEnabled, runtimeStat, errorCode);

        /*
         * Record sql info to sql.log
         */
        boolean slowRecorded = recordSlowSqlState(user, host, String.valueOf(port), schema, sql, lastAffectedRows);
        LogUtils.recordSql(this, "", sql, params, trxPolicy, lastAffectedRows, finishFetchSqlRsNano, metrics,
            baselineId, planId, workloadType, cost, ec.getExecuteMode(), slowRecorded, ec.getSqlType(),
            this.conn.getTrx() != null ? this.conn.getTrx().getStartTimeInNano() : 0);

        cclMetrics(ec, metrics);
        statCpu(ec, finishFetchSqlRsNano, sqlMetricEnabled, runtimeStat);
        feedBackWorkload(ec, workloadType, finishFetchSqlRsNano,
            ec.getFinalPlan() != null && ec.getFinalPlan().isHitCache());

        //statement summary
        try {
            summaryStmt(ec, lastAffectedRows, getSqlBeginTimestamp(), finishFetchSqlRsNano, runtimeStat, workloadType,
                ec.getExecuteMode(), ec.getSqlType(),
                this.conn.getTrx() != null ? this.conn.getTrx().getStartTimeInMs() : 0, slowRecorded);
        } catch (Throwable throwable) {
            logger.error("Failed to summary stmt ", throwable);
        }

        // Misc.
        this.setLastActiveTime(finishFetchSqlRsNano);
        this.sqlSample = null;

        //stat for gsi usage
        try {
            GsiStatisticsManager statisticsManager = GsiStatisticsManager.getInstance();
            if (ec.getFinalPlan() != null && statisticsManager.enableGsiStatisticsCollection()) {
                ExecutionPlan plan = ec.getFinalPlan();
                Map<String, Set<String>> referencedGsiNames = plan.getReferencedGsiNames();
                for (Map.Entry<String, Set<String>> entry : referencedGsiNames.entrySet()) {
                    String schemaName = entry.getKey();
                    for (String gsi : entry.getValue()) {
                        statisticsManager.increaseGsiVisitFrequency(schemaName, gsi);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("Failed to stat gsi usage ", e);
        }

        // Record user SQL metadata to AgentSession for NL2SQL context
        if (exception == null && this.agentSession != null) {
            SqlType sqlType = ec.getSqlType();
            if (sqlType == SqlType.SELECT || sqlType == SqlType.SELECT_FOR_UPDATE
                || sqlType == SqlType.INSERT || sqlType == SqlType.UPDATE
                || sqlType == SqlType.DELETE || sqlType == SqlType.INSERT_INTO_SELECT) {
                try {
                    JSONObject msg = new JSONObject();
                    msg.put("role", "user");
                    msg.put("content", "[SQL] " + sql.toString()
                        + "\n结果: " + lastAffectedRows + " 行, 耗时 "
                        + String.format("%.1f", executeTimeMs) + "ms");
                    this.agentSession.addMessage(msg);
                } catch (Throwable t) {
                    logger.debug("Failed to record SQL to AgentSession", t);
                }
            }
        }

        if (conn != null) {
            this.conn.newExecutionContext();
        }
    }

    private void statCpu(ExecutionContext ec, long finishFetchSqlRsNano, boolean sqlMetricEnabled,
                         RuntimeStatistics runtimeStat) {
        if (sqlMetricEnabled && runtimeStat != null) {
            try {
                long finishClearEnvAndLogSqlNano = System.nanoTime();
                long clearEnvAndLogSqlTimeCost = finishClearEnvAndLogSqlNano - finishFetchSqlRsNano;
                CpuStat cpuStat = runtimeStat.getSqlWholeStageCpuStat();
                cpuStat.addCpuStatItem(CpuStatAttribute.CpuStatAttr.CLOSE_AND_LOG, clearEnvAndLogSqlTimeCost);
            } catch (Throwable e) {
                logger.warn("Failed to add cpu stat ", e);
            }

            SqlType sqlType = ec.getSqlType();
            if (sqlType != null && !SqlTypeUtils.isShowSqlType(sqlType)) {
                //Don't record the last sql on load data mode.
                this.lastSqlRunTimeStat = runtimeStat;
            }
        }
    }

    private void summaryStmt(ExecutionContext ec, long affectRow, long sqlBeginTs, long endTimeNano,
                             RuntimeStatistics runtimeStatistics, WorkloadType workloadType, ExecutorMode mode,
                             SqlType sqlType, long transStartTimeMs, boolean slowRecorded) {
        // check if the statement summary is enabled.
        boolean stmtSummaryEnabled =
            StatementSummaryManager.getInstance().getConfig().getEnableStmtSummary() > 0 ? true : false;
        if (stmtSummaryEnabled && ec != null && ec.getExtraCmds()
            .containsKey(ConnectionProperties.ENABLE_STATEMENTS_SUMMARY)) {
            stmtSummaryEnabled = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_STATEMENTS_SUMMARY);
        }
        if (!stmtSummaryEnabled) {
            if (this.execInfo != null) {
                this.execInfo = null;
            }
            return;
        }

        if (this.execInfo == null) {
            this.execInfo = new ExecInfo();
            //check if this is internal user.
            if (PolarPrivUtil.POLAR_ROOT.equalsIgnoreCase(user)) {
                execInfo.setInternalUser(true);
            }
        }

        boolean recordInternal = StatementSummaryManager.getInstance().getConfig().getRecordIntervalStatement() > 0;
        if ((!recordInternal) && execInfo.isInternalUser()) {
            return;
        }
        execInfo.setSlow(slowRecorded);
        execInfo.setTimestamp(sqlBeginTs);
        execInfo.setSchema(schema);
        execInfo.setSqlType(String.valueOf(sqlType));
        //clear pre
        execInfo.setPrevTemplateHash(0);
        execInfo.setPrevTemplateText(null);

        //transaction time

        execInfo.setTransTime(0);
        boolean hasTrans = false;
        if (transStartTimeMs != 0) {
            long transTime = System.currentTimeMillis() - transStartTimeMs;
            execInfo.setTransTime(transTime);
            hasTrans = true;
        }

        if (SqlType.COMMIT.equals(sqlType) || SqlType.ROLLBACK.equals(sqlType)) {
            if (!hasTrans) {
                return;
            }
            execInfo.setPrevTemplateText(execInfo.getTemplateText());
            execInfo.setPrevTemplateHash(execInfo.getTemplateHash());
            execInfo.setSampleSql(sqlType.toString());
            execInfo.setTemplateText(sqlType.toString());
            execInfo.setTemplateHash(sqlType.toString().hashCode());
            execInfo.setPlanHash(0);
        } else {
            execInfo.setSampleSql(sqlSample);
            execInfo.setTemplateText(null);
            execInfo.setTemplateHash(0);
            execInfo.setPlanHash(0);
        }

        if (ec != null) {
            ExecutionPlan plan = ec.getFinalPlan();
            if (plan != null && plan.getCacheKey() != null) {
                String templateSql = plan.getCacheKey().getParameterizedSql();
                execInfo.setTemplateText(templateSql);
                if (templateSql != null) {
                    execInfo.setTemplateHash(templateSql.hashCode());
                }
                if (plan.getPlan() != null) {
                    PlanInfo planInfo = PlannerContext.getPlannerContext(plan.getPlan()).getPlanInfo();
                    if (planInfo != null) {
                        execInfo.setPlanHash(planInfo.getId());
                    }
                }
            }
        }

        if (execInfo.getTemplateHash() == 0) {
            //ignore some statements like show、create
            return;
        }

        boolean success = affectRow >= 0;
        //errorCount
        // error count
        execInfo.setErrorCount(success ? 0 : 1);
        //affected rows
        execInfo.setAffectedRows(success ? affectRow : 0);
        //response time, unit: microsecond
        execInfo.setResponseTime((endTimeNano - getLastActiveTime()) / 1000);
        execInfo.setParseTime(0);
        execInfo.setExecPlanCpuTime(0);
        execInfo.setPhyFetchRows(0);
        execInfo.setPhysicalExecCount(0);
        execInfo.setPhysicalTime(0);
        if (runtimeStatistics != null) {
            RuntimeStatistics.Metrics storedMetrics = runtimeStatistics.getStoredMetrics();
            if (storedMetrics == null) {
                storedMetrics = runtimeStatistics.toMetrics();
            }
            execInfo.setParseTime(storedMetrics.sqlToPlanTc / 1000);
            if (storedMetrics.execPlanTc > 0) {
                execInfo.setExecPlanCpuTime(storedMetrics.execPlanTc / 1000);
            }
            execInfo.setPhyFetchRows(storedMetrics.fetchedRows);
            execInfo.setPhysicalExecCount(storedMetrics.phySqlCount);
            execInfo.setPhysicalTime(storedMetrics.phyCpuTc / 1000);
            runtimeStatistics.setStoredMetrics(null);
        }
        execInfo.setWorkloadType(null);
        execInfo.setExecuteMode(null);
        if (workloadType != null) {
            execInfo.setWorkloadType(String.valueOf(workloadType));
        }
        if (mode != null) {
            execInfo.setExecuteMode(String.valueOf(mode));
        }
        execInfo.setSampleTraceId(traceId);
        try {
            StatementSummaryManager.getInstance().summaryStmt(execInfo);
        } catch (Throwable throwable) {
            new TddlNestableRuntimeException(throwable, "Failed to summaryStmt ExecInfo");
        }
    }

    private LogUtils.QueryMetrics getQueryMetrics(ExecutionContext ec, long startExecTimeNano, boolean sqlMetricEnabled,
                                                  RuntimeStatistics runtimeStat, int errorCode) {
        // Collect the metrics info after query was executed
        LogUtils.QueryMetrics metrics = new LogUtils.QueryMetrics();
        metrics.hasMultiShards = ec.hasScanWholeTable();
        metrics.hasUnpushedJoin = ec.hasUnpushedJoin();
        metrics.hasTempTable = ec.hasTempTable();
        metrics.optimizedWithReturning = ec.isOptimizedWithReturning();
        metrics.xplanIndex = XplanStat.getXplanIndex(ec.getXplanStat());
        metrics.examinedRowCount = XplanStat.getExaminedRowCount(ec.getXplanStat());

        metrics.errorCode = errorCode;
        metrics.planType = ec.getPlanType();
        if (ec != null && ec.getCclContext() != null && ec.getCclContext().getCclRule() != null) {
            metrics.isDryRun = ec.getCclContext().isDryRun();
        }

        if (runtimeStat != null) {
            runtimeStat.setFinishExecution(true);
            if (sqlMetricEnabled) {
                metrics.runTimeStat = runtimeStat;
                runtimeStat.collectThreadCpu(ThreadCpuStatUtil.getThreadCpuTimeNano() - startExecTimeNano);
            }
            metrics.sqlTemplateId = ec.getSqlTemplateId();
        }
        return metrics;
    }

    private void feedBackWorkload(ExecutionContext ec, WorkloadType workloadType, long finishFetchSqlRsNano,
                                  boolean isHitCache) {
        if (WorkloadUtil.isApWorkload(workloadType)) {
            getStatistics().apLoad++;
        } else {
            getStatistics().tpLoad++;
            if (!isHitCache) {
                OptimizerAlertUtil.tpAlert(ec, (finishFetchSqlRsNano - getLastActiveTime()) / 1e6);
            }
        }
        if (ExecUtils.isMppMode(ec)) {
            getStatistics().cluster++;
        } else {
            getStatistics().local++;
        }
        getStatistics().timeCost += (finishFetchSqlRsNano - getLastActiveTime()) / 1000;
        getStatistics().request++;
    }

    private void cclMetrics(ExecutionContext ec, LogUtils.QueryMetrics metrics) {
        CclContext cclContext = ec.getCclContext();
        if (isRescheduled() && !rescheduleTask.isSwitchoverReschedule()) {
            metrics.cclMetric =
                new CclMetric(CclMetric.RESCHEDULE, rescheduleTask.getWaitEndTs() - rescheduleTask.getWaitStartTs(),
                    rescheduleTask.getCclRuleInfo().getCclRuleRecord().id, rescheduleTask.isHitCache());
        } else if (cclContext != null) {
            metrics.cclMetric = cclContext.getMetric();
        }

        if (metrics.cclMetric != null) {
            switch (metrics.cclMetric.getType()) {
            case CclMetric.KILLED:
                getStatistics().cclKill++;
                break;
            case CclMetric.RUN:
                getStatistics().cclRun++;
                break;
            case CclMetric.WAIT:
                getStatistics().cclWait++;
                break;
            case CclMetric.RESCHEDULE:
                getStatistics().cclReschedule++;
                getStatistics().cclWait++;
                break;
            case CclMetric.WAIT_K:
                getStatistics().cclWaitKill++;
                break;
            }
        }
    }

    public void setSqlSample(String sqlSample) {
        this.sqlSample = sqlSample;
    }

    /**
     * 记录sql执行信息
     */
    private boolean recordSlowSqlState(String user, String host, String port, SchemaConfig schema, ByteString sqlBytes,
                                       long affectRow) {
        boolean slowRecorded = false;
        SQLRecorder sqlRecorder = schema.getDataSource().getRecorder();
        long endTime = System.nanoTime() / 1000_000;
        long startTime = getLastActiveTime() / 1000_000;
        try {
            long time = endTime - startTime;
            long threshold = conn.getExecutionContext().getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);

            if (threshold > 0 && time > threshold) {

                slowRecorded = true;
                this.getStatistics().slowRequest++;

                String sql;
                long length = sqlRecorder.getMaxSizeThreshold();
                if (sqlBytes.length() > length) {
                    sql = sqlBytes.substring(0, (int) length) + "...";
                } else {
                    sql = sqlBytes.toString();
                }
                sql = LogUtils.maskSecretPassword(sql);

                if (!PolarPrivUtil.isPolarxRootUser(user)) {
                    // show slow 不记录polardbx_root账号的慢SQL
                    sqlRecorder.recordSql(sql, startTime, user, host, port, this.schema, affectRow, endTime,
                        traceId);
                }
            }
        } catch (Throwable e) {
            logger.error("error when record sql", e);
        }
        return slowRecorded;
    }

    /**
     * 提交事务
     */
    public synchronized boolean commit(boolean hasMore) {
        countTransactionQpsIfNeeded();

        if (isTrxFatalAndShouldClose()) {
            close();
        }

        checkTrxCanCommit();

        long transStartTime = getTransactionBeginTime();

        if (conn != null && conn.getExecutionContext() != null) {
            conn.getExecutionContext().setLogicalSqlStartTime(getLastActiveTime());
            conn.getExecutionContext().setLogicalSqlStartTimeInMs(getSqlBeginTimestamp());
        }

        try {
            innerCommit();

            ByteBufferHolder buffer = this.allocate();
            PacketOutputProxyFactory.getInstance().createProxy(this, buffer)
                .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        } catch (Exception ex) {
            this.handleError(ERR_HANDLE_DATA, ex, "commit", false);
            return false;
        }
        stmtSummaryTransaction(SqlType.COMMIT, transStartTime);
        return true;
    }

    /**
     * 提交事务
     */
    public synchronized void innerCommit() throws SQLException {
        this.txId = null;

        if (this.conn != null) {
            conn.commit();
        }

        if (this.beginTransaction) {
            this.beginTransaction = false;
            this.setAutocommit(true);
            this.setReadOnly(false);
            this.recoverTxIsolation();
        }
    }

    /**
     * 回滚事务
     */
    public synchronized boolean rollback(boolean hasMore) {
        countTransactionQpsIfNeeded();

        long transStartTime = getTransactionBeginTime();

        if (conn != null && conn.getExecutionContext() != null) {
            conn.getExecutionContext().setLogicalSqlStartTime(getLastActiveTime());
            conn.getExecutionContext().setLogicalSqlStartTimeInMs(getSqlBeginTimestamp());
        }

        try {
            innerRollback();

            ByteBufferHolder buffer = this.allocate();
            PacketOutputProxyFactory.getInstance().createProxy(this, buffer)
                .writeArrayAsPacket(hasMore ? OkPacket.OK_WITH_MORE : OkPacket.OK);
        } catch (Exception ex) {
            this.handleError(ERR_HANDLE_DATA, ex, "rollback", false);
            return false;
        }
        stmtSummaryTransaction(SqlType.ROLLBACK, transStartTime);
        return true;
    }

    private long getTransactionBeginTime() {
        if (this.conn != null) {
            if (this.conn.getTrx() != null) {
                return this.conn.getTrx().getStartTimeInMs();
            }
        }
        return getSqlBeginTimestamp();
    }

    public long getTrxStartTimeNano() {
        if (this.conn != null && this.conn.getTrx() != null) {
            return this.conn.getTrx().getStartTimeInNano();
        }
        return 0;
    }

    private void stmtSummaryTransaction(SqlType sqlType, long transactionBeginTime) {
        try {
            summaryStmt(null, 0, getSqlBeginTimestamp(), System.nanoTime(), null, null, null, sqlType,
                transactionBeginTime, false);
        } catch (Throwable throwable) {
            logger.error(String.format("Failed to record %s statement", sqlType.toString()), throwable);
        }
    }

    /**
     * Rollback a transaction
     */
    public void innerRollback() throws SQLException {
        this.txId = null;

        if (this.conn != null) {
            conn.rollback();
        }

        if (this.beginTransaction) {
            this.beginTransaction = false;
            this.setAutocommit(true);
            this.setReadOnly(false);
            this.recoverTxIsolation();
        }
    }

    /**
     * The connection must close when the switch is disabled, the client is a
     * known unsafe Connector/J version, or the packet boundary is not clean.
     */
    private boolean shouldCloseConnectionAfterPartialResult(boolean partialResultEnabled, boolean clientSafe,
                                                            PacketOutputState packetOutputState) {
        return !partialResultEnabled
            || !clientSafe
            || packetOutputState != PacketOutputState.CLEAN;
    }

    @Override
    public void handleError(ErrorCode errCode, Throwable t) {
        handleError(errCode, t, null, false);
    }

    public void handleError(ErrorCode errCode, Throwable t, String sql, boolean fatal) {
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();

        String message = t.getMessage();
        if (!ErrorCode.match(message)) {
            if (t instanceof NullPointerException) {
                message = "unknown NPE";
                // NPE (NullPointerException) may not have been handled correctly,
                // to avoid the exception being swallowed, print the exception stack trace again.
            } else {
                message = t.getMessage();
            }
        }

        String sqlState = null;
        int errorCode = errCode.getCode();// 输出给用户看的errorCode
        if (errCode == ERR_HANDLE_DATA) {
            List<Throwable> ths = ExceptionUtils.getThrowableList(t);
            for (int i = ths.size() - 1; i >= 0; i--) {
                Throwable e = ths.get(i);
                if (e instanceof SQLIntegrityConstraintViolationException) {
                    if (schema != null) {
                        schema.getDataSource().getStatistics().integrityConstraintViolationErrorCount++;
                    }
                }
            }

            // 获取一下原始异常中带的sql errorcode
            // TODO remove TddlException
            if (t instanceof TddlException) {
                int code = ((TddlException) t).getErrorCode();
                sqlState = ((TddlException) t).getSQLState();
                if (code > 0) {
                    errorCode = ((TddlException) t).getErrorCode();
                }
            } else if (t instanceof TddlNestableRuntimeException) {
                int code = ((TddlNestableRuntimeException) t).getErrorCode();
                sqlState = ((TddlNestableRuntimeException) t).getSQLState();
                if (code > 0) {
                    errorCode = ((TddlNestableRuntimeException) t).getErrorCode();
                }
            }
        }

        // 根据异常类型和信息，选择日志输出级别。
        if (this.schema == null) {
            // DNS探测日志的schema一定为null,正常应用访问日志schema绝大多数情况下不为null
            if (logger.isInfoEnabled()) {
                buildMDC();
                logger.info(toString(), t);
            }
        } else {
            // use origin exception t to judge log level
            logError(logger, errCode, sql, t, schema);
        }

        if (conn != null
            && conn.getExecutionContext().getParamManager() != null
            && conn.getExecutionContext().getParamManager().getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE)
            && GeneralUtil.isNotEmpty(DynamicConfig.getInstance().getErrorCodeMapping())
            && DynamicConfig.getInstance().getErrorCodeMapping().containsKey(errorCode)) {
            errorCode = DynamicConfig.getInstance().getErrorCodeMapping().get(errorCode);
        }

        if (DynamicConfig.getInstance().isEnableConsistentErrorCode()) {
            Pair<Integer, String> result = ExceptionUtils.transformErrorCode(errorCode, message);
            if (result != null) {
                errorCode = result.getKey();
                sqlState = result.getValue();
            }
        }

        final PacketOutputState packetOutputState = getPacketOutputState();
        final boolean hasPacketOutput =
            errCode == ERR_HANDLE_DATA && packetOutputState != PacketOutputState.NONE;
        final boolean hasPartialResult = errCode == ERR_HANDLE_DATA && fatal;
        final boolean partialResultErrEnabled =
            hasPartialResult && DynamicConfig.getInstance().enableErrPacketAfterPartialResult();
        final boolean clientSafe = hasPartialResult && isClientSafeForErrAfterPartialResult();
        final boolean closeAfterError = hasPartialResult
            && shouldCloseConnectionAfterPartialResult(partialResultErrEnabled, clientSafe, packetOutputState);

        if (hasPartialResult && logger.isDebugEnabled()) {
            buildMDC();
            logger.debug("Partial-result error decision: enableErrPacketAfterPartialResult="
                + partialResultErrEnabled
                + ", clientName=" + JSON.toJSONString(getClientName())
                + ", clientVersion=" + JSON.toJSONString(getClientVersion())
                + ", packetOutputState=" + packetOutputState
                + ", clientSafe=" + clientSafe
                + ", closeAfterError=" + closeAfterError);
        }

        switch (errCode) {
        case ERR_HANDLE_DATA:
            if ((hasPartialResult || hasPacketOutput) && packetOutputState != PacketOutputState.CLEAN) {
                // Never append an ERR packet to an incomplete MySQL packet.
                close();
                return;
            }

            // packetBegin() may register a raw proxy while the output state is still NONE.
            // Always detach or flush it after the incomplete-packet guard before creating the ERR proxy.
            try {
                // Flush complete pending packets before allocating the ERR packet ID.
                flushActivePacketOutputProxy();
            } catch (Throwable flushError) {
                logger.warn("Failed to flush pending packet output before sending ERR", flushError);
                close();
                return;
            }

            writeErrMessage(errorCode, sqlState, message == null ? t.getClass().getSimpleName() : message);
            if (closeAfterError) {
                // The ERR packet is valid, but the instance or client is not eligible for connection reuse.
                close();
                return;
            }
            break;
        default:
            close();
        }
    }

    protected void logError(Logger logger, ErrorCode errCode, String sql, Throwable ex, SchemaConfig schema) {
        if (ex instanceof EOFException || ex instanceof ClosedChannelException) {
            if (logger.isInfoEnabled()) {
                buildMDC();
                logger.info(ex);
            }
        } else if (isConnectionReset(ex)) {
            if (logger.isInfoEnabled()) {
                buildMDC();
                logger.info(ex);
            }
        } else if (isTableNotFount(ex) || isColumnNotFount(ex)) {
            if (logger.isDebugEnabled()) {
                buildMDC();
                logger.debug(ex);
            }
        } else if (isMySQLIntegrityConstraintViolationException(ex)) {
            if (logger.isDebugEnabled()) {
                buildMDC();
                logger.debug(ex);
            }
        } else if (isCClError(ex)) {
            if (logger.isInfoEnabled()) {
                buildMDC();
                logger.info(ex);
            }
        } else {
            if (logger.isWarnEnabled()) {
                buildMDC();
                if (schema != null) {
                    schema.getDataSource().getStatistics().errorCount++;
                }
                logger.warn("[ERROR-CODE: " + errCode + "][" + this.traceId + "] SQL: " + sql, ex);
            }
        }
    }

    public void handleErrorForTrx(Throwable t, ExecutionContext ec) {
        // Handle specific errors.
        if (t.getMessage().contains("Unknown system variable 'innodb_mark_distributed'")) {
            // Maybe old version DN is switched to be leader, turn off cts option.
            EventLogger.log(EventType.TRX_ERR,
                "Error: Unknown system variable 'innodb_mark_distributed', "
                    + "set ENABLE_XA_CTS and ENABLE_AUTO_COMMIT_CTS to false");
            XATsoTransaction.disable();
        }

        // Handle error for trx.
        if (null != this.conn && null != this.conn.getTrx()) {
            final ITransaction trx = this.conn.getTrx();
            if (isDeadLockException(t, schema)) {
                // Handle deadlock error.
                // Prevent this transaction from committing.
                trx.setCrucialError(ER_LOCK_DEADLOCK, t.getMessage());

                // Rollback this trx.
                try {
                    innerRollback();
                } catch (SQLException exception) {
                    logger.warn("rollback failed when deadlock found", exception);
                }
            } else if (null != ec && ec.isEnableTrxFatalOnAnyError()) {
                if (t instanceof TddlRuntimeException
                    && ((TddlRuntimeException) t).getErrorCode() == ERR_TRANS_FATAL_CANNOT_CONTINUE.getCode()) {
                    // ignore ERR_TRANS_FATAL_CANNOT_CONTINUE
                } else {
                    logger.warn("Set fatal error for trx, caused by " + t.getMessage());
                    trx.setCrucialError(ERR_TRANS_FATAL_CANNOT_CONTINUE, t.getMessage());
                }
            } else {
                // Handle other errors.
                try {
                    trx.handleStatementError(t, traceId);
                } catch (Throwable throwable) {
                    // In case that some unexpected errors occur when handling statement errors,
                    // forbid this trx from committing.
                    trx.setCrucialError(ERR_TRANS_ROLLBACK_STATEMENT_FAIL, throwable.getMessage());
                    logger.warn(throwable.getMessage());
                }
            }
        }
    }

    static boolean isDeadLockException(Throwable t, String schema) {
        if (t instanceof TddlNestableRuntimeException && vendorErrorIs((TddlNestableRuntimeException) t,
            SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)) {
            // A local deadlock causes this exception
            Optional.ofNullable(OptimizerContext.getTransStat(schema))
                .ifPresent(s -> s.countLocalDeadlock.incrementAndGet());
            return true;
        }

        if (t instanceof TddlRuntimeException && (((TddlRuntimeException) t).getErrorCodeType()
            .equals(ERR_TRANS_DEADLOCK) || ((TddlRuntimeException) t).getErrorCodeType().equals(ER_LOCK_DEADLOCK))) {
            // A global/MDL deadlock causes this exception
            return true;
        }

        return false;
    }

    @Override
    protected void closeConfirm() {
        //notify ccl to response this killed query if it is waiting for the rule.
        if (this.conn != null) {
            ExecutionContext executionContext = this.conn.getExecutionContext();
            if (executionContext != null) {
                CclContext cclContext = executionContext.getCclContext();
                if (cclContext != null) {
                    cclContext.getThread().interrupt();
                }
            }
            cancelRescheduleTask(ERR_HANDLE_DATA, false);
        }
        this.getStatistics().activeConnection.decrementAndGet();
    }

    @Override
    public synchronized boolean reschedule(Function<CclRuleInfo<RescheduleTask>, Boolean> function) {
        final RescheduleTask currentRescheduleTask = this.rescheduleTask;
        if (currentRescheduleTask == null) {
            return false;
        }
        currentRescheduleTask.setWaitEndTs(System.currentTimeMillis());
        boolean active = currentRescheduleTask.getActivation().compareAndSet(false, true);
        if (active) {
            final Future<?> prev = this.executingFuture.get();
            final Future<?> task = processor.getHandler().submit(this.schema, null, () -> {
                final ExecutionContext ec = getExecutionContext();
                try {
                    if (!rescheduled || this.isClosed()) {
                        throw new TddlNestableRuntimeException("can not be rescheduled.");
                    }
                    if (rescheduleParam != null) {
                        int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
                        ByteString sql = rescheduleParam.sql;
                        sqlSample = SecretMaskUtils.mask(sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length())));
                        final Future<Boolean> future =
                            innerExecute(rescheduleParam.sql, rescheduleParam.params, rescheduleParam.handler,
                                rescheduleParam.dataContext);
                        if (ec != null && future.isDone()) {
                            ec.getRescheduleStmtDone().set(future.get());
                        } // or new reschedule occurs
                    }

                } catch (Throwable e) {
                    if (ec != null) {
                        ec.getRescheduleStmtDone().set(false);
                    }
                    handleError(ERR_HANDLE_DATA, e);
                } finally {
                    if (function != null) {
                        function.apply(currentRescheduleTask.getCclRuleInfo());
                    }
                    // only clear when same task(will reschedule again when switchover)
                    if (currentRescheduleTask == this.rescheduleTask) {
                        setRescheduled(false, null);
                    }
                }
            });
            // set if last one or single stmt
            if (!currentRescheduleTask.isMultiStmtHasMore()) {
                this.executingFuture.compareAndSet(prev, task);
            }
        }
        return active;
    }

    @Override
    protected void waitReschedule() {
        final ExecutionContext ec = getExecutionContext();
        if (ec != null) {
            final FutureTask<Boolean> future = ec.getRescheduleStmtFuture();
            if (future != null) {
                try {
                    future.get();
                    logger.info("wait rescheduled query in previous statement");
                } catch (Throwable e) {
                    logger.error("waitReschedule error", e);
                }
            }
        }
    }

    @Override
    protected void showCloseInfo(Throwable t) {
        logger.warn("Connection force closed. " + traceId, t);
    }

    @Override
    public boolean isRescheduled() {
        return this.rescheduled;
    }

    @Override
    public void setRescheduled(boolean rescheduled, RescheduleTask rescheduleTask) {
        this.rescheduled = rescheduled;
        this.rescheduleTask = rescheduleTask;
        if (!this.rescheduled) {
            this.rescheduleParam = null;
        }
        // set/clear switchover reschedule time
        final ExecutionContext ec = this.getExecutionContext();
        if (null == rescheduleTask) {
            if (ec != null) {
                ec.setFirstSwitchoverWaitTime(0);
                ec.getSwitchoverPerfCollections().clear();
                final FutureTask<Boolean> future = ec.getRescheduleStmtFuture();
                if (future != null) {
                    future.run();
                }
                ec.setRescheduleStmtFuture(null);
            }
        } else if (rescheduleTask.isSwitchoverReschedule() && ec != null && 0 == ec.getFirstSwitchoverWaitTime()) {
            ec.setFirstSwitchoverWaitTime(rescheduleTask.getWaitStartTs());
            ec.getSwitchoverPerfCollections().clear();
            ec.getRescheduleStmtDone().set(false);
            ec.setRescheduleStmtFuture(new FutureTask<>(() -> ec.getRescheduleStmtDone().get()));
        }
    }

    @Override
    public void handleRescheduleError(Throwable throwable) {
        try {
            rescheduleParam.handler.handleError(throwable, rescheduleParam.sql, false);
        } catch (Throwable ex) {
            logger.error("Failed to send error message", ex);
        }
    }

    public CdcRpcClient.CdcRpcStreamingProxy getProxy() {
        return proxy;
    }

    private void cancelRescheduleTask(ErrorCode errorCode, boolean killQuery) {
        final RescheduleTask currentRescheduleTask = this.rescheduleTask;
        if (currentRescheduleTask != null) {
            boolean active = currentRescheduleTask.getActivation().compareAndSet(false, true);
            if (active) {
                if (!currentRescheduleTask.isSwitchoverReschedule()) {
                    currentRescheduleTask.getCclRuleInfo().getStayCount().decrementAndGet();
                }
                if (killQuery) {
                    handleError(errorCode, new TddlNestableRuntimeException("The query is cancelled."));
                }
                setRescheduled(false, null);
            }
        }
    }

    @Override
    public boolean isCanReschedule() {
        return !isClosed.get();
    }

    @Override
    public RescheduleTask getRescheduleTask() {
        return rescheduleTask;
    }

    public void setPreparedStmt(PreparedStmtCache preparedStmtCache) {
        if (this.conn != null && this.conn.getExecutionContext() != null) {
            this.conn.getExecutionContext().setIsExecutingPreparedStmt(true);
            this.conn.getExecutionContext().setPreparedStmtCache(preparedStmtCache);
        }
    }

    public void clearPreparedStmt() {
        if (this.conn != null && this.conn.getExecutionContext() != null) {
            this.conn.getExecutionContext().setIsExecutingPreparedStmt(false);
            this.conn.getExecutionContext().setPreparedStmtCache(null);
        }
    }

    public void removePreparedCache(String stmtId, boolean isServerPrepare) {
        if (isServerPrepare) {
            this.smForPrepare.delete(stmtId);
        } else {
            this.smForQuery.delete(stmtId);
        }
    }

    public void resetPreparedParams(String stmtId, boolean isServerPrepare) {
        PreparedStmtCache cache = null;
        if (isServerPrepare) {
            cache = this.smForPrepare.find(stmtId);
        } else {
            cache = this.smForQuery.find(stmtId);
        }
        if (cache != null) {
            cache.getStmt().clearLongDataParams();
        }
    }

    public void setPartitionHint(String partitionHint) {
        this.partitionHint = partitionHint;
    }

    public String getPartitionHint() {
        return partitionHint;
    }

    /**
     * Cancel a query with retry.
     * The killing action should be executed in KillExecutor thread-pool
     * The retry waiting should be submitted to TimerExecutor
     */
    private class CancelQueryTask implements Runnable {

        /**
         * If a user sends a kill query command, or the memory is running out, or a slow sql is killed automatically,
         * the errorCode is {@link com.alibaba.polardbx.common.exception.code.ErrorCode#ERR_USER_CANCELED}.
         * If a deadlock occurs and causes this kill,
         * the errorCode is {@link com.alibaba.polardbx.common.exception.code.ErrorCode#ER_LOCK_DEADLOCK}.
         * If an MDL is preempted and causes this kill,
         * the errorCode is {@link com.alibaba.polardbx.common.exception.code.ErrorCode#ERR_TRANS_PREEMPTED_BY_DDL}.
         */
        private final com.alibaba.polardbx.common.exception.code.ErrorCode errorCode;
        private final int retried;
        private final int retryLimit;
        // Only kill query with this traceId.
        private final String traceIdToBeKilled;

        public CancelQueryTask(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode, int retried,
                               int retryLimit) {
            this.errorCode = errorCode;
            this.retried = retried;
            this.retryLimit = retryLimit;
            this.traceIdToBeKilled = ServerConnection.this.getTraceId();
        }

        public CancelQueryTask(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode, int retried,
                               int retryLimit, String traceIdToBeKilled) {
            this.errorCode = errorCode;
            this.retried = retried;
            this.retryLimit = retryLimit;
            this.traceIdToBeKilled = traceIdToBeKilled;
        }

        @Override
        public void run() {
            if (!StringUtils.equals(ServerConnection.this.getTraceId(), this.traceIdToBeKilled)) {
                // Maybe the query to be killed has already finishes executing,
                // just return and do not kill the current executing query.
                return;
            }

            if (retried >= retryLimit || !statementExecuting.get()) {
                return;
            }

            try {
                doCancel();
            } catch (Exception e) {
                logger.warn("Error when cancel query: ", e);
            }
            retryWithBackoff();
        }

        private void doCancel() throws SQLException {
            // First, set the futureCancelErrorCode,
            // which will be used for error handling in innerExecute().
            futureCancelErrorCode = this.errorCode;

            // Then, kill the trx in the connection,
            // during which all physical connections are killed.
            // If a physical connection is still executing a statement,
            // innerExecute() is waiting for this statement to be finished.
            // If such a physical connection is killed,
            // it throws an interrupted exception to innerExecute(),
            // and innerExecute() will handle the futureCancelErrorCode set before
            // instead of the interrupted exception.
            if (conn != null) {
                conn.kill();
            }

            Future f = executingFuture.get();
            if (f != null) {
                // Finally, cancel(interrupt) the thread in which innerExecute() is executing.
                f.cancel(true);
            }
        }

        private void retryWithBackoff() {
            long delay = 5 + (long) this.retried * 10;
            CobarServer.getInstance().getTimerTaskExecutor().schedule(() -> {
                CancelQueryTask task =
                    new CancelQueryTask(this.errorCode, this.retried + 1, retryLimit, traceIdToBeKilled);
                CobarServer.getInstance().getKillExecutor().execute(task);
            }, delay, TimeUnit.MILLISECONDS);
        }
    }

    public void cancelQuery(com.alibaba.polardbx.common.exception.code.ErrorCode errorCode) {
        // Lightweight cancel: always set the flag so NL2SQL agent loop can detect it
        // without requiring statementExecuting=true or going through CancelQueryTask.
        this.queryCancelled = true;

        if (loadDataHandler != null) {
            loadDataHandler.close();
            loadDataHandler = null;
        }
        if (this.statementExecuting.get()) {
            CobarServer.getInstance().getKillExecutor().execute(() -> {
                buildMDC();
                CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);

                (new CancelQueryTask(errorCode, 0, 10)).run();
            });
        }
        if (rescheduled) {
            cancelRescheduleTask(ERR_HANDLE_DATA, true);
        }
    }

    @Override
    public boolean close() {
        if (super.close()) {
            final TConnection conn = this.conn;
            if (loadDataHandler != null) {
                loadDataHandler.close();
                loadDataHandler = null;
            }
            if (agentSession != null) {
                agentSession.clear();
                agentSession = null;
            }
            if (proxy != null) {
                proxy.cancel();
                this.proxy = null;
            }
            BinlogDumpMetricsManager.getInstance().unregister(id);
            clearPreparedStatement();
            if (this.statementExecuting.get()) {
                if (conn.isDdlStatement()) {
                    logger.warn("Connection Killed By Client While Executing DDL");
                    if (conn.getExecutionContext().getDdlContext() != null) {
                        conn.getExecutionContext().getDdlContext().setClientConnectionResetAsTrue();
                    }
                    DbTopologyManager.killDdlQueryByConnId(conn.getId());
                    return true;
                }
                CobarServer.getInstance().getKillExecutor().execute(() -> {
                    buildMDC();
                    CobarServer.getInstance().getServerExecutor().closeByTraceId(traceId);
                    try {
                        int retry = 0;
                        do {
                            try {
                                if (conn != null) {
                                    conn.kill();
                                }

                                Future f = executingFuture.get();
                                if (f != null) {
                                    f.cancel(true);
                                    futureCancelErrorCode = null;
                                }
                            } catch (Exception ex) {
                                logger.warn("error when kill", ex);
                            }

                            try {
                                Thread.sleep(5 + retry * 10);
                            } catch (InterruptedException e) {
                                logger.warn(e);
                            }

                        } while (statementExecuting.get() && ++retry < 10);
                        try {
                            if (conn != null) {
                                conn.close();
                            }
                        } catch (Exception ex) {
                            logger.warn("error when kill close", ex);
                        }

                        if (retry >= 10) {
                            logger.error("KILL Failed, retry: " + retry);
                        } else {
                            logger.warn("Connection Killed");
                        }
                    } finally {
                        releaseLockAndRemoveMdlContext();
                    }
                });
            } else {
                if (conn != null) {
                    CobarServer.getInstance().getKillExecutor().execute(() -> {
                        buildMDC();
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            logger.warn("error when close", e);
                        } finally {
                            releaseLockAndRemoveMdlContext();
                        }
                    });
                } else {
                    releaseLockAndRemoveMdlContext();
                }
            }

            releaseAllLocksOnConnClose();
            // Clear all cached resultSet when frontend connection is closed.
            clearCachedResultSet();
            if (cursorFetchMemoryPool != null) {
                cursorFetchMemoryPool.destroy();
                cursorFetchMemoryPool = null;
            }
            ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.SESSION.clean(getExecutionContext());

            return true;
        }
        return false;
    }

    private void clearCachedResultSet() {
        if (null != getResultSetMap()) {
            for (ResultSetCachedObj resultSetCachedObj : getResultSetMap().values()) {
                if (null != resultSetCachedObj) {
                    resultSetCachedObj.close();
                }
            }
            getResultSetMap().clear();
        }
    }

    public void closeCacheResultSet(int statementId) {
        final ResultSetCachedObj rs = getResultSetMap().remove(statementId);
        if (null != rs) {
            rs.close();
        }
    }

    private void clearPreparedStatement() {
        if (smForQuery != null) {
            smForQuery.clear();
        }
        if (smForPrepare != null) {
            smForPrepare.clear();
        }
    }

    /**
     * release Transactional locks and remove context
     */
    private void releaseLockAndRemoveMdlContext() {
        synchronized (mdlContextLock) {
            // remove mdl context
            if (null != this.mdlContext) {
                this.mdlContext.releaseAllTransactionalLocks();
                MdlManager.removeContext(this.mdlContext);
                this.mdlContext = null;
            }
        }
    }

    private void releaseAllLocksOnConnClose() {
        try {
            LockingFunctionManager.getInstance().releaseAllLocksOnConnClose(this.getId());
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Nonnull
    public MatrixStatistics getStatistics() {
        if (this.stats == null) {
            return MatrixStatistics.EMPTY;
        }
        return this.stats;
    }

    public synchronized boolean initOptimizerContext() {
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + getSchema() + "'");
            return false;
        }
        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            ds.init();
        }
        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

        return true;
    }

    void getConnection(SchemaConfig schema) throws SQLException {
        if (this.isClosed()) {
            return;
        }

        if (this.conn == null || !this.conn.getSchema().equalsIgnoreCase(schema.getName())) { // double-check

            TDataSource ds = schema.getDataSource();
            if (!ds.isInited()) {
                ds.init();
            }

            OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());

            TConnection oldConn = this.conn;
            /**
             * oldConn should be null here !
             */
            if (null != oldConn) {
                /**
                 * In mysql, its transaction can be commit
                 *  on different database after using 'use xxx_db',
                 * such as:
                 * <pre>
                 *      use d1;
                 *      begin;
                 *      delete from t1;--trx1
                 *      use d2;
                 *      commit;
                 * the transaction trx1 can be commit or rollback after using database d2.
                 * </pre>
                 *
                 * But in PolarDB-X, after use a new database by using `use xxx_db`;
                 * the trx of the last db will be auto rollback and closed.
                 */
                // Clear trx of old conn
                oldConn.close();
            }

            if (InstConfUtil.getBool(ConnectionParams.ENABLE_USERNAME_PUSHDOWN)) {
                this.serverVariables.put("polarx_slow_query_pushdown_username", user);
            }
            this.conn = (TConnection) ds.getConnection();
            this.conn.setMdlContext(this.mdlContext);
            this.conn.setServerVariables(this.serverVariables);
            this.conn.setGlobalServerVariables(this.globalServerVariables);
            this.conn.setUserDefVariables(this.userDefVariables);
            this.conn.setExtraServerVariables(this.extraServerVariables);
            this.conn.setConnectionVariables(this.connectionVariables);
            this.conn.setUser(this.getUser() + '@' + this.getHost());
            this.conn.setFrontendConnectionInfo(this.getUser() + '@' + this.getHost() + ':' + this.getPort());
            this.conn.setId(this.getId());
            this.conn.setReadOnly(isReadOnly());
            int txIsolation = getTxIsolation();
            this.conn.setTraceId(this.getTraceId());
            this.conn.setClientFoundRows(clientFoundRows());
            if (txIsolation >= 0) {
                this.conn.setTransactionIsolation(txIsolation);
            }
            if (!autocommit) {
                this.conn.setAutoCommit(autocommit);
            }
            this.conn.setShareReadView(this.shareReadView);

            if (this.groupParallelism != null) {
                this.conn.setGroupParallelism(this.groupParallelism);
            }

            if (sqlMode != null) {
                this.conn.setSqlMode(sqlMode);
            }

            if (resultSetCharset != null) {
                this.conn.setEncoding(resultSetCharset);
            }

            if (this.trxPolicy != null) {
                this.conn.setTrxPolicy(this.trxPolicy, false);
            }

            // set time zone for session
            if (this.timeZone != null) {
                this.conn.setTimeZone(this.timeZone);
            }
        } else {
            this.conn.setReadOnly(isReadOnly());
        }
    }

    public TConnection getTddlConnection() {
        return this.conn;
    }

    public ITransactionPolicy getTrxPolicy() {
        return this.trxPolicy;
    }

    public void setTrxPolicy(ITransactionPolicy policy) {
        this.trxPolicy = policy;

        if (this.conn != null) {
            this.conn.setTrxPolicy(policy, true);
        }
    }

    public void setShareReadView(boolean shareReadView) {
        if (shareReadView) {
            ShareReadViewPolicy.checkTxIsolation(txIsolation);
        }
        ShareReadViewPolicy policy = shareReadView ? ShareReadViewPolicy.ON : ShareReadViewPolicy.OFF;
        if (this.conn != null) {
            this.conn.setShareReadView(policy);
        }
        this.shareReadView = policy;
    }

    public void setGroupParallelism(Long groupParallelism) {
        if (this.conn != null) {
            this.conn.setGroupParallelism(groupParallelism);
        }
        this.groupParallelism = groupParallelism;
    }

    public void setBatchInsertPolicy(BatchInsertPolicy policy) {

        if (this.conn != null) {
            this.conn.setBatchInsertPolicy(policy);
        }
    }

    private void fillParams(TPreparedStatement stmt, List<Pair<Integer, ParameterContext>> params,
                            boolean isExecPrepare) throws SQLException {
        if (params == null) {
            return;
        }
        if (isExecPrepare) {
            boolean first = true;
            boolean isBatch = false;
            for (Pair<Integer, ParameterContext> param : params) {
                // resolve batch prepare params
                if (first) {
                    first = false;
                } else if (param.getKey() == 1) {
                    stmt.addBatch();
                    isBatch = true;
                }
                stmt.setParam(param.getKey(), param.getValue());
            }

            if (isBatch) {
                stmt.addBatch();
            }
        } else {
            for (Pair<Integer, ParameterContext> param : params) {
                stmt.setParam(param.getKey(), param.getValue());
            }
        }
    }

    public StatementMap getSmForQuery() {
        return smForQuery;
    }

    public StatementMap getSmForPrepare() {
        return smForPrepare;
    }

    public int getIncStmtId() {
        return stmtId.incrementAndGet();
    }

    /**
     * check whether session-level prepared statement count exceeds limit
     */
    public void checkPreparedStmtCount() throws SQLException {
        int count = smForPrepare.size() + smForQuery.size();
        if (count >= DynamicConfig.getInstance().getMaxSessionPreparedStmtCount()) {
            throw new SQLException(String.format("Can't create more than %s statements in one session "
                + "(current value: %d)", ConnectionProperties.MAX_SESSION_PREPARED_STMT_COUNT, count));
        }
    }

    @Override
    public void fieldList(byte[] data) {
        String db = this.schema;
        if (db == null) {
            writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        }

        // 取得查询语句
        MySQLMessage mm = new MySQLMessage(data);
        mm.position(5);
        String tableName = null;

        try {
            tableName = mm.readStringWithNull(CharsetUtil.getJavaCharset(resultSetCharset));
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + resultSetCharset + "'");
            return;
        }

        String columnPattern = null;

        try {
            columnPattern = mm.readString(CharsetUtil.getJavaCharset(resultSetCharset));
        } catch (UnsupportedEncodingException e) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + resultSetCharset + "'");
            return;
        }

        TDataSource ds = schema.getDataSource();
        if (!ds.isInited()) {
            try {
                ds.init();
            } catch (Throwable e) {
                handleError(ErrorCode.ERR_HANDLE_DATA, e);
            }
        }

        String javaCharset = CharsetUtil.getJavaCharset(resultSetCharset);
        OptimizerContext.setContext(ds.getConfigHolder().getOptimizerContext());
        TableMeta table = ds.getConfigHolder().getOptimizerContext().getLatestSchemaManager().getTable(tableName);
        if (table == null) {
            writeErrMessage(ErrorCode.ER_UNKNOWN_TABLE, "Table '" + this.schema + "." + tableName + "' doesn't exit");
            return;
        }
        ByteBufferHolder buffer = this.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(this, buffer);
        proxy.packetBegin();

        for (ColumnMeta cm : table.getAllColumns()) {
            if (columnPattern != null) {
                Object value = new Like(Arrays.asList(new StringType(), new StringType()), null)
                    .compute(new Object[] {cm.getName(), columnPattern}, null);
                boolean res = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(value));
                if (!res) {
                    continue;
                }
            }

            String fieldName = cm.getOriginColumnName();
            if (fieldName != null && fieldName.toLowerCase().equals(IMPLICIT_COL_NAME)) {
                continue;
            }

            FieldPacket field = new FieldPacket();
            field.catalog = StringUtil.encode_0(FieldPacket.DEFAULT_CATALOG_STR, javaCharset);
            field.orgName = StringUtil.encode_0(cm.getOriginColumnName(), javaCharset);
            field.name = StringUtil.encode_0(cm.getOriginColumnName(), javaCharset);
            field.orgTable = StringUtil.encode_0(cm.getOriginTableName(), javaCharset);
            field.table = StringUtil.encode_0(cm.getOriginTableName(), javaCharset);
            field.db = StringUtil.encode_0(this.schema, javaCharset);
            field.length = cm.getLength();
            field.flags = toFlag(cm);
            field.decimals = (byte) 0;
            field.charsetIndex = resultSetCharsetIndex;

            if (cm.getDataType().getSqlType() != DataType.UNDECIDED_SQL_TYPE) {
                field.type = (byte) (
                    MysqlDefs.javaTypeMysql(MysqlDefs.javaTypeDetect(cm.getDataType().getSqlType(), field.decimals))
                        & 0xff);
            } else {
                field.type = MysqlDefs.FIELD_TYPE_STRING; // 默认设置为string
            }

            field.definition = new byte[] {(byte) 0xfb};
            field.packetId = getNewPacketId();

            proxy = field.write(proxy);
        }

        EOFPacket eof = new EOFPacket();
        eof.packetId = this.getNewPacketId();
        proxy = eof.write(proxy);

        proxy.packetEnd();
    }

    public void begin(boolean readOnly, IsolationLevel level) {
        countTransactionQpsIfNeeded();

        if (level != null) {
            setStmtTxIsolation(level.getCode());
        }
        this.beginTransaction = true;
        this.setReadOnly(readOnly);
        this.setAutocommit(false, true);
    }

    public void begin() {
        begin(false, null);
    }

    /**
     * Count transaction control statement (BEGIN/COMMIT/ROLLBACK) in QPS if enabled.
     */
    private void countTransactionQpsIfNeeded() {
        if (DynamicConfig.getInstance().isEnableTransactionQpsCount()) {
            getStatistics().request++;
        }
    }

    @Override
    protected void addNetInBytes(long bytes) {
        super.addNetInBytes(bytes);
        this.getStatistics().netIn += bytes;
    }

    @Override
    protected void addNetOutBytes(long bytes) {
        super.addNetOutBytes(bytes);
        this.getStatistics().netOut += bytes;
    }

    @Override
    public void addConnectionCount() {
        this.getStatistics().activeConnection.incrementAndGet();
        this.getStatistics().connectionCount.incrementAndGet();
    }

    public AtomicBoolean isStatementExecuting() {
        return statementExecuting;
    }

    /**
     * Check if the current query has been cancelled (via KILL QUERY / Ctrl+C).
     * Used by NL2SQL agent loop to detect cancellation signals that don't close the connection.
     */
    public boolean isQueryCancelled() {
        return queryCancelled || futureCancelErrorCode != null;
    }

    /**
     * Reset the lightweight cancellation flag. Must be called at the start of a new query
     * to ensure a previous cancellation doesn't affect subsequent queries on the same connection.
     */
    public void resetQueryCancelled() {
        this.queryCancelled = false;
    }

    @Override
    public synchronized void setSchema(String schema) {
        if (this.isClosed()) {
            return;
        }
        super.setSchema(schema);
        switchDb(this.schema);
    }

    public SchemaConfig getDroppedSchemaConfigIfExists() {
        if (ConfigDataMode.isPolarDbX()) {
            if (schemaConfig != null && schemaConfig.isDropped()) {
                return schemaConfig;
            }
        }
        return null;
    }

    public SchemaConfig getSchemaConfig() {
        if (schemaConfig == null || schemaConfig.isDropped()) {
            switchDb(this.schema);
        }
        return schemaConfig;
    }

    /**
     * 如果没有TConnection就初始化一个，参考innerExecute的方法
     */
    synchronized public boolean initTddlConnection() {
        if (!CobarServer.getInstance().isOnline()) {
            shutDownError.write(PacketOutputProxyFactory.getInstance().createProxy(this));
            return false;
        }

        // 取得SCHEMA
        String db = this.schema;
        // 取得配置文件
        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return false;
        }

        try {
            buildMDC();
            getConnection(schema);
        } catch (Throwable e) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Initialize failed '" + db + "'");
            return false;
        } finally {
            try {
                if (this.conn != null) {
                    this.conn.tryClose();
                }
            } catch (Throwable e) {
                logger.error("", e);
            }
        }
        return true;
    }

    @Override
    public synchronized boolean prepareLoadInfile(String sql) {

        // 针对非事务的请求进行链接中断
        if (!CobarServer.getInstance().isOnline() && isAutocommit()) {
            shutDownError.write(PacketOutputProxyFactory.getInstance().createProxy(this));
            return false;
        }

        if (isClosed()) {
            return false;
        }

        SchemaConfig schema = getSchemaConfig();
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + this.schema + "'");
            return false;
        }

        statementExecuting.set(true);

        try {
            getConnection(schema);
            if (conn == null) {
                logger.warn("connection has been closed");
                return false;
            }

        } catch (Throwable e) {
            logger.error("Initialize LOAD DATA failed!", e);
            writeErrMessage(ErrorCode.ERR_HANDLE_DATA, "Initialize LOAD DATA failed!");
            return false;
        }

        this.loadDataHandler = new ServerLoadDataHandler(this);
        loadDataHandler.open(sql);
        return true;
    }

    @Override
    public void writeErrMessage(byte id, int errno, String sqlState, String msg) {
        StringBuilder sb = new StringBuilder();
        if (this.traceId != null) {
            sb.append("[").append(traceId).append("]");
        }
        sb.append("[").append(CobarServer.getInstance().getServerAddress()).append("]");

        if (schema != null) {
            sb.append("[").append(schema).append("]");
        }
        sb.append(msg);

        super.writeErrMessage(id, errno, sqlState, sb.toString());

    }

    public String getTraceId() {
        return traceId;
    }

    public Long getTxId() {
        return txId;
    }

    public boolean isReadOnly() {
        if (ConfigDataMode.isFastMock()) {
            return false;
        }
        return this.readOnly;
    }

    public boolean isTxReadOnly() {
        return this.txReadOnly;
    }

    public void setReadOnly(boolean b) {
        this.txReadOnly = b;
        if (!ConfigDataMode.isMasterMode()) {
            return;
        }
        this.readOnly = b;
    }

    public boolean isCurrentUserReadOnlyAccount() {
        return isReadOnlyAccount(getUser());
    }

    /**
     * 判断给定用户是否为只读账号
     */

    public boolean isReadOnlyAccount(String user) {
        if (user == null) {
            return false;
        }

        return StringUtils.endsWith(user.toUpperCase(), "_RO");
    }

    /**
     * 判断给定用户是否为管理员账号, 也称为读写账号: 即具有所有权限点和授权选项的用户. 注意: 一个数据库只能有一个管理员账号.
     */
    @Deprecated
    public boolean isAdministrator(String user) {
        String schema = getSchema();
        if (user == null || schema == null) {
            return false;
        }
        return TStringUtil.equals(user, schema);
    }

    /**
     * Do not call  {@link #isGod()}, {@link #isSuperUser()}, {@link #isSuperUserOrAllPrivileges()}
     * in the core code path when executing SQLs due to performance issues.
     */
    public boolean isGod() {
        updatePrivilegeContext();
        if (null != conn && null != conn.getExecutionContext()) {
            return conn.getExecutionContext().isGod();
        }
        return false;
    }

    public boolean isSuperUser() {
        updatePrivilegeContext();
        if (null != conn && null != conn.getExecutionContext()) {
            return conn.getExecutionContext().isSuperUser();
        }
        return false;
    }

    public boolean isSuperUserOrAllPrivileges() {
        updatePrivilegeContext();
        if (null != conn && null != conn.getExecutionContext()) {
            return conn.getExecutionContext().isSuperUserOrAllPrivileges();
        }
        return false;
    }

    @Override
    public boolean isPrivilegeMode() {
        return true;
    }

    @Override
    public void read() throws IOException {
        if (loadDataHandler != null) {
            if (loadDataHandler.isFull() && loadDataHandler.isBlocked()) {
                //阻塞当前channel
                ((ServerLoadDataHandler) loadDataHandler).disableConnection();
                return;
            }
        }
        super.read();
    }

    public boolean isEnableANSIQuotes() {
        return enableANSIQuotes;
    }

    public void setEnableANSIQuotes(boolean enableANSIQuotes) {
        this.enableANSIQuotes = enableANSIQuotes;
    }

    public String getSqlSample() {
        if (isRescheduled() && rescheduleParam != null && StringUtils.isEmpty(sqlSample)) {
            ByteString sql = rescheduleParam.sql;
            int sqlSimpleMaxLen = CobarServer.getInstance().getConfig().getSystem().getSqlSimpleMaxLen();
            sqlSample = SecretMaskUtils.mask(sql.substring(0, Math.min(sqlSimpleMaxLen, sql.length())));
        }
        return sqlSample;
    }

    public Object getServerVariable(String key) {
        return serverVariables.get(key);
    }

    public long getLastSqlStartTime() {
        return lastSqlStartTime;
    }

    public void setLastSqlStartTime(long lastSqlStartTime) {
        this.lastSqlStartTime = lastSqlStartTime;
    }

    public boolean isSqlMock() {
        return sqlMock;
    }

    public void setSqlMock(boolean sqlMock) {
        this.sqlMock = sqlMock;
    }

    void processMock(ByteString sql) {
        if (mockExecutor == null) {
            mockExecutor = new MockExecutor();
        }
        String plan = mockExecutor.getPlan(sql);

        ArrayResultCursor result = new ArrayResultCursor("mock");
        result.addColumn("plan", DataTypes.StringType);
        result.initMeta();
        if (!StringUtils.equalsIgnoreCase(sql.toString(), "show warnings")) {
            result.addRow(new Object[] {plan});
        }

        try {
            IPacketOutputProxy proxy =
                ResultSetUtil.resultSetToPacket(new TResultSet(result, null), resultSetCharset,
                    this, new AtomicLong(0), null, ResultSetUtil.NO_SQL_SELECT_LIMIT);
            ResultSetUtil.eofToPacket(proxy, this, 2);
        } catch (Exception ex) {
            logger.error("sql mock error", ex);
        }
    }

    public RuntimeStatistics getLastSqlRunTimeStat() {
        return lastSqlRunTimeStat;
    }

    public void setLastSqlRunTimeStat(RuntimeStatistics lastSqlRunTimeStat) {
        this.lastSqlRunTimeStat = lastSqlRunTimeStat;
    }

    public void invalidatePrivCache() {
        this.schemaPrivCache = null;
        this.tablePrivCache = null;
    }

    public void setActiveRoles(ActiveRoles newActiveRoles) {
        Preconditions.checkNotNull(newActiveRoles, "Active roles can't be null!");
        this.activeRoles = newActiveRoles;
    }

    public ConnectionInfo getConnectionInfo() {
        return new ConnectionInfo(getInstanceId(), getUser(), getHost(), getPort(), getSchema(), getTraceId());
    }

    public ActiveRoles getActiveRoles() {
        return activeRoles;
    }

    public QueryResultHandler createResultHandler(boolean hasMore) {
        return new ServerResultHandler(hasMore);
    }

    public synchronized void fetchData(int statementId, int numRows) {
        if (numRows <= 0) {
            writeErrMessage(ErrorCode.ERR_HANDLE_DATA,
                "Fetch data failed: fetch size <= 0 for statement " + statementId);
            return;
        }

        // 1. Get the corresponding result set.
        final ResultSetCachedObj resultSetCachedObj = getResultSetMap().get(statementId);
        if (null == resultSetCachedObj || null == resultSetCachedObj.getResultSet()) {
            writeErrMessage(ErrorCode.ERR_HANDLE_DATA,
                "Fetch data failed: no result set found for statement " + statementId);
            return;
        }

        PreparedStmtCache preparedStmtCache = this.getSmForPrepare().find(String.valueOf(statementId));
        final ServerPreparedResultHandler resultHandler =
            new ServerPreparedResultHandler(false, statementId, traceId, MySQLPacket.READ_ONLY_CURSOR_FLAG,
                preparedStmtCache);
        try {
            // 2. Fetch {numRows} rows of data.
            resultHandler.sendFetchResult(resultSetCachedObj, numRows);
        } catch (Exception e) {
            // If we fail to fetch data, close the result set and clear;
            try {
                resultSetCachedObj.getResultSet().close();
                getResultSetMap().remove(statementId);
            } catch (Throwable t) {
                // Ignore.
                logger.warn("Close cached result set failed, caused by " + t);
            }
            writeErrMessage(ErrorCode.ERR_HANDLE_DATA,
                "Fetch data failed: fetch data from result set failed for statement " + statementId
                    + ". Result set is closed. Caused by " + e.getMessage());
            return;
        }

        // 3. Write an EOF packet and send all packets to client.
        resultHandler.sendPacketEnd(false);
    }

    private synchronized MemoryPool getOrCreateCursorFetchMemoryPool() {
        if (cursorFetchMemoryPool == null) {
            long memoryLimit;
            Object value = this.getConnectionVariables().get(ConnectionProperties.CURSOR_FETCH_CONN_MEMORY_LIMIT);
            if (value == null) {
                // Use global config
                ParamManager paramManager = new ParamManager(schemaConfig.getDataSource().getConnectionProperties());
                value = paramManager.getLong(ConnectionParams.CURSOR_FETCH_CONN_MEMORY_LIMIT);
            }
            memoryLimit = Long.parseLong(String.valueOf(value));
            String poolName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
            cursorFetchMemoryPool = MemoryManager.getInstance().createCursorFetchMemoryPool(poolName, memoryLimit);
        }
        return cursorFetchMemoryPool;
    }

    public synchronized void setTrxLastActiveTime() {
        if (isClosed()) {
            return;
        }

        ITransaction trx = getTrx();
        if (null != trx) {
            trx.setLastActiveTime();
        }

    }

    public void resetTrxLastActiveTime() {
        ITransaction trx = getTrx();
        if (null != trx) {
            trx.resetLastActiveTime();
        }
    }

    private ITransaction getTrx() {
        if (null != conn) {
            return conn.getTrx();
        }
        return null;
    }

    public ExecutionContext getExecutionContext() {
        if (null != conn) {
            return conn.getExecutionContext();
        }
        return null;
    }

    public EncdbSessionState getEncdbSessionState() {
        if (encdbSessionState == null) {
            byte[] nonce = EncdbServer.createNonce();
            byte[] mek = EncdbKeyManager.getInstance().getMek();
            //mek不持久化存储，所以当普通客户端连接时，内存cache mek可能为null
            if (mek == null) {
                if (!InstConfUtil.getBool(ConnectionParams.ENCDB_ENABLE_RANDOM_MEK)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ENCDB, "mek is null");
                }
                //随机生成mek密钥
                mek = new byte[EncdbServer.DEFAULT_MEK_LENGTH];
                ThreadLocalRandom.current().nextBytes(mek);
            }
            byte[] dek = EncdbServer.createDEK(EncdbServer.cipherSuite.getHashAlgo(), mek, nonce,
                EncdbServer.cipherSuite.getSymmAlgo());
            encdbSessionState =
                new EncdbSessionState(EncdbServer.cipherSuite.getHashAlgo(), EncdbServer.cipherSuite.getSymmAlgo(),
                    EncdbServer.ccFlags, dek, nonce);
        }
        return encdbSessionState;
    }

    public void setEncdbSessionState(EncdbSessionState encdbSessionState) {
        this.encdbSessionState = encdbSessionState;
    }

    public Map<PlanCache.CacheKey, DeepPageCache> getDeepPageCacheMap() {
        return deepPageCacheMap;
    }

    public AgentSession getAgentSession() {
        if (agentSession == null) {
            int maxHistory = InstConfUtil.getInt(ConnectionParams.NL2SQL_MAX_HISTORY);
            agentSession = new AgentSession(maxHistory);
        }
        return agentSession;
    }

    class ServerResultHandler implements QueryResultHandler {

        private final boolean hasMore;
        IPacketOutputProxy proxy = null;
        private OkPacket ok = null;
        private final byte flags;
        private final int statementId;
        protected boolean lastRow = false;

        public ServerResultHandler(boolean hasMore) {
            this.hasMore = hasMore;
            this.flags = (byte) 0;
            this.statementId = -1;
        }

        public ServerResultHandler(boolean hasMore, int statementId, byte flags) {
            this.hasMore = hasMore;
            this.flags = flags;
            this.statementId = statementId;
        }

        public boolean isCursorFetch() {
            return (flags & MySQLPacket.READ_ONLY_CURSOR_FLAG) == MySQLPacket.READ_ONLY_CURSOR_FLAG;
        }

        public int getStatementId() {
            return statementId;
        }

        @Override
        public void sendUpdateResult(long affectedRows) {
            ok = new OkPacket();
            ok.packetId = getNewPacketId();
            ok.insertId = conn.getReturnedLastInsertId();
            ok.affectedRows = affectedRows;
            ok.serverStatus = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
            if (!autocommit) {
                ok.serverStatus = MySQLPacket.SERVER_STATUS_IN_TRANS;
            }
            ok.warningCount = conn.getWarningCount();

            // To be compatible with MySQL's behavior, i.e. not display the warning detail.
            if (!ConfigDataMode.isPolarDbX() && !conn.getExecutionContext().getAsyncDDLContext()
                .isAsyncDDLSupported()) {
                if (ok.warningCount != 0) {
                    ok.message = encodeString(conn.getWarningSimpleMessage(), resultSetCharset);
                }
            }
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows, long sqlSelectLimit)
            throws Exception {
            proxy = ResultSetUtil.resultSetToPacket(resultSet, resultSetCharset,
                ServerConnection.this, outAffectedRows, null, sqlSelectLimit);
        }

        @Override
        public void sendPacketEnd(boolean hasMoreResults) {
            hasMoreResults |= hasMore;

            if (proxy != null) {
                // 写最后的eof包
                int statusFlags = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
                if (!autocommit) {
                    statusFlags = MySQLPacket.SERVER_STATUS_IN_TRANS;
                }
                if (hasMoreResults) {
                    statusFlags |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
                }

                if (isCursorFetch()) {
                    if (lastRow) {
                        statusFlags |= MySQLPacket.SERVER_LAST_ROW_SENT;
                    } else {
                        // For cursor-fetch mode, set SERVER_STATUS_CURSOR_EXISTS in EOF packet
                        // if it is not the last data packet.
                        statusFlags |= MySQLPacket.SERVER_STATUS_CURSOR_EXISTS;
                    }
                }

                ResultSetUtil.eofToPacket(proxy, ServerConnection.this, statusFlags);
                proxy = null;

            } else if (ok != null) {
                // 为OK包添加SERVER_MORE_RESULTS_EXISTS标记
                if (hasMoreResults) {
                    ok.serverStatus |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
                }
                ok.write(PacketOutputProxyFactory.getInstance().createProxy(ServerConnection.this));
                ok = null;

            } else {
                throw new AssertionError("illegal state");
            }
        }

        @Override
        public void handleError(Throwable ex, ByteString sql, boolean fatal) {
            if (ex instanceof TddlRuntimeException && ((TddlRuntimeException) ex).getErrorCodeType()
                ==ErrorCode.ERR_NO_DB_ERROR) {
                writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return;
            }
            ServerConnection.this.handleError(ERR_HANDLE_DATA, ex, sql.toString(), fatal);
        }
    }

    private class ServerPreparedResultHandler extends ServerResultHandler {
        final PreparedStmtCache preparedStmtCache;
        final String traceId;

        public ServerPreparedResultHandler(boolean hasMore, int statementId, String traceId, byte flags,
                                           PreparedStmtCache preparedStmtCache) {
            super(hasMore, statementId, flags);
            if (preparedStmtCache == null) {
                throw new IllegalArgumentException("empty prepare stmt cache in result set handler");
            }
            this.traceId = traceId;
            this.preparedStmtCache = preparedStmtCache;
        }

        @Override
        public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows, long sqlSelectLimit)
            throws Exception {
            if (isCursorFetch()) {
                // For cursor-fetch mode.
                // 1. Cache the resultSet.
                int stmtId = this.getStatementId();
                // Close previous result set
                closeCacheResultSet(stmtId);
                final ResultSetCachedObj resultSetCachedObj =
                    new ResultSetCachedObj(resultSet, traceId, getOrCreateCursorFetchMemoryPool(), logger,
                        sqlSelectLimit);
                getResultSetMap().put(stmtId, resultSetCachedObj);
                // 2. Send header packet containing only meta-data.
                proxy = BinaryResultSetUtil.resultSetToHeaderPacket(resultSetCachedObj, ServerConnection.this,
                    preparedStmtCache, outAffectedRows);
            } else {
                proxy =
                    BinaryResultSetUtil.resultSetToPacket(resultSet, resultSetCharset,
                        ServerConnection.this, outAffectedRows, preparedStmtCache, sqlSelectLimit);
            }
        }

        public void sendFetchResult(ResultSetCachedObj resultSetCachedObj, int fetchRows)
            throws SQLException, IllegalAccessException {
            if (!isCursorFetch()) {
                ServerConnection.this.writeErrMessage(ERR_HANDLE_DATA,
                    "Fetching data in not cursor-fetch mode is not permitted!");
                return;
            }

            proxy =
                BinaryResultSetUtil.resultSetToDataPacket(resultSetCachedObj, resultSetCharset, ServerConnection.this,
                    fetchRows,
                    preparedStmtCache);
            lastRow = resultSetCachedObj.isLastRow();
        }
    }

    public Long getGroupParallelism() {
        return groupParallelism;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class RescheduleParam {
        private ByteString sql;
        private List<Pair<Integer, ParameterContext>> params;
        private QueryResultHandler handler;
        private LoadDataContext dataContext;
    }

    @Override
    public void logout() {
        setLastActiveTime(System.nanoTime());
        if (getUser() != null && SqlAuditInterceptor.hasLogoutPerm(getUser())) {
            sqlAuditLogout();
        }
        AuditUtil.logAuditInfo(this, getInstanceId(), getSchema(),
            getUser(), getHost(), getPort(), AuditAction.LOGOUT);
    }

    @Override
    public void logAuditInfo(String instId, String database, String user, String host, int port,
                             AuditAction action) {
        AuditUtil.logAuditInfo(this, instId, database, user, host, port, action);
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getJdbcUrl() {
        return this.jdbcUrl;
    }

    @Override
    public boolean isManagerConnection() {
        return false;
    }
}
