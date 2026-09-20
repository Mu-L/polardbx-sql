package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.atom.utils.EncodingUtils;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.trx.ITimestampOracle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropForeignKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableLock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.OmcAccessor;
import com.alibaba.polardbx.gms.metadb.misc.OmcRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.utils.GeneralUtil.mapToList;
import static com.alibaba.polardbx.common.utils.GeneralUtil.prepareParam;
import static com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor.isAllDnUseXDataSource;

/**
 * @author wumu
 */
public class OmcUtils {
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    public final static int MAX_RETRY = 3;
    //key:jobId, value:<DN, XDataSource>
    private static final Map<Long, Map<String, XDataSource>> dataSourcePool = new ConcurrentHashMap<>();

    public static final String OMC_HINT_TEMPLATE = "/* omc %s.%s.%s */";
    public static final String SHOW_CREATE_SQL = "show create table /* omc */ %s.%s";
    public static final String GET_CONNECTION_ID = "select /* omc %s.%s.%s */ connection_id()";
    public static final String DROP_TABLE_SQL = "drop table /* omc %s.%s.%s */ if exists %s.%s";
    public static final String CREATE_OMC_TABLE_SQL = "create /* omc %s.%s.%s */ table %s.%s like %s.%s";
    public static final String CREATE_SENTRY_TABLE_SQL =
        "create /* omc %s.%s.%s */ table %s.%s (id int auto_increment primary key) engine=innodb comment='omc sentry table of %s'";
    public static final String RENAME_TABLE_SQL = "rename /* omc %s.%s.%s */ table %s.%s to %s.%s, %s.%s to %s.%s";
    public static final String RENAME_TABLE_RECYCLE_BIN = "rename /* omc %s.%s.%s */ table %s.%s to %s.%s";
    public static final String CREATE_RECYCLE_BIN_DB = "create /* omc %s.%s.%s */ database if not exists %s";
    public static final String SET_LOCK_WAIT_TIMEOUT = "set /* omc %s.%s.%s */ session lock_wait_timeout=%s";
    public static final String LOCK_TABLE_SQL = "lock /* omc %s.%s.%s */ tables %s.%s write, %s.%s write, %s.%s write";
    public static final String LOCK_TABLE_FOR_CHECK_SQL = "lock /* omc %s.%s.%s */ tables %s.%s write, %s.%s write";
    public static final String UNLOCK_TABLE_SQL = "unlock /* omc %s.%s.%s */ tables";
    public static final String ROLLBACK_TRANSACTION_SQL = "rollback /* omc %s.%s.%s */";
    public static final String ADD_COLUMN_SQL = "alter /* omc %s.%s.%s */ table %s.%s add column %s, %s";
    public static final String DROP_COLUMN_SQL = "alter /* omc %s.%s.%s */ table %s.%s drop column %s, %s";
    public static final String BLOCK_PURGE_SQL = "select /* omc %s.%s.%s */ 1 from %s.%s limit 1";
    public static final String CHECK_PROCESSLIST_SQL =
        "select /* omc %s.%s.%s */ id from "
            + "information_schema.processlist "
            + "where id != connection_id() "
            + "and %s in (0, id) "
            + "and state like '%s' "
            + "and info like '%s'";
    public static final String SELECT_MAX_PK_SQL = "select /* omc %s.%s.%s */ %s from %s.%s order by %s limit 1";
    public static final String SELECT_SAMPLE_SQL = "select /*+sample_percentage(%s)*/ /* omc %s.%s.%s */ %s from %s.%s";
    public static final String SELECT_NEXT_BOUND_SQL =
        "select /* omc %s.%s.%s */ %s from "
            + "%s.%s "
            + "where %s "
            + "order by %s "
            + "limit 1 "
            + "offset %s";
    public static final String INSERT_SELECT_SQL =
        "insert /* omc %s.%s.%s */ into "
            + "%s.%s "
            + "(%s) "
            + "("
            + "select %s "
            + "from %s.%s "
            + "force index (%s) "
            + "where %s "
            + ")";
    public static final String INSERT_IGNORE_SELECT_SQL =
        "insert /* omc %s.%s.%s */ ignore into "
            + "%s.%s "
            + "(%s) "
            + "("
            + "select %s "
            + "from %s.%s "
            + "force index (%s) "
            + "where %s "
            + ")";
    public static final String REPLACE_SELECT_APPLY_SQL =
        "replace /* omc %s.%s.%s */ into "
            + "%s.%s "
            + "(%s) "
            + "("
            + "select %s "
            + "from %s.%s "
            + "force index (%s) "
            + "where %s"
            + ")";
    public static final String REPLACE_SELECT_APPLY_SQL_FOR_INPLACE_SPLIT =
        "replace /* omc %s.%s.%s */ into "
            + "%s.%s "
            + "(%s) "
            + "("
            + "select %s "
            + "from %s.%s "
            + "force index (%s) "
            + "where %s in (%s) and %s"
            + ")";
    public static final String DELETE_APPLY_SQL =
        "delete /* omc %s.%s.%s */  "
            + "from %s.%s "
            + "where %s in (%s)";
    public static final String DELETE_APPLY_SQL_FORCE_INDEX =
        "delete /* omc %s.%s.%s */ t "
            + "from %s.%s as t "
            + "force index(%s) "
            + "where %s in (%s)";
    public static final String SELECT_HASH_CHECK_SQL = "select /* omc %s.%s.%s */ "
        + "hashcheck(%s) as commonHash, "
        + "hashcheck(%s) as originColumnHash, "
        + "hashcheck(%s) as checkColumnHash "
        + "from %s.%s "
        + "force index(%s) "
        + "where %s ";

    public static final String SELECT_SLOW_CHECK_SQL = "select /* omc %s.%s.%s */ "
        + "%s "
        + "from %s.%s "
        + "force index(%s) "
        + "where %s ";

    public static final String SELECT_PHY_PARTITION_NAMES =
        "SELECT partition_name FROM INFORMATION_SCHEMA.PARTITIONS "
            + "WHERE TABLE_NAME = '%s' and table_schema='%s' and partition_name is not null";

    public static final String SELECT_TABLE_SPACE_ID_57 =
        "SELECT space FROM INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES WHERE NAME = '%s'";

    public static final String SELECT_TABLE_SPACE_ID_80 =
        "SELECT space FROM INFORMATION_SCHEMA.INNODB_TABLESPACES WHERE NAME = '%s'";

    public static final String BATCH_SELECT_TABLE_SPACE_ID_57 =
        "SELECT NAME, space FROM INFORMATION_SCHEMA.INNODB_SYS_TABLESPACES WHERE NAME IN (%s)";

    public static final String BATCH_SELECT_TABLE_SPACE_ID_80 =
        "SELECT NAME, space FROM INFORMATION_SCHEMA.INNODB_TABLESPACES WHERE NAME IN (%s)";

    public static final String CHECK_FOREIGN_KEY =
        "SELECT COUNT(1) FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '%s' and REFERENCED_TABLE_SCHEMA = '%s' and REFERENCED_TABLE_NAME = '%s'";

    public static boolean supportOmc30(ExecutionContext ec) {
        if (ec.getParamManager().getBoolean(ConnectionParams.FORCE_USING_OMC_30)) {
            return true;
        }
        return ec.getParamManager().getBoolean(ConnectionParams.ENABLE_OMC_30) &&
            ChangeSetUtils.isChangeSetProcedure(ec) && ChangeSetUtils.supportChangeSetBackPressure(ec);
    }

    public static boolean supportOmc30(ExecutionContext ec, String schemaName, String tableName) {
        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        final boolean allDnUseXDataSource = isAllDnUseXDataSource(topologyHandler);
        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        return allDnUseXDataSource && supportOmc30(ec) && ChangeSetUtils.supportUseChangeSet(
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN, tableMeta);
    }

    public static int getLockWaitTimeout(ExecutionContext ec) {
        return ec.getParamManager().getInt(ConnectionParams.OMC_LOCK_WAIT_TIMEOUT);
    }

    public static XDataSource initializeDataSource(OmcStorageInfo omcStorageInfo, String name) {
        synchronized (dataSourcePool) {
            Map<String, XDataSource> jobDataSources =
                dataSourcePool.computeIfAbsent(omcStorageInfo.jobId, key -> new ConcurrentHashMap<>());
            final XDataSource clientPool =
                jobDataSources
                    .computeIfAbsent(omcStorageInfo.getDigest(),
                        key -> {
                            SQLRecorderLogger.ddlLogger.info(
                                String.format("HostInfo:%s, name:%s", omcStorageInfo, name));
                            return new XDataSource(omcStorageInfo.host, omcStorageInfo.port, omcStorageInfo.username,
                                omcStorageInfo.password, omcStorageInfo.defaultDb, name);
                        });
            return clientPool;
        }
    }

    public static void destroyDataSources(Long jobId) {
        synchronized (dataSourcePool) {
            if (dataSourcePool.isEmpty()) {
                return;
            }
            String msg = "Begin to destroy omc data sources for job ID: "
                + jobId
                + ". DataSourcePool contains: "
                + dataSourcePool.keySet();
            SQLRecorderLogger.ddlLogger.info(msg);

            if (GeneralUtil.isEmpty(dataSourcePool.get(jobId))) {
                return;
            }
            Map<String, XDataSource> jobDataSources = dataSourcePool.get(jobId);
            boolean allClosed = true;
            Iterator<Map.Entry<String, XDataSource>> iterator = jobDataSources.entrySet().iterator();
            while (iterator.hasNext()) {
                boolean isClosed = false;
                Map.Entry<String, XDataSource> dataSourceEntry = iterator.next();
                try {
                    dataSourceEntry.getValue().close();
                    isClosed = true;
                } catch (Exception e) {
                    allClosed = false;
                    LOG.warn("Failed to close dataSource for key: " + dataSourceEntry.getKey(), e);
                } finally {
                    if (isClosed) {
                        // Remove the entry immediately after attempting to close
                        iterator.remove();
                    }
                }
            }
            // Only remove the entire job from the pool after all data sources have been processed
            if (allClosed) {
                dataSourcePool.remove(jobId);
            }
        }
    }

    public static Connection getPhysicalConnection(OmcStorageInfo omcStorageInfo)
        throws SQLException {
        XDataSource dataSource = initializeDataSource(omcStorageInfo, "OnlineModifyColumn");
        return dataSource.getConnection();
    }

    public static int executeWithNewConn(OmcPhyDdlContext phyDdlContext, String sql) {
        return executeWithNewConn(phyDdlContext.getExecutionContext(), phyDdlContext.getOmcStorageInfo(), sql, null);
    }

    public static int executeWithNewConn(ExecutionContext ec,
                                         OmcStorageInfo omcStorageInfo, String sql) {
        return executeWithNewConn(ec, omcStorageInfo, sql, null);
    }

    /**
     * 使用新的数据库连接执行SQL语句
     *
     * @param ec 执行上下文
     * @param omcStorageInfo 存储信息，用于获取数据库连接
     * @param sql 要执行的SQL语句
     * @param params SQL参数映射，键为参数位置，值为参数上下文
     * @return SQL执行结果
     * @throws RuntimeException 当数据库连接或执行失败时抛出
     */
    public static int executeWithNewConn(ExecutionContext ec,
                                         OmcStorageInfo omcStorageInfo, String sql,
                                         Map<Integer, ParameterContext> params) {
        try (Connection conn = getPhysicalConnection(omcStorageInfo)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.setAutoCommit(true);
            setException(conn);
            setEncoding(conn, ec.getEncoding());
            setServerVariables(conn, ec.getServerVariables());
            setTraceId(conn, ec.getTraceId());
            // 使用连接执行SQL语句
            return executeWithConn(ec, conn, sql, params);
        } catch (SQLException e) {
            LOG.error(String.format("failed to execute on host(%s): %s , Caused by: %s", omcStorageInfo, sql,
                e.getMessage()));
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e, e.getMessage());
        }
    }

    public static int executeWithConn(Connection conn, String sql, Long connId, ExecutionContext ec) {
        LOG.info(String.format("[start] with conn %s: %s", connId, sql));
        return executeWithConn(ec, conn, sql, new HashMap<>());
    }

    /**
     * 使用给定的数据库连接执行SQL语句
     *
     * @param ec 执行上下文
     * @param conn 数据库连接对象
     * @param sql 要执行的SQL语句
     * @param params SQL参数映射，键为参数位置，值为参数上下文对象
     * @return 受影响的行数
     * @throws RuntimeException 当SQL执行失败时抛出包装后的异常
     */
    public static int executeWithConn(ExecutionContext ec, Connection conn, String sql,
                                      Map<Integer, ParameterContext> params) {
        long affectRows = 0;
        byte[] hint = myBuildDRDSTraceCommentBytes(ec);
        // LOG.info(String.format("[start] with new conn: %s", sql));
        try {
            final XConnection xConnection = conn.unwrap(XConnection.class);
            // 根据是否有参数选择使用Statement或PreparedStatement执行SQL
            if (MapUtils.isEmpty(params)) {
                try (XStatement statement = (XStatement) xConnection.createStatement()) {
                    affectRows = statement.executeUpdateX(BytesSql.getBytesSql(sql), hint);
                }
            } else {
                try (XPreparedStatement prepareStatement = (XPreparedStatement) xConnection.prepareStatement(
                    BytesSql.getBytesSql(sql), hint)) {
                    handleParamsMap(params, conn, prepareStatement);
                    affectRows = prepareStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            LOG.error(String.format("failed to execute sql %s , Caused by: %s", sql, e.getMessage()));
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e, e.getMessage());
        }

        // LOG.info(String.format("[finish] with new conn: %s, affectRows: %s", sql, affectRows));
        return (int) affectRows;
    }

    public static List<Map<Integer, ParameterContext>> queryUpperBound(ExecutionContext ec,
                                                                       OmcStorageInfo omcStorageInfo,
                                                                       String sql) {
        return queryWithNewConn(ec, omcStorageInfo, sql, null, true, null);
    }

    public static List<Map<Integer, ParameterContext>> queryWithNewConn(ExecutionContext ec,
                                                                        OmcStorageInfo omcStorageInfo,
                                                                        String sql) {
        return queryWithNewConn(ec, omcStorageInfo, sql, null, false, null);
    }

    /**
     * 使用新连接执行查询操作
     *
     * @param ec 执行上下文
     * @param omcStorageInfo 存储信息，用于获取物理连接
     * @param sql 要执行的SQL查询语句
     * @param params SQL参数映射，键为参数位置，值为参数上下文
     * @param queryUpperBound 是否查询上界值的标志
     * @return 查询结果列表，每个元素为参数上下文的映射
     * @throws RuntimeException 当数据库连接或查询执行失败时抛出
     */
    public static List<Map<Integer, ParameterContext>> queryWithNewConn(ExecutionContext ec,
                                                                        OmcStorageInfo omcStorageInfo,
                                                                        String sql,
                                                                        Map<Integer, ParameterContext> params,
                                                                        boolean queryUpperBound,
                                                                        Long tsoTimestamp) {
        List<Map<Integer, ParameterContext>> result;
        // 建立数据库连接并设置连接属性
        try (Connection conn = getPhysicalConnection(omcStorageInfo)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.setAutoCommit(true);
            setException(conn);
            setEncoding(conn, ec.getEncoding());
            setServerVariables(conn, ec.getServerVariables());
            setTraceId(conn, ec.getTraceId());
            setTsoTimestamp(conn, tsoTimestamp);
            // 执行查询操作
            result = queryWithConn(ec, conn, sql, params, queryUpperBound);
        } catch (SQLException e) {
            LOG.error(String.format("failed to execute on host(%s): %s , Caused by: %s", omcStorageInfo, sql,
                e.getMessage()));
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e, e.getMessage());
        }
        return result;
    }

    /**
     * 使用给定的数据库连接执行SQL查询，并返回结果列表
     *
     * @param ec 执行上下文
     * @param conn 数据库连接对象
     * @param sql 要执行的SQL查询语句
     * @param params SQL查询参数映射，键为参数位置，值为参数上下文
     * @param queryUpperBound 是否查询上界值的标志
     * @return 包含查询结果的参数上下文映射列表
     */
    public static List<Map<Integer, ParameterContext>> queryWithConn(ExecutionContext ec,
                                                                     Connection conn, String sql,
                                                                     Map<Integer, ParameterContext> params,
                                                                     boolean queryUpperBound) {
        List<Map<Integer, ParameterContext>> result;
        byte[] hint = myBuildDRDSTraceCommentBytes(ec);
        try {
            final XConnection xConnection = conn.unwrap(XConnection.class);
            // 处理非参数化的SQL查询
            if (MapUtils.isEmpty(params)) {
                try (XStatement statement = (XStatement) xConnection.createStatement()) {
                    final XResult keyResult = statement.executeQueryX(BytesSql.getBytesSql(sql), hint, null);
                    result = queryUpperBound ? convertUpperBoundWithDefault(keyResult) : buildBatchParam(keyResult);
                }
            } else {
                // 处理参数化的SQL查询
                try (XPreparedStatement prepareStatement = (XPreparedStatement) xConnection.prepareStatement(
                    BytesSql.getBytesSql(sql), hint)) {
                    handleParamsMap(params, conn, prepareStatement);
                    final XResult keyResult = prepareStatement.executeQueryX();
                    result = queryUpperBound ? convertUpperBoundWithDefault(keyResult) : buildBatchParam(keyResult);
                }
            }
        } catch (SQLException e) {
            LOG.error(String.format("failed to execute sql %s , Caused by: %s", sql, e.getMessage()));
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e, e.getMessage());
        }
        // LOG.info(String.format("[finish] with new conn: %s", sql));
        return result;
    }

    /**
     * Execute a physical query through X Protocol stream mode and expose one converted row at a
     * time. The callback must not retain the whole result set. Closing {@link XResultSet} consumes
     * any unread response before the physical connection is returned or discarded.
     */
    public static void queryWithNewConnStreaming(ExecutionContext ec,
                                                 OmcStorageInfo omcStorageInfo,
                                                 String sql,
                                                 Map<Integer, ParameterContext> params,
                                                 Consumer<Map<Integer, ParameterContext>> rowConsumer) {
        try (Connection conn = getPhysicalConnection(omcStorageInfo)) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.setAutoCommit(true);
            setException(conn);
            setEncoding(conn, ec.getEncoding());
            setServerVariables(conn, ec.getServerVariables());
            setTraceId(conn, ec.getTraceId());
            queryWithConnStreaming(ec, conn, sql, params, rowConsumer);
        } catch (SQLException e) {
            LOG.error(String.format("failed to stream query on host(%s): %s, Caused by: %s",
                omcStorageInfo, sql, e.getMessage()));
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e, e.getMessage());
        }
    }

    private static void queryWithConnStreaming(ExecutionContext ec,
                                               Connection conn,
                                               String sql,
                                               Map<Integer, ParameterContext> params,
                                               Consumer<Map<Integer, ParameterContext>> rowConsumer)
        throws SQLException {
        byte[] hint = myBuildDRDSTraceCommentBytes(ec);
        XConnection xConnection = conn.unwrap(XConnection.class);
        xConnection.setStreamMode(true);
        if (MapUtils.isEmpty(params)) {
            try (XStatement statement = (XStatement) xConnection.createStatement()) {
                consumeStreamingResult(
                    statement.executeQueryX(BytesSql.getBytesSql(sql), hint, null), rowConsumer);
            }
        } else {
            try (XPreparedStatement prepareStatement =
                (XPreparedStatement) xConnection.prepareStatement(BytesSql.getBytesSql(sql), hint)) {
                handleParamsMap(params, conn, prepareStatement);
                consumeStreamingResult(prepareStatement.executeQueryX(), rowConsumer);
            }
        }
    }

    private static void consumeStreamingResult(XResult xResult,
                                               Consumer<Map<Integer, ParameterContext>> rowConsumer)
        throws SQLException {
        List<PolarxResultset.ColumnMetaData> columns = xResult.getMetaData();
        try (XResultSet resultSet = new XResultSet(xResult)) {
            while (resultSet.next()) {
                rowConsumer.accept(buildCurrentRow(resultSet, columns));
            }
        }
    }

    public static byte[] myBuildDRDSTraceCommentBytes(ExecutionContext ec) {
        if (!ec.getParamManager().getBoolean(ConnectionParams.OMC_ENABLE_TRACE)) {
            return null;
        }
        return ExecUtils.buildDRDSTraceCommentBytes(ec);
    }

    /**
     * 获取元信息，会自动设置 information_schema_stats_expiry = 0
     *
     * @param omcStorageInfo 存储信息
     * @param sql 要执行的SQL查询语句
     * @return 查询结果
     */
    public static List<List<Object>> fetchInformation(OmcStorageInfo omcStorageInfo, String sql) {
        String setSql = "set information_schema_stats_expiry = 0";
        try (Connection conn = getPhysicalConnection(omcStorageInfo)) {
            setException(conn);
            List<List<Object>> result = new ArrayList<>();
            try (Statement stmt = conn.createStatement()) {
                if (InstanceVersion.isMYSQL80()) {
                    stmt.executeUpdate(setSql);
                }
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    int columns = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= columns; i++) {
                            row.add(rs.getObject(i));
                        }
                        result.add(row);
                    }
                }
                return result;
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("failed to execute on host(%s) , Caused by: %s", omcStorageInfo.defaultDb,
                    e.getMessage()), e);
        }
    }

    /**
     * 设置连接异常状态，用于标记连接需要被丢弃
     *
     * @param conn 数据库连接对象，需要支持XConnection包装接口
     * @throws SQLException 当连接解包失败或设置异常状态失败时抛出
     */
    public static void setException(Connection conn) throws SQLException {
        // 解包连接对象获取XConnection实例
        final XConnection xConnection = conn.unwrap(XConnection.class);
        // 设置最后异常状态，标记连接需要丢弃
        xConnection.setLastException(new Exception("discard connection due to change some variables in this session"),
            true);
    }

    /**
     * 设置数据库连接的字符编码
     *
     * @param conn 数据库连接对象
     * @param encoding 要设置的字符编码
     * @throws SQLException 当数据库操作出现异常时抛出
     */
    public static void setEncoding(Connection conn, String encoding) throws SQLException {
        if (StringUtils.isEmpty(encoding)) {
            return;
        }
        String mysqlEncoding = EncodingUtils.mysqlEncoding(encoding);
        final XConnection xConnection = conn.unwrap(XConnection.class);

        // 检查是否为兼容的UTF-8编码配置
        boolean compatibleEncoding = encoding.equalsIgnoreCase("utf8")
            && xConnection.getSession().getRequestEncodingMySQL().equalsIgnoreCase("utf8mb4");

        // 当编码不匹配且不是兼容的UTF-8配置时，设置延迟编码
        if (!mysqlEncoding.equalsIgnoreCase(xConnection.getSession().getRequestEncodingMySQL())
            && !compatibleEncoding) {
            // Set lazy encoding.
            xConnection.getSession().setDefalutEncodingMySQL(mysqlEncoding);
        } else {
            xConnection.getSession().setDefalutEncodingMySQL(null);
        }
    }

    public static void setServerVariables(Connection conn, Map<String, Object> serverVariables) throws SQLException {
        if (serverVariables == null) {
            return;
        }
        final XConnection xConnection = conn.unwrap(XConnection.class);
        xConnection.setSessionVariables(serverVariables);
        // set socket timeout
        Executor socketTimeoutExecutor = TGroupDirectConnection.socketTimeoutExecutor;
        xConnection.setNetworkTimeout(socketTimeoutExecutor, TddlConstants.LONG_ENOUGH_TIMEOUT_FOR_DDL_ON_XPROTO_CONN);
    }

    public static void setTraceId(Connection conn, String traceId) throws SQLException {
        if (StringUtils.isEmpty(traceId)) {
            return;
        }
        final XConnection xConnection = conn.unwrap(XConnection.class);
        xConnection.setTraceId(traceId);
    }

    /**
     * 设置数据库连接的时间戳序列号（TSO时间戳）
     *
     * @param conn 数据库连接对象
     * @param tsoTimestamp 需要设置的时间戳序列号，如果为空则不做任何处理
     * @throws SQLException 当数据库操作出现异常时抛出
     */
    public static void setTsoTimestamp(Connection conn, Long tsoTimestamp) throws SQLException {
        if (tsoTimestamp == null) {
            return;
        }
        final XConnection xConnection = conn.unwrap(XConnection.class);
        xConnection.setLazySnapshotSeq(tsoTimestamp);
    }

    /**
     * 设置数据库连接的提交序列号（TSO时间戳）
     *
     * @param conn 数据库连接对象
     * @param tsoTimestamp 需要设置的时间戳序列号，如果为空则不做任何处理
     * @throws SQLException 当数据库操作出现异常时抛出
     */
    public static void setCommitSeq(Connection conn, Long tsoTimestamp) throws SQLException {
        if (tsoTimestamp == null) {
            return;
        }
        final XConnection xConnection = conn.unwrap(XConnection.class);
        xConnection.setLazyCommitSeq(tsoTimestamp);
    }

    protected static void handleParamsMap(Map<Integer, ParameterContext> params,
                                          Connection connection,
                                          PreparedStatement ps)
        throws SQLException {
        if (!connection.isWrapperFor(XConnection.class)) {
            ParameterMethod.setParameters(ps, params);
            return;
        }
        final XConnection xConnection = connection.unwrap(XConnection.class);
        if (xConnection.supportRawString()) {
            ParameterMethod.setParameters(ps, params);
        } else {
            List<ParameterContext> paramList = prepareParam(mapToList(params, false));
            ParameterMethod.setParameters(ps, paramList);
        }
    }

    public static List<Map<Integer, ParameterContext>> buildBatchParam(XResult xResult) throws SQLException {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        List<PolarxResultset.ColumnMetaData> columns = xResult.getMetaData();
        final XResultSet sirs = new XResultSet(xResult);
        while (sirs.next()) {
            batchParams.add(buildCurrentRow(sirs, columns));
        }
        return batchParams;
    }

    private static Map<Integer, ParameterContext> buildCurrentRow(
        XResultSet resultSet, List<PolarxResultset.ColumnMetaData> columns) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            PolarxResultset.ColumnMetaData.OriginalType columnType = columns.get(i).getOriginalType();
            params.put(i + 1, buildColumnParam(resultSet, i + 1, columnType));
        }
        return params;
    }

    public static List<Map<Integer, ParameterContext>> convertUpperBoundWithDefault(XResult xResult)
        throws SQLException {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        List<PolarxResultset.ColumnMetaData> columns = xResult.getMetaData();
        final XResultSet sirs = new XResultSet(xResult);
        while (sirs.next()) {
            final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                PolarxResultset.ColumnMetaData columnMeta = columns.get(i);
                PolarxResultset.ColumnMetaData.OriginalType columnType = columnMeta.getOriginalType();

                ParameterContext parameterContext = buildColumnParam(sirs, i + 1, columnType);

                if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_FLOAT
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DOUBLE) {
                    if (null != parameterContext.getArgs()[1]) {
                        // For float value like "-100.003", query like "c_float <= -100.003" returns nothing.
                        // Should replace upper bound with "c_float <= -100"
                        parameterContext = new ParameterContext(parameterContext.getParameterMethod(),
                            new Object[] {
                                parameterContext.getArgs()[0],
                                Math.ceil((Double) parameterContext.getArgs()[1])});
                    }
                }

                params.put(i + 1, parameterContext);
            }
            batchParams.add(params);
        }

        if (batchParams.isEmpty()) {
            // Build default
            final int columnCount = columns.size();

            final Map<Integer, ParameterContext> params = new HashMap<>(columnCount);
            for (int i = 0; i < columnCount; i++) {

                PolarxResultset.ColumnMetaData columnMeta = columns.get(i);

                ParameterMethod defaultMethod = ParameterMethod.setString;
                Object defaultValue = "0";

                final PolarxResultset.ColumnMetaData.OriginalType columnType = columnMeta.getOriginalType();
                if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DATE
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DATETIME
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DATETIME2
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_TIMESTAMP
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_TIMESTAMP2
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_TIME
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_YEAR) {
                    // For time data type, use number zero as upper bound
                    defaultMethod = ParameterMethod.setLong;
                    defaultValue = 0L;
                }

                ParameterContext parameterContext = new ParameterContext(defaultMethod, new Object[] {i, defaultValue});

                params.put(i + 1, parameterContext);
            }

            batchParams.add(params);
        }

        return batchParams;
    }

    public static ParameterContext buildColumnParam(XResultSet sirs, int i,
                                                    PolarxResultset.ColumnMetaData.OriginalType columnType)
        throws SQLException {
        Object value = null;
        ParameterMethod method = ParameterMethod.setObject1;
        try {
            value = sirs.getObject(i);
            if (value != null) {
                if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_ENUM) {
                    value = sirs.getString(i);
                    method = ParameterMethod.setString;
                } else if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DATE
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DATETIME
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DATETIME2
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_TIMESTAMP
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_TIMESTAMP2
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_TIME
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_YEAR) {
                    // 针对 0000-00-00 01:01:01.12 的时间类型或 0000 的year 类型，
                    // getObject 返回的结果错误，getBytes 后转为 String 没问题
                    value = sirs.getString(i);
                    method = ParameterMethod.setString;
                } else if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_BIT) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    final byte[] bytes = sirs.getBytes(i);
                    value = new BigInteger(ByteBuffer.allocate(1 + bytes.length).put((byte) 0).put(bytes).array());
                    method = ParameterMethod.setBit;
                } else if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_FLOAT
                    || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_DOUBLE) {
                    // 使用表示范围更大的类型，规避序列化/反序列化上下界时丢失数据
                    value = sirs.getDouble(i);
                    method = ParameterMethod.setDouble;
                } else if (columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_BLOB) {
                    // 使用 setBytes 标记，序列化时使用16进制字符串
                    value = sirs.getBytes(i);
                    method = ParameterMethod.setBytes;
                } else if (value instanceof byte[] && (
                    columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_STRING
                        || columnType == PolarxResultset.ColumnMetaData.OriginalType.MYSQL_TYPE_VARCHAR)) {
                    // binary/varbinary 类型
                    value = sirs.getBytes(i);
                    method = ParameterMethod.setBytes;
                }
            }
        } catch (TddlNestableRuntimeException e) {
            SQLRecorderLogger.ddlLogger.warn("Convert data type failed, use getBytes. message: " + e.getMessage());

            // 类似 -01:01:01 的时间类型 getObject 会抛异常，getBytes 没问题
            // Ignore exception, use getBytes instead
            value = sirs.getBytes(i);
            method = ParameterMethod.setBytes;
        }
        return new ParameterContext(method, new Object[] {i + 1, value, columnType});
    }

    /**
     * 获取TSO时间戳
     */
    public static Long getTsoTimestamp() {
        ITimestampOracle tso = ITimestampOracle.getInstance();
        return tso.nextTimestamp();
    }

    /**
     * 获取采样数据
     *
     * @param baseEc ExecutionContext对象，用于执行查询的基础执行环境
     * @param phyDbName 物理数据库名称
     * @param phyTable 物理表名称
     * @param primaryKeyColumns 主键列名列表
     * @param calSamplePercentage 采样百分比
     * @return 包含样本数据的Map列表，每个Map的键为整数，值为ParameterContext对象
     */
    public static List<Map<Integer, ParameterContext>> getSampleData(ExecutionContext baseEc,
                                                                     String phyDbName, String phyTable,
                                                                     List<String> primaryKeyColumns,
                                                                     float calSamplePercentage,
                                                                     long jobId, long taskId, long backfillId,
                                                                     OmcStorageInfo omcStorageInfo) {
        // 构造主键列名字符串，用逗号分隔并加上反引号包围
        String pkList = primaryKeyColumns.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.joining(","));
        // 格式化SQL查询语句，替换占位符
        String sql = String.format(SELECT_SAMPLE_SQL,
            calSamplePercentage,
            jobId, taskId, backfillId,
            pkList,
            SqlIdentifier.surroundWithBacktick(phyDbName),
            SqlIdentifier.surroundWithBacktick(phyTable)
        );
        // 使用新连接执行查询并返回结果
        return queryWithNewConn(baseEc, omcStorageInfo, sql);
    }

    /**
     * 获取指定物理表的列信息
     *
     * @param omcStorageInfo 存储信息对象，包含数据库连接相关信息
     * @param physicalTableName 物理表名
     * @return 返回指定表的列信息列表
     * @throws RuntimeException 当获取列信息失败时抛出异常
     */
    public static List<ColumnsInfoSchemaRecord> getColumnsInfo(OmcStorageInfo omcStorageInfo,
                                                               String physicalTableName) {
        // 建立数据库连接并获取表列信息
        try (Connection conn = getPhysicalConnection(omcStorageInfo)) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            return tableInfoManager.fetchColumnInfoSchema(omcStorageInfo.defaultDb, physicalTableName, null, conn);
        } catch (SQLException e) {
            // 处理SQL异常，包装成通用异常并抛出
            throw GeneralUtil.nestedException(
                String.format("failed to get columns info on host(%s) , Caused by: %s", omcStorageInfo, e.getMessage()),
                e);
        }
    }

    /**
     * 获取指定物理表的主键信息
     *
     * @param omcStorageInfo 存储信息对象，包含数据库连接配置
     * @param physicalTableName 物理表名
     * @return 主键信息列表，包含索引相关的元数据记录
     * @throws RuntimeException 当获取主键信息失败时抛出异常
     */
    public static List<IndexesInfoSchemaRecord> getPrimaryKeyInfo(OmcStorageInfo omcStorageInfo,
                                                                  String physicalTableName) {
        try (Connection conn = getPhysicalConnection(omcStorageInfo)) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            return tableInfoManager.fetchIndexMetaFromInfoSchemaForPrimaryKey(omcStorageInfo.defaultDb,
                physicalTableName,
                conn);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("failed to get columns info on host(%s) , Caused by: %s", omcStorageInfo.defaultDb,
                    e.getMessage()), e);
        }
    }

    /**
     * 构建创建表的SQL语句.
     *
     * @param omcStorageInfo 存储信息对象
     * @param physicalDbName 物理数据库名称
     * @param physicalTableName 物理表名称
     * @return 创建表的SQL语句
     */
    public static String getCreateTableSql(OmcStorageInfo omcStorageInfo, String physicalDbName,
                                           String physicalTableName) {
        String sql = String.format(SHOW_CREATE_SQL,
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(physicalTableName)
        );

        // 执行SQL查询并获取结果集
        List<List<Object>> result = fetchInformation(omcStorageInfo, sql);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                String.format("db %s can not find table %s", physicalDbName, physicalTableName));
        }

        // 返回创建表的SQL语句
        return (String) result.get(0).get(1);
    }

    public static long getTableRowsCount(OmcStorageInfo omcStorageInfo, String physicalDb, String physicalTableName) {
        String sql = StatsUtils.genTableRowsCountSQL(physicalDb, physicalTableName);
        List<List<Object>> result = fetchInformation(omcStorageInfo, sql);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("db %s can not find table %s", physicalDb, physicalTableName));
        }
        return Long.parseLong(String.valueOf(result.get(0).get(0)));
    }

    public static int getTableAvgRowLength(OmcStorageInfo omcStorageInfo, String physicalDb,
                                           String physicalTableName) {
        String sql = StatsUtils.genAvgTableRowLengthSQL(physicalDb, physicalTableName);
        List<List<Object>> result = fetchInformation(omcStorageInfo, sql);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("db %s can not find table %s", physicalDb, physicalTableName));
        }
        return Integer.parseInt(String.valueOf(result.get(0).get(0)));
    }

    public static int getActualBatchSize(OmcStorageInfo omcStorageInfo, String physicalDb,
                                         String physicalTableName, int batchSize, int batchFileSize) {
        int actualBatchSize = batchSize;
        int tableAvgRowLength = getTableAvgRowLength(omcStorageInfo, physicalDb, physicalTableName);
        if (tableAvgRowLength != 0) {
            int idealBatchSize = batchFileSize / tableAvgRowLength;
            if (idealBatchSize <= 0) {
                actualBatchSize = 1;
            } else {
                actualBatchSize = Math.min(idealBatchSize, batchSize);
            }
        }
        return actualBatchSize;
    }

    public static Long getConnectionId(Connection conn, String sql) {
        if (StringUtils.isEmpty(sql)) {
            return -1L;
        }
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                return rs.getLong(1);
            }
            return -1L;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static String getOmcTableName(String tableName, Long changesetId, boolean withJobId) {
        if (withJobId) {
            return String.format("%s_%s_omc", tableName, changesetId);
        }
        return String.format("%s_omc", tableName);
    }

    public static String getSentryTableName(String tableName, Long changesetId, boolean withJobId) {
        if (withJobId) {
            return String.format("%s_%s_del", tableName, changesetId);
        }
        return String.format("%s_del", tableName);
    }

    public static String getOmcBinTableName(Long changesetId) {
        return String.format("%s_omc_bin", changesetId);
    }

    /**
     * 构建物理表拓扑结构
     *
     * @param schemaName 数据库模式名称
     * @param tableTopology 逻辑表拓扑结构，key为 group名，value为二维字符串列表表示的表名集合
     * @return 物理表拓扑结构，key为存储实例ID，value为物理数据库名和物理表名的配对列表
     */
    public static Map<String, List<Pair<String, String>>> buildPhysicalTableTopology(String schemaName,
                                                                                     TreeMap<String, List<List<String>>> tableTopology) {
        Map<String, List<Pair<String, String>>> physicalTableTopology = new HashMap<>();
        if (tableTopology == null || tableTopology.isEmpty()) {
            return physicalTableTopology;
        }

        for (String groupName : tableTopology.keySet()) {
            String storageInstId = DbTopologyManager.getStorageInstIdByGroupName(schemaName, groupName);
            String physicalDbName = GroupInfoUtil.buildPhysicalDbNameFromGroupName(schemaName, groupName);
            List<String> physicalTableNames = tableTopology.get(groupName).stream().
                flatMap(List::stream).collect(Collectors.toList());
            List<Pair<String, String>> physicalDbAndTable =
                physicalTableTopology.computeIfAbsent(storageInstId, v -> new ArrayList<>());
            for (String physicalTableName : physicalTableNames) {
                physicalDbAndTable.add(Pair.of(physicalDbName, physicalTableName));
            }
        }
        return physicalTableTopology;
    }

    public static Map<String, Pair<String, String>> getSourceTableInfo(String phyDbName,
                                                                       String physicalTableName,
                                                                       List<String> phyPartNames,
                                                                       boolean hasNoPhyPart,
                                                                       OmcStorageInfo omcStorageInfo) {
        String msg = "begin to get the source table[" + phyDbName + "." + physicalTableName + ":]'s innodb data file";
        SQLRecorderLogger.ddlLogger.info(msg);

        Map<String, Pair<String, String>> srcFileAndDirs = new HashMap<>();
        if (InstanceVersion.isMYSQL80()) {
            if (GeneralUtil.isNotEmpty(phyPartNames)) {
                for (String phyPartName : phyPartNames) {
                    String name = String.format("%s/%s#p#%s", phyDbName, physicalTableName, phyPartName);
                    Pair<String, String> srcFileAndDir = Pair.of(name, name);
                    srcFileAndDirs.put(phyPartName, srcFileAndDir);
                }
            } else {
                String name = String.format("%s/%s", phyDbName, physicalTableName);
                Pair<String, String> srcFileAndDir = Pair.of(name, name);
                srcFileAndDirs.put(physicalTableName, srcFileAndDir);
            }
        } else {
            PolarxPhysicalBackfill.GetFileInfoOperator getFileInfoOperator =
                checkFileExistence(phyDbName, physicalTableName, phyPartNames, hasNoPhyPart, omcStorageInfo);
            for (PolarxPhysicalBackfill.FileInfo fileInfo : getFileInfoOperator.getTableInfo().getFileInfoList()) {
                Pair<String, String> srcFileAndDir = Pair.of(fileInfo.getFileName(), fileInfo.getDirectory());
                srcFileAndDirs.put(fileInfo.getPartitionName(), srcFileAndDir);
            }
        }

        msg = "already get the source table[" + phyDbName + "." + physicalTableName + ":]'s innodb data file";
        SQLRecorderLogger.ddlLogger.info(msg);
        return srcFileAndDirs;
    }

    /**
     * 批量获取指定物理库下多个物理表的 table space id
     *
     * @param phyDbName 物理库名
     * @param tableNames 物理表名列表
     * @param omcStorageInfo 存储信息
     * @param executionContext 执行上下文
     * @return 表名到 table space id 的映射
     */
    public static Map<String, Long> batchFetchTableSpaceId(String phyDbName,
                                                           List<String> tableNames,
                                                           OmcStorageInfo omcStorageInfo,
                                                           ExecutionContext executionContext) {
        Map<String, Long> result = new HashMap<>();
        if (tableNames == null || tableNames.isEmpty()) {
            return result;
        }

        // 按表收集分区和表名映射
        Map<String, Map<String, Pair<String, String>>> tableToPartInfo = new HashMap<>();
        for (String tableName : tableNames) {
            String sql = String.format(SELECT_PHY_PARTITION_NAMES, tableName, phyDbName);
            List<Map<Integer, ParameterContext>> partitionInfo =
                queryWithNewConn(executionContext, omcStorageInfo, sql);

            boolean hasNoPhyPart = true;
            List<String> partNameList = new ArrayList<>();
            if (GeneralUtil.isNotEmpty(partitionInfo)) {
                hasNoPhyPart = false;
                for (Map<Integer, ParameterContext> parameterContextMap : partitionInfo) {
                    partNameList.add((String) parameterContextMap.get(1).getValue());
                }
            }

            Map<String, Pair<String, String>> tableInfo = getSourceTableInfo(
                phyDbName.toLowerCase(),
                tableName.toLowerCase(),
                partNameList,
                hasNoPhyPart,
                omcStorageInfo
            );
            tableToPartInfo.put(tableName, tableInfo);
        }

        // 收集所有需要查询的 NAME
        List<String> allNames = new ArrayList<>();
        for (Map<String, Pair<String, String>> partInfo : tableToPartInfo.values()) {
            for (Pair<String, String> value : partInfo.values()) {
                allNames.add(value.getKey());
            }
        }

        if (allNames.isEmpty()) {
            return result;
        }

        // 批量查询 table space id
        String nameList = allNames.stream()
            .map(name -> String.format("'%s'", name))
            .collect(Collectors.joining(","));
        String sql = String.format(
            InstanceVersion.isMYSQL80() ? BATCH_SELECT_TABLE_SPACE_ID_80 : BATCH_SELECT_TABLE_SPACE_ID_57,
            nameList
        );

        List<Map<Integer, ParameterContext>> queryResult =
            queryWithNewConn(executionContext, omcStorageInfo, sql);

        // 构建 NAME -> space id 映射
        Map<String, Long> nameToSpaceId = new HashMap<>();
        for (Map<Integer, ParameterContext> row : queryResult) {
            String name = (String) row.get(1).getValue();
            Long spaceId = (Long) row.get(2).getValue();
            if (spaceId == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    String.format("Failed to get space id, name: %s, space id is null", name));
            }
            nameToSpaceId.put(name, spaceId);
        }

        // 汇总每个表的 table space id
        for (Map.Entry<String, Map<String, Pair<String, String>>> entry : tableToPartInfo.entrySet()) {
            String tableName = entry.getKey();
            Map<String, Pair<String, String>> partInfo = entry.getValue();

            long totalSpaceId = 0L;
            for (Pair<String, String> value : partInfo.values()) {
                String name = value.getKey();
                Long spaceId = nameToSpaceId.get(name);
                if (spaceId == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Failed to get space id for table %s, name: %s", tableName, name));
                }
                totalSpaceId += spaceId;
            }
            result.put(tableName, totalSpaceId);
            LOG.info(String.format("Batch get space id, table: %s.%s, spaceId: %s",
                phyDbName, tableName, totalSpaceId));
        }

        return result;
    }

    public static boolean hasReferencedForeignKey(String phyDbName, String physicalTableName,
                                                  OmcStorageInfo omcStorageInfo) {
        String sql = String.format(CHECK_FOREIGN_KEY, phyDbName, phyDbName, physicalTableName);
        List<List<Object>> result = ChangeSetUtils.queryGroup(null, null, omcStorageInfo, sql);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                String.format("table %s.%s check foreign key failed", phyDbName, physicalTableName));
        }
        return Integer.parseInt(String.valueOf(result.get(0).get(0))) > 0;
    }

    public static PolarxPhysicalBackfill.GetFileInfoOperator checkFileExistence(String phyDbName,
                                                                                String physicalTableName,
                                                                                List<String> phyPartNames,
                                                                                boolean hasNoPhyPart,
                                                                                OmcStorageInfo omcStorageInfo) {
        PolarxPhysicalBackfill.GetFileInfoOperator getFileInfoOperator = null;
        boolean success = false;
        int tryTime = 1;
        boolean isPartitioned = !hasNoPhyPart;
        do {
            try (XConnection conn = (XConnection) getPhysicalConnection(omcStorageInfo)) {
                PolarxPhysicalBackfill.GetFileInfoOperator.Builder builder =
                    PolarxPhysicalBackfill.GetFileInfoOperator.newBuilder();

                builder.setOperatorType(PolarxPhysicalBackfill.GetFileInfoOperator.Type.CHECK_SRC_FILE_EXISTENCE);
                PolarxPhysicalBackfill.TableInfo.Builder tableInfoBuilder =
                    PolarxPhysicalBackfill.TableInfo.newBuilder();
                tableInfoBuilder.setTableSchema(phyDbName);
                tableInfoBuilder.setTableName(physicalTableName);
                tableInfoBuilder.setPartitioned(isPartitioned);
                if (isPartitioned) {
                    tableInfoBuilder.addAllPhysicalPartitionName(phyPartNames);
                }
                builder.setTableInfo(tableInfoBuilder.build());
                getFileInfoOperator = conn.execCheckFileExistence(builder);

                success = true;
            } catch (SQLException ex) {
                if (tryTime > MAX_RETRY) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, ex, ex.getMessage());
                }
                tryTime++;
            }
        } while (!success);
        return getFileInfoOperator;
    }

    public static String rewriteAlterTableSql(String sql) {
        List<SQLStatement> statementList = FastsqlUtils.parseSql(sql);

        if (statementList.get(0) instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) statementList.get(0);

            alterTableStatement.setHeadHints(null);
            // 使用迭代器遍历并删除元素
            Iterator<SQLAlterTableItem> iterator = alterTableStatement.getItems().iterator();

            while (iterator.hasNext()) {
                SQLAlterTableItem item = iterator.next();

                if (item instanceof MySqlAlterTableOption) {
                    MySqlAlterTableOption alterTableOption = (MySqlAlterTableOption) item;

                    if ("ALGORITHM".equalsIgnoreCase(alterTableOption.getName())) {
                        iterator.remove();
                    }
                } else if (item instanceof MySqlAlterTableLock) {
                    iterator.remove();
                }
            }

            return SQLUtils.toSQLString(alterTableStatement, DbType.mysql, new SQLUtils.FormatOption(true, false));
        } else {
            return sql;
        }
    }

    public static List<OmcRecord> getOmcRecords(long jobId, long taskId) {
        OmcAccessor omcAccessor = new OmcAccessor();
        try (Connection metaDatabaseConnection = MetaDbDataSource.getInstance().getConnection()) {
            // Set up a new connection to the meta database specifically
            omcAccessor.setConnection(metaDatabaseConnection);
            return omcAccessor.query(jobId, taskId);
        } catch (Exception exception) {
            throw new TddlNestableRuntimeException(exception);
        }
    }

    public static void validateStmt(String alterStmt) {
        final List<SQLStatement> alterStatement =
            SQLUtils.parseStatementsWithDefaultFeatures(alterStmt, JdbcConstants.MYSQL);

        if (alterStatement.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "The DDL job includes multiple DDLs is not supported.");
        }

        if (!(alterStatement.get(0) instanceof SQLAlterTableStatement)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, "Should be alter table statement.");
        }

        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) alterStatement.get(0);
        for (SQLAlterTableItem sqlAlterTableItem : alterTableStmt.getItems()) {
            if (sqlAlterTableItem instanceof SQLAlterTableRename) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Alter table rename is not supported.");
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropPrimaryKey) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Alter table drop primary key is not supported.");
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropForeignKey) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Alter table drop foreign key is not supported.");
            }
        }
    }

    /**
     * Validate the REBUILD_TABLE_KEEP_FILTER expression before OMC execution.
     */
    public static Set<String> validateKeepFilter(String filterExpr) {
        return validateKeepFilter(filterExpr, null);
    }

    public static void validateRebuildCleanupTable(TableMeta tableMeta, boolean forceWithGsi) {
        if (tableMeta.isGsi() || tableMeta.isColumnar()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "REBUILD CLEANUP must be executed on the primary table");
        }
        if ((tableMeta.withGsiExcludingPureCci() || tableMeta.withCci()) && !forceWithGsi) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "REBUILD CLEANUP does not support GSI or CCI by default; set "
                    + "FORCE_REBUILD_CLEANUP_WITH_GSI=TRUE to rebuild published GSIs and notify published CCIs");
        }
        if (tableMeta.withGsiExcludingPureCci()) {
            long gsiCount = tableMeta.getGsiTableMetaBean().indexMap.values().stream()
                .filter(index -> !index.columnarIndex)
                .count();
            if (tableMeta.getGsiPublished() == null || tableMeta.getGsiPublished().size() != gsiCount) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "REBUILD CLEANUP requires all GSIs to be PUBLIC");
            }
        }
        if (tableMeta.withCci()) {
            long cciCount = tableMeta.getGsiTableMetaBean().indexMap.values().stream()
                .filter(index -> index.columnarIndex)
                .count();
            if (tableMeta.getColumnarIndexPublished() == null
                || tableMeta.getColumnarIndexPublished().size() != cciCount) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "REBUILD CLEANUP requires all CCIs to be PUBLIC");
            }
        }
    }

    public static Set<String> validateKeepFilterForTable(String filterExpr, TableMeta tableMeta) {
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableMeta.getAllColumns().forEach(column -> columnNames.add(column.getName()));
        return validateKeepFilter(filterExpr, columnNames);
    }

    /**
     * Validate the REBUILD_TABLE_KEEP_FILTER expression before OMC execution.
     *
     * @param filterExpr the filter expression to validate
     * @param validColumnNames set of column names that exist in the target table;
     * if non-null and non-empty, referenced columns are checked against this set
     */
    public static Set<String> validateKeepFilter(String filterExpr, Set<String> validColumnNames) {
        // 1. Parse expression
        SQLExpr expr;
        try {
            expr = SQLUtils.toSQLExpr(filterExpr, DbType.mysql);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Invalid REBUILD_TABLE_KEEP_FILTER: syntax error - " + e.getMessage());
        }

        // 2. Visit AST to detect forbidden patterns
        FilterExprValidator validator = new FilterExprValidator();
        expr.accept(validator);

        if (validator.getError() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "Invalid REBUILD_TABLE_KEEP_FILTER: " + validator.getError());
        }

        // 3. Check that all referenced columns exist in the target table
        if (validColumnNames != null && !validColumnNames.isEmpty()) {
            for (String col : validator.getReferencedColumns()) {
                if (!validColumnNames.contains(col)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "Invalid REBUILD_TABLE_KEEP_FILTER: column '" + col + "' does not exist");
                }
            }
        }
        return new TreeSet<>(validator.getReferencedColumns());
    }

    private static class FilterExprValidator extends SQLASTVisitorAdapter {

        private static final Set<String> NON_DETERMINISTIC_FUNCTIONS = new HashSet<>(Arrays.asList(
            "NOW", "RAND", "UUID", "UUID_SHORT", "SYSDATE", "UNIX_TIMESTAMP",
            "SLEEP", "BENCHMARK",
            "CONNECTION_ID", "FOUND_ROWS", "LAST_INSERT_ID", "ROW_COUNT",
            "GET_LOCK", "RELEASE_LOCK", "IS_FREE_LOCK",
            "USER", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER", "VERSION"
        ));

        private String error;
        private final Set<String> referencedColumns = new HashSet<>();

        public String getError() {
            return error;
        }

        public Set<String> getReferencedColumns() {
            return referencedColumns;
        }

        @Override
        public boolean visit(SQLQueryExpr x) {
            error = "subqueries are not allowed";
            return false;
        }

        @Override
        public boolean visit(SQLInSubQueryExpr x) {
            error = "subqueries are not allowed";
            return false;
        }

        @Override
        public boolean visit(SQLExistsExpr x) {
            error = "subqueries are not allowed";
            return false;
        }

        @Override
        public boolean visit(SQLCurrentTimeExpr x) {
            error = String.format(
                "non-deterministic function '%s' is not allowed, use a fixed value instead",
                x.getType().name);
            return false;
        }

        @Override
        public boolean visit(SQLMethodInvokeExpr x) {
            String funcName = x.getMethodName().toUpperCase();
            if (NON_DETERMINISTIC_FUNCTIONS.contains(funcName)) {
                // UNIX_TIMESTAMP with args is deterministic
                if ("UNIX_TIMESTAMP".equalsIgnoreCase(funcName) && !x.getArguments().isEmpty()) {
                    return true;
                }
                error = String.format(
                    "non-deterministic function '%s' is not allowed, use a fixed value instead",
                    x.getMethodName());
                return false;
            }
            return true;
        }

        @Override
        public boolean visit(SQLIdentifierExpr x) {
            if (error == null) {
                referencedColumns.add(x.normalizedName());
            }
            return false;
        }

        @Override
        public boolean visit(SQLPropertyExpr x) {
            if (error == null) {
                referencedColumns.add(SQLUtils.normalize(x.getName()));
            }
            return false;
        }
    }
}
