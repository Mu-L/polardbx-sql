package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author yaozhili
 */
public abstract class AbstractCciChecker {
    protected final String schemaName;
    protected final String tableName;
    protected final String indexName;
    protected long primaryHashCode = -1;
    protected long columnarHashCode = -1;
    protected String columnarCheckSql;
    protected String primaryCheckSql;
    protected final List<String> errors = new ArrayList<>();
    protected final IInnerConnectionManager connManager;
    protected final ServerThreadPool threadPool;

    public AbstractCciChecker(String schemaName, String tableName, String indexName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        connManager = ExecutorContext.getContext(schemaName).getInnerConnectionManager();
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        threadPool = executorContext.getTopologyExecutor().getExecutorService();
    }

    public void checkSnapshot(ExecutionContext baseEc) throws Throwable {
        throw new UnsupportedOperationException();
    }

    public void checkIncrement(ExecutionContext baseEc, long tsoV1, long tsoV2, long innodbTso) throws Throwable {
        throw new UnsupportedOperationException();
    }

    abstract void log(String msg);

    /**
     * Record errors in log.
     */
    abstract void error(String msg, Throwable t);

    /**
     * Errors printed for user.
     */
    protected void handleError(Throwable t) {
        errors.add(t.getMessage());
    }

    /**
     * @param reports [OUT] check reports returned
     * @return true if anything is ok (reports may be empty),
     * or false if inconsistency detected (inconsistency details are in reports)
     */
    public boolean getCheckReports(Collection<String> reports) {
        if (!errors.isEmpty()) {
            reports.addAll(errors);
        }
        return errors.isEmpty();
    }

    /**
     * get table id from {schema name, index name}
     */
    protected long getTableId(String schemaName, String indexName) throws SQLException {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(indexName);
        return DynamicColumnarManager.getInstance().getTableId(Long.MAX_VALUE, schemaName, indexName, tableMeta);
    }

    /**
     * Get all orc/csv/del files whose tso <= given tso
     */
    protected List<FilesRecordSimplifiedWithChecksum> getFilesRecords(long tso, long tableId, String schemaName)
        throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);
            return filesAccessor.querySnapshotWithChecksumByTsoAndTableId(tso, schemaName, String.valueOf(tableId));
        }
    }

    protected void setBasicHint(ExecutionContext ec, StringBuilder sb) {
        long parallelism;
        if ((parallelism = ec.getParamManager().getInt(ConnectionParams.MPP_PARALLELISM)) > 0) {
            sb.append(" MPP_PARALLELISM=")
                .append(parallelism)
                .append(" ");
        }
        if ((parallelism = ec.getParamManager().getInt(ConnectionParams.PARALLELISM)) > 0) {
            sb.append(" PARALLELISM=")
                .append(parallelism)
                .append(" ");
        }
        boolean enableMpp = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MPP);
        if (enableMpp) {
            sb.append(" ENABLE_MPP=true");
        }
        boolean enableMasterMpp = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MASTER_MPP);
        if (enableMasterMpp) {
            sb.append(" ENABLE_MASTER_MPP=true");
        }
        sb.append(
            " ENABLE_ACCURATE_REL_TYPE_TO_DATA_TYPE=true FORCE_CCI_VISIBLE=true ENABLE_CONSISTENT_REPLICA_READ=true");
    }

    protected static void createReadViewForInnodb(Connection connection, String table) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show topology from " + table);
            List<Pair<String, String>> topologies = new ArrayList<>();
            while (rs.next()) {
                topologies.add(new Pair<>(rs.getString("GROUP_NAME"), rs.getString("TABLE_NAME")));
            }
            // For each group, send a query.
            for (Pair<String, String> topology : topologies) {
                stmt.executeQuery(String.format("/*+TDDL:node(%s)*/ select 1 from %s limit 1", topology.getKey(),
                    topology.getValue()));
            }
        }
    }

    /**
     * Call columnar flush and wait for the flush_tso.
     *
     * @return (Innodb tso, Columnar tso)
     */
    protected Pair<Long, Long> getAndWaitColumnarFlush(String schemaName, String indexName) {
        long binlogTso;
        try {
            // Columnar flush
            binlogTso = ExecUtils.columnarFlush(getTableId(schemaName, indexName));
        } catch (SQLException e) {
            throw new RuntimeException("Call columnar flush failed.", e);
        }
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException("Wait columnar flush interrupted.", e);
            }
            try (Connection connection = MetaDbUtil.getConnection()) {
                ColumnarCheckpointsAccessor accessor = new ColumnarCheckpointsAccessor();
                accessor.setConnection(connection);
                List<ColumnarCheckpointsRecord> records;
                if (!(records = accessor.queryByBinlogTso(binlogTso)).isEmpty()) {
                    return new Pair<>(records.get(0).binlogTso, records.get(0).checkpointTso);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Wait columnar flush failed.", e);
            }
        } while (true);
    }

    protected void interruptIfDdlCanceled(ServerThreadPool threadPool, ExecutionContext baseEc,
                                          Runnable task, Runnable cancelCallback) {
        Future future = threadPool.submit(null, null, () -> {
            MDC.put(MDC.MDC_KEY_APP, schemaName);
            task.run();
        });
        while (true) {
            try {
                future.get(1, TimeUnit.SECONDS);
                // Normal exit.
                break;
            } catch (TimeoutException e) {
                // ignore timeout.
            } catch (InterruptedException | ExecutionException e) {
                handleError(e);
                throw new RuntimeException(e);
            }

            if (baseEc.getDdlContext().isInterrupted()) {
                future.cancel(true);
                if (cancelCallback != null) {
                    cancelCallback.run();
                }
                handleError(new InterruptedException("DDL interrupted when checking cci."));
                throw new RuntimeException("DDL interrupted when checking cci.");
            }
        }
    }
}
