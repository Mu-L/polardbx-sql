package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.StorageNodeHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Sync action to fetch index usage statistics from DN nodes
 */
public class FetchIndexUsageSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(FetchIndexUsageSyncAction.class);

    // Custom thread pool for querying DN nodes
    private static final ThreadPoolExecutor QUERY_EXECUTOR = new ThreadPoolExecutor(
        32,
        64,
        60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(100),
        new NamedThreadFactory("IndexUsageQuery", true),
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    private static final long DEFAULT_QUERY_TIMEOUT_SECONDS = 120L;

    private String schemaName;
    private String sql;
    private ExecutionContext executionContext;

    /**
     * Set after sync() if DN queries timed out
     */
    private volatile boolean timeout = false;
    private volatile String warningMessage = null;

    public FetchIndexUsageSyncAction(String schemaName, String sql) {
        this.schemaName = schemaName;
        this.sql = sql;
        this.executionContext = null;
    }

    public FetchIndexUsageSyncAction(String schemaName, String sql, ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.sql = sql;
        this.executionContext = executionContext;
    }

    /**
     * Get query timeout from connection parameters or use default.
     */
    private long getQueryTimeout() {
        if (executionContext != null && executionContext.getParamManager() != null) {
            return executionContext.getParamManager().getLong(ConnectionParams.INDEX_USAGE_QUERY_TIMEOUT);
        }
        return DEFAULT_QUERY_TIMEOUT_SECONDS;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("index_usage");
        result.addColumn("OBJECT_SCHEMA", DataTypes.StringType);
        result.addColumn("OBJECT_NAME", DataTypes.StringType);
        result.addColumn("INDEX_NAME", DataTypes.StringType);
        result.addColumn("COUNT_STAR", DataTypes.LongType);
        result.addColumn("COUNT_FETCH", DataTypes.LongType);
        result.addColumn("SUM_TIMER_WAIT", DataTypes.LongType);
        result.addColumn("MAX_TIMER_WAIT", DataTypes.LongType);
        result.initMeta();

        try {
            executeQueryOnAllGroups(sql, result);
        } catch (Exception e) {
            logger.error("Failed to fetch index usage statistics for schema: " + schemaName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, "Failed to fetch index usage statistics");
        }

        return result;
    }

    private void executeQueryOnAllGroups(String querySql, ArrayResultCursor result) {
        long queryTimeoutSeconds = getQueryTimeout();
        boolean hasTimeout = false;
        int completedCount = 0;
        int totalCount = 0;

        try {
            ConcurrentHashMap<String, Long> threadExecutionTimes = new ConcurrentHashMap<>();
            long startTime = System.currentTimeMillis();

            List<StorageNodeHelper.StorageNode> storageNodes = StorageNodeHelper.getUniqueStorageNodes(schemaName);
            totalCount = storageNodes.size();

            logger.info("Querying " + totalCount + " DN nodes with timeout " + queryTimeoutSeconds + "s");

            List<CompletableFuture<List<Object[]>>> futures = storageNodes.stream()
                .map(node -> CompletableFuture.supplyAsync(() -> {
                    String hostPort = node.getAddress();
                    IDataSource dataSource = node.getDataSource();
                    long threadStartTime = System.currentTimeMillis();

                    List<Object[]> nodeResults = new ArrayList<>();
                    try {
                        if (dataSource != null) {
                            nodeResults = executeQueryOnDataSource(
                                dataSource, querySql, hostPort, (int) queryTimeoutSeconds);
                        } else {
                            logger.warn("DataSource is null for DN: " + hostPort);
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to query DN: " + hostPort, e);
                    } finally {
                        threadExecutionTimes.put(hostPort, System.currentTimeMillis() - threadStartTime);
                    }
                    return nodeResults;
                }, QUERY_EXECUTOR))
                .collect(Collectors.toList());

            try {
                CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0]));
                allFutures.get(queryTimeoutSeconds, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                hasTimeout = true;
                logger.warn("DN queries timed out after " + queryTimeoutSeconds + " seconds");
                // Cancel remaining futures to release DN connections promptly
                for (CompletableFuture<List<Object[]>> future : futures) {
                    future.cancel(true);
                }
            } catch (Exception e) {
                logger.warn("Error waiting for DN queries to complete", e);
            }

            for (CompletableFuture<List<Object[]>> future : futures) {
                try {
                    List<Object[]> rows = future.getNow(Collections.emptyList());
                    if (!rows.isEmpty()) {
                        completedCount++;
                        for (Object[] row : rows) {
                            result.addRow(row);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to get query result from future", e);
                }
            }

            long totalExecutionTime = System.currentTimeMillis() - startTime;
            logExecutionStatistics(threadExecutionTimes, totalExecutionTime);

            if (hasTimeout) {
                int incompletedCount = totalCount - completedCount;
                String warningMsg = String.format(
                    "WARNING: Query timed out after %d seconds. "
                        + "Retrieved results from %d/%d DN nodes. "
                        + "%d DN node(s) did not respond in time. Results may be incomplete. "
                        + "To increase timeout, run: SET INDEX_USAGE_QUERY_TIMEOUT = <seconds>; "
                        + "(current value: %d)",
                    queryTimeoutSeconds, completedCount, totalCount, incompletedCount, queryTimeoutSeconds
                );
                logger.warn(warningMsg);
                this.timeout = true;
                this.warningMessage = warningMsg;
            }

        } catch (Exception e) {
            logger.warn("Failed to execute query on all groups", e);
        }
    }

    private List<Object[]> executeQueryOnDataSource(IDataSource dataSource, String querySql,
                                                    String hostPort, int queryTimeoutSeconds) {
        List<Object[]> resultList = new ArrayList<>();
        try (IConnection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(querySql)) {
            ps.setQueryTimeout(queryTimeoutSeconds);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Object[] row = new Object[7];
                    row[0] = DataTypes.StringType.convertFrom(rs.getObject("OBJECT_SCHEMA"));
                    row[1] = DataTypes.StringType.convertFrom(rs.getObject("OBJECT_NAME"));
                    row[2] = DataTypes.StringType.convertFrom(rs.getObject("INDEX_NAME"));
                    row[3] = DataTypes.LongType.convertFrom(rs.getObject("COUNT_STAR"));
                    row[4] = DataTypes.LongType.convertFrom(rs.getObject("COUNT_FETCH"));
                    row[5] = DataTypes.LongType.convertFrom(rs.getObject("SUM_TIMER_WAIT"));
                    row[6] = DataTypes.LongType.convertFrom(rs.getObject("MAX_TIMER_WAIT"));
                    resultList.add(row);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to query DN: " + hostPort, e);
        }
        return resultList;
    }

    private void logExecutionStatistics(ConcurrentHashMap<String, Long> threadExecutionTimes, long totalExecutionTime) {
        StringBuilder sb = new StringBuilder();
        sb.append("Index usage query stats for schema ").append(schemaName)
            .append(": total=").append(totalExecutionTime).append("ms, per-DN=[");

        boolean first = true;
        for (Map.Entry<String, Long> entry : threadExecutionTimes.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("ms");
            first = false;
        }
        sb.append("]");

        logger.info(sb.toString());
    }

    public boolean isTimeout() {
        return timeout;
    }

    public String getWarningMessage() {
        return warningMessage;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}