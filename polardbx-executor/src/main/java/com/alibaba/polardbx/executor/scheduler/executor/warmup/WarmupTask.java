package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.google.common.collect.ImmutableList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class WarmupTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");
    private final long taskId;
    private final String schemaName;
    private final String sqlDef;
    private final String instId;
    private final String cronExpr;
    private final String nextExecutionTime;

    private long startMillis;
    private Timestamp startTime;
    private Timestamp endTime;
    private long timeCost;
    private String ioStatus;

    private Throwable throwable;
    private List<Object[]> packetList;
    private IInnerConnection connection;

    public WarmupTask(long taskId, String schemaName, String sqlDef, String instId, String cronExpr,
                      String nextExecutionTime) {
        this.taskId = taskId;
        this.schemaName = schemaName;
        this.sqlDef = sqlDef;
        this.instId = instId;
        this.cronExpr = cronExpr;
        this.nextExecutionTime = nextExecutionTime;
        this.packetList = new ArrayList<>();
    }

    public static WarmupTask createInstance(long taskId, String schemaName, String sqlDef, String instId,
                                            String cronExpr,
                                            String nextExecutionTime) {
        return new WarmupTask(taskId, schemaName, sqlDef, instId, cronExpr, nextExecutionTime);
    }

    public void cancel() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        // get Start time.
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        MysqlDateTime mysqlDateTime = MySQLTimeTypeUtil.fromZonedDatetime(zonedDateTime);
        startTime = new OriginalTimestamp(mysqlDateTime);
        startMillis = System.currentTimeMillis();

        ExecutorContext executorContext = ExecutorContext.getContext(DEFAULT_DB_NAME);

        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = executorContext.getInnerConnectionManager().getConnection(schemaName);
            statement = connection.createStatement();

            // Execute the sql: warmup select ...
            // It will be handled by LogicalWarmupHandler
            resultSet = statement.executeQuery("warmup " + sqlDef);

            // Print resultSet to event-log.
            while (resultSet.next()) {

                startTime = resultSet.getTimestamp(1);
                endTime = resultSet.getTimestamp(2);
                timeCost = resultSet.getLong(3);
                ioStatus = resultSet.getString(4);

                final Map savedMdcContext = MDC.getCopyOfContextMap();
                try {
                    MDC.put(MDC.MDC_KEY_APP, schemaName);
                    LOGGER.info(MessageFormat.format(
                        "Finish the warmup task with result taskId: {0}, StartTimeStr: {1}, "
                            + "endTimeStr: {2}, timeCost: {3}, IOStatus: {4}",
                        taskId, startTime, endTime, timeCost, ioStatus)
                    );
                } finally {
                    MDC.setContextMap(savedMdcContext);
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(MessageFormat.format(
                        "Finish the warmup task with result taskId: {0}, StartTimeStr: {1}, "
                            + "endTimeStr: {2}, timeCost: {3}, IOStatus: {4}",
                        taskId, taskId, startTime, endTime, timeCost, ioStatus)
                    );
                }

                // build one packet for this result set row.
                Object[] packetInfo = buildOnePacketInfo();
                packetList.add(packetInfo);
            }

        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            // close connection and statement.
            try {
                if (connection != null) {
                    connection.close();
                }

                if (statement != null) {
                    statement.close();
                }

                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                throw GeneralUtil.nestedException(e);
            }
        }
    }

    public long getTaskId() {
        return taskId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getSqlDef() {
        return sqlDef;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void setThrowable(Throwable throwable) {
        this.throwable = throwable;
    }

    @Override
    public String toString() {
        return "WarmupTask{" +
            "taskId=" + taskId +
            ", schemaName='" + schemaName + '\'' +
            ", sqlDef='" + sqlDef + '\'' +
            '}';
    }

    public List<Object[]> getPacketInfo() {
        if (packetList.isEmpty()) {
            return ImmutableList.of(buildOnePacketInfo());
        }
        return packetList;
    }

    // According to InformationSchemaWarmupExecutionLogs
    // "TASK_ID", 0
    // "STATUS", 1
    // "CRON_EXPR", 2
    // "CRON_EXEC_TIME", 3
    // "START_TIME", 4
    // "FINISH_TIME", 5
    // "TIME_COST", 6
    // "INST_ID", 7
    // "SCHEMA_NAME", 8
    // "SQL_DEF", 9
    // "IO_MESSAGE", 10
    // "NODE_INFO" 11
    public Object[] buildOnePacketInfo() {

        if (endTime == null) {
            timeCost = System.currentTimeMillis() - startMillis;
        }

        Object[] packet = new Object[12];
        packet[0] = taskId;
        packet[1] = null; // status
        packet[2] = cronExpr;
        packet[3] = nextExecutionTime;
        packet[4] = startTime;
        packet[5] = endTime;
        packet[6] = timeCost;
        packet[7] = instId;
        packet[8] = schemaName;
        packet[9] = sqlDef;
        packet[10] = throwable == null ? ioStatus : throwable.getMessage();
        return packet;
    }
}