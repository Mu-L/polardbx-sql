package com.alibaba.polardbx.executor.scheduler.executor.warmup;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.scheduler.executor.SchedulerExecutor;
import com.alibaba.polardbx.gms.config.SqlEngineAlert;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupAccessor;
import com.alibaba.polardbx.gms.scheduler.ColumnarWarmupRecord;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

public class ColumnarWarmupScheduleJob extends SchedulerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger("mpp_log");
    public static final int WAIT_FOR_NODE_READY_IN_MILLIS = 2000;
    public static final int WAIT_FOR_NODE_READY_RETRY_TIMES = 5;

    private final CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX);
    private final CronParser parser = new CronParser(cronDefinition);

    public ColumnarWarmupScheduleJob(final ExecutableScheduledJob executableScheduledJob) {
    }

    @Override
    public boolean execute() {
        // check time zone.
        ZoneId defaultZoneId = ZoneId.systemDefault();
        ZoneOffset offset = ZonedDateTime.now(defaultZoneId).getOffset();
        if (isGMTorUTC(defaultZoneId, offset)) {
            // the timezone is not correct, don't schedule.
            LOGGER.error("warmup schedule time zone error: " + defaultZoneId);
            return false;
        }

        String instanceId = InstIdUtil.getInstId();

        // Clear canceled task tags first.
        // In case that another thread (warmup suspend / warmup delete) register a invalid task tag in warmup task manager.
        WarmupTaskManager.getInstance().clearCanceledTasks();

        // 1. Read warmup tasks from MetaDB.
        List<ColumnarWarmupRecord> records;
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarWarmupAccessor accessor = new ColumnarWarmupAccessor();
            accessor.setConnection(connection);

            records = accessor.queryResumedByInstId(instanceId);
        } catch (SQLException e) {
            // log
            throw GeneralUtil.nestedException(e);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Get records " + records);
        }

        // 2. wait cluster nodes are all active.
        try {
            if (ConfigDataMode.isColumnarMode()) {
                boolean nodeReady;
                if (!(nodeReady = WarmupClusterUtil.checkAllComputeNodeReady())) {
                    LOGGER.error("cluster nodes are not ready");
                    Thread.sleep(WAIT_FOR_NODE_READY_IN_MILLIS);

                    int retryTimes = WAIT_FOR_NODE_READY_RETRY_TIMES;
                    while (retryTimes-- > 0) {
                        if (!WarmupClusterUtil.checkAllComputeNodeReady()) {
                            LOGGER.error("cluster nodes are not ready, retryTimes=" +
                                (WAIT_FOR_NODE_READY_RETRY_TIMES - retryTimes));
                            Thread.sleep(WAIT_FOR_NODE_READY_IN_MILLIS);
                        } else {
                            nodeReady = true;
                            break;
                        }
                    }
                }

                if (!nodeReady) {
                    String msg = MessageFormat.format(
                        "cluster nodes are not ready after retries, retryTimes={0}", WAIT_FOR_NODE_READY_RETRY_TIMES);
                    // cluster is break down.
                    EventLogger.log(EventType.COLUMNAR_WARMUP, msg);
                    SqlEngineAlert.getInstance().putColumnarWarmUp(msg);
                    return false;
                }
            }
        } catch (InterruptedException e) {
            throw GeneralUtil.nestedException(e);
        }

        // 3. Check cron of tasks to determine if it should be executed.
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime nextFireTime = now.plusMinutes(1);
        for (int i = 0; i < records.size(); i++) {
            ColumnarWarmupRecord record = records.get(i);

            String cronExpression = record.getCronExpression();
            String sqlDef = record.getSqlDef();
            String schemaName = record.getSchemaName();
            long taskId = record.getTaskId();

            // Get next execution time
            Cron cron = parser.parse(cronExpression);
            ExecutionTime executionTime = ExecutionTime.forCron(cron);
            Optional<ZonedDateTime> nextExecution = executionTime.nextExecution(now);

            // Check if the next execution time is within the next minute.
            if (nextExecution.isPresent() && nextExecution.get().isBefore(nextFireTime)) {
                LOGGER.info(MessageFormat.format(
                    "Try to start the warmup task: {0}, cron: {1}, nextExecution: {2}", taskId, cronExpression,
                    nextExecution.get()));

                WarmupTaskManager.getInstance()
                    .addTask(taskId, schemaName, sqlDef, instanceId, cronExpression,
                        nextExecution.get().toString());
            }
        }

        return true;
    }

    private static boolean isGMTorUTC(ZoneId zoneId, ZoneOffset offset) {
        String zoneIdStr = zoneId.getId();
        if ("GMT".equals(zoneIdStr) || "UTC".equals(zoneIdStr) || "Z".equals(zoneIdStr)) {
            return true;
        }

        if (ZoneOffset.UTC.equals(offset)) {
            return true;
        }

        return false;
    }

    @Override
    public Pair<Boolean, String> needInterrupted() {
        return super.needInterrupted();
    }
}
