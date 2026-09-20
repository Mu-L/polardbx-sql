package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Action that expand database by adding new storage nodes
 *
 * @author yijin
 * @since 2025/07
 */
public class ActionExpandDatabase implements BalanceAction, Comparable<ActionExpandDatabase> {

    private String schema;
    private String sql;
    private BalanceStats stats;
    private Long backfillRows;
    private Long diskSize;
    static public final String ACTION_EXPAND_DATABASE_NAME = "ExpandDatabase";

    private static final double DEFAULT_EXPAND_RATIO = 0.3;

    public ActionExpandDatabase(String schema, String sql, BalanceStats stats) {
        this.schema = schema;
        this.sql = sql;
        this.stats = stats;
        this.backfillRows = -1L;
        this.diskSize = -1L;
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public String getName() {
        return ACTION_EXPAND_DATABASE_NAME;
    }

    @Override
    public String getStep() {
        return getSql();
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        long totalRows = 0L;
        long totalSize = 0L;
        try {
            // 计算扩容比例
            double expandRatio = getExpandRatio(sql);

            // 计算总的数据量
            Pair<Long, Long> totalData = calculateTotalDatabaseData();
            long databaseTotalRows = totalData.getKey();
            long databaseTotalSize = totalData.getValue();

            // 根据扩容比例计算需要处理的数据量
            totalRows = (long) (databaseTotalRows * expandRatio);
            totalSize = (long) (databaseTotalSize * expandRatio);

            EventLogger.log(EventType.REBALANCE_INFO,
                String.format("[schema %s] expand database calculated: expandRatio=%.2f, totalRows=%d, totalSize=%d",
                    schema, expandRatio, totalRows, totalSize));

        } catch (Exception e) {
            EventLogger.log(EventType.DDL_WARN, "calculate expand database rows error. " + e.getMessage());
        }

        return ActionUtils.convertToDelegatorJob(schema, sql,
            CostEstimableDdlTask.createCostInfo(totalRows, totalSize, null));
    }

    public String getSql() {
        return sql;
    }

    @Override
    public Long getBackfillRows() {
        if (backfillRows == -1L) {
            try {
                double expandRatio = getExpandRatio(sql);
                Pair<Long, Long> totalData = calculateTotalDatabaseData();
                backfillRows = (long) (totalData.getKey() * expandRatio);
            } catch (Exception e) {
                EventLogger.log(EventType.DDL_WARN, "calculate expand database backfill rows error. " + e.getMessage());
                backfillRows = 0L;
            }
        }
        return backfillRows;
    }

    @Override
    public Long getDiskSize() {
        if (diskSize == -1L) {
            try {
                double expandRatio = getExpandRatio(sql);
                Pair<Long, Long> totalData = calculateTotalDatabaseData();
                diskSize = (long) (totalData.getValue() * expandRatio);
            } catch (Exception e) {
                EventLogger.log(EventType.DDL_WARN, "calculate expand database disk size error. " + e.getMessage());
                diskSize = 0L;
            }
        }
        return diskSize;
    }

    /**
     * 计算数据库中的总数据量
     *
     * @return Pair<总行数, 总磁盘大小>
     */
    private Pair<Long, Long> calculateTotalDatabaseData() {
        long totalRows = 0L;
        long totalSize = 0L;

        if (stats == null) {
            return Pair.of(0L, 0L);
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            // AUto库：使用 PartitionStat 统计
            List<PartitionStat> partitionStats = stats.getPartitionStats();
            if (GeneralUtil.isNotEmpty(partitionStats)) {
                for (PartitionStat partitionStat : partitionStats) {
                    totalRows += partitionStat.getPartitionRows();
                    totalSize += partitionStat.getPartitionDiskSize();
                }
            }
        } else {
            // DRDS库：使用 GroupStats 统计
            List<GroupStats.GroupsOfStorage> groups = stats.getGroups();
            if (GeneralUtil.isNotEmpty(groups)) {
                for (GroupStats.GroupsOfStorage groupsOfStorage : groups) {
                    if (groupsOfStorage == null || groupsOfStorage.getGroupDataSizeMap() == null) {
                        continue;
                    }
                    for (Map.Entry<String, Pair<Long, Long>> entry : groupsOfStorage.getGroupDataSizeMap().entrySet()) {
                        totalRows += entry.getValue().getKey();
                        totalSize += entry.getValue().getValue();
                    }
                }
            }
        }

        return Pair.of(totalRows, totalSize);
    }

    /**
     * 从SQL中获取扩容比例
     *
     * @param sql 扩容SQL语句
     * @return 扩容比例 (0.0 - 1.0)
     */
    private double getExpandRatio(String sql) {
        return DEFAULT_EXPAND_RATIO;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionExpandDatabase)) {
            return false;
        }
        ActionExpandDatabase expandDatabase = (ActionExpandDatabase) o;
        return Objects.equals(sql, expandDatabase.getSql()) && Objects
            .equals(schema, expandDatabase.getSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, sql);
    }

    @Override
    public String toString() {
        return "ActionExpandDatabase{" +
            "schemaName=" + schema +
            ", sql=" + sql +
            '}';
    }

    @Override
    public int compareTo(ActionExpandDatabase o) {
        return getDiskSize().compareTo(o.getDiskSize());
    }

    public SubJobTask toSubJobTask() {
        SubJobTask rebalanceDatabaseSubJobTask =
            new SubJobTask(schema, sql, null);
        return rebalanceDatabaseSubJobTask;
    }
}