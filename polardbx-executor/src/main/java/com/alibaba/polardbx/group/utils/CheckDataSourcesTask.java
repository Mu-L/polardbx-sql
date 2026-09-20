package com.alibaba.polardbx.group.utils;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;

import java.util.List;
import java.util.Map;

public class CheckDataSourcesTask implements Runnable {
    @Override
    public void run() {
        try {
            boolean enableFollowerRead = OptimizerUtils.enableFollowRead();
            if (!ConfigDataMode.isMasterMode()) {
                return;
            }
            if (!existAvailableFollower()) {
                return;
            }
            if (!isMatchFollowerReadSetting(enableFollowerRead)) {
                StorageHaManager.getInstance().setNeedRefreshFollowSources(true);
                MetaDbLogUtil.META_DB_LOG.warn("Need Refresh the Follower DataSources!");
            } else {
                StorageHaManager.getInstance().setNeedRefreshFollowSources(false);
            }
        } catch (Throwable ex) {
            StorageHaManager.getInstance().setNeedRefreshFollowSources(false);
            MetaDbLogUtil.META_DB_LOG.error(ex);
        }
    }

    public static void checkFollowerConnection(long timeout) {
        long endTime = System.currentTimeMillis() + timeout;
        try {
            while (System.currentTimeMillis() < endTime) {
                if (isMatchFollowerReadSetting(true)) {
                    return;
                }
                Thread.sleep(100); // 每 100 毫秒检查一次
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("timeout waiting for follower connecting for follower read account.");
    }

    static boolean isMatchFollowerReadSetting(boolean enableFollowerRead) {
        List<Group> allGroups = OptimizerContext.getActiveGroups();
        for (Group group : allGroups) {
            if (SystemDbHelper.isDBBuildIn(group.getSchemaName())) {
                continue;
            }
            TopologyHandler topology =
                ExecutorContext.getContext(group.getSchemaName()).getTopologyHandler();
            IGroupExecutor groupExecutor = topology.get(group.getName());
            if (groupExecutor == null) {
                continue;
            }
            Object o = groupExecutor.getDataSource();

            if (o instanceof TGroupDataSource) {
                TGroupDataSource ds = (TGroupDataSource) o;
                boolean existFollowerDB = ds.getAtomDataSources().stream().anyMatch(
                    atom -> atom.isFollowerDB());
                if (enableFollowerRead != existFollowerDB) {
                    return false;
                }
            }
        }
        return true;
    }

    public static void setInMemoryFollowReadAndWait(
        long timeout, boolean newFollowerRead) throws InterruptedException {
        if (newFollowerRead) {
            if (!existAvailableFollower() && ConfigDataMode.isMasterMode()) {
                throw new RuntimeException("No available followers!");
            }
        }
        long startTime = System.currentTimeMillis();
        boolean oldEnableFollowerRead = OptimizerUtils.enableFollowRead();
        if (newFollowerRead != oldEnableFollowerRead) {
            try {
                DynamicConfig.getInstance().enableFollowReadInMemory(newFollowerRead);
                while (!isMatchFollowerReadSetting(newFollowerRead) && ConfigDataMode.isMasterMode()) {
                    if (System.currentTimeMillis() - startTime > timeout) {
                        throw new RuntimeException("timeout waiting for build the follower read.");
                    }
                    Thread.sleep(100); // 每 100 毫秒检查一次
                }
            } catch (Throwable e) {
                DynamicConfig.getInstance().enableFollowReadInMemory(oldEnableFollowerRead);
                MetaDbLogUtil.META_DB_LOG.error(e);
                throw e;
            }
        }
    }

    static boolean existAvailableFollower() {
        Map<String, StorageInstHaContext> storageStatusMap =
            StorageHaManager.getInstance().getStorageHaCtxCache();
        for (StorageInstHaContext ctx : storageStatusMap.values()) {
            if (!ctx.getAvailableFollowerNodes().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
