package com.alibaba.polardbx.server.response;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.LongUtil;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.spill.SpillSpaceManager;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.server.util.StringUtil;
import com.sun.management.OperatingSystemMXBean;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.nio.file.Files.getFileStore;

public class FetchCnStatusSyncAction implements ISyncAction {
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("CN_STATUS");

        result.addColumn("NODE", DataTypes.StringType);
        result.addColumn("CPU_CORE", DataTypes.LongType);
        result.addColumn("HEAP_USED", DataTypes.LongType);
        result.addColumn("HEAP_FREE", DataTypes.LongType);
        result.addColumn("NON_HEAP_USED", DataTypes.LongType);
        result.addColumn("SPILL_USAGE", DataTypes.LongType);
        result.addColumn("LOG_USAGE", DataTypes.LongType);
        result.addColumn("APP_CONN", DataTypes.LongType);
        result.addColumn("TO_DN_CLIENT_NUM", DataTypes.LongType);
        result.addColumn("TO_DN_IDLE_SESSION", DataTypes.LongType);
        result.addColumn("TO_DN_WORKING_SESSION", DataTypes.LongType);

        Map<String, SchemaConfig> schemas = CobarServer.getInstance().getConfig().getSchemas();
        Set<String> uniq = new HashSet<>();

        long clientNum = 0;
        long idleSessionNum = 0;
        long workingSessionNum = 0;

        for (SchemaConfig schema : schemas.values()) {
            TDataSource ds = schema.getDataSource();
            if (ds != null && ds.isInited()) {
                for (TGroupDataSource groupDataSource : ds.getGroupDataSources()) {
                    for (DataSourceWrapper dataSourceWrapper : groupDataSource.getConfigManager().getDataSourceWrapperMap().values()) {
                        final DataSource rawDS = dataSourceWrapper.getWrappedDataSource().getDataSource();
                        if (rawDS instanceof XDataSource) {
                            final XDataSource x = (XDataSource) rawDS;
                            if (uniq.contains(x.getDigest())) {
                                continue;
                            }
                            uniq.add(x.getDigest());
                            final XClientPool.XStatus s = x.getStatus();
                            clientNum += s.client;
                            idleSessionNum += s.idleSession;
                            workingSessionNum += s.workingSession;
                        }
                    }
                }
            }
        }


        long activeConnections = 0;
        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc instanceof ServerConnection) {
                    ServerConnection sc = (ServerConnection) fc;
                    // Only count active connections (either executing a statement or recently active)
                    if (sc.isStatementExecuting().get()) {
                        activeConnections++;
                        // If you want to see the details, uncomment the following line
                        // addRowByConnection(result, sc);
                    }
                }
            }
        }

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            ThreadCpuStatUtil.NUM_CORES,
            memoryMXBean.getHeapMemoryUsage().getUsed(),
            memoryMXBean.getHeapMemoryUsage().getMax(),
            memoryMXBean.getNonHeapMemoryUsage().getUsed(),
            SpillSpaceManager.getInstance().getTotalSpillSpace(),
            LogUtils.getTotalLogSpace(),
            activeConnections,
            clientNum,
            idleSessionNum,
            workingSessionNum,
        });
        return result;
    }
}
