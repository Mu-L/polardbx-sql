package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import org.apache.commons.lang3.StringUtils;

public interface RemoteExecutableDdlRebalanceTask extends RemoteExecutableDdlTask {

    @Override
    default boolean forbidRemoteDdlTask() {
        String forbidRemoteDdlTaskStr =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.DISABLE_REBALANCE_MPP, Boolean.FALSE.toString());
        return StringUtils.equalsIgnoreCase(forbidRemoteDdlTaskStr, Boolean.TRUE.toString());
    }
}
