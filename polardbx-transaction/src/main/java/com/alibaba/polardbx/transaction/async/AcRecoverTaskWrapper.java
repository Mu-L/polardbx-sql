package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_AC_RECOVER_PARAM;

/**
 * @author yaozhili
 */
public class AcRecoverTaskWrapper extends BaseTimerTaskWrapper {
    public AcRecoverTaskWrapper(Map<String, Object> properties, AsyncTaskQueue asyncTaskQueue) {
        super(properties, asyncTaskQueue);
        // Enable an active task when this timer task wrapper is created.
        resetTask();
    }

    @Override
    Set<String> getParamsDef() {
        return MODIFIABLE_AC_RECOVER_PARAM;
    }

    @Override
    void validateParams(Map<String, String> newParams) {
        for (Map.Entry<String, String> keyAndVal : newParams.entrySet()) {
            final String key = keyAndVal.getKey();
            final String val = keyAndVal.getValue();
            ParamValidationUtils.validateAcRecoverParam(key, val);
        }
    }

    @Override
    Map<String, String> getNewParams() {
        final Map<String, String> newParam = new HashMap<>(2);
        ParamManager paramManager = new ParamManager(properties);
        newParam.put(ConnectionProperties.ENABLE_TRANSACTION_RECOVER_TASK,
            paramManager.get(ConnectionParams.ENABLE_TRANSACTION_RECOVER_TASK));
        newParam.put(ConnectionProperties.XA_RECOVER_INTERVAL,
            paramManager.get(ConnectionParams.XA_RECOVER_INTERVAL));
        return newParam;
    }

    @Override
    TimerTask createTask(Map<String, String> newParam) {
        if (ConfigDataMode.isFastMock()) {
            return null;
        }

        final Boolean enable =
            GeneralUtil.convertStringToBoolean(newParam.get(ConnectionProperties.ENABLE_TRANSACTION_RECOVER_TASK));
        if (null == enable || !enable) {
            return null;
        }

        final long interval = Long.parseLong(newParam.get(ConnectionProperties.XA_RECOVER_INTERVAL));

        return asyncTaskQueue.scheduleAcRecoverTask(interval, getTask(new AcRecoverTask()));
    }
}
