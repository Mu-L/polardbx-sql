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

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_CCL_EXECUTION_TIME_PARAM;

public class CCLExecutionTimeTaskWrapper extends BaseTimerTaskWrapper {

    public CCLExecutionTimeTaskWrapper(Map<String, Object> properties, AsyncTaskQueue asyncTaskQueue) {
        super(properties, asyncTaskQueue);
        // Enable an active task when this timer task wrapper is created.
        resetTask();
    }

    @Override
    public void resetTask() {
        if (ConfigDataMode.isFastMock()) {
            cancel();
            return;
        }

        // 1. Get new parameters.
        final Map<String, String> newParams = getNewParams();

        // 2. Validate parameters.
        validateParams(newParams);

        // 3. If new parameters are identical to the current running ones, ignore the reset.
        final Map<String, String> currentParam = getCurrentParam();
        if (null != currentParam &&
            ParamValidationUtils.isIdentical(newParams, currentParam, MODIFIABLE_CCL_EXECUTION_TIME_PARAM)) {
            return;
        }

        // 4. Reset the timer task.
        innerReset(newParams);
    }

    @Override
    Set<String> getParamsDef() {
        return MODIFIABLE_CCL_EXECUTION_TIME_PARAM;
    }

    @Override
    public TimerTask createTask(Map<String, String> newParam) {
        if (ConfigDataMode.isFastMock()) {
            return null;
        }
        final Boolean enable =
            GeneralUtil.convertStringToBoolean(newParam.get(ConnectionProperties.ENABLE_CCL_EXECUTION_TIME_TASK));
        if (enable == null || !enable) {
            return null;
        }

        final int interval = Integer.parseInt(newParam.get(ConnectionProperties.CCL_EXECUTION_TIME_DETECT_INTERVAL));

        return asyncTaskQueue
            .scheduleSqlExceedMaxStatementTimeTask(interval, getTask(new CCLExecutionTime()));
    }

    @Override
    Map<String, String> getNewParams() {
        final Map<String, String> newParam = new HashMap<>(2);
        ParamManager paramManager = new ParamManager(properties);
        newParam.put(ConnectionProperties.ENABLE_CCL_EXECUTION_TIME_TASK,
            paramManager.get(ConnectionParams.ENABLE_CCL_EXECUTION_TIME_TASK));
        newParam.put(ConnectionProperties.CCL_EXECUTION_TIME_DETECT_INTERVAL,
            paramManager.get(ConnectionParams.CCL_EXECUTION_TIME_DETECT_INTERVAL));
        return newParam;
    }

    @Override
    void validateParams(Map<String, String> newParams) {
        for (Map.Entry<String, String> keyAndVal : newParams.entrySet()) {
            final String key = keyAndVal.getKey();
            final String value = keyAndVal.getValue();
            ParamValidationUtils.validateSqlIdleTimeoutParam(key, value);
        }
    }

}
