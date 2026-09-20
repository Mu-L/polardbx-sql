package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;

import java.util.Map;

public class ShowRoutingRulesSyncAction implements ISyncAction {
    public ShowRoutingRulesSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("RoutingRules");
        result.addColumn("ID", DataTypes.LongType);
        result.addColumn("COUNT", DataTypes.LongType);

        result.initMeta();
        Map<Long, Long> collects = RoutingRuleManager.getInstance().getClassifier().getIdCounterMap();
        for (Map.Entry<Long, Long> collect : collects.entrySet()) {
            result.addRow(new Object[] {
                collect.getKey(),
                collect.getValue()
            });
        }
        return result;
    }
}
