package com.alibaba.polardbx.optimizer.optimizeralert.statisticalert;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertLoggerBaseImpl;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;

import java.util.Map;

import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.isStatisticAlertType;

/**
 * @author pangzhaoxing
 */
public class StatisticAlertLoggerBaseImpl extends OptimizerAlertLoggerBaseImpl {

    protected final static Logger logger = LoggerFactory.getLogger("STATISTICS", true);

    public StatisticAlertLoggerBaseImpl(OptimizerAlertType optimizerAlertType) {
        super();
        if (!isStatisticAlertType(optimizerAlertType)) {
            throw new IllegalArgumentException(optimizerAlertType == null ? null : optimizerAlertType.name());
        }
        this.optimizerAlertType = optimizerAlertType;
    }

    @Override
    public boolean logDetail(ExecutionContext ec, Object object) {
        return logDetail(ec, object, null);
    }

    @Override
    public boolean logDetail(ExecutionContext ec, Object object, Map<String, Object> extra) {
        String log = String.format("statistic_alert_type{ %s }: schema{ %s } trace_id { %s } object { %s }",
            optimizerAlertType.name(),
            ec == null ? null : ec.getSchemaName(),
            ec == null ? null : ec.getTraceId(),
            object == null ? null : object.toString());
        if (extra != null) {
            for (Map.Entry<String, Object> entry : extra.entrySet()) {
                log += String.format(" %s{ %s }", entry.getKey(), entry.getValue());
            }
        }
        logger.warn(log);
        return true;
    }

}
