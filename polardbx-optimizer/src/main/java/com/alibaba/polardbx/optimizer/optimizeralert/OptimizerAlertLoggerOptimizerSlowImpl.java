package com.alibaba.polardbx.optimizer.optimizeralert;

public class OptimizerAlertLoggerOptimizerSlowImpl extends OptimizerAlertLoggerBaseImpl {
    public OptimizerAlertLoggerOptimizerSlowImpl() {
        super();
        this.optimizerAlertType = OptimizerAlertType.OPTIMIZER_SLOW;
    }
}
