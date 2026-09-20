package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import lombok.Getter;

@Getter
public class CCLDetectConfig {

    private boolean isDryRun;
    private boolean isCclDetectEnable;
    private String rootColumn;
    private int connectionLimit;
    private int killBatch;
    private int killMinConcurrency;
    private int slowThreshold;
    private int maxThreshold;
    private long dnDelayInterval;
    private int dnRuleExpireTime;
    private static final CCLDetectConfig CCL_DETECT_CONFIG = new CCLDetectConfig();

    public static CCLDetectConfig getInstance() {
        return CCL_DETECT_CONFIG;
    }

    public CCLDetectConfig() {
        this.refresh();
    }

    public void refresh() {
        isDryRun = DynamicConfig.getInstance().isCclDetectDryRun();
        isCclDetectEnable = DynamicConfig.getInstance().isCclDetectEnable();
        rootColumn = DynamicConfig.getInstance().getCclDetectRootColumn();
        connectionLimit = DynamicConfig.getInstance().getCclDetectConnectionLimit();
        killBatch = DynamicConfig.getInstance().getCclDetectKillBatch();
        killMinConcurrency = DynamicConfig.getInstance().getCclDetectKillMinConcurrency();
        slowThreshold = DynamicConfig.getInstance().getCclDetectSlowThreshold();
        maxThreshold = DynamicConfig.getInstance().getCclDetectMaxThreshold();
        dnDelayInterval = DynamicConfig.getInstance().getCclDetectDnDelayInterval();
        dnRuleExpireTime = DynamicConfig.getInstance().getCclDetectDnRuleExpireTime();
    }

}
