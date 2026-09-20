package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import java.util.Map;
import java.util.TreeMap;

public class AlterTableToggleFullScanPreparedData extends DdlPreparedData {

    public AlterTableToggleFullScanPreparedData() {
    }

    private boolean enable = false;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }
}
