package com.alibaba.polardbx.gms.ha.impl;

import lombok.Data;

import java.util.Map;

@Data
public class ColumnarHaContext {
    public enum ColumnarHaStatus {
        NORMAL,
        SWITCHING
    }

    private volatile String currAvailableNodeAddr;

    private volatile int currRpcPort;

    /**
     * columnar IP addr -> columnar HA info
     */
    private volatile Map<String, ColumnarHaInfo> allColumnarHaInfoMap;

    private volatile ColumnarHaStatus haStatus = ColumnarHaStatus.NORMAL;

    private volatile long leaseTime;

    public ColumnarHaContext(String currAvailableNodeAddr, int currRpcPort,
                             Map<String, ColumnarHaInfo> allColumnarHaInfoMap) {
        this.currAvailableNodeAddr = currAvailableNodeAddr;
        this.currRpcPort = currRpcPort;
        this.allColumnarHaInfoMap = allColumnarHaInfoMap;
    }
}
