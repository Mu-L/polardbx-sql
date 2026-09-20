package com.alibaba.polardbx.gms.ha.impl;

import lombok.Data;

@Data
public class ColumnarHaInfo {

    private String addr;
    private ColumnarRole role;
    private boolean isHealthy;
    private int rpcPort;

    public ColumnarHaInfo(String addr, ColumnarRole role, boolean isHealthy, int rpcPort) {
        this.addr = addr;
        this.role = role;
        this.isHealthy = isHealthy;
        this.rpcPort = rpcPort;
    }
}
