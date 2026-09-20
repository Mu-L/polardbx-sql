package com.alibaba.polardbx.server.encdb.handler;

public enum EncdbRuleVersion {
    VERSION_1(1);

    int version;

    EncdbRuleVersion(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }
}
