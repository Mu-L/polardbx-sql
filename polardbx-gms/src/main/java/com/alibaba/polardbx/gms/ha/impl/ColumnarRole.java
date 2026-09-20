package com.alibaba.polardbx.gms.ha.impl;

public enum ColumnarRole {
    MASTER("master"),
    STANBY("standby");

    final private String role;

    ColumnarRole(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }
}
