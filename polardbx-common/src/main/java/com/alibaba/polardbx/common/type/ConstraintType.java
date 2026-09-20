package com.alibaba.polardbx.common.type;

public enum ConstraintType {
    PRIMARY_KEY("PRIMARY"), UNIQUE("UNIQUE"), FOREIGN_KEY("FOREIGN KEY"), CHECK("CHECK");

    public final String name;
    public final String name_lcase;

    ConstraintType(String name) {
        this.name = name;
        this.name_lcase = name.toLowerCase();
    }

    public String getText() {
        return name;
    }
}
