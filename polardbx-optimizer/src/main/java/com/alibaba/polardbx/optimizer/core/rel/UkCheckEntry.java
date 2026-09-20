/*
 * Copyright [2013-2021] Alibaba Cloud All rights reserved
 */
package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;

/**
 * Unique key duplicate-check information for one physical table.
 */
public class UkCheckEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> ukColumns;
    private final String localIndexName;
    private final boolean partitionLocal;

    public UkCheckEntry(List<String> ukColumns, String localIndexName, boolean partitionLocal) {
        this.ukColumns = ImmutableList.copyOf(ukColumns);
        this.localIndexName = localIndexName;
        this.partitionLocal = partitionLocal;
    }

    public List<String> getUkColumns() {
        return ukColumns;
    }

    public String getLocalIndexName() {
        return localIndexName;
    }

    public boolean isPartitionLocal() {
        return partitionLocal;
    }
}
