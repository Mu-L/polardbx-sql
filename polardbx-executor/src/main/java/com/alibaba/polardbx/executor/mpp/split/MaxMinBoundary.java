package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MaxMinBoundary {

    private List<Object> boundaryValues;

    private Comparator<List<Object>> comparator;

    private List<OrderByOption> orderByOptions;

    public MaxMinBoundary(List<OrderByOption> orderByOptions,
                          List<ColumnMeta> columnMetas) {
        this.orderByOptions = orderByOptions;
        this.comparator = new Comparator<List<Object>>() {
            @Override
            public int compare(List<Object> row1, List<Object> row2) {
                for (int i = 0; i < row1.size(); i++) {
                    Object c1 = row1.get(i);
                    Object c2 = row2.get(i);
                    if (c1 == null && c2 == null) {
                        continue;
                    }
                    OrderByOption orderByOption = orderByOptions.get(i);
                    int n =
                        ExecUtils.comp(c1, c2, columnMetas.get(orderByOption.index).getDataType(), orderByOption.asc);
                    if (n == 0) {
                        continue;
                    }
                    return n;
                }
                return 0;
            }

        };
    }

    public synchronized List<Object> getBoundaryValues() {
        return boundaryValues;
    }

    public synchronized void setBoundaryValues(Chunk.ChunkRow row) {
        List<Object> arrayListComp = new ArrayList<>();
        List<Object> arrayListJava = new ArrayList<>();
        for (int i = 0; i < orderByOptions.size(); i++) {
            arrayListComp.add(row.getObjectForCmp(orderByOptions.get(i).index));
            arrayListJava.add(row.getJavaValues().get(orderByOptions.get(i).index));
        }
        if (boundaryValues == null) {
            boundaryValues = arrayListJava;
        } else {
            boundaryValues = comparator.compare(boundaryValues, arrayListComp) > 0 ? boundaryValues : arrayListJava;
        }
    }
}
