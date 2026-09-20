/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.optimizer.utils.OrderByOption;

/**
 * @version 1.0
 */
public class IndexColumnMeta {

    // might be null for function index, not null for primary index
    private final ColumnMeta columnMeta;
    private final long subPart;
    private final OrderByOption orderByOption;

    public IndexColumnMeta(ColumnMeta columnMeta, long subPart, OrderByOption orderByOption) {
        this.columnMeta = columnMeta;
        this.subPart = subPart;
        this.orderByOption = orderByOption;
    }

    public ColumnMeta getColumnMeta() {
        return columnMeta;
    }

    public String getName() {
        if (columnMeta == null) {
            return null;
        }
        return columnMeta.getName();
    }

    public boolean hasColumn() {
        return columnMeta != null;
    }

    public long getSubPart() {
        return subPart;
    }

    public OrderByOption getOrderByOption() {
        return orderByOption;
    }

    @Override
    public String toString() {
        return (columnMeta == null ? "null" : columnMeta.getName())
            + (subPart == 0 ? "" : "(" + subPart + ")")
            + (orderByOption.isAsc() ? " ASC" : " DESC");
    }
}
