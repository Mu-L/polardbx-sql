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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Comparator;
import java.util.List;

public class ChunkWithPositionComparator {
    private final Comparator<Chunk.ChunkRow> rowComparator;
    private final List<OrderByOption> orderBys;
    private final List<DataType> columnMetas;

    public ChunkWithPositionComparator(List<OrderByOption> orderBys, List<DataType> columnMetas) {
        this.rowComparator = ExecUtils.getAssertedSameTypeComparator(orderBys, columnMetas);
        this.orderBys = orderBys;
        this.columnMetas = columnMetas;
    }

    public int compareTo(Chunk left, int leftPosition, Chunk right, int rightPosition) {
        int n = 0;
        final int orderByOptionSize = orderBys.size();
        for (int i = 0; i < orderByOptionSize; i++) {
            OrderByOption option = orderBys.get(i);

            // NOTE: null == null
            n = left.compare(leftPosition, right, rightPosition, option.index);

            if (n == 0) {
                continue;
            }

            if (!option.asc) {
                n = n < 0 ? 1 : -1;
            }

            break;
        }
        return n;
    }

    public int compareTo(Chunk.ChunkRow row, Chunk right, int rightPosition) {
        return rowComparator.compare(row, right.rowAt(rightPosition));
    }
}