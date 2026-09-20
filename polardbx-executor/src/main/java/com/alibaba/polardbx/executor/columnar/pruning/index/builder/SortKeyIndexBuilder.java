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

package com.alibaba.polardbx.executor.columnar.pruning.index.builder;

import com.alibaba.polardbx.executor.columnar.pruning.index.LongSortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.StringSortKeyIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.orc.OrcProto;

import java.util.Iterator;
import java.util.List;

/**
 * @author fangwu
 */
public class SortKeyIndexBuilder {
    private int colId;
    private DataType dt;
    private boolean isAsc;
    private LongArrayList longDataEntry = new LongArrayList();
    private List<String> stringDataEntry = Lists.newArrayList();

    public void appendDataEntry(OrcProto.ColumnStatistics columnStatistics) {
        if (DataTypeUtil.isStringSqlType(dt)) {
            OrcProto.StringStatistics stringStatistics = columnStatistics.getStringStatistics();
            stringDataEntry.add(
                stringStatistics.getMinimum().isEmpty() ? stringStatistics.getLowerBound() :
                    stringStatistics.getMinimum());
            stringDataEntry.add(
                stringStatistics.getMaximum().isEmpty() ? stringStatistics.getUpperBound() :
                    stringStatistics.getMaximum());

        } else {
            OrcProto.IntegerStatistics integerStatistics = columnStatistics.getIntStatistics();
            longDataEntry.add(integerStatistics.getMinimum());
            longDataEntry.add(integerStatistics.getMaximum());
        }
    }

    public void appendDataEntry(String min, String max) {
        if (DataTypeUtil.isStringSqlType(dt)) {
            stringDataEntry.add(min);
            stringDataEntry.add(max);
        } else {
            throw new IllegalArgumentException("dt is not string type, dt = " + dt);
        }
    }

    public void appendAllLongDataEntry(long[] data) {
        longDataEntry.addElements(longDataEntry.size(), data);
    }

    public void appendDataEntry(long min, long max) {
        if (DataTypeUtil.isStringSqlType(dt)) {
            throw new IllegalArgumentException("dt is string type, dt = " + dt);
        } else {
            longDataEntry.add(min);
            longDataEntry.add(max);
        }
    }

    //only for test
    public void appendMockDataEntry(Object min, Object max, DataType dt) {
        if (DataTypeUtil.isStringSqlType(dt)) {
            stringDataEntry.add((String) min);
            stringDataEntry.add((String) max);
        } else {
            longDataEntry.add(((Number) min).longValue());
            longDataEntry.add(((Number) max).longValue());
        }
    }

    public SortKeyIndex build() {
        // Graceful skip: when the caller decided not to build a sort-key index
        // for this file (e.g. DECIMAL/FLOAT sort key falling back to full scan),
        // dt is left unset or no data entry is appended. Return null so the
        // IndexPruner treats sort-key pruning as disabled for this file instead
        // of asserting on empty data.
        if (dt == null) {
            return null;
        }
        if (DataTypeUtil.isStringSqlType(dt)) {
            if (stringDataEntry.isEmpty()) {
                return null;
            }
            String[] data = new String[stringDataEntry.size()];
            Iterator<String> it = stringDataEntry.iterator();
            int cur = 0;
            while (it.hasNext()) {
                String l = it.next();
                data[cur++] = l;
            }
            return StringSortKeyIndex.build(colId, data, dt, isAsc);
        } else {
            if (longDataEntry.isEmpty()) {
                return null;
            }
            long[] data = new long[longDataEntry.size()];
            data = longDataEntry.toLongArray(data);
            return LongSortKeyIndex.build(colId, data, dt, isAsc);
        }
    }

    public void setDt(DataType dt) {
        this.dt = dt;
    }

    public DataType getDt() {
        return this.dt;
    }

    public void setColId(int colId) {
        this.colId = colId;
    }

    public void setAsc(boolean isAsc) {
        this.isAsc = isAsc;
    }
}
