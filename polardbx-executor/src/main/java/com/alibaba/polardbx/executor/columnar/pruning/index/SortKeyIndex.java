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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.index.Index;
import org.roaringbitmap.RoaringBitmap;

import java.util.Objects;

public abstract class SortKeyIndex extends BaseColumnIndex {

    /**
     * col index in the orc file, start with 0
     */
    protected final int colId;

    /**
     * column type
     */
    protected final DataType dt;

    protected final boolean isAsc;

    protected SortKeyIndex(long rgNum, int colId, DataType dt, boolean isAsc) {
        super(rgNum);
        this.colId = colId;
        this.dt = dt;
        this.isAsc = isAsc;
    }

    abstract public void pruneEqual(Object param, RoaringBitmap cur, IndexPruneContext ipc);

    abstract public void pruneRange(Object startObj, Object endObj, RoaringBitmap cur, IndexPruneContext ipc);

    @Override
    public DataType getColumnDataType(int colId) {
        return dt;
    }

    public int getColId() {
        return colId;
    }

    public DataType getDt() {
        return dt;
    }

    protected Pair<Integer, Integer> handleInterval(Pair<Integer, Boolean> sIndex, Pair<Integer, Boolean> eIndex) {
        int startRgIndex;
        int endRgIndex;

        if (!isAsc) {
            //step1: (startRgIndex, endRgIndex]
            if (!eIndex.getValue() && !Objects.equals(sIndex.getKey(), eIndex.getKey())) {
                endRgIndex = eIndex.getKey() - 1;
            } else {
                endRgIndex = eIndex.getKey();
            }

            if (sIndex.getValue()) {
                startRgIndex = sIndex.getKey() - 1;
            } else {
                startRgIndex = sIndex.getKey();
            }
            //step2: [startRgIndex', endRgIndex')
            int preStartRgIndex = startRgIndex;
            startRgIndex = (int) rgNum() - endRgIndex - 1;
            endRgIndex = (int) rgNum() - preStartRgIndex - 1;
        } else {
            // if lower rg index was not included, plus it was different from upper index, then add 1 to lower rg index
            //  [startRgIndex, endRgIndex)
            if (!sIndex.getValue() && !Objects.equals(sIndex.getKey(), eIndex.getKey())) {
                startRgIndex = sIndex.getKey() + 1;
            } else {
                startRgIndex = sIndex.getKey();
            }
            if (eIndex.getValue()) {
                endRgIndex = eIndex.getKey() + 1;
            } else {
                endRgIndex = eIndex.getKey();
            }
        }
        return Pair.of(startRgIndex, endRgIndex);
    }

}
