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

package com.alibaba.polardbx.optimizer.partition.boundspec;

/**
 * Use partition count to
 * split all the hash value space and compute hash partition bound value of hash space
 *
 * @author chenghui.lch
 */
public class UdfHashPartBoundValBuilder implements PartBoundValBuilder {

    protected int partitionCount;
    protected Long[] upBoundValArr;
    protected boolean genBoundValsForDbleRouting = false;
    protected boolean lastPartUseCatchAllBndVal = true;

//    public UdfHashPartBoundValBuilder(int partitionCount) {
//        this(partitionCount, false, true);
//    }

    public UdfHashPartBoundValBuilder(int partitionCount,
                                      boolean genBoundValsForDbleRouting,
                                      boolean lastPartUseCatchAllBndVal) {
        this.partitionCount = partitionCount;
        this.upBoundValArr = new Long[partitionCount];
        this.genBoundValsForDbleRouting = genBoundValsForDbleRouting;
        this.lastPartUseCatchAllBndVal = lastPartUseCatchAllBndVal;
        partitionLongValueSpace(this.partitionCount);
    }

    @Override
    public Object getPartBoundVal(int partPosition) {
        return this.upBoundValArr[partPosition - 1];
    }

    protected Object partitionLongValueSpaceForDbleRoute(int partitionCount) {
        assert partitionCount > 0;
        for (int i = 0; i < upBoundValArr.length - 1; i++) {
            upBoundValArr[i] = Long.valueOf(i + 1);
        }
        if (lastPartUseCatchAllBndVal) {
            upBoundValArr[upBoundValArr.length - 1] = Long.MAX_VALUE;
        } else {
            upBoundValArr[upBoundValArr.length - 1] = Long.valueOf(partitionCount);
        }

        return upBoundValArr;
    }

    protected Object partitionLongValueSpace(int partitionCount) {
        if (isGenBoundValsForDbleRouting()) {
            return partitionLongValueSpaceForDbleRoute(partitionCount);
        }

        assert partitionCount > 0;
        if (partitionCount == 1) {
            upBoundValArr[0] = Long.MAX_VALUE;
            return upBoundValArr;
        }
        long minVal = Long.MIN_VALUE;
        long maxVal = Long.MAX_VALUE;
        long valInterval = 2 * (maxVal / partitionCount);
        long lastBoundVal = maxVal;

        upBoundValArr[partitionCount - 1] = lastBoundVal;
        for (int j = partitionCount - 2; j >= 0; j--) {
            lastBoundVal = lastBoundVal - valInterval;
            upBoundValArr[j] = lastBoundVal;
        }
        return upBoundValArr;
    }

    public boolean isGenBoundValsForDbleRouting() {
        return genBoundValsForDbleRouting;
    }

    public void setGenBoundValsForDbleRouting(boolean genBoundValsForDbleRouting) {
        this.genBoundValsForDbleRouting = genBoundValsForDbleRouting;
    }
}
