/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.alibaba.polardbx.qatest.ddl.auto.partition.dblehash.dbleudf;

import com.alibaba.polardbx.optimizer.core.function.calc.UserDefinedJavaFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.DblePartitionAlgorithm;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.BuildParams;
import com.alibaba.polardbx.optimizer.core.function.calc.dble.builder.DbleAlgorithmBuilder;

public class PartStrUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public PartStrUdfImpl() {
    }

    public PartStrUdfImpl(String count, String partLen, String hashSlice) {
        init(count, partLen, hashSlice);
    }

    private void init(String count, String partLen, String hashSlice) {

        // 1=PartitionByLong
        // 2=PartitionByString
        // 3=PartitionByFileMap
        // 4=AutoPartitionByLong
        // 5=PartitionByPattern
        // 6=PartitionByDate
        // 7=PartitionByJumpConsistentHash
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByString);
        params.setPartitionCount(count);
        params.setPartitionLength(partLen);
        params.setHashSlice(hashSlice);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }
}
