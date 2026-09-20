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

public class JumpStrUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public JumpStrUdfImpl() {
    }

    public JumpStrUdfImpl(int count, String hashSlice) {
        init(count, hashSlice);
    }

    private void init(int count, String hashSlice) {
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByJumpConsistentHash);
        params.setPartitionCount(String.valueOf(count));
        params.setHashSlice(hashSlice);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }
}
