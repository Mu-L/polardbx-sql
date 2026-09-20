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

public class PartLongUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public PartLongUdfImpl(String count, String partLen) {
        init(count, partLen);
    }

    private void init(String count, String partLen) {
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByLong);
        params.setPartitionCount(count);
        params.setPartitionLength(partLen);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }
}
