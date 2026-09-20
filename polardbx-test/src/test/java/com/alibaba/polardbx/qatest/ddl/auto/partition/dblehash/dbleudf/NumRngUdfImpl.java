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
import com.alibaba.polardbx.optimizer.core.function.calc.dble.util.Pair;

import java.util.List;

public class NumRngUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public NumRngUdfImpl() {
    }

    public NumRngUdfImpl(int defaultNode, List<Pair<String, Integer>> partitionMappings) {
        init(defaultNode, partitionMappings);
    }

    private void init(int defaultNode, List<Pair<String, Integer>> partitionMappings) {
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_AutoPartitionByLong);
        params.setDefaultNode(defaultNode);
        params.setPartitionMappings(partitionMappings);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }
}
