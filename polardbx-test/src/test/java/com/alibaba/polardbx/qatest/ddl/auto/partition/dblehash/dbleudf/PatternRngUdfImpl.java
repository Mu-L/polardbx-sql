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

public class PatternRngUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public PatternRngUdfImpl() {
    }

    public PatternRngUdfImpl(int defaultNode, int partitionValue, List<Pair<String, Integer>> partitionMappings) {
        init(defaultNode, partitionValue, partitionMappings);
    }

    private void init(int defaultNode, int partitionValue, List<Pair<String, Integer>> partitionMappings) {
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByPattern);
        params.setDefaultNode(defaultNode);
        params.setPartitionValue(partitionValue);
        params.setPartitionMappings(partitionMappings);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return routeFunc.compute(args);
    }
}
