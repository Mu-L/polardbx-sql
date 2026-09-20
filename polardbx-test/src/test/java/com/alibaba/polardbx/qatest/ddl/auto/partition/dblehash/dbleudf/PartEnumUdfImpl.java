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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartEnumUdfImpl extends UserDefinedJavaFunction {

    private DblePartitionAlgorithm routeFunc;

    public PartEnumUdfImpl() {
    }

    public PartEnumUdfImpl(int type, int defaultNode, Map<String,Integer> mappings) {
        List<Pair<String, Integer>> partMappings = new ArrayList<>();
        for (Map.Entry<String, Integer> mapItem : mappings.entrySet()) {
            partMappings.add(new Pair<>(mapItem.getKey(), mapItem.getValue()));
        }
        init(type, defaultNode, partMappings);
    }
    protected void init(int type, int defaultNode, List<Pair<String, Integer>> mappings) {
        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByFileMap);
        params.setType(type);
        params.setDefaultNode(defaultNode);
        params.setPartitionMappings(mappings);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }
}
