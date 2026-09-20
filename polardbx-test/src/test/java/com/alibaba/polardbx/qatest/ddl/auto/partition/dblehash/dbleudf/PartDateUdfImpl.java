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

public class PartDateUdfImpl extends UserDefinedJavaFunction {
    private DblePartitionAlgorithm routeFunc;

    public PartDateUdfImpl(int defaultNode,
                           String dateFormat,
                           String sBeginDate,
                           String sEndDate,
                           String sPartitionDay) {
        init(defaultNode, dateFormat, sBeginDate, sEndDate, sPartitionDay);
    }

    private void init(int defaultNode,
                      String dateFormat,
                      String sBeginDate,
                      String sEndDate,
                      String sPartitionDay) {
//        routeFunc = new PartitionByDate();
//        routeFunc.setDefaultNode(defaultNode);
//        routeFunc.setDateFormat(dateFormat);
//        routeFunc.setsBeginDate(sBeginDate);
//        routeFunc.setsEndDate(sEndDate);
//        routeFunc.setsPartionDay(sPartitionDay);
//        routeFunc.init();

        BuildParams params = new BuildParams();
        params.setAlgorithmType(DbleAlgorithmBuilder.DBLE_PartitionByDate);
        params.setDefaultNode(defaultNode);
        params.setDateFormat(dateFormat);
        params.setsBeginDate(sBeginDate);
        params.setsEndDate(sEndDate);
        params.setsPartitionDay(sPartitionDay);
        routeFunc = DbleAlgorithmBuilder.buildAlgorithm(params);
    }

    @Override
    public Object compute(Object[] args) {
        return  routeFunc.compute(args);
    }
}
