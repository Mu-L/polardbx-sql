package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.mpp.metadata.SplitType;

import java.util.ArrayList;
import java.util.List;

public class DynamicMergeSortJdbcSplit extends JdbcSplit {

    private JdbcSplit jdbcSplit;
    private MaxMinBoundary boundary;

    public DynamicMergeSortJdbcSplit(JdbcSplit jdbcSplit, MaxMinBoundary boundary) {
        super(jdbcSplit);
        this.jdbcSplit = jdbcSplit;
        this.boundary = boundary;
    }

    @Override
    public BytesSql getUnionBytesSql(boolean ignore) {
        return jdbcSplit.getUnionBytesSql(ignore);
    }

    @Override
    public List<ParameterContext> getFlattedParams() {
        if (flattenParams == null) {
            synchronized (this) {
                if (flattenParams == null) {
                    List<ParameterContext> params = jdbcSplit.getFlattedParams();
                    flattenParams = new ArrayList<>(params.size());

                    for (ParameterContext parameterContext : params) {
                        if (boundary.getBoundaryValues() != null
                            && parameterContext.getParameterMethod() == ParameterMethod.setDelegateDynamicSort) {
                            int index = (int) parameterContext.getArgs()[0];
                            if (index == 0) {
                                flattenParams.add(
                                    new ParameterContext(ParameterMethod.setBoolean, new Object[] {
                                        parameterContext.getArgs()[0], false}));
                            } else {
                                flattenParams.add(
                                    new ParameterContext(ParameterMethod.setObject1, new Object[] {
                                        parameterContext.getArgs()[0], boundary.getBoundaryValues().get(index - 1)}));
                            }
                        } else {
                            flattenParams.add(parameterContext);
                        }
                    }
                }
            }
        }
        return flattenParams;
    }

    @Override
    public void reset() {
        this.jdbcSplit.reset();
    }

    @Override
    public SplitType getSplitType() {
        return SplitType.DYNAMIC_JDBC;
    }
}
