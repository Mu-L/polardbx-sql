package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.GroupTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

public class GroupTopExecutorFactory extends ExecutorFactory {

    private GroupTopN groupTopN;
    private int parallelism;
    private int taskNumber;
    private int rowCount;
    private List<Executor> executors = new ArrayList<>();

    private final SpillerFactory spillerFactory;

    private final List<DataType> inputDataTypes;

    public GroupTopExecutorFactory(GroupTopN groupTopN, int parallelism, int taskNumber, int rowCount,
                                   SpillerFactory spillerFactory,
                                   List<DataType> inputDataTypes) {
        this.groupTopN = groupTopN;
        this.parallelism = parallelism;
        this.taskNumber = taskNumber;
        this.rowCount = rowCount;
        this.spillerFactory = spillerFactory;
        this.inputDataTypes = inputDataTypes;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutors(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutors(context);
    }

    private synchronized List<Executor> createAllExecutors(ExecutionContext context) {

        int expectedOutputRowCount = rowCount / (taskNumber * parallelism);
        int estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);

        if (executors.isEmpty()) {
            for (int j = 0; j < parallelism; j++) {
                Executor exec =
                    new GroupTopNExec(inputDataTypes, groupTopN, estimateHashTableSize, spillerFactory,
                        getFetchValue(context), context);
                registerRuntimeStat(exec, groupTopN, context);
                executors.add(exec);
            }
        }
        return executors;
    }

    private long getFetchValue(ExecutionContext context) {
        long limit = -1;
        RexNode fetch = groupTopN.getFetch();
        if (fetch == null) {
            return Long.MAX_VALUE;
        }

        Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
        limit = getRexParam(fetch, params);
        // For simplicity, assuming fetch is a constant value
        // In practice, this would need to handle parameterized fetch values
        return limit == -1 ? 100L : limit;
    }
}