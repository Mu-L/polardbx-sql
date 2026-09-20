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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.operator.DirectExchanger;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverExec;
import com.alibaba.polardbx.executor.mpp.operator.LocalExchanger;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.mpp.planner.PipelineProperties;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.operator.AbstractJoinExec;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PipelineFactory {

    protected final ExecutorFactory produceFactory;
    protected final ConsumeExecutorFactory consumeFactory;
    protected PipelineFragment fragment;

    public PipelineFactory(ExecutorFactory produceFactory, ConsumeExecutorFactory consumeFactory,
                           PipelineFragment fragment) {
        this.produceFactory = produceFactory;
        this.consumeFactory = consumeFactory;
        this.fragment = fragment;
    }

    public DriverExec createDriverExec(ExecutionContext context, DriverContext driverContext, int index) {
        ExecutorMode executorMode = context.getExecuteMode();
        switch (executorMode) {
        case MPP:
            if (context.getParamManager().getBoolean(ConnectionParams.ENABLE_QUERY_MEMORY_TRACKER)
                && ConfigDataMode.isColumnarMode()) {
                return doCreateDriverExecWithMemTracker(context, driverContext, index);
            }
        default:
            return doCreateDriverExecWithoutMemTracker(context, driverContext, index);
        }
    }

    public DriverExec doCreateDriverExecWithoutMemTracker(ExecutionContext context, DriverContext driverContext,
                                                          int index) {
        Executor producer = produceFactory.createExecutor(context, index);
        ConsumerExecutor consumerExecutor = consumeFactory.createExecutor(context, index);
        int pipelineId = fragment.getPipelineId();
        return new DriverExec(pipelineId, driverContext, producer, consumerExecutor, fragment.getParallelism());
    }

    public DriverExec doCreateDriverExecWithMemTracker(ExecutionContext context, DriverContext driverContext,
                                                       int parallelIndex) {
        Executor producer = produceFactory.createExecutor(context, parallelIndex);

        // all producer executors from root.
        List<Executor> producerList = new ArrayList<>();
        // all consumer executors.
        List<ConsumerExecutor> consumerList = new ArrayList<>();

        // traverse the executor pipe and build memory id.
        int currentDepth = setProducerOperatorMemoryId(producerList, producer, driverContext, parallelIndex);

        ConsumerExecutor consumerExecutor = consumeFactory.createExecutor(context, parallelIndex);

        // traverse the consumer and build memory id.
        setConsumerOperatorMemoryId(consumerList, consumerExecutor, driverContext, parallelIndex, currentDepth + 1);

        int pipelineId = fragment.getPipelineId();

        return new DriverExec(pipelineId, driverContext, producer, consumerExecutor, fragment.getParallelism());
    }

    // traverse
    private int setProducerOperatorMemoryId(List<Executor> producerList, Executor producer,
                                            DriverContext driverContext, final int parallelIndex) {
        producerList.add(producer);

        int depth;
        Executor input;
        if (producer.getInputs() == null || producer.getInputs().isEmpty()) {
            // source exec.
            depth = 0;
        } else {
            if (producer instanceof AbstractJoinExec && producer.getInputs().size() > 1) {
                // binary inputs, index = 1 is outer input.
                input = producer.getInputs().get(1);
            } else {
                // unary input.
                input = producer.getInputs().get(0);
            }

            int childDepth = setProducerOperatorMemoryId(producerList, input, driverContext, parallelIndex);
            depth = childDepth + 1;
        }

        OperatorMemoryOwnerId producerMemoryOwnerId =
            buildProducerMemoryOwnerId(parallelIndex, depth, driverContext, producer);
        Preconditions.checkNotNull(producerMemoryOwnerId);
        producer.setProducerMemoryOwnerId(producerMemoryOwnerId);
        producerMemoryOwnerId.setOwner(producer);

        return depth;
    }

    // traverse
    private void setConsumerOperatorMemoryId(List<ConsumerExecutor> consumerList, ConsumerExecutor consumer,
                                             DriverContext driverContext, int parallelIndex, int depth) {
        consumerList.add(consumer);

        OperatorMemoryOwnerId consumerMemoryOwnerId =
            buildConsumerMemoryOwnerId(parallelIndex, depth, driverContext, consumer);
        Preconditions.checkNotNull(consumerMemoryOwnerId);
        consumer.setConsumerOperatorMemoryOwnerId(consumerMemoryOwnerId);
        consumerMemoryOwnerId.setOwner(consumer);

        if (consumer instanceof LocalExchanger) {
            // only local exchanger has downstream operators.
            List<ConsumerExecutor> downStreamOperators = ((LocalExchanger) consumer).getExecutors();

            if (downStreamOperators == null || downStreamOperators.isEmpty()) {
                return;
            }

            if (consumer instanceof DirectExchanger) {
                // case 1: direct downstream, use same pipeline and parallelism.
                // DirectExchanger
                ConsumerExecutor downstream = downStreamOperators.get(0);
                if (downstream.getConsumerMemoryOwnerId() != null) {
                    // set by other driver.
                    return;
                }

                consumerList.add(downstream);

                OperatorMemoryOwnerId downstreamMemoryOwnerId =
                    buildConsumerMemoryOwnerId(parallelIndex, depth + 1, driverContext, downstream);
                Preconditions.checkNotNull(downstreamMemoryOwnerId);
                downstream.setConsumerOperatorMemoryOwnerId(downstreamMemoryOwnerId);
                downstreamMemoryOwnerId.setOwner(downstream);

            } else {
                // case 2: multi thread will write into one downstream, use partition id to replace the parallel id.
                // BroadcastExchanger, PartitioningBucketExchanger, PartitioningExchanger, RandomExchanger, SingleExchanger

                for (int partitionIndex = 0; partitionIndex < downStreamOperators.size(); partitionIndex++) {
                    ConsumerExecutor downstream = downStreamOperators.get(partitionIndex);

                    if (downstream.getConsumerMemoryOwnerId() != null) {
                        // set by other driver.
                        continue;
                    }

                    consumerList.add(downstream);

                    // query -> stage -> pipeline -> partitionIndex
                    String queryId = driverContext.getTaskId().getQueryId();

                    long stagePipelineId = driverContext.getStagePipelineId();

                    OperatorMemoryOwnerId downstreamMemoryOwnerId = MemoryTrackerManager.getGlobalMemoryTrackerManager()
                        .createQueryMemoryOwnerId(queryId)
                        .createChild(stagePipelineId)
                        .createChild(partitionIndex)
                        .createChild(depth + 1, downstream.getClass().getSimpleName());

                    Preconditions.checkNotNull(downstreamMemoryOwnerId);
                    downstream.setConsumerOperatorMemoryOwnerId(downstreamMemoryOwnerId);
                    downstreamMemoryOwnerId.setOwner(downstream);
                }

            }
        }
    }

    protected static OperatorMemoryOwnerId buildConsumerMemoryOwnerId(int parallelism, int consumerDepth,
                                                                      DriverContext consumerDriverContext,
                                                                      ConsumerExecutor consumerExecutor) {

        final String queryId = consumerDriverContext.getTaskId().getQueryId();

        final long stagePipelineId = consumerDriverContext.getStagePipelineId();

        final long driverId = parallelism;

        final long operatorId = consumerDepth;

        OperatorMemoryOwnerId consumerOperatorMemoryOwnerId = MemoryTrackerManager.getGlobalMemoryTrackerManager()
            .createQueryMemoryOwnerId(queryId)
            .createChild(stagePipelineId)
            .createChild(driverId)
            .createChild(operatorId, consumerExecutor.getClass().getSimpleName());

        return consumerOperatorMemoryOwnerId;
    }

    protected static OperatorMemoryOwnerId buildProducerMemoryOwnerId(int probeParallelism, int producerDepth,
                                                                      DriverContext producerDriverContext,
                                                                      Executor producerExecutor) {

        final String queryId = producerDriverContext.getTaskId().getQueryId();

        final long stagePipelineId = producerDriverContext.getStagePipelineId();

        final long driverId = probeParallelism;

        final long operatorId = producerDepth;

        OperatorMemoryOwnerId producerOperatorMemoryOwnerId = MemoryTrackerManager.getGlobalMemoryTrackerManager()
            .createQueryMemoryOwnerId(queryId)
            .createChild(stagePipelineId)
            .createChild(driverId)
            .createChild(operatorId, producerExecutor.getClass().getSimpleName());

        return producerOperatorMemoryOwnerId;
    }

    public PipelineFragment getFragment() {
        return fragment;
    }

    public List<LogicalView> getLogicalView() {
        return fragment.getLogicalView();
    }

    public SplitInfo getSource(Integer id) {
        return fragment.getSource(id);
    }

    public Set<Integer> getDependency() {
        return fragment.getDependency();
    }

    public int getPipelineId() {
        return fragment.getPipelineId();
    }

    public int getParallelism() {
        return fragment.getParallelism();
    }

    public ExecutorFactory getProduceFactory() {
        return produceFactory;
    }

    public ConsumeExecutorFactory getConsumeFactory() {
        return consumeFactory;
    }

    public PipelineProperties getProperties() {
        return fragment.getProperties();
    }
}
