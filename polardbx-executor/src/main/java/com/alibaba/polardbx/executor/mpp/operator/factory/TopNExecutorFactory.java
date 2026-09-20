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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.mpp.planner.EarlyStopManager;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SpilledTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.DateTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DecimalTopNHeap;
import com.alibaba.polardbx.executor.operator.util.DefaultTopNHeap;
import com.alibaba.polardbx.executor.operator.util.GlobalTopNThreshold;
import com.alibaba.polardbx.executor.operator.util.IntTopNHeap;
import com.alibaba.polardbx.executor.operator.util.LongTopNHeap;
import com.alibaba.polardbx.executor.operator.util.TopNThresholdFilter;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.getRexParam;

public class TopNExecutorFactory extends ExecutorFactory {

    private TopN topN;
    private int parallelism;
    private SpillerFactory spillerFactory;
    private List<DataType> dataTypeList;
    private List<Executor> executors = new ArrayList<>();
    private FragmentRFManager manager;
    private boolean inputSorted = false;

    /**
     * If the top-n operator contains valid fetch and offset information,
     * it indicates that the top-n operator only needs to return the final set of data with a count equal to fetch.
     */
    private Long limitedFetch = null;

    private SettableFuture<GlobalTopNThreshold> thresholdFuture = null;
    private boolean isParent = false;

    private EarlyStopManager earlyStopManager;

    public TopNExecutorFactory(TopN topN, int parallelism, List<DataType> dataTypeList,
                               SpillerFactory spillerFactory) {
        this.topN = topN;
        this.parallelism = parallelism;
        this.spillerFactory = spillerFactory;
        this.dataTypeList = dataTypeList;
    }

    public void setEarlyStopManager(EarlyStopManager earlyStopManager) {
        this.earlyStopManager = earlyStopManager;
    }

    public void setInputSorted(boolean inputSorted) {
        this.inputSorted = inputSorted;
    }

    public void setParentGlobalThresholdFuture(SettableFuture<GlobalTopNThreshold> thresholdFuture,
                                               boolean isParent) {
        this.thresholdFuture = thresholdFuture;
        this.isParent = isParent;
    }

    public void setManager(FragmentRFManager manager) {
        this.manager = manager;
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
        if (executors.isEmpty()) {
            long fetch = -1, skip = 0;
            Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
            if (topN.fetch != null) {
                fetch = getRexParam(topN.fetch, params);
                if (topN.offset != null) {
                    skip = getRexParam(topN.offset, params);
                    if (fetch > 0 && skip > 0) {
                        limitedFetch = Long.valueOf(fetch);
                    }
                }
            }
            long topSize = skip + fetch;
            if (skip > 0 && fetch > 0 && topSize < 0) {
                topSize = Long.MAX_VALUE;
            }

            List<RelFieldCollation> sortList = topN.getCollation().getFieldCollations();
            List<OrderByOption> orderBys = ExecUtils.convertFrom(sortList);

            GlobalTopNThreshold globalTopNThreshold = null;
            boolean enableTypeSpecSort = context.getParamManager().getBoolean(ConnectionParams.ENABLE_PARALLEL_TOP_N);
            if (enableTypeSpecSort
                && !orderBys.isEmpty()
                && !dataTypeList.isEmpty()) {
                int orderByColumnIndex = orderBys.get(0).index;
                boolean useRF = false;

                if (dataTypeList.get(orderByColumnIndex) instanceof IntegerType && orderBys.size() == 1) {
                    globalTopNThreshold =
                        new IntTopNHeap.IntGlobalTopNThresholdImpl((int) topSize, orderBys.get(0).asc);
                    useRF = true;
                } else if (dataTypeList.get(orderByColumnIndex) instanceof LongType && orderBys.size() == 1) {
                    globalTopNThreshold =
                        new LongTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, orderBys.get(0).asc);
                    useRF = true;
                } else if (DataTypeUtil.isTemporalTypeWithDate(dataTypeList.get(orderByColumnIndex))
                    && orderBys.size() == 1) {
                    globalTopNThreshold =
                        new DateTopNHeap.LongGlobalTopNThresholdImpl((int) topSize, orderBys.get(0).asc);
                    useRF = true;
                } else if (dataTypeList.get(orderByColumnIndex) instanceof DecimalType && orderBys.size() == 1) {
                    globalTopNThreshold = new DecimalTopNHeap.RowGlobalThresholdImpl(dataTypeList, orderBys,
                        (int) topSize);
                } else {
                    globalTopNThreshold = new DefaultTopNHeap.RowGlobalThresholdImpl(dataTypeList, orderBys,
                        (int) topSize);
                }

                if (earlyStopManager != null) {
                    earlyStopManager.registerThreshold(globalTopNThreshold);
                }

                // the normal row-based global threshold is not supported for runtime filter.
                if (manager != null && globalTopNThreshold != null && useRF) {
                    List<FragmentRFItemKey> itemKeys = FragmentRFItemKey.buildItemKeys(topN);
                    FragmentRFItemKey uniqueItemKey = itemKeys.get(0);
                    FragmentRFItem item = manager.getAllItems().get(uniqueItemKey);
                    if (item != null) {
                        RFBloomFilter filter = new TopNThresholdFilter(globalTopNThreshold, uniqueItemKey.getSqlKind());
                        item.assignRF(new RFBloomFilter[] {filter});
                    }
                }

                if (isParent && thresholdFuture != null && !thresholdFuture.isDone()) {
                    thresholdFuture.set(globalTopNThreshold);
                }
            }

            for (int j = 0; j < parallelism; j++) {

                SpilledTopNExec exec = new SpilledTopNExec(dataTypeList, orderBys, topSize, context, spillerFactory, j);
                exec.setId(topN.getRelatedId());
                exec.setGlobalTopNThreshold(globalTopNThreshold);
                exec.setLimitedFetch(limitedFetch);
                exec.setInputSorted(inputSorted);

                if (!isParent && thresholdFuture != null) {
                    exec.setParentThresholdFuture(thresholdFuture);
                }
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(topN, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }
}
