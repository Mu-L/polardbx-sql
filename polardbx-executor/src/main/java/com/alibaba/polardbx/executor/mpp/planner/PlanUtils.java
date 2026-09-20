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

package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.SqlQueryManager;
import com.alibaba.polardbx.executor.mpp.execution.SqlTaskExecutionFactory;
import com.alibaba.polardbx.executor.mpp.execution.TaskManager;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.ColumnarNodeSelector;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.NodeScheduler;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.NodeSelector;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.OutputBufferManager;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.RandomNodeMode;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.SqlQueryScheduler;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.EmptyExecutor;
import com.alibaba.polardbx.executor.mpp.operator.LocalExchanger;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.operator.factory.ConsumeExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExchangeExecFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.ExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.LogicalViewExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.mpp.split.SplitManagerImpl;
import com.alibaba.polardbx.executor.mpp.util.MoreExecutors;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SourceExec;
import com.alibaba.polardbx.executor.operator.spill.MemorySpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.lang.String.format;

import com.alibaba.polardbx.executor.mpp.execution.LocationFactory;
import com.alibaba.polardbx.executor.mpp.execution.NodeTaskMap;
import com.alibaba.polardbx.executor.mpp.execution.QueryBloomFilter;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTaskFactory;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import com.alibaba.polardbx.executor.mpp.execution.scheduler.StageScheduler;

public class PlanUtils {

    private static final String EXECUTOR_MODE = "ExecutorMode";
    private static final String SPACE = " ";
    private static final int LEVEL_SPACES = 2;
    private static final int FRAGMENT_LEVEL = 0;
    private static final int PIPELINE_LEVEL = 2;
    private static final int SCHEDULE_LEVEL = 4;

    public static String textLocalPlan(ExecutionContext context, RelNode relNode, ExecutorMode type) {
        if (context.getMemoryPool() == null) {
            context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                WorkloadUtil.isApWorkload(
                    context.getWorkloadType()), context.getTraceId(), context.getExtraCmds()));
        }

        if (context.getExecuteMode() == ExecutorMode.TP_LOCAL && context.getParamManager().getInt(
            ConnectionParams.PARALLELISM) == -1) {
            context.getExtraCmds().put(ConnectionProperties.PARALLELISM, 1);
        }

        int parallelism = ExecUtils.getParallelismForLocal(context);

        StringBuilder builder = new StringBuilder();
        builder.append(EXECUTOR_MODE).append(": ").append(type).append(" ").append("\n");
        outputLocalFragment(context, parallelism, relNode, null, builder, null, false);
        return builder.toString();
    }

    private static void outputLocalFragment(ExecutionContext context, int parallelism, RelNode relNode,
                                            PlanFragment planFragment, StringBuilder builder,
                                            Map<Integer, Map<Node, List<Split>>> splitAssignments,
                                            boolean withSchedule) {
        boolean isSpill =
            MemorySetting.ENABLE_SPILL && context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL);
        TaskManager taskManager = ((MppServer) ServiceProvider.getInstance().getServer()).getTaskManager();
        SqlTaskExecutionFactory sqlTaskExecutionFactory = taskManager.getSqlTaskExecutionFactory();
        LocalExecutionPlanner planner =
            new LocalExecutionPlanner(context, sqlTaskExecutionFactory.getExchangeClientSupplier(),
                parallelism, parallelism, 1,
                context.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS), MoreExecutors.directExecutor(),
                isSpill ? new MemorySpillerFactory() : null, null, null, true,
                -1, -1, ImmutableMap.of(), new SplitManagerImpl());
        List<DataType> columns = CalciteUtils.getTypes(relNode.getRowType());
        OutputBufferMemoryManager localBufferManager = planner.createLocalMemoryManager();
        LocalBufferExecutorFactory factory = new LocalBufferExecutorFactory(localBufferManager, columns, 1);
        List<PipelineFactory> pipelineFactories = planner.plan(relNode, factory, localBufferManager,
            context.getTraceId());

        PropUtil.ExplainOutputFormat outputFormat = (PropUtil.ExplainOutputFormat) context.getParamManager()
            .getEnum(ConnectionParams.EXPLAIN_OUTPUT_FORMAT);
        if (outputFormat == PropUtil.ExplainOutputFormat.LEGACY) {
            // old behavior
            for (PipelineFactory pipelineFactory : pipelineFactories) {
                builder.append(
                    formatPipelineFragment(context, pipelineFactory, context.getParams().getCurrentParameter()));
            }
        } else {
            for (PipelineFactory pipelineFactory : pipelineFactories) {
                builder.append(
                    formatPipelineFragment2(context, pipelineFactory, planFragment, splitAssignments, withSchedule));
            }
        }
    }

    public static String formatPipelineFragment(ExecutionContext executionContext,
                                                PipelineFactory pipelineFactory,
                                                Map<Integer, ParameterContext> params) {
        int level = PIPELINE_LEVEL;
        PipelineFragment fragment = pipelineFactory.getFragment();
        StringBuilder builder = new StringBuilder();
        if (!fragment.getPrefetchLists().isEmpty()) {
            appendWithFormat(builder, level++, "pipeline=%s dependency=[%s] parallelism=%s prefetch=[%s]",
                fragment.getPipelineId(),
                Joiner.on(", ").join(fragment.getDependency()), fragment.getParallelism(), fragment.getPrefetchLists());
        } else {
            appendWithFormat(builder, level++, "pipeline=%s dependency=[%s] parallelism=%s", fragment.getPipelineId(),
                Joiner.on(", ").join(fragment.getDependency()), fragment.getParallelism());
        }

        builder.append(RelUtils.toString(executionContext.getSqlExplainLevel(),
                fragment.getProperties().getRelNode(), params, getSpaceCount(level)))
            .append("\n");
        return builder.toString();
    }

    public static String formatPipelineFragment2(ExecutionContext executionContext,
                                                 PipelineFactory pipelineFactory,
                                                 PlanFragment planFragment,
                                                 Map<Integer, Map<Node, List<Split>>> splitAssignments,
                                                 boolean withSchedule) {
        int level = PIPELINE_LEVEL;
        PipelineFragment fragment = pipelineFactory.getFragment();
        StringBuilder builder = new StringBuilder();
        appendWithFormat(builder, level++, "pipeline=%s dependency=[%s] parallelism=%d(per node)",
            fragment.getPipelineId(),
            Joiner.on(", ").join(fragment.getDependency()), fragment.getParallelism());
        ExecutorFactory produceFactory = pipelineFactory.getProduceFactory();
        ConsumeExecutorFactory consumeFactory = pipelineFactory.getConsumeFactory();
        appendProducer(executionContext, produceFactory, builder, level, planFragment, splitAssignments, withSchedule);
        appendConsumer(executionContext, consumeFactory, builder, level);
        return builder.toString();
    }

    private static void appendProducer(ExecutionContext executionContext, ExecutorFactory produceFactory,
                                       StringBuilder builder, int level, PlanFragment planFragment,
                                       Map<Integer, Map<Node, List<Split>>> splitAssignments, boolean withSchedule) {
        if (produceFactory instanceof ExchangeExecFactory) {
            String line = "[Producer]: RemoteSource(" + ((ExchangeExecFactory) produceFactory).getExplainOutput() + ")";
            appendWithFormat(builder, level, line);
        } else {
            Executor producer = produceFactory.createExecutor(executionContext, 0);
            StringBuilder treePrinter = new StringBuilder();
            visit(producer, 0, treePrinter);
            appendWithFormat(builder, level, String.format("[Producer]: %s", treePrinter));
            if (planFragment != null && withSchedule) {
                visitProducerSplits(producer, planFragment, builder, splitAssignments);
            }
        }
    }

    private static String formatPipelineSchedule(Map<Node, List<Split>> splitAssignment, boolean isPartitionWise) {
        int level = SCHEDULE_LEVEL;
        StringBuilder builder = new StringBuilder();
        appendWithFormat(builder, level, "Splits Distribution[%s]",
            isPartitionWise ? "partition-wise" : "non-partition-wise");
        for (Map.Entry<Node, List<Split>> entry : splitAssignment.entrySet()) {
            appendWithFormat(builder, level + 1, "%s: %s splits",
                entry.getKey().getHost() + ":" + entry.getKey().getPort(),
                entry.getValue().size());
        }
        return builder.toString();
    }

    private static void visit(Executor producer, int level, StringBuilder builder) {
        builder.append(producer.getClass().getSimpleName());
        if (producer instanceof SourceExec) {
            String sourceName = ((SourceExec) producer).getSourceName();
            if (StringUtils.isNotEmpty(sourceName)) {
                builder.append('(').append(sourceName).append(')');
            }
        }
        List<Executor> inputs = producer.getInputs();
        for (Executor input : inputs) {
            if (!(input instanceof EmptyExecutor)) {
                builder.append(" <-- ");
                visit(input, level + 1, builder);
            }
        }
    }

    private static void visitProducerSplits(Executor producer, PlanFragment planFragment, StringBuilder builder,
                                            Map<Integer, Map<Node, List<Split>>> splitAssignments) {
        if (producer instanceof SourceExec) {
            SourceExec sourceExec = (SourceExec) producer;
            if (splitAssignments != null && splitAssignments.containsKey(sourceExec.getId())) {
                builder.append(formatPipelineSchedule(splitAssignments.get(sourceExec.getId()),
                    planFragment.isRemotePairWise() || planFragment.isLocalPairWise()));
            }
        }
        for (Executor input : producer.getInputs()) {
            if (!(input instanceof EmptyExecutor)) {
                visitProducerSplits(input, planFragment, builder, splitAssignments);
            }
        }
    }

    private static void visit(ConsumerExecutor consumerExecutor, int level, StringBuilder treePrinter) {
        treePrinter.append(consumerExecutor.getClass().getSimpleName());
        if (consumerExecutor instanceof LocalExchanger) {
            LocalExchanger localExchanger = (LocalExchanger) consumerExecutor;
            List<ConsumerExecutor> consumerExecutorList = (localExchanger).getExecutors();
            treePrinter.append(" --> ");
            visit(consumerExecutorList.get(0), level + 1, treePrinter);
        }
    }

    private static void appendConsumer(ExecutionContext executionContext, ConsumeExecutorFactory consumeFactory,
                                       StringBuilder builder, int level) {
        ConsumerExecutor consumer = consumeFactory.createExecutor(executionContext, 0);
        StringBuilder treePrinter = new StringBuilder();
        visit(consumer, 0, treePrinter);
        appendWithFormat(builder, level, String.format("[Consumer]: %s", treePrinter));
    }

    public static String textPlan(ExecutionContext executionContext, Session session, RelNode relNode,
                                  boolean withSchedule) {
        Pair<SubPlan, Integer> plan = PlanFragmenter.buildRootFragment(relNode, session);
        StringBuilder builder = new StringBuilder();
        builder.append(EXECUTOR_MODE).append(": ").append("MPP").append("\n");
        builder.append("The Query's MaxConcurrentParallelism: ").append(plan.getValue()).append("\n");
        boolean columnarMode = session.getClientContext().getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCHEDULE);
        int mppNodeSize = executionContext.getParamManager().getInt(ConnectionParams.MPP_NODE_SIZE);
        if (mppNodeSize <= 0) {
            mppNodeSize = ExecUtils.getMppLimitNodes(columnarMode,
                session.getClientContext().getParamManager(), plan.getValue());
        }

        PropUtil.ExplainOutputFormat outputFormat = (PropUtil.ExplainOutputFormat) executionContext.getParamManager()
            .getEnum(ConnectionParams.EXPLAIN_OUTPUT_FORMAT);
        if (outputFormat == PropUtil.ExplainOutputFormat.LEGACY) {
            builder.append("MPP node size: ").append(mppNodeSize).append("\n");
            for (PlanFragment fragment : plan.getKey().getAllFragments()) {
                builder.append(formatFragment(
                    executionContext, fragment, session.getClientContext().getParams().getCurrentParameter()));
                if (executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_PIPELINE_INFO_UNDER_MPP)) {
                    int parallelism = fragment.getPartitioning().getPartitionCount();
                    outputLocalFragment(executionContext, Math.max(1, parallelism), fragment.getRootNode(), fragment,
                        builder, null, false);
                }
            }
            return builder.toString();
        }
        SqlQueryManager queryManager = (SqlQueryManager) ServiceProvider.getInstance().getServer().getQueryManager();
        RandomNodeMode randomNodeMode = RandomNodeMode.getRandomNodeMode(
            executionContext.getParamManager().getString(ConnectionParams.MPP_NODE_RANDOM_MODE));
        SqlQueryExecution.SqlQueryExecutionFactory queryExecutionFactory = queryManager.getQueryExecutionFactory();
        NodeScheduler nodeScheduler = queryExecutionFactory.getNodeScheduler();
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, mppNodeSize, randomNodeMode);
        if (withSchedule && (nodeSelector instanceof ColumnarNodeSelector)) {
            SqlQueryExecution.optimizeScheduleUnderColumnar(session, relNode, (ColumnarNodeSelector) nodeSelector);
        }

        outputMppWorkers(nodeSelector.getOrderedNode(), builder, withSchedule);
        // if not using legacy format, output the stage-based execution plan
        List<PlanFragment> planFragmentList = new ArrayList<>();
        StageExecutionPlan stagePlan = SqlQueryExecution.getStagePlan(plan.getKey(), planFragmentList);

        Map<Integer, Map<Node, List<Split>>> splitAssignments = null;

        if (withSchedule) {
            splitAssignments =
                prepareSplitAssignments(stagePlan, executionContext, session, nodeSelector, queryExecutionFactory);
        }

        outputStageFragment(stagePlan, nodeSelector, executionContext, session, builder, withSchedule,
            splitAssignments);
        return builder.toString();
    }

    private static Map<Integer, Map<Node, List<Split>>> prepareSplitAssignments(
        StageExecutionPlan stagePlan, ExecutionContext executionContext, Session session, NodeSelector nodeSelector,
        SqlQueryExecution.SqlQueryExecutionFactory queryExecutionFactory) {

        Map<Integer, StageId> stageIdMapping = new HashMap<>();
        Multimap<StageId, StageId> c2pLinkages = LinkedHashMultimap.create();

        SqlQueryScheduler.initStageIds(Optional.empty(), stagePlan, executionContext.getTraceId(), stageIdMapping,
            c2pLinkages);

        Map<StageId, SqlStageExecution> initedStages = new HashMap<>();
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers = ImmutableMap.builder();
        ImmutableMultimap.Builder<StageId, SqlQueryScheduler.StageLinkage> stageLinkages =
            ImmutableMultimap.builder();
        Map<StageId, Set<SqlStageExecution>> stage2childStages = new HashMap<>();
        Map<StageId, OutputBufferManager> outputBufferManagers = new HashMap<>();

        // 获取必要的服务和工厂
        LocationFactory locationFactory = queryExecutionFactory.getLocationFactory();
        RemoteTaskFactory remoteTaskFactory = queryExecutionFactory.getRemoteTaskFactory();
        NodeTaskMap nodeTaskMap = queryExecutionFactory.getNodeTaskMap();
        NodePartitioningManager nodePartitioningManager = queryExecutionFactory.getNodePartitioningManager();
        ExecutorService executor = queryExecutionFactory.getExecutor();
        boolean summarizeTaskInfo = false;

        // 调用 SqlQueryScheduler.createStages() 构造 stages
        List<SqlStageExecution> stages = SqlQueryScheduler.createStages(
            Optional.empty(),
            stageIdMapping,
            initedStages,
            locationFactory,
            stagePlan.copy(),
            partitioningHandler -> nodePartitioningManager.getNodePartitioningMap(nodeSelector,
                partitioningHandler),
            remoteTaskFactory,
            session,
            executor,
            nodeTaskMap,
            stageSchedulers,
            stageLinkages,
            stage2childStages,
            c2pLinkages,
            outputBufferManagers,
            new QueryBloomFilter(new ArrayList<>()),
            nodeSelector,
            summarizeTaskInfo,
            new HashMap<>()
        );

        return getSplitAssignments(stageSchedulers.build());
    }

    private static void outputMppWorkers(List<Node> workerNodes, StringBuilder builder, boolean withSchedule) {
        // update mpp node size based on the selected nodes
        int mppNodeSize = workerNodes.size();
        builder.append("MPP node size: ").append(mppNodeSize).append("\n");
        if (withSchedule) {
            builder.append("MPP node list: ").append(outputNodeList(workerNodes)).append("\n");
        }
    }

    private static String outputNodeList(List<Node> workerNodes) {
        Map<String, List<Node>> instId2NodeList = workerNodes.stream().collect(Collectors.groupingBy(Node::getInstId));
        StringBuilder nodeListBuilder = new StringBuilder();

        instId2NodeList.forEach((instId, nodes) -> {
            String nodeDetails = nodes.stream()
                .map(node -> node.getHost() + ":" + node.getPort())
                .collect(Collectors.joining(", "));
            nodeListBuilder.append(instId).append("[").append(nodeDetails).append("], ");
        });

        if (nodeListBuilder.length() > 2) {
            // remove trailing ", "
            nodeListBuilder.setLength(nodeListBuilder.length() - 2);
        }

        return nodeListBuilder.toString();
    }

    private static void outputStageFragment(StageExecutionPlan stagePlan, NodeSelector nodeSelector,
                                            ExecutionContext executionContext, Session session, StringBuilder builder,
                                            boolean withSchedule,
                                            Map<Integer, Map<Node, List<Split>>> splitAssignments) {
        PlanFragment fragment = stagePlan.getFragment();
        builder.append(formatFragment2(executionContext, fragment));
        if (executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_PIPELINE_INFO_UNDER_MPP)) {
            int driverParallelism;
            SqlQueryManager queryManager =
                (SqlQueryManager) ServiceProvider.getInstance().getServer().getQueryManager();
            SqlQueryExecution.SqlQueryExecutionFactory queryExecutionFactory =
                queryManager.getQueryExecutionFactory();
            int totalParallelism = fragment.getPartitioning().getPartitionCount();
            List<SplitInfo> splitInfos = stagePlan.getSplitInfos();

            if (fragment.getExpandSources().isEmpty() ||
                stagePlan.getFragment().getPartitionedSources().size() != stagePlan.getFragment().getExpandSources()
                    .size()) {
                // SourcePartitionedScheduler
                if (splitInfos.size() == 1 && splitInfos.get(0).isUnderSort()) {
                    driverParallelism = 1;
                } else {
                    int taskNum = nodeSelector.getOrderedNode().size();
                    driverParallelism = totalParallelism % taskNum > 0 ? totalParallelism / taskNum + 1 :
                        totalParallelism / taskNum;
                }
            } else {
                NodePartitioningManager nodePartManager = queryExecutionFactory.getNodePartitioningManager();
                Map<Integer, Node> nodePartitioningMap =
                    nodePartManager.getNodePartitioningMap(nodeSelector, fragment.getPartitioning());
                int taskNum = nodePartitioningMap.size();
                // FixedCountScheduler or FixedExpandSourceScheduler
                driverParallelism = totalParallelism % taskNum > 0 ? totalParallelism / taskNum + 1 :
                    totalParallelism / taskNum;
            }
            outputLocalFragment(executionContext, Math.max(1, driverParallelism), fragment.getRootNode(), fragment,
                builder, splitAssignments, withSchedule);
        }
        for (StageExecutionPlan subStagePlan : stagePlan.getSubStages()) {
            outputStageFragment(subStagePlan, nodeSelector, executionContext, session, builder, withSchedule,
                splitAssignments);
        }
    }

    /**
     * @param stageSchedulers stageId -> node -> relNodeId -> splits
     * @return relNodeId -> node -> splitList
     */
    private static Map<Integer, Map<Node, List<Split>>> getSplitAssignments(
        ImmutableMap<StageId, StageScheduler> stageSchedulers) {
        Map<Integer, Map<Node, List<Split>>> splitAssignmentsByRelNodeId = new HashMap<>();
        for (StageScheduler stageScheduler : stageSchedulers.values()) {
            Map<Node, Multimap<Integer, Split>> splitAssignments = stageScheduler.getSplitAssignments();
            for (Map.Entry<Node, Multimap<Integer, Split>> entry : splitAssignments.entrySet()) {
                Node node = entry.getKey();
                Multimap<Integer, Split> relNodeSplitMap = entry.getValue();

                for (Map.Entry<Integer, Split> relNodeSplitEntry : relNodeSplitMap.entries()) {
                    Integer relNodeId = relNodeSplitEntry.getKey();
                    Split split = relNodeSplitEntry.getValue();
                    Map<Node, List<Split>> nodeSplitMap =
                        splitAssignmentsByRelNodeId.computeIfAbsent(relNodeId, k -> new HashMap<>());
                    nodeSplitMap.computeIfAbsent(node, k -> new ArrayList<>()).add(split);
                }
            }
        }
        return splitAssignmentsByRelNodeId;
    }

    private static String formatFragment(ExecutionContext executionContext, PlanFragment fragment,
                                         Map<Integer, ParameterContext> params) {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s \n", fragment.getId()));

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(format("Output partitioning: %s [%s] ",
            partitioningScheme.getPartitionMode(), Joiner.on(", ").join(partitioningScheme.getPartChannels())));
        builder.append(format("Parallelism: %s ",
            fragment.getPartitioning().getPartitionCount()));
        if (fragment.getAllSplitNums() > 0) {
            builder.append(format("Splits: %s \n", fragment.getAllSplitNums()));
        } else {
            builder.append("\n");
        }

        builder.append(RelUtils.toString(executionContext.getSqlExplainLevel(), fragment.getRootNode(), params, 2))
            .append("\n");
        return builder.toString();
    }

    private static String formatFragment2(ExecutionContext executionContext, PlanFragment fragment) {
        int level = FRAGMENT_LEVEL;
        StringBuilder builder = new StringBuilder();
        appendWithFormat(builder, level++, "Fragment=%s Parallelism=%s",
            fragment.getId(), fragment.getPartitioning().getPartitionCount());

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        appendWithFormat(builder, level++, "Output partitioning: %s [%s]",
            partitioningScheme.getPartitionMode(), Joiner.on(", ").join(partitioningScheme.getPartChannels()));
        return builder.toString();
    }

    /**
     * append one line at a time
     */
    private static void appendWithFormat(StringBuilder stringBuilder, int level,
                                         String format, Object... args) {
        if (format == null) {
            throw new IllegalArgumentException("Format output of null value");
        }
        for (int i = 0; i < getSpaceCount(level); i++) {
            stringBuilder.append(SPACE);
        }
        stringBuilder.append(String.format(format, args)).append('\n');
    }

    private static int getSpaceCount(int level) {
        if (level <= 0) {
            return 0;
        }
        return level * LEVEL_SPACES;
    }
}