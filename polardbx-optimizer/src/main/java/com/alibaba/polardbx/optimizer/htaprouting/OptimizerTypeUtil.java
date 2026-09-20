package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryType;
import com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;

import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.htaprouting.OptimizerType.COLUMNAR;
import static com.alibaba.polardbx.optimizer.htaprouting.OptimizerType.MPP;
import static com.alibaba.polardbx.optimizer.htaprouting.OptimizerType.SMP;
import static com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil.determineWorkloadType;

public class OptimizerTypeUtil {

    /**
     * filter valid optimizer types based on current info
     *
     * @param input a physical plan optimized
     * @param pc context of planner
     * @param routingType routing type for current sql
     * @return optimizer can be used for the sql
     */
    public static Set<OptimizerType> determineCandidateOptimizerType(RelNode originInput,
                                                                     RelNode input,
                                                                     PlannerContext pc,
                                                                     RoutingType routingType) {
        HtapTrace.addTrace(pc.getHtapTrace(), "\ndetermine candidate optimizer types:");

        // optimizer type has the highest priority
        OptimizerType specificType = null;
        if (routingType != null) {
            if (pc.getExecutionContext().isUseHint()) {
                specificType = OptimizerType.getType(String.valueOf(pc.getExecutionContext().getHintCmds()
                    .get(ConnectionProperties.OPTIMIZER_TYPE)));
            }
        } else {
            specificType = OptimizerType.getType(pc.getParamManager().getString(ConnectionParams.OPTIMIZER_TYPE));
        }
        if (specificType != null) {
            HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), Sets.newHashSet(specificType),
                "setting OPTIMIZER_TYPE=" + specificType.name());
            return Sets.newHashSet(specificType);
        }

        if (ConfigDataMode.isColumnarMode()) {
            return determineCandidateOptimizerTypeColumnarNode(originInput, input, pc, routingType,
                RoutingType.validOptimizerType(routingType));
        } else {
            return determineCandidateOptimizerTypeForMasterNode(originInput, input, pc, routingType,
                RoutingType.validOptimizerType(routingType));
        }

    }

    private static Set<OptimizerType> determineCandidateOptimizerTypeColumnarNode(RelNode originInput,
                                                                                  RelNode input,
                                                                                  PlannerContext pc,
                                                                                  RoutingType routingType,
                                                                                  Set<OptimizerType> routingCandidates) {
        HtapTrace.addTrace(pc.getHtapTrace(), "valid optimizer types from routing type:"
            + routingCandidates.stream().map(OptimizerType::name).collect(Collectors.joining(",")));

        // only smp is valid
        if ((!MppPlanCheckers.supportsMppPlan(input, pc, pc.getExecutionContext(), routingType,
            MppPlanCheckers.BASIC_CHECKERS))) {
            HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), Sets.newHashSet(SMP),
                "basic checker failed in columnar mode");
            return Sets.newHashSet(SMP);
        }

        if ((MppPlanCheckers.supportsMppPlan(input, pc, pc.getExecutionContext(), routingType,
            MppPlanCheckers.COLUMNAR_BASIC_CHECKERS))) {
            boolean allTablesHaveColumnarIndex =
                CBOUtil.allTablesHaveColumnarIndex(originInput, pc.getExecutionContext());
            HtapTrace.addTrace(pc.getHtapTrace(),
                "ALL_TABLE_HAVE_COLUMNAR_INDEX " + (allTablesHaveColumnarIndex ? "succeed" : "failed"));
            if (allTablesHaveColumnarIndex) {
                if (!routingCandidates.contains(COLUMNAR)) {
                    HtapTrace.addTrace(pc.getHtapTrace(), "set columnar optimizer to candidates");
                }
                HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(),
                    Sets.newHashSet(COLUMNAR), "all checker passed in columnar mode");
                return Sets.newHashSet(COLUMNAR);
            }
        }
        HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), Sets.newHashSet(SMP),
            "some columnar checker failed before in columnar mode");
        return Sets.newHashSet(SMP);
    }

    private static Set<OptimizerType> determineCandidateOptimizerTypeForMasterNode(RelNode originInput,
                                                                                   RelNode input,
                                                                                   PlannerContext pc,
                                                                                   RoutingType routingType,
                                                                                   Set<OptimizerType> routingCandidates) {
        HtapTrace.addTrace(pc.getHtapTrace(), "valid optimizer types from routing type: "
            + routingCandidates.stream().map(OptimizerType::name).collect(Collectors.joining(",")));
        if (!MppPlanCheckers.supportsMppPlan(input, pc, pc.getExecutionContext(), routingType,
            MppPlanCheckers.BASIC_CHECKERS)) {
            HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), Sets.newHashSet(SMP),
                "basic checker failed in master mode");
            return Sets.newHashSet(SMP);
        }

        if (MppPlanCheckers.supportsMppPlan(input, pc, pc.getExecutionContext(), routingType,
            MppPlanCheckers.COLUMNAR_BASIC_CHECKERS)) {
            boolean allTablesHaveColumnarIndex =
                CBOUtil.allTablesHaveColumnarIndex(originInput, pc.getExecutionContext());
            HtapTrace.addTrace(pc.getHtapTrace(),
                "ALL_TABLE_HAVE_COLUMNAR_INDEX " + (allTablesHaveColumnarIndex ? "succeed" : "failed"));
            if (allTablesHaveColumnarIndex) {
                routingCandidates.add(COLUMNAR);
                HtapTrace.addTrace(pc.getHtapTrace(), "add columnar optimizer to candidates");
                HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), routingCandidates,
                    "all columnar checker passed in master mode");
                return routingCandidates;
            }
        }

        // columnar is invalid
        routingCandidates.remove(COLUMNAR);
        if (!routingCandidates.isEmpty()) {
            HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), routingCandidates,
                "columnar optimizer unavailable in master mode");
            return routingCandidates;
        }
        HtapTrace.traceCandidateOptimizerTypes(pc.getHtapTrace(), Sets.newHashSet(SMP, MPP),
            "no valid optimizer in master mode");
        return Sets.newHashSet(SMP, MPP);
    }

    /**
     * determine the optimizer to be used and the workload of the sql
     *
     * @param pc context of planner
     * @return optimizer to be used for the sql
     */
    public static OptimizerType determineWorkloadAndOptimizerTypeThenWorkLoad(PlannerContext pc,
                                                                              Set<OptimizerType> validOptimizerTypes) {
        HtapTrace.addTrace(pc.getHtapTrace(), "\ndetermine workload type and optimizer type:");
        HtapTrace.addTrace(pc.getHtapTrace(), "optimizer type then workload type");
        // columnar mode
        if (ConfigDataMode.isColumnarMode()) {
            return determineOptimizerTypeForColumnarNode(pc, validOptimizerTypes);
        } else {
            return determineOptimizerTypeForMasterNodeBeforeCBO(pc, validOptimizerTypes);
        }
    }

    /**
     * determine the workload of the sql by cost and the optimizer to be used
     *
     * @param input a physical plan optimized
     * @param input a plan optimized
     * @param pc context of planner
     * @return optimizer to be used for the sql
     */
    public static OptimizerType determineWorkloadThenOptimizerType(RelNode input,
                                                                   PlannerContext pc,
                                                                   Set<OptimizerType> validOptimizerTypes) {
        HtapTrace.addTrace(pc.getHtapTrace(), "workload type then optimizer type");
        try {
            pc.setWorkloadType(WorkloadType.valueOf(
                pc.getParamManager().getString(ConnectionParams.WORKLOAD_TYPE).toUpperCase()));
            HtapTrace.traceWorkloadType(pc.getHtapTrace(), pc.getWorkloadType(),
                "setting WORKLOAD_TYPE=" + pc.getParamManager().getString(ConnectionParams.WORKLOAD_TYPE));
        } catch (Throwable t) {
            HtapTrace.addTrace(pc.getHtapTrace(), "determine workload type by cost");
            pc.setWorkloadType(determineWorkloadType(input, input.getCluster().getMetadataQuery()));
            if (pc.getParamManager().getBoolean(ConnectionParams.FORCE_TTL_TRANSPARENT_QUERY_WORKLOAD) &&
                TtlQueryType.needHybridSchedule(pc.getTtlQueryType())) {
                HtapTrace.addTrace(pc.getHtapTrace(), "determine workload type by hybrid ttl query");
                pc.setWorkloadType(WorkloadType.AP);
            }
        }
        // columnar mode
        if (ConfigDataMode.isColumnarMode()) {
            return determineOptimizerTypeForColumnarNode(pc, validOptimizerTypes);
        } else {
            return determineOptimizerTypeForMasterNode(pc, validOptimizerTypes);
        }
    }

    private static OptimizerType determineOptimizerTypeForColumnarNode(PlannerContext pc,
                                                                       Set<OptimizerType> validOptimizerTypes) {
        if (validOptimizerTypes.contains(COLUMNAR)) {
            pc.setWorkloadType(WorkloadType.AP);
            HtapTrace.traceOptimizerType(pc.getHtapTrace(), COLUMNAR, "columnar optimizer available in columnar mode");
            HtapTrace.traceWorkloadType(pc.getHtapTrace(), pc.getWorkloadType(),
                "columnar optimizer required in columnar mode");
            return COLUMNAR;
        }
        pc.setWorkloadType(WorkloadType.TP);
        HtapTrace.traceOptimizerType(pc.getHtapTrace(), SMP, "columnar optimizer not available in columnar mode");
        HtapTrace.traceWorkloadType(pc.getHtapTrace(), pc.getWorkloadType(), "smp optimizer required in columnar mode");
        return SMP;
    }

    private static OptimizerType determineOptimizerTypeForMasterNodeBeforeCBO(PlannerContext pc,
                                                                              Set<OptimizerType> validOptimizerTypes) {
        if (validOptimizerTypes.size() != 1) {
            HtapTrace.addTrace(pc.getHtapTrace(), "failed due to multiple candidate optimizer types exist");
            return null;
        }
        // only one optimizer available
        if (validOptimizerTypes.contains(COLUMNAR)) {
            pc.setWorkloadType(WorkloadType.AP);
            HtapTrace.traceOptimizerType(pc.getHtapTrace(), COLUMNAR,
                "no optimizer except columnar available in master mode");
            HtapTrace.traceWorkloadType(pc.getHtapTrace(), pc.getWorkloadType(),
                "columnar optimizer required AP in master mode");
            return COLUMNAR;
        }
        if (validOptimizerTypes.contains(OptimizerType.MPP)) {
            pc.setWorkloadType(WorkloadType.AP);
            HtapTrace.traceOptimizerType(pc.getHtapTrace(), MPP, "only mpp optimizer available in master mode");
            HtapTrace.traceWorkloadType(pc.getHtapTrace(), pc.getWorkloadType(),
                "mpp optimizer required AP in master mode");
            return OptimizerType.MPP;
        }
        if (validOptimizerTypes.contains(OptimizerType.SMP)) {
            pc.setWorkloadType(WorkloadType.TP);
            HtapTrace.traceOptimizerType(pc.getHtapTrace(), SMP,
                "only smp optimizer available in master mode");
            HtapTrace.traceWorkloadType(pc.getHtapTrace(), pc.getWorkloadType(),
                "smp optimizer required TP in master mode");
            return SMP;
        }
        HtapTrace.addTrace(pc.getHtapTrace(), "impossible to reach here in master mode");
        return SMP;

    }

    private static OptimizerType determineOptimizerTypeForMasterNode(PlannerContext pc,
                                                                     Set<OptimizerType> validOptimizerTypes) {
        if (WorkloadType.AP == pc.getWorkloadType()) {
            if (validOptimizerTypes.contains(COLUMNAR)) {
                HtapTrace.traceOptimizerType(pc.getHtapTrace(), COLUMNAR,
                    "columnar optimizer available with workload=ap in master mode");
                return COLUMNAR;
            }
            if (validOptimizerTypes.contains(OptimizerType.MPP)) {
                HtapTrace.traceOptimizerType(pc.getHtapTrace(), MPP,
                    "mpp optimizer available with workload=ap in master mode");
                return OptimizerType.MPP;
            }
            HtapTrace.traceOptimizerType(pc.getHtapTrace(), SMP,
                "only smp optimizer available with workload=ap in master mode");
            return SMP;
        }
        HtapTrace.traceOptimizerType(pc.getHtapTrace(), SMP,
            "only smp optimizer valid with workload=tp in master mode");
        return SMP;
    }
}
