package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HtapTrace {
    RoutingType routingType;
    Set<OptimizerType> candidateOptimizerTypes;
    WorkloadType workloadType;
    OptimizerType optimizerType;
    PlanType planType;

    List<String> records;

    public HtapTrace() {
        this.records = Lists.newArrayList();
    }

    private void addTrace(String record) {
        records.add(record);
    }

    private void traceRoutingType(RoutingType routingType, String cause) {
        this.routingType = routingType;
        records.add(
            String.format("routing type: %s, caused by %s", routingType == null ? "null" : routingType.name(), cause));
    }

    private void traceCandidateOptimizerTypes(Set<OptimizerType> candidateOptimizerTypes, String cause) {
        this.candidateOptimizerTypes = candidateOptimizerTypes;
        records.add(String.format("candidate optimizer types: {%s}, caused by %s",
            candidateOptimizerTypes == null ? "null" :
                candidateOptimizerTypes.stream().sorted(Comparator.comparingInt(Enum::ordinal))
                    .map(OptimizerType::name).collect(Collectors.joining(",")),
            cause));
    }

    private void traceWorkloadType(WorkloadType workloadType, String cause) {
        this.workloadType = workloadType;
        records.add(
            String.format("workload type: %s, caused by %s", workloadType == null ? "null" : workloadType.name(),
                cause));
    }

    private void traceOptimizerType(OptimizerType optimizerType, String record) {
        this.optimizerType = optimizerType;
        records.add(
            String.format("optimizer type: %s, caused by %s", optimizerType == null ? "null" : optimizerType.name(),
                record));
    }

    private void tracePlanType(PlanType planType, String record) {
        this.planType = planType;
        records.add(String.format("plan type: %s, caused by %s", planType == null ? "null" : planType.name(), record));
    }

    public static void addTrace(Optional<HtapTrace> tracer, String record) {
        tracer.ifPresent(htapTrace -> htapTrace.addTrace(record));
    }

    public static void traceRoutingType(Optional<HtapTrace> tracer, CountedRoutingType countedRoutingType,
                                        String record) {
        tracer.ifPresent(htapTrace ->
            htapTrace.traceRoutingType(countedRoutingType == null ? null : countedRoutingType.getRoutingType(),
                record));
    }

    public static void traceCandidateOptimizerTypes(Optional<HtapTrace> tracer,
                                                    Set<OptimizerType> candidateOptimizerTypes, String cause) {
        tracer.ifPresent(
            htapTrace -> htapTrace.traceCandidateOptimizerTypes(candidateOptimizerTypes, cause));
    }

    public static void traceWorkloadType(Optional<HtapTrace> tracer, WorkloadType workloadType, String cause) {
        tracer.ifPresent(htapTrace -> htapTrace.traceWorkloadType(workloadType, cause));
    }

    public static void traceOptimizerType(Optional<HtapTrace> tracer, OptimizerType optimizerType,
                                          String cause) {
        tracer.ifPresent(htapTrace -> htapTrace.traceOptimizerType(optimizerType, cause));
    }

    public static void tracePlanType(Optional<HtapTrace> tracer, PlanType planType,
                                     String cause) {
        tracer.ifPresent(htapTrace -> htapTrace.tracePlanType(planType, cause));
    }

    public static void traceFromPlanCache(ExecutionContext ec, PlannerContext pc) {
        if (ec.getHtapTrace().isPresent()) {
            HtapTrace trace = ec.getHtapTrace().get();
            trace.addTrace("\ncached plan from plan cache:");
            trace.traceRoutingType(pc.getRoutingType(), "plan cache");
            trace.traceCandidateOptimizerTypes(null, "plan cache doesn't record");
            trace.traceWorkloadType(pc.getWorkloadType(), "plan cache");
            trace.traceOptimizerType(pc.getOptimizerType(), "plan cache");
            trace.tracePlanType(pc.getPlanType(), "plan cache");
        }
    }

    public static void traceFromSpm(ExecutionContext ec, PlannerContext pc) {
        if (ec.getHtapTrace().isPresent()) {
            HtapTrace trace = ec.getHtapTrace().get();
            trace.addTrace("\ncached plan from spm:");
            trace.traceCandidateOptimizerTypes(null, "spm plan doesn't record");
            trace.traceWorkloadType(pc.getWorkloadType(), "spm plan");
            trace.traceOptimizerType(null, "spm plan doesn't record");
            trace.tracePlanType(pc.getPlanType(), "spm plan");
        }
    }

    public String printRoutType() {
        return routingType == null ? "NULL" : routingType.name();
    }

    public String printCandidateOptimizerTypes() {
        if (candidateOptimizerTypes == null) {
            return "NULL";
        }
        return candidateOptimizerTypes.stream().sorted(Comparator.comparingInt(Enum::ordinal)).map(OptimizerType::name)
            .collect(Collectors.joining(","));
    }

    public String printWorkLoadType() {
        return workloadType == null ? "NULL" : workloadType.name();
    }

    public String printOptimizerType() {
        return optimizerType == null ? "NULL" : optimizerType.name();
    }

    public String printPlanType() {
        return planType == null ? "NULL" : planType.name();
    }

    public String printDetails() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        for (String record : records) {
            println(pw, record);
        }
        sw.flush();
        return sw.toString();
    }

    private void println(PrintWriter printWriter, String line) {
        if (StringUtils.isEmpty(line)) {
            return;
        }
        printWriter.println(line);
    }
}
