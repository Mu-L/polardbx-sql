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

package com.alibaba.polardbx.optimizer.utils.mppchecker;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.htaprouting.HtapTrace;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryType;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.Optional;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.COLUMNAR_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.EXPLICIT_TRANSACTION;

public class MppPlanCheckers {
    public static final MppPlanChecker MPP_ENABLED_CHECKER = input -> {
        RoutingType routingType = input.getRoutingType();
        if (routingType == RoutingType.COLUMNAR || routingType == RoutingType.HTAP || TtlQueryType.needHybridSchedule(
            input.getPlannerContext().getTtlQueryType())) {
            String val = input.getHintVariable(ConnectionProperties.ENABLE_MPP);
            // false only if val is set to 'false'
            return !(val != null && !Boolean.parseBoolean(val));
        }
        if (routingType == null || routingType == RoutingType.ROW || RoutingType.isFollower(routingType)) {
            return input.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_MPP);
        }
        return false;
    };

    public static final MppPlanChecker COLUMNAR_ENABLE_CHECKER = input -> {
        RoutingType routingType = input.getRoutingType();
        if (routingType == null) {
            return OptimizerUtils.enableColumnarOptimizer(input.getExecutionContext().getParamManager());
        }
        if (routingType == RoutingType.ROW || RoutingType.isFollower(routingType)) {
            String val1 = input.getHintVariable(ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER);
            String val2 = input.getHintVariable(ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR);
            if (!DynamicConfig.getInstance().existColumnarNodes()) {
                val2 = "false";
            }
            if (Boolean.parseBoolean(val1) || Boolean.parseBoolean(val2)) {
                return true;
            }
            return false;
        }
        if (routingType == RoutingType.COLUMNAR || routingType == RoutingType.HTAP) {
            String val1 = input.getHintVariable(ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER);
            String val2 = input.getHintVariable(ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR);
            if (!DynamicConfig.getInstance().existColumnarNodes()) {
                val2 = "false";
            }
            if (val1 != null && !Boolean.parseBoolean(val1) && val2 != null && !Boolean.parseBoolean(val2)) {
                return false;
            }
            return true;
        }
        return false;
    };

    public static final MppPlanChecker WORKLOAD_CHECKER = input -> {
        RoutingType routingType = input.getRoutingType();
        if (routingType == null || routingType == RoutingType.ROW || RoutingType.isFollower(routingType)) {
            String workload = input.getPlannerContext().getParamManager().getString(ConnectionParams.WORKLOAD_TYPE);
            if (workload == null) {
                return true;
            }
            try {
                WorkloadType workloadType = WorkloadType.valueOf(workload.toUpperCase());
                return workloadType == WorkloadType.AP;
            } catch (Throwable t) {
                return true;
            }
        }
        if (routingType == RoutingType.COLUMNAR || routingType == RoutingType.HTAP) {
            if (!input.getExecutionContext().isUseHint()) {
                return true;
            }
            String val = input.getHintVariable(ConnectionProperties.WORKLOAD_TYPE);
            if (val != null) {
                try {
                    WorkloadType workloadType = WorkloadType.valueOf(val.toUpperCase());
                    return workloadType == WorkloadType.AP;
                } catch (Throwable t) {
                    return true;
                }
            }
            return true;
        }
        return false;
    };

    public static final MppPlanChecker TRANSACTION_CHECKER =
        input -> Optional.ofNullable(input.getExecutionContext())
            .map(MppPlanCheckers::mppSupportTransaction)
            .orElse(true);

    public static final MppPlanChecker COLUMNAR_TRANSACTION_CHECKER =
        input -> Optional.ofNullable(input.getExecutionContext())
            .map(MppPlanCheckers::columnarSupportTransaction)
            .orElse(true);

    public static final MppPlanChecker UPDATE_CHECKER =
        input -> Optional.of(input.getExecutionContext())
            .map(c -> c.getSqlType() != SqlType.SELECT_FOR_UPDATE)
            .orElse(true);

    public static final MppPlanChecker INTERNAL_SYSTEM_SQL_CHECKER =
        input -> Optional.ofNullable(input.getExecutionContext())
            .map(c -> !c.isInternalSystemSql())
            .orElse(true);

    public static final MppPlanChecker SUBQUERY_CHECKER = input -> !input.getPlannerContext().isInSubquery();

    public static final MppPlanChecker QUERY_CHECKER = input -> input.getPlannerContext()
        .getSqlKind()
        .belongsTo(SqlKind.QUERY) || (CBOUtil.isInsertSelectFromColumnar(input.getOriginalPlan()));

    // ExternalTable doesn't support mpp currently
    public static final MppPlanChecker EXTERNAL_CHECKER =
        input -> !input.getPlannerContext().hasExternalTableOperation();

    public static final MppPlanChecker CTE_CHECKER = input -> !input.getPlannerContext().isHasRecursiveCte();

    public static final MppPlanChecker EXPLAIN_EXECUTE_CHECKER =
        input -> Optional.ofNullable(input.getExecutionContext())
            .map(c -> !ExplainResult.isExplainExecute(c.getExplain()))
            .orElse(true);

    public static final MppPlanChecker EXPLAIN_STATISTICS_CHECKER =
        input -> Optional.ofNullable(input.getExecutionContext())
            .map(c -> !ExplainResult.isExplainStatistics(c.getExplain()))
            .orElse(true);

    public static final MppPlanChecker SELECT_INTO_OUT_STATISTICS_CHECKER =
        input -> !(input.getOriginalPlan() instanceof LogicalOutFile);

    public static final MppPlanChecker SAMPLE_HINT_CHECKER = input -> {
        RoutingType routingType = input.getRoutingType();
        if (routingType == null || routingType == RoutingType.ROW || RoutingType.isFollower(routingType)) {
            return !(input.getPlannerContext().getParamManager().getFloat(ConnectionParams.SAMPLE_PERCENTAGE) >= 0F
                && input.getPlannerContext().getParamManager().getFloat(ConnectionParams.SAMPLE_PERCENTAGE) <= 100F);
        }
        if (routingType == RoutingType.COLUMNAR || routingType == RoutingType.HTAP) {
            String val = input.getHintVariable(ConnectionProperties.SAMPLE_PERCENTAGE);
            return val == null;
        }
        return false;
    };

    public static ImmutableMap<MppPlanChecker, String> CHECKER_NAMES =
        ImmutableMap.<MppPlanChecker, String>builder()
            .put(MPP_ENABLED_CHECKER, "MPP_ENABLED_CHECKER")
            .put(TRANSACTION_CHECKER, "TRANSACTION_CHECKER")
            .put(COLUMNAR_TRANSACTION_CHECKER, "COLUMNAR_TRANSACTION_CHECKER")
            .put(UPDATE_CHECKER, "UPDATE_CHECKER")
            .put(INTERNAL_SYSTEM_SQL_CHECKER, "INTERNAL_SYSTEM_SQL_CHECKER")
            .put(SUBQUERY_CHECKER, "SUBQUERY_CHECKER")
            .put(QUERY_CHECKER, "QUERY_CHECKER")
            .put(EXTERNAL_CHECKER, "EXTERNAL_CHECKER")
            .put(CTE_CHECKER, "CTE_CHECKER")
            .put(EXPLAIN_EXECUTE_CHECKER, "EXPLAIN_EXECUTE_CHECKER")
            .put(EXPLAIN_STATISTICS_CHECKER, "EXPLAIN_STATISTICS_CHECKER")
            .put(SELECT_INTO_OUT_STATISTICS_CHECKER, "SELECT_INTO_OUT_STATISTICS_CHECKER")
            .put(SAMPLE_HINT_CHECKER, "SAMPLE_HINT_CHECKER")
            .put(COLUMNAR_ENABLE_CHECKER, "COLUMNAR_ENABLE_CHECKER")
            .put(WORKLOAD_CHECKER, "WORKLOAD_CHECKER")
            .build();

    public static final MppPlanChecker BASIC_CHECKERS =
        input -> {
            boolean support =
                Lists.newArrayList(MPP_ENABLED_CHECKER, SUBQUERY_CHECKER, QUERY_CHECKER, INTERNAL_SYSTEM_SQL_CHECKER,
                        EXPLAIN_EXECUTE_CHECKER, EXTERNAL_CHECKER, CTE_CHECKER, SELECT_INTO_OUT_STATISTICS_CHECKER,
                        EXPLAIN_STATISTICS_CHECKER, WORKLOAD_CHECKER)
                    .stream()
                    .allMatch(c -> checkAndTrace(c, input));
            HtapTrace.addTrace(input.getPlannerContext().getHtapTrace(),
                "BASIC_CHECKERS " + (support ? "succeed" : "failed"));
            return support;
        };

    public static final MppPlanChecker COLUMNAR_BASIC_CHECKERS =
        input -> {
            boolean support =
                Lists.newArrayList(COLUMNAR_ENABLE_CHECKER, SAMPLE_HINT_CHECKER, COLUMNAR_TRANSACTION_CHECKER,
                        UPDATE_CHECKER)
                    .stream()
                    .allMatch(c -> checkAndTrace(c, input));
            HtapTrace.addTrace(input.getPlannerContext().getHtapTrace(),
                "COLUMNAR_BASIC_CHECKERS " + (support ? "succeed" : "failed"));
            return support;
        };

    private static boolean checkAndTrace(MppPlanChecker checker, MppPlanCheckerInput input) {
        boolean support = checker.supportsMpp(input);
        if (CHECKER_NAMES.containsKey(checker)) {
            HtapTrace.addTrace(input.getPlannerContext().getHtapTrace(),
                CHECKER_NAMES.get(checker) + (support ? " succeed" : " failed"));
        }
        return support;
    }

    public static boolean supportsMppPlan(RelNode plan, PlannerContext context, ExecutionContext ec,
                                          RoutingType routingType,
                                          MppPlanChecker... checkers) {
        MppPlanCheckerInput input = new MppPlanCheckerInput(plan, context, ec, routingType);

        return Arrays.stream(checkers)
            .allMatch(c -> checkAndTrace(c, input));
    }

    public static boolean mppSupportTransaction(ExecutionContext context) {
        if (!ConfigDataMode.isMasterMode()) {
            //只读实例上任何事务策略下都可以跑 mpp
            return true;
        } else {
            boolean mppNotSupportTransaction = false;
            if (context.getTransaction() != null &&
                context.getTransaction().getTransactionClass().isA(EXPLICIT_TRANSACTION)) {
                mppNotSupportTransaction = true;
            } else if (!context.isAutoCommit()) {
                mppNotSupportTransaction = true;
            }
            return !mppNotSupportTransaction;
        }
    }

    public static boolean columnarSupportTransaction(ExecutionContext context) {
        // 允许列存事务
        if (context.getTransaction() != null &&
            context.getTransaction().getTransactionClass().isA(COLUMNAR_TRANSACTION)) {
            return true;
        }

        // 除列存事务外，允许 auto commit 事务
        boolean mppNotSupportTransaction = false;
        if (context.getTransaction() != null &&
            context.getTransaction().getTransactionClass().isA(EXPLICIT_TRANSACTION)) {
            mppNotSupportTransaction = true;
        } else if (!context.isAutoCommit()) {
            mppNotSupportTransaction = true;
        }
        return !mppNotSupportTransaction;
    }
}
