package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.htaprouting.HtapTrace;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleClassifier;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class RoutingRuleTestCommon extends ParameterizedTestCommon {
    protected static final String fakeUser = "fakeUser";

    public RoutingRuleTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan,
                                 String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        try (MockedStatic<RoutingRuleManager> mocked = Mockito.mockStatic(RoutingRuleManager.class)) {
            RoutingRuleManager manager = mock(RoutingRuleManager.class);
            when(manager.getClassifier()).thenReturn(buildRoutingClassifier());
            doCallRealMethod().when(manager).determineRoutingType(any(), any(), any());
            mocked.when(RoutingRuleManager::getInstance).thenReturn(manager);

            Map<Integer, ParameterContext> currentParameter = new HashMap<>();
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.setServerVariables(new HashMap<>());
            executionContext.setAppName(appName);
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
                ByteString.from(testSql), currentParameter, executionContext, false);
            setSysDefVariable(sqlParameterized.getParameters());
            Map<Integer, ParameterContext> param = OptimizerUtils.buildParam(sqlParameterized.getParameters());
            SqlNodeList astList = new FastsqlParser().parse(
                sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
            SqlNode ast = astList.get(0);
            executionContext.setInternalSystemSql(false);

            executionContext.setPrivilegeContext(new PrivilegeContext());
            executionContext.getPrivilegeContext().setUser(fakeUser);

            final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
            executionContext.setParams(new Parameters(param, false));
            executionContext.getExtraCmds().putAll(configMaps);

            final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName,
                new HashMap<>(),
                executionContext.getGroupHint());
            hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
            processParameter(sqlParameterized, executionContext);

            executionContext.putAllHintCmds(cmdBean.getExtraCmd());
            executionContext.setHtapTrace(new HtapTrace());
            executionContext.setRoutingType(RoutingType.determineRoutingType(
                executionContext, null, sqlParameterized));
            PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
            plannerContext.setSchemaName(appName);

            ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
            executionPlan = PostPlanner.getInstance().optimize(executionPlan, executionContext);
            executionPlan = afterOptimize(testSql, executionContext, executionPlan);
            String planStr = RelUtils
                .toString(executionPlan.getPlan(), param, RexUtils.getEvalFunc(executionContext), executionContext);

            String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
                executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
                executionContext.getSqlExplainLevel());
            return String.format("routing type: %s\noptimizer type: %s\n%s",
                executionContext.getHtapTrace().get().printRoutType(),
                executionContext.getHtapTrace().get().printOptimizerType(),
                code);
        }
    }

    protected abstract RoutingRuleClassifier buildRoutingClassifier();
}
