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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * make sure don't record parameters of big sql
 */
public class CacheKeyLenTest extends ParameterizedTestCommon {

    public CacheKeyLenTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CacheKeyLenTest.class);
    }

    @Override
    protected String getPlan(String testSql) {

        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setAppName(appName);
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(testSql), currentParameter, executionContext, false);
        setSysDefVariable(sqlParameterized.getParameters());
        Map<Integer, ParameterContext> param = OptimizerUtils.buildParam(sqlParameterized.getParameters());
        executionContext.setParams(new Parameters(param, false));
        SqlNodeList astList = new FastsqlParser().parse(
            sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        SqlNode ast = astList.get(0);
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.getExtraCmds().putAll(configMaps);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName,
            executionContext.getExtraCmds(),
            executionContext.getGroupHint());
        executionContext.setInternalSystemSql(false);

        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        processParameter(sqlParameterized, executionContext);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);
        plannerContext.setAddForcePrimary(addForcePrimary);

        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);

        PlanCache.CacheKey cacheKey = new PlanCache.CacheKey(appName, sqlParameterized, null, null, true, true,
            true, null, 0L, false, false, null);
        return "record para:" + (cacheKey.getParameters() != null)
            + "\ntypeDigest:" + cacheKey.getTypeDigest();
    }
}
