/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.planner.dml;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.PartitionGather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-JVM reproduction and fix-mechanism characterization for the NPE that {@link PartitionGather}
 * hits when gathering partitions for an INSERT ... ON DUPLICATE KEY UPDATE during a DN HA switchover.
 *
 * <p>This test builds the real optimized upsert plan (parameterized like a PreparedStatement) and
 * exercises the real {@code RexUtils.updateParam}:
 * <ul>
 *     <li>with {@code handleDuplicateKeyUpdateList = true} (old behavior): throws the switchover NPE;</li>
 *     <li>with {@code handleDuplicateKeyUpdateList = false} (the fix): does not evaluate the update
 *     list and therefore does not throw.</li>
 * </ul>
 */
public class PartitionGatherUpsertTest extends ParameterizedTestCommon {

    private LogicalInsert capturedInsert;
    private Map<Integer, ParameterContext> capturedParams;
    private String capturedSchema;

    public PartitionGatherUpsertTest(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile, 0, "select 1", "", "0");
        this.ignoreBaseTest = true;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(
            new Object[] {"PartitionGatherUpsertTest", "/com/alibaba/polardbx/planner/dml/PartitionGatherUpsertTest"});
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
        Map<Integer, ParameterContext> param =
            OptimizerUtils.buildParam(sqlParameterized.getParameters());
        executionContext.setParams(new Parameters(param, false));
        SqlNodeList astList = new FastsqlParser().parse(
            sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        SqlNode ast = astList.get(0);
        final HintPlanner hintPlanner =
            HintPlanner.getInstance(appName, executionContext);
        executionContext.getExtraCmds().put(
            ConnectionProperties.ENABLE_AUTO_FORCE_INDEX, enableAutoForceIndex);
        executionContext.getExtraCmds().putAll(configMaps);
        final HintCmdOperator.CmdBean cmdBean =
            new HintCmdOperator.CmdBean(appName,
                executionContext.getExtraCmds(), executionContext.getGroupHint());
        executionContext.setInternalSystemSql(false);
        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        processParameter(sqlParameterized, executionContext);
        PlannerContext plannerContext =
            PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);
        plannerContext.setAddForcePrimary(addForcePrimary);

        ExecutionPlan executionPlan =
            Planner.getInstance().getPlan(ast, plannerContext);
        executionPlan = PostPlanner.getInstance().optimize(
            executionPlan, executionContext);

        // --- Capture the LogicalInsert and params for our test ---
        final LogicalInsert[] holder = new LogicalInsert[1];
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof LogicalInsert && holder[0] == null) {
                    holder[0] = (LogicalInsert) node;
                }
                super.visit(node, ordinal, parent);
            }
        }.go(executionPlan.getPlan());
        capturedInsert = holder[0];
        capturedParams = executionContext.getParamMap() != null
            ? new HashMap<>(executionContext.getParamMap()) : new HashMap<>();
        capturedSchema = executionContext.getSchemaName();
        // --- End capture ---

        String planStr = RelUtils.toString(executionPlan.getPlan(), param,
            RexUtils.getEvalFunc(executionContext), executionContext);
        return removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null
                ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());
    }

    /**
     * Build an ExecutionContext mirroring the one {@link PartitionGather} creates for a LogicalInsert.
     */
    private ExecutionContext newGatherContext() {
        final ExecutionContext gatherEc = new ExecutionContext();
        gatherEc.setParams(new Parameters(new HashMap<>(capturedParams), false));
        gatherEc.setSchemaName(capturedSchema);
        if (capturedInsert.getBatchSize() > 0) {
            capturedInsert.buildParamsForBatch(gatherEc);
        }
        return gatherEc;
    }

    @Test
    public void testUpdateParamSkipsDuplicateKeyUpdateDuringGather() {
        final String sql = "INSERT INTO address_balance_chain "
            + "(address, chain_index, token_address, coin_type, sub_balance_raw, "
            + " sub_balance_height, ext_data, sub_last_transaction_time, sub_upd_time) "
            + "VALUES "
            + "('0xe75e', '8453', '''''', 0, '359603002073839154', 47135497, '', 1781060341, 1781060341969), "
            + "('0xd057', '8453', '''''', 0, '58899701461779', 47135497, '', 1781060341, 1781060341969), "
            + "('0x278d', '8453', '''''', 0, '21162261333253', 47135497, '', 1781060341, 1781060341969) "
            + "ON DUPLICATE KEY UPDATE "
            + " coin_type = values(coin_type), "
            // Self-reference introduces a RexInputRef in the update expression, evaluated with a null
            // row during partition gathering -> reproduces the switchover NPE.
            + " sub_balance_raw = sub_balance_raw + values(sub_balance_raw), "
            + " sub_balance_height = values(sub_balance_height), "
            + " ext_data = values(ext_data), "
            + " sub_last_transaction_time = values(sub_last_transaction_time), "
            + " sub_upd_time = values(sub_upd_time)";

        getPlan(sql);
        Assert.assertNotNull("plan must contain a LogicalInsert", capturedInsert);
        Assert.assertTrue("plan must be an upsert", capturedInsert.withDuplicateKeyUpdate());

        // The fix: evaluating partitions must NOT process the ON DUPLICATE KEY UPDATE list.
        RexUtils.updateParam(capturedInsert, newGatherContext(), false, null);

        // The bug: processing the ON DUPLICATE KEY UPDATE list evaluates a RexInputRef with a null
        // row and throws NPE - this is exactly what the old PartitionGather code triggered.
        try {
            RexUtils.updateParam(capturedInsert, newGatherContext(), true, null);
            Assert.fail("Expected NullPointerException when evaluating ON DUPLICATE KEY UPDATE with a null row");
        } catch (NullPointerException expected) {
            // expected: reproduces the switchover NPE
        }
    }
}
