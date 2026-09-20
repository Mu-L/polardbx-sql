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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.ai.ModelManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.function.calc.scalar.ai.AiFunctionManager;
import com.alibaba.polardbx.executor.function.calc.scalar.ai.AiFunctionMetadata;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.IScalarFunction;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowAiFunction;

import java.util.List;

/**
 * Handler for SHOW AI FUNCTION [FROM function_name].
 *
 * <p>Shows information about AI functions including the default built-in model
 * and the actual model that will be invoked at runtime.
 */
public class ShowAiFunctionHandler extends HandlerCommon {

    public ShowAiFunctionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowAiFunction showAiFunction = (SqlShowAiFunction) show.getNativeSqlNode();
        final String targetFunction = showAiFunction.getFunctionName();

        ArrayResultCursor result = new ArrayResultCursor("AI_FUNCTIONS");
        result.addColumn("FUNCTION", DataTypes.StringType);
        result.addColumn("DEFAULT_MODEL", DataTypes.StringType);
        result.addColumn("ACTUAL_MODEL", DataTypes.StringType);
        result.addColumn("DESCRIPTION", DataTypes.StringType);

        List<IScalarFunction> functions = AiFunctionManager.getRegisteredFunctions();

        for (IScalarFunction function : functions) {
            String functionName = function.getFunctionNames()[0];

            if (targetFunction != null
                && !targetFunction.trim().isEmpty()
                && !functionName.equalsIgnoreCase(targetFunction.trim())) {
                continue;
            }

            AiFunctionMetadata metadata = (AiFunctionMetadata) function;
            String defaultModel = metadata.getDefaultModelName() != null ? metadata.getDefaultModelName() : "";
            String description = metadata.getDescription() != null ? metadata.getDescription() : "";
            String actualModel = resolveActualModel(defaultModel);

            result.addRow(new Object[] {
                functionName,
                defaultModel,
                actualModel,
                description
            });
        }

        return result;
    }

    /**
     * Resolve the actual underlying model name from the default built-in model name.
     * For example, "__POLARDBX_QWEN_PLUS" resolves to "qwen-plus".
     */
    private String resolveActualModel(String defaultModelName) {
        if (defaultModelName == null || defaultModelName.isEmpty()) {
            return "";
        }
        ModelConfigRecord config = ModelManager.getInstance().getModelConfig(defaultModelName);
        if (config != null && config.model != null) {
            return config.model;
        }
        return "";
    }
}
