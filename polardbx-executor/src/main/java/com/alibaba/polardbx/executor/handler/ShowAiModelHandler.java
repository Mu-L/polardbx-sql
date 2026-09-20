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
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.model.ModelConfigRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowAiModel;

import java.util.List;

/**
 * Handler for SHOW AI MODEL [FROM model_name].
 *
 * <p>Lists all registered AI models or a specific one from the model cache.
 */
public class ShowAiModelHandler extends HandlerCommon {

    public ShowAiModelHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowAiModel showAiModel = (SqlShowAiModel) show.getNativeSqlNode();
        final String targetModel = showAiModel.getModelName();

        ArrayResultCursor result = new ArrayResultCursor("AI_MODELS");
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("MODEL", DataTypes.StringType);
        result.addColumn("PROVIDER", DataTypes.StringType);
        result.addColumn("ENDPOINT", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("DESCRIPTION", DataTypes.StringType);

        ModelManager modelManager = ModelManager.getInstance();
        List<ModelConfigRecord> allModels = modelManager.getAllModels();

        for (ModelConfigRecord record : allModels) {
            if (targetModel != null
                && !targetModel.trim().isEmpty()
                && !record.name.equalsIgnoreCase(targetModel.trim())) {
                continue;
            }

            result.addRow(new Object[] {
                record.name,
                record.model,
                record.provider,
                record.endpoint,
                record.status,
                record.description
            });
        }

        return result;
    }
}
