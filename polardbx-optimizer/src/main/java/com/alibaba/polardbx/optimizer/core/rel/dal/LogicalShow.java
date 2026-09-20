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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.rel.externalize.RelDrdsWriter;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalShow extends LogicalDal {

    public static final int DB_INDEX_MODE_NORMAL = 0;
    public static final int DB_INDEX_MODE_RANDOM = 1;

    private boolean showForTruncateTable = false;

    private int dbIndexMode = DB_INDEX_MODE_NORMAL;

    private LogicalShow(Show show, String dbIndex,
                        String phyTable, String schemaName, int dbIndexMode) {
        super(show, dbIndex, phyTable, schemaName);
        this.dbIndexMode = dbIndexMode;
    }

    public static LogicalShow create(Show show, String dbIndex, String phyTable, String schemaName) {
        return new LogicalShow(show, dbIndex, phyTable, schemaName, DB_INDEX_MODE_NORMAL);
    }

    public static LogicalShow create(Show show, String dbIndex, String phyTable, String schemaName, int dbIndexMode) {
        return new LogicalShow(show, dbIndex, phyTable, schemaName, dbIndexMode);
    }

    @Override
    protected String getExplainName() {
        return "LogicalShow";
    }

    @Override
    public LogicalShow copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return create((Show) dal.copy(traitSet, inputs), dbIndex, phyTable, schemaName, dbIndexMode);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExecutionContext ec = PlannerContext.getPlannerContext(this).getExecutionContext();
        boolean isShowDbIndexMode = ec.getParamManager().getBoolean(ConnectionParams.EXPLAIN_SHOW_DB_INDEX_MODE);
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        if (isShowDbIndexMode) {
            pw.item("node_mode", getNodeMode(dbIndexMode));
        }
        pw.item("sql", this.bytesSql.display());
        return pw;
    }

    public static String getNodeMode(int dbIndexMode) {
        if (dbIndexMode == DB_INDEX_MODE_NORMAL) {
            return "normal";
        } else if (dbIndexMode == DB_INDEX_MODE_RANDOM) {
            return "random";
        } else {
            return "unknown";
        }
    }

    public boolean isShowForTruncateTable() {
        return showForTruncateTable;
    }

    public void setShowForTruncateTable(boolean showForTruncateTable) {
        this.showForTruncateTable = showForTruncateTable;
    }

    public int getDbIndexMode() {
        return dbIndexMode;
    }

    public void setDbIndexMode(int dbIndexMode) {
        this.dbIndexMode = dbIndexMode;
    }
}
