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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsJsonWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.TABLE_NAME_PARAM_INDEX;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.buildParameterContextForTableName;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.changeParameterContextIndex;

/**
 * @author lingce.ldm 2017-11-16 13:43
 */
public abstract class BaseTableOperation extends BaseQueryOperation {

    /**
     * TableScan 中过滤条件对应原始参数的下标
     */
    protected List<Integer> paramIndex;

    private LockMode lockMode = LockMode.UNDEF;

    // the logical plan that produce the current phy relnode
    protected RelNode logicalPlan;

    // Whether this plan is executed successfully.
    private boolean successExecuted = true;

    protected BaseTableOperation(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, null, null, null);
    }

    protected BaseTableOperation(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType,
                                 CursorMeta cursorMeta, RelNode logicalPlan) {
        super(cluster, traitSet, null, null, null);
        this.rowType = rowType;
        this.cursorMeta = cursorMeta;
        if (cursorMeta == null && rowType != null) {
            this.cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "TableScan"));
        }
        this.logicalPlan = logicalPlan;
        if (logicalPlan != null && logicalPlan instanceof AbstractRelNode) {
            this.schemaName = ((AbstractRelNode) logicalPlan).getSchemaName();
        }
    }

    protected BaseTableOperation(BaseTableOperation operation) {
        super(operation);
        logicalPlan = operation.logicalPlan;
        paramIndex = operation.paramIndex;
        lockMode = operation.lockMode;
        if (logicalPlan != null && logicalPlan instanceof AbstractRelNode) {
            this.schemaName = ((AbstractRelNode) logicalPlan).getSchemaName();
        }
    }

    public void initOperation() {
    }

    @Override
    public String getNativeSql() {
        return bytesSql.toString(null);
    }

    public void setParamIndex(List<Integer> paramIndex) {
        this.paramIndex = paramIndex;
    }

    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        return null;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExecutionContext ec = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        ExplainInfo explainInfo = buildExplainInfo(((RelDrdsWriter) pw).getParams(), ec);
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        String groupAndTableName = explainInfo.groupName + (TStringUtil.isNotBlank(explainInfo.groupName) ? "." : "")
            + StringUtils.join(explainInfo.tableNames, ",");
        groupAndTableName = rebuildGroupAndTableNamesByUsingPartitionsIfNeed(ec, explainInfo, groupAndTableName);

        pw.itemIf("tables", groupAndTableName, groupAndTableName != null);
        String sql = TStringUtil.replace(getNativeSql(), "\n", " ");
        pw.item("sql", sql);
        StringBuilder builder = new StringBuilder();
        if (MapUtils.isNotEmpty(explainInfo.params)) {
            String operator = "";
            for (Object c : explainInfo.params.values()) {
                Object v = ((ParameterContext) c).getValue();
                builder.append(operator);
                if (v instanceof TableName) {
                    builder.append(((TableName) v).getTableName());
                } else {
                    builder.append(v == null ? "NULL" : v.toString());
                }
                operator = ",";
            }
            pw.item("params", builder.toString());
        }

        // XPlan explain.
        final ExecutionContext executionContext;
        if (pw instanceof RelDrdsWriter) {
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        } else if (pw instanceof RelDrdsJsonWriter) {
            executionContext = (ExecutionContext) ((RelDrdsJsonWriter) pw).getExecutionContext();
        } else {
            executionContext = null;
        }
        if (XTemplate != null && executionContext != null &&
            executionContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_X_PLAN)) {
            final JsonFormat format = new JsonFormat();
            final PolarxExecPlan.ExecPlan plan = XTemplate.explain(executionContext);
            if (null == plan) {
                pw.item("XPlan", "Denied by param.");
            } else {
                pw.item("XPlan", format.printToString(plan));
            }
        }

        if (executionContext != null && executionContext.getParamManager()
            .getBoolean(ConnectionParams.EXPLAIN_SHOW_PHYSICAL_PLAN)
            && executionContext.getExplain() != null
            && (executionContext.getExplain().explainMode == ExplainResult.ExplainMode.DETAIL
            || executionContext.getExplain().explainMode == ExplainResult.ExplainMode.COST
            || executionContext.getExplain().explainMode == ExplainResult.ExplainMode.ANALYZE)) {
            RelUtils.displayPhysicalPlan(this, pw, executionContext);
        }

        return pw;
    }

    private String rebuildGroupAndTableNamesByUsingPartitionsIfNeed(ExecutionContext ec, ExplainInfo explainInfo,
                                                                    String groupAndTableName) {
        boolean usePartitionsOfTablesInLvForShowCreateTable = false;
        if (ec != null) {
            usePartitionsOfTablesInLvForShowCreateTable =
                ec.getParamManager().getBoolean(ConnectionParams.SHOW_PARTITIONS_IN_LOGICALVIEW_FOR_SHOW_CREATE_TABLE);
        }
        if (usePartitionsOfTablesInLvForShowCreateTable) {
            boolean isPhyOp0rPhyDdlOp = this instanceof PhyDdlTableOperation || this instanceof PhyTableOperation;
            ;
            if (DbInfoManager.getInstance().isNewPartitionDb(this.schemaName) && !isPhyOp0rPhyDdlOp) {
                String dbName = this.schemaName;
                List<String> logTblList = this.getLogicalTableNames();
                List<String> phyTblList = (List<String>) explainInfo.tableNames;
                SchemaManager sc = ec.getSchemaManager(dbName);
                String phyGrp = explainInfo.groupName;
                List<String> targetListOfLogTblNameWithParts = new ArrayList<>();
                for (int i = 0; i < logTblList.size(); i++) {
                    String logTb = logTblList.get(i);
                    String phyTb = phyTblList.get(i);
                    TableMeta tm = sc.getTable(logTb);
                    PartitionInfo partInfo = tm.getPartitionInfo();
                    PartitionSpec ps = partInfo.getPartSpecSearcher().getPartSpec(phyGrp, phyTb);
                    if (ps != null) {
                        String logTblNameWithPart = String.format("%s[%s]", logTb, ps.getName());
                        targetListOfLogTblNameWithParts.add(logTblNameWithPart);
                    }
                }
                groupAndTableName = phyGrp + (TStringUtil.isNotBlank(phyGrp) ? "." : "")
                    + StringUtils.join(targetListOfLogTblNameWithParts, ",");
            }
        }
        return groupAndTableName;
    }

    protected Map<Integer, ParameterContext> buildParam(String tableName, Map<Integer, ParameterContext> param) {
        Map<Integer, ParameterContext> newParam = new HashMap<>();
        /**
         * 添加 TableName
         */
        int index = 1;
        for (int i : paramIndex) {
            if (i == TABLE_NAME_PARAM_INDEX) {
                newParam.put(index, buildParameterContextForTableName(tableName, index));
            } else {
                final ParameterContext parameterContext = param.get(i + 1);
                if (null != parameterContext) {
                    newParam.put(index, changeParameterContextIndex(parameterContext, index));
                }
            }
            index++;
        }
        return newParam;
    }

    public abstract <T> List<T> getTableNames();

    public abstract List<String> getLogicalTableNames();

    @Override
    protected String getExplainName() {
        return "LogicalView";
    }

    protected abstract ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params,
                                                    ExecutionContext executionContext);

    public boolean withLock() {
        return lockMode != LockMode.UNDEF;
    }

    public boolean isForUpdate() {
        return lockMode == LockMode.EXCLUSIVE_LOCK || lockMode == LockMode.SHARED_LOCK;
    }

    public void setLockMode(LockMode lockMode) {
        this.lockMode = lockMode;
    }

    public LockMode getLockMode() {
        return this.lockMode;
    }

    class ExplainInfo<T> {

        public final List<T> tableNames;
        public final String groupName;
        public final Map<Integer, ParameterContext> params;

        public ExplainInfo(List<T> tableNames, String groupName, Map<Integer, ParameterContext> params) {
            this.tableNames = tableNames;
            this.groupName = groupName;
            this.params = params;
        }

    }

    public <T extends RelNode> T getParent() {
        return (T) logicalPlan;
    }

    public List<Integer> getParamIndex() {
        return paramIndex;
    }

    public void setSuccessExecuted(boolean successExecuted) {
        this.successExecuted = successExecuted;
    }

    public boolean isSuccessExecuted() {
        return successExecuted;
    }
}
