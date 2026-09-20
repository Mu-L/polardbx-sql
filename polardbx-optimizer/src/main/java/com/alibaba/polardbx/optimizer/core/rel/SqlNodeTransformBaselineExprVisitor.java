package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;

import java.util.Map;

/**
 * @author fangwu
 */
public class SqlNodeTransformBaselineExprVisitor extends SqlShuttle {
    private final Map<String, RexNode> rexNodeTableMap;

    private final SchemaManager schemaManager;

    private final Parameters parameters;

    public SqlNodeTransformBaselineExprVisitor(Map<String, RexNode> rexNodeTableMap,
                                               SchemaManager schemaManager,
                                               Parameters parameters) {
        this.rexNodeTableMap = rexNodeTableMap;
        this.schemaManager = schemaManager;
        this.parameters = parameters;
    }

    /**
     * A RexShuttle used in the implementation of
     * {@link RexProgram#expandLocalRef}.
     */
    static class IdentifierShuttle extends RexShuttle {
        private final String targetColName;
        private final String tableName;
        private final SchemaManager schemaManager;
        private final Parameters parameters;

        private SqlDynamicParam sqlDynamicParam;

        IdentifierShuttle(String targetColName, String tableName, SchemaManager schemaManager, Parameters parameters) {
            this.targetColName = targetColName;
            this.tableName = tableName;
            this.schemaManager = schemaManager;
            this.parameters = parameters;
        }

        public RexNode visitCall(final RexCall call) {
            if (call.getOperator() == SqlStdOperatorTable.EQUALS) {
                RexInputRef colRef = null;
                RexDynamicParam dynamicParam = null;
                if (call.getOperands().get(0) instanceof RexInputRef &&
                    call.getOperands().get(1) instanceof RexDynamicParam) {
                    colRef = (RexInputRef) call.getOperands().get(0);
                    dynamicParam = (RexDynamicParam) call.getOperands().get(1);
                } else if (call.getOperands().get(1) instanceof RexInputRef &&
                    call.getOperands().get(0) instanceof RexDynamicParam) {
                    colRef = (RexInputRef) call.getOperands().get(1);
                    dynamicParam = (RexDynamicParam) call.getOperands().get(0);
                }
                if (colRef == null || dynamicParam == null) {
                    return super.visitCall(call);
                }
                TableMeta tableMeta = schemaManager.getTableWithNull(tableName);
                String colName = tableMeta.getAllColumns().get(colRef.getIndex()).getName();
                if (colName.equalsIgnoreCase(targetColName)) {
                    ParameterContext pc = parameters.getCurrentParameter().get(dynamicParam.getIndex() + 1);
                    if (pc.getValue() instanceof Number) {
                        sqlDynamicParam =
                            new SqlDynamicParam(dynamicParam.getIndex(), SqlTypeName.BIGINT, SqlParserPos.ZERO);
                        throw Util.FoundOne.NULL;
                    } else if (pc.getValue() instanceof String) {
                        sqlDynamicParam =
                            new SqlDynamicParam(dynamicParam.getIndex(), SqlTypeName.VARCHAR, SqlParserPos.ZERO);
                        throw Util.FoundOne.NULL;
                    }
                }
            }
            return super.visitCall(call);
        }
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        assert id.names.size() == 2;
        String tb = id.names.get(0);
        if (rexNodeTableMap.containsKey(tb)) {
            String colName = id.names.get(1);
            RexNode rex = rexNodeTableMap.get(tb);

            // find column rexnode inside rex
            IdentifierShuttle identifierShuttle = new IdentifierShuttle(colName, tb, schemaManager, parameters);
            try {
                rex.accept(identifierShuttle);
            } catch (Util.FoundOne e) {
                return identifierShuttle.sqlDynamicParam;
            }
        }
        return SqlNode.clone(id);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        return SqlNode.clone(param);
    }
}
