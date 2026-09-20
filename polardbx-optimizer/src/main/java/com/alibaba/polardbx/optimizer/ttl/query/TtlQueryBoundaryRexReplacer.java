package com.alibaba.polardbx.optimizer.ttl.query;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.Arrays;

public class TtlQueryBoundaryRexReplacer extends RexShuttle {

    private final ExecutionContext ec;

    private final RelOptCluster cluster;

    public TtlQueryBoundaryRexReplacer(ExecutionContext ec, RelOptCluster cluster) {
        this.ec = ec;
        this.cluster = cluster;
    }

    @Override
    public RexNode visitCall(RexCall call) {
//            if (call.getOperator() == TddlOperatorTable.TTL_QUERY_BOUNDARY) {
//                Object o = RexUtils.getEvalFunc(ec).apply(call);
//                if (o == null) {
//                    return cluster.getRexBuilder().makeNullLiteral(call.getType());
//                }
//                // TODO : convert ttl col datatype
//                this.ttlQueryBoundary = DataTypes.StringType.convertFrom(o);
//                return cluster.getRexBuilder().makeLiteral(ttlQueryBoundary);
//            }

        if (call.getOperator() == TddlOperatorTable.GREATER_THAN
            || call.getOperator() == TddlOperatorTable.GREATER_THAN_OR_EQUAL
            || call.getOperator() == TddlOperatorTable.LESS_THAN
            || call.getOperator() == TddlOperatorTable.LESS_THAN_OR_EQUAL) {

            int ttlQueryBoundaryCallIndex = -1;
            if (call.getOperands().get(0) instanceof RexCall
                && ((RexCall) call.getOperands().get(0)).getOperator() == TddlOperatorTable.TTL_QUERY_BOUNDARY) {
                ttlQueryBoundaryCallIndex = 0;
            } else if (call.getOperands().get(1) instanceof RexCall
                && ((RexCall) call.getOperands().get(1)).getOperator() == TddlOperatorTable.TTL_QUERY_BOUNDARY) {
                ttlQueryBoundaryCallIndex = 1;
            }
            if (ttlQueryBoundaryCallIndex < 0) {
                return super.visitCall(call);
            }
            RexCall ttlQueryBoundaryCall = (RexCall) call.getOperands().get(ttlQueryBoundaryCallIndex);
            Object o = RexUtils.getEvalFunc(ec).apply(ttlQueryBoundaryCall);
            if (o == null) {
                return cluster.getRexBuilder().makeLiteral(true);
            }

            String schemaName = DataTypes.StringType.convertFrom(
                RexUtils.getEvalFunc(ec).apply(ttlQueryBoundaryCall.getOperands().get(0)));
            String tableName = DataTypes.StringType.convertFrom(
                RexUtils.getEvalFunc(ec).apply(ttlQueryBoundaryCall.getOperands().get(1)));
            ColumnMeta ttlColMeta =
                ec.getSchemaManager(schemaName).getTable(tableName).getTtlDefinitionInfo().getTtlColMeta(ec);
            RexNode ttlQueryBoundaryRexNode = null;
            if (DataTypeUtil.isNumberSqlType(ttlColMeta.getDataType())) {
                long ttlQueryBoundary = DataTypes.LongType.convertFrom(o);
                ttlQueryBoundaryRexNode = cluster.getRexBuilder().makeBigIntLiteral(ttlQueryBoundary);
            } else {
                String ttlQueryBoundary = DataTypes.StringType.convertFrom(o);
                ttlQueryBoundaryRexNode = cluster.getRexBuilder().makeLiteral(ttlQueryBoundary);
            }

            return call.clone(call.getType(),
                ttlQueryBoundaryCallIndex == 0 ?
                    Arrays.asList(ttlQueryBoundaryRexNode, call.getOperands().get(1)) :
                    Arrays.asList(call.getOperands().get(0), ttlQueryBoundaryRexNode));

        }
        return super.visitCall(call);
    }
}

