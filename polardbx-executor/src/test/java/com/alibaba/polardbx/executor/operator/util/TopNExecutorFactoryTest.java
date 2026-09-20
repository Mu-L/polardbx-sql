package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.mpp.operator.factory.TopNExecutorFactory;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SpilledTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopNExecutorFactoryTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private ExecutionContext context;

    @Before
    public void before() {
        context = new ExecutionContext();
        context.setTraceId("mock_trace_id");
        context.setMemoryPool(
            MemoryManager.getInstance().createQueryMemoryPool(true, context.getTraceId(), context.getExtraCmds()));
    }

    @Test
    public void test() {

        RexNode offset = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0);
        RexNode fetch = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1);

        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        currentParameter.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {null, 1000000L}));
        currentParameter.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {null, 100L}));

        Parameters parameters = new Parameters(currentParameter);
        context.setParams(parameters);

        RelNode input = Mockito.mock(RelNode.class);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);
        RelDataType rowType = new RelRecordType(
            ImmutableList.of(
                new RelDataTypeFieldImpl("f1", 0, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)),
                new RelDataTypeFieldImpl("f2", 1, TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            )
        );
        Mockito.when(input.getCluster()).thenReturn(cluster);
        Mockito.when(cluster.getRexBuilder()).thenReturn(REX_BUILDER);
        Mockito.when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
        Mockito.when(input.getRowType()).thenReturn(rowType);

        final TopN topN = TopN.create(
            RelTraitSet.createEmpty(),
            input,
            RelCollations.of(
                new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING),
                new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)
            ),
            offset, fetch
        );

        final int parallelism = 16;

        final List<DataType> dataTypeList = ImmutableList.of(
            DataTypes.LongType,
            new SliceType()
        );

        final SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        TopNExecutorFactory topNExecutorFactory =
            new TopNExecutorFactory(topN, parallelism, dataTypeList, spillerFactory);
        topNExecutorFactory.setParentGlobalThresholdFuture(SettableFuture.create(), false);
        topNExecutorFactory.setInputSorted(false);
        topNExecutorFactory.setManager(null);

        for (int i = 0; i < parallelism; i++) {
            Executor exec = topNExecutorFactory.createExecutor(context, i);
            Assert.assertTrue(exec instanceof SpilledTopNExec);
        }
    }
}
