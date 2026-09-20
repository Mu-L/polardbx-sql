package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.GroupTopNExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupTopExecutorFactoryTest {
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
        // Create dynamic parameters for fetch
        RexNode fetch = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0);

        // Set up parameter context
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        currentParameter.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {null, 100L}));

        Parameters parameters = new Parameters(currentParameter);
        context.setParams(parameters);

        // Mock RelNode and related components
        RelNode input = Mockito.mock(RelNode.class);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);
        RelDataType rowType = new RelRecordType(
            ImmutableList.of(
                new RelDataTypeFieldImpl("group_col", 0, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
                new RelDataTypeFieldImpl("sort_col", 1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)),
                new RelDataTypeFieldImpl("data_col", 2, TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
            )
        );
        Mockito.when(input.getCluster()).thenReturn(cluster);
        Mockito.when(cluster.getRexBuilder()).thenReturn(REX_BUILDER);
        Mockito.when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
        Mockito.when(input.getRowType()).thenReturn(rowType);

        // Create GroupTop with group by first column and sort by second column
        ImmutableBitSet groupSet = ImmutableBitSet.of(0); // Group by first column
        final GroupTopN groupTopN = GroupTopN.create(
            RelTraitSet.createEmpty(),
            input,
            RelCollations.of(
                new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING)
            ),
            null, // offset
            fetch,
            groupSet,
            false // not partial
        );

        final int parallelism = 4;
        final int taskNumber = 2;
        final int rowCount = 1000;

        final List<DataType> inputDataTypes = ImmutableList.of(
            DataTypes.IntegerType,
            DataTypes.LongType,
            new SliceType()
        );

        final SpillerFactory spillerFactory = Mockito.mock(SpillerFactory.class);

        // Create GroupTopExecutorFactory
        GroupTopExecutorFactory groupTopExecutorFactory =
            new GroupTopExecutorFactory(groupTopN, parallelism, taskNumber, rowCount, spillerFactory, inputDataTypes);

        // Test createExecutor method - this will trigger createAllExecutors internally
        for (int i = 0; i < parallelism; i++) {
            Executor exec = groupTopExecutorFactory.createExecutor(context, i);
            Assert.assertTrue(exec instanceof GroupTopNExec, "Executor should be instance of GroupTopNExec");
        }

        // Test getAllExecutors method - this should return the cached executors
        List<Executor> allExecutors = groupTopExecutorFactory.getAllExecutors(context);
        Assert.assertEqual(parallelism, allExecutors.size());

        // Verify all executors are GroupTopNExec instances
        for (Executor exec : allExecutors) {
            Assert.assertTrue(exec instanceof GroupTopNExec, "All executors should be GroupTopNExec instances");
        }

        // Test that calling createExecutor again returns the same cached executors
        for (int i = 0; i < parallelism; i++) {
            Executor exec = groupTopExecutorFactory.createExecutor(context, i);
            Assert.assertTrue(allExecutors.get(i) == exec, "Should return cached executor");
        }
    }
}