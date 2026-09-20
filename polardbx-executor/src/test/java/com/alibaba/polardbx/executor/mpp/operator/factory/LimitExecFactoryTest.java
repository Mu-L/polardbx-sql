package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.LimitExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Verifies that a negative LIMIT/OFFSET bound to a re-executed (e.g. plan-cache-hit)
 * dynamic parameter is rejected at execution time, since CBOUtil#validateNonNegativeFetch
 * and CBOUtil#calPushDownFetch only run while the plan is being built/optimized and are
 * skipped on subsequent executions of an already-cached plan.
 */
public class LimitExecFactoryTest {

    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private RelNode input;

    @Before
    public void before() {
        input = Mockito.mock(RelNode.class);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);
        Mockito.when(input.getCluster()).thenReturn(cluster);
        Mockito.when(cluster.getRexBuilder()).thenReturn(REX_BUILDER);
    }

    private ExecutionContext contextWithParam(long fetchParamValue) {
        ExecutionContext context = new ExecutionContext();
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        currentParameter.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {null, fetchParamValue}));
        context.setParams(new Parameters(currentParameter));
        return context;
    }

    private ExecutionContext contextWithParams(long fetchParamValue, long offsetParamValue) {
        ExecutionContext context = new ExecutionContext();
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        currentParameter.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {null, fetchParamValue}));
        currentParameter.put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {null, offsetParamValue}));
        context.setParams(new Parameters(currentParameter));
        return context;
    }

    @Test
    public void testNegativeOffsetRejectedAtExecutionTime() {
        RexNode fetch = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0);
        RexNode offset = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1);
        Limit limit = Limit.create(RelTraitSet.createEmpty().replace(DrdsConvention.INSTANCE), input, offset, fetch);
        LimitExecFactory factory = new LimitExecFactory(limit, Mockito.mock(ExecutorFactory.class), 1);

        try {
            factory.createExecutor(contextWithParams(10L, -1L), 0);
            Assert.fail("Expected TddlRuntimeException for negative offset param");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("-1"));
        }
    }

    @Test
    public void testNegativeFetchRejectedAtExecutionTime() {
        RexNode fetch = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0);
        Limit limit = Limit.create(RelTraitSet.createEmpty().replace(DrdsConvention.INSTANCE), input, null, fetch);
        LimitExecFactory factory = new LimitExecFactory(limit, Mockito.mock(ExecutorFactory.class), 1);

        try {
            factory.createExecutor(contextWithParam(-1L), 0);
            Assert.fail("Expected TddlRuntimeException for negative fetch param");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("-1"));
        }
    }

    @Test
    public void testPositiveFetchBuildsExecutor() {
        RexNode fetch = REX_BUILDER.makeDynamicParam(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0);
        Limit limit = Limit.create(RelTraitSet.createEmpty().replace(DrdsConvention.INSTANCE), input, null, fetch);
        LimitExecFactory factory = new LimitExecFactory(limit, Mockito.mock(ExecutorFactory.class), 1);

        Executor exec = factory.createExecutor(contextWithParam(10L), 0);
        Assert.assertTrue(exec instanceof LimitExec);
    }
}
