package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Regression test for the literal (non-dynamic-param) offset+fetch branch of
 * calPushDownFetch: a negative literal offset or fetch must be rejected instead of
 * being silently summed into a smaller/incorrect fetch value.
 */
public class CBOUtilTest {

    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private RelNode input;

    @Before
    public void before() {
        input = Mockito.mock(RelNode.class);
        RelOptCluster cluster = Mockito.mock(RelOptCluster.class);
        RelOptPlanner planner = Mockito.mock(RelOptPlanner.class);
        Context context = PlannerContext.EMPTY_CONTEXT;
        Mockito.when(input.getCluster()).thenReturn(cluster);
        Mockito.when(cluster.getRexBuilder()).thenReturn(REX_BUILDER);
        Mockito.when(cluster.getPlanner()).thenReturn(planner);
        Mockito.when(planner.getContext()).thenReturn(context);
    }

    private Limit createLimit(RexNode offset, RexNode fetch) {
        return Limit.create(RelTraitSet.createEmpty().replace(DrdsConvention.INSTANCE), input, offset, fetch);
    }

    @Test
    public void testNegativeLiteralOffsetRejected() {
        RexNode offset = REX_BUILDER.makeBigIntLiteral(-1L);
        RexNode fetch = REX_BUILDER.makeBigIntLiteral(10L);
        Limit limit = createLimit(offset, fetch);

        try {
            CBOUtil.calPushDownFetch(limit);
            Assert.fail("Expected TddlRuntimeException for negative literal offset");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("-1"));
        }
    }

    @Test
    public void testNegativeLiteralFetchRejected() {
        RexNode offset = REX_BUILDER.makeBigIntLiteral(10L);
        RexNode fetch = REX_BUILDER.makeBigIntLiteral(-1L);
        Limit limit = createLimit(offset, fetch);

        try {
            CBOUtil.calPushDownFetch(limit);
            Assert.fail("Expected TddlRuntimeException for negative literal fetch");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("-1"));
        }
    }

    @Test
    public void testPositiveLiteralOffsetFetchSummed() {
        RexNode offset = REX_BUILDER.makeBigIntLiteral(10L);
        RexNode fetch = REX_BUILDER.makeBigIntLiteral(5L);
        Limit limit = createLimit(offset, fetch);

        RexNode result = CBOUtil.calPushDownFetch(limit);
        Assert.assertEquals("15", result.toString());
    }
}
