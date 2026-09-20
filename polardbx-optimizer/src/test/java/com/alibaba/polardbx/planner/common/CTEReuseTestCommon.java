package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.Assert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class CTEReuseTestCommon extends ParameterizedTestCommon {
    protected boolean enableParserReuse = true;

    public CTEReuseTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan,
                              String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        String oldValue = String.valueOf(DynamicConfig.getInstance().isEnableCTEReuse());
        String oldValue1 = String.valueOf(DynamicConfig.getInstance().getCteParserThreshold());
        try {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_CTE_REUSE, String.valueOf(enableParserReuse));
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.CTE_PARSER_THRESHOLD, String.valueOf(0));
            return super.getPlan(testSql);
        } finally {
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_CTE_REUSE, oldValue);
            DynamicConfig.getInstance().loadValue(null, ConnectionProperties.CTE_PARSER_THRESHOLD, oldValue1);
        }
    }

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        validateCTEStructure(plan);
        return super.removeSubqueryHashCode(planStr, plan, param, sqlExplainLevel);
    }

    /**
     * Validate CTE plan structure invariants via recursive visitor:
     * 1. No different CTEAnchors reference the same cteId
     * 2. CTEProducer must be the left child of CTEAnchor with matching cteId
     * 3. CTEConsumer must be under the subtree of a CTEAnchor with the same cteId
     */
    private void validateCTEStructure(RelNode plan) {
        Map<Integer, CTEAnchor> cteAnchorMap = new HashMap<>();
        doValidateCTE(plan, new HashSet<>(), cteAnchorMap);
    }

    private void doValidateCTE(RelNode node, Set<Integer> activeCteIds, Map<Integer, CTEAnchor> cteAnchorMap) {
        if (node instanceof CTEAnchor) {
            CTEAnchor anchor = (CTEAnchor) node;
            Integer cteId = anchor.getCteId();

            // Check 1: no different CTEAnchors reference the same cteId
            Assert.assertFalse(
                "Different CTEAnchors reference the same cteId=" + cteId,
                cteAnchorMap.containsKey(cteId));
            cteAnchorMap.put(cteId, anchor);

            // Check 2: CTEProducer must be the left child of CTEAnchor, with matching cteId
            RelNode left = anchor.getLeft();
            Assert.assertTrue(
                "Left child of CTEAnchor(cteId=" + cteId + ") must be CTEProducer, but got "
                    + left.getClass().getSimpleName(),
                left instanceof CTEProducer);
            Assert.assertEquals(
                "CTEProducer cteId must match CTEAnchor cteId", cteId, ((CTEProducer) left).getCteId());

            // Traverse both subtrees with this cteId added to active scope
            activeCteIds.add(cteId);
            doValidateCTE(left, activeCteIds, cteAnchorMap);
            doValidateCTE(anchor.getRight(), activeCteIds, cteAnchorMap);
            activeCteIds.remove(cteId);

        } else if (node instanceof CTEConsumer) {
            CTEConsumer consumer = (CTEConsumer) node;
            Integer cteId = consumer.getCteId();

            // Check 3: CTEConsumer must be under a CTEAnchor subtree with the same cteId
            Assert.assertTrue(
                "CTEConsumer(cteId=" + cteId + ") is not under a CTEAnchor with the same cteId."
                    + " Active cteIds: " + activeCteIds,
                activeCteIds.contains(cteId));
            if (node instanceof LogicalCTEConsumer) {
                doValidateCTE(((LogicalCTEConsumer) node).getInnerRel(), activeCteIds, cteAnchorMap);
            }
        } else {
            // Recursively validate all children
            for (RelNode child : node.getInputs()) {
                doValidateCTE(child, activeCteIds, cteAnchorMap);
            }
        }
    }
}