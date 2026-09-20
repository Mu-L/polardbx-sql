package com.alibaba.polardbx.optimizer.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for ExplainResult DDL_DAG mode
 */
public class ExplainResultDdlDagTest {

    @Test
    public void testDdlDagEnumExists() {
        // Verify DDL_DAG enum value exists
        ExplainResult.ExplainMode mode = ExplainResult.ExplainMode.DDL_DAG;
        Assert.assertNotNull(mode);
        Assert.assertEquals("DDL_DAG", mode.name());
    }

    @Test
    public void testIsDdlDag() {
        Assert.assertTrue(ExplainResult.ExplainMode.DDL_DAG.isDdlDag());
        Assert.assertFalse(ExplainResult.ExplainMode.ONLINE_DDL.isDdlDag());
        Assert.assertFalse(ExplainResult.ExplainMode.DETAIL.isDdlDag());
        Assert.assertFalse(ExplainResult.ExplainMode.EXECUTE.isDdlDag());
        Assert.assertFalse(ExplainResult.ExplainMode.ADVISOR.isDdlDag());
        Assert.assertFalse(ExplainResult.ExplainMode.SCHEDULE.isDdlDag());
    }

    @Test
    public void testIsExplainDdlDag_withNullExplainResult() {
        Assert.assertFalse(ExplainResult.isExplainDdlDag(null));
    }

    @Test
    public void testIsExplainDdlDag_withDdlDagMode() {
        ExplainResult result = new ExplainResult();
        result.explainMode = ExplainResult.ExplainMode.DDL_DAG;
        Assert.assertTrue(ExplainResult.isExplainDdlDag(result));
    }

    @Test
    public void testIsExplainDdlDag_withOtherModes() {
        ExplainResult result = new ExplainResult();

        result.explainMode = ExplainResult.ExplainMode.ONLINE_DDL;
        Assert.assertFalse(ExplainResult.isExplainDdlDag(result));

        result.explainMode = ExplainResult.ExplainMode.DETAIL;
        Assert.assertFalse(ExplainResult.isExplainDdlDag(result));

        result.explainMode = ExplainResult.ExplainMode.ADVISOR;
        Assert.assertFalse(ExplainResult.isExplainDdlDag(result));
    }

    @Test
    public void testDdlDagNotAffectOtherModes() {
        // Ensure existing modes still work correctly
        Assert.assertTrue(ExplainResult.ExplainMode.ONLINE_DDL.isOnlineDDL());
        Assert.assertFalse(ExplainResult.ExplainMode.DDL_DAG.isOnlineDDL());

        Assert.assertTrue(ExplainResult.ExplainMode.SCHEDULE.isSchedule());
        Assert.assertFalse(ExplainResult.ExplainMode.DDL_DAG.isSchedule());

        Assert.assertTrue(ExplainResult.ExplainMode.ADVISOR.isAdvisor());
        Assert.assertFalse(ExplainResult.ExplainMode.DDL_DAG.isAdvisor());
    }

    @Test
    public void testDdlDagModeCanBeFoundByName() {
        // Verify that DDL_DAG can be resolved by name (used in Planner.handleExplain)
        boolean found = false;
        for (ExplainResult.ExplainMode mode : ExplainResult.ExplainMode.values()) {
            if (mode.name().equalsIgnoreCase("DDL_DAG")) {
                found = true;
                Assert.assertEquals(ExplainResult.ExplainMode.DDL_DAG, mode);
                break;
            }
        }
        Assert.assertTrue("DDL_DAG should be found by name iteration", found);
    }
}
