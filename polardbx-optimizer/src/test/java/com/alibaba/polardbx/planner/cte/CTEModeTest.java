package com.alibaba.polardbx.planner.cte;

import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEMode;
import org.junit.Assert;
import org.junit.Test;

public class CTEModeTest {
    @Test
    public void testCte() {
        Assert.assertEquals(CTEMode.INLINE, CTEMode.parse(null));
        Assert.assertEquals(CTEMode.INLINE, CTEMode.parse("inline"));
        Assert.assertEquals(CTEMode.REUSE, CTEMode.parse("reuse"));
        Assert.assertEquals(CTEMode.AUTO, CTEMode.parse("AuTo"));
        Assert.assertEquals(CTEMode.INLINE, CTEMode.parse("AuTo1"));
    }
}
