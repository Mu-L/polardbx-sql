package com.alibaba.polardbx.optimizer.config.mqtest;

import com.alibaba.polardbx.optimizer.config.meta.CostModel.CostModelWeightService;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.ImmutableCostModelWeightV1;
import org.junit.Test;

import static org.junit.Assert.fail;

public class CostModelWeightServiceSetTest {

    @Test
    public void testSetFailed() {
        ImmutableCostModelWeightV1 v1 = new ImmutableCostModelWeightV1();
        try {
            v1.setMemoryWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setIoWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setColNetWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setNetWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setBuildWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setProbeWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setReverseSemiProbeWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setReverseAntiProbeWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setMergeWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setHashAggWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setSortAggWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setSortWindowWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setSortWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setAvgTupleMatch(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setShardWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setNlWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }
        try {
            v1.setStartUpWeight(1);
            fail("set should failed");
        } catch (UnsupportedOperationException e) {
            // pass
        }

    }

    @Test
    public void testSetSucceed() {
        CostModelWeightService v0 = new CostModelWeightService(new ImmutableCostModelWeightV1());
        v0.setMemoryWeight(1);
        v0.setMemoryWeight(-1);
        v0.setIoWeight(1);
        v0.setIoWeight(-1);
        v0.setColNetWeight(1);
        v0.setColNetWeight(-1);
        v0.setNetWeight(1);
        v0.setNetWeight(-1);
        v0.setBuildWeight(1);
        v0.setBuildWeight(-1);
        v0.setProbeWeight(1);
        v0.setProbeWeight(-1);
        v0.setReverseSemiProbeWeight(1);
        v0.setReverseSemiProbeWeight(-1);
        v0.setReverseAntiProbeWeight(1);
        v0.setReverseAntiProbeWeight(-1);
        v0.setMergeWeight(1);
        v0.setMergeWeight(-1);
        v0.setHashAggWeight(1);
        v0.setHashAggWeight(-1);
        v0.setSortAggWeight(1);
        v0.setSortAggWeight(-1);
        v0.setSortWindowWeight(1);
        v0.setSortWindowWeight(-1);
        v0.setSortWeight(1);
        v0.setSortWeight(-1);
        v0.setAvgTupleMatch(1);
        v0.setAvgTupleMatch(-1);
        v0.setShardWeight(1);
        v0.setShardWeight(-1);
        v0.setNlWeight(1);
        v0.setNlWeight(-1);
        v0.setStartUpWeight(1);
        v0.setStartUpWeight(-1);
    }
}
