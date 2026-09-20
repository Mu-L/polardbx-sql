
package com.alibaba.polardbx.optimizer.config.mqtest;

import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.CostModelWeightService;
import com.alibaba.polardbx.optimizer.config.meta.CostModel.ImmutableCostModelWeightV1;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class CostModelWeightTest {

    private CostModelWeightService instance;
    private Field currentField;

    @Before
    public void setUp() throws Exception {
        // 获取 INSTANCE 实例
        Field instanceField = CostModelWeight.class.getDeclaredField("INSTANCE");
        instanceField.setAccessible(true);
        instance = (CostModelWeightService) instanceField.get(null);

        // 获取 CURRENT 字段
        currentField = CostModelWeight.class.getDeclaredField("CURRENT");
        currentField.setAccessible(true);
    }

    @After
    public void tearDown() {
        // 每次测试后重置 CURRENT 为默认值（EARLIEST = V1）
        try {
            currentField.set(null, "V1");
            instance.setImmutable(new ImmutableCostModelWeightV1());
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testSetVersion_NullInput_ShouldUseLatest() throws Exception {
        CostModelWeight.setVersion(null);
        assertEquals(PropUtil.COST_MODEL_LATEST, currentField.get(null));
    }

    @Test
    public void testSetVersion_EmptyString_ShouldUseLatest() throws Exception {
        CostModelWeight.setVersion("");
        assertEquals(PropUtil.COST_MODEL_LATEST, currentField.get(null));
    }

    @Test
    public void testSetVersion_LowerCaseLatest_ShouldUseLatest() throws Exception {
        CostModelWeight.setVersion(CostModelWeight.LATEST_KEY);
        assertEquals(PropUtil.COST_MODEL_LATEST, currentField.get(null));
    }

    @Test
    public void testSetVersion_ValidV1_ShouldSetToV1() throws Exception {
        CostModelWeight.setVersion(CostModelWeight.EARLIEST);
        assertEquals(CostModelWeight.EARLIEST, currentField.get(null));
    }

    @Test
    public void testSetVersion_ValidV2_ShouldSetToV2() throws Exception {
        CostModelWeight.setVersion("V2");
        assertEquals("V2", currentField.get(null));
    }

    @Test
    public void testSetVersion_InvalidVersion_ShouldFallbackToLatest() throws Exception {
        CostModelWeight.setVersion("V1");
        assertEquals("V1", currentField.get(null));
    }

    @Test
    public void testSetVersion_LowerCaseV1_ShouldConvertToUpperCase() throws Exception {
        CostModelWeight.setVersion("v1");
        assertEquals("V1", currentField.get(null));
    }
}