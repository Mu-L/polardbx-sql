package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.gms.module.LogPattern.START_OVER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class ModuleLogInfoTest {
    @Test
    public void testSizeOverflow() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        for (int i = 0; i < 10000; i++) {
            ModuleLogInfo.getInstance()
                .logRecord(Module.STATISTICS, START_OVER, new String[] {"test", "test"}, LogLevel.NORMAL);
        }
        System.out.println(ModuleLogInfo.getInstance().resources());
        Assert.assertTrue("STATISTICS:1000;".equalsIgnoreCase(ModuleLogInfo.getInstance().resources()));
    }

    /**
     * 测试用例: 当参数映射为空时，返回空字符串。
     */
    @Test
    public void testBuildParametersLogWithEmptyParameterMap() {
        assertEquals("", ModuleUtil.buildParametersLog(null));
        assertEquals("", ModuleUtil.buildParametersLog(new HashMap<>()));
    }

    /**
     * 测试用例: 参数映射中有有效上下文时，构建正确的日志字符串。
     */
    @Test
    public void testBuildParametersLogWithValidContexts() {
        ParameterContext mockContext1 = mock(ParameterContext.class);
        when(mockContext1.toString()).thenReturn("context1");
        ParameterContext mockContext2 = mock(ParameterContext.class);
        when(mockContext2.toString()).thenReturn("context2");
        Map<Integer, ParameterContext> parameterMap = new HashMap<>();
        parameterMap.put(1, mockContext1);
        parameterMap.put(2, mockContext2);

        assertEquals("context1-context2", ModuleUtil.buildParametersLog(parameterMap));
    }

    /**
     * 测试用例3: 参数映射中有无效上下文时，忽略无效上下文并构建日志字符串。
     */
    @Test
    public void testBuildParametersLogWithInvalidContexts() {
        Map<Integer, ParameterContext> parameterMap = new HashMap<>();

        parameterMap.put(1, null);
        parameterMap.put(2, new ParameterContext(ParameterMethod.setInt, new Object[] {1, 2}));

        String parametersLog = ModuleUtil.buildParametersLog(parameterMap);
        assertEquals("2", parametersLog);
    }

    /**
     * 测试用例: 日志字符串长度超过限制时，截断到最大长度。
     */
    @Test
    public void testBuildParametersLogWithMaxLengthLimit() {
        ParameterContext mockContext = mock(ParameterContext.class);
        when(mockContext.toString()).thenReturn(StringUtils.repeat("a", 1024 * 8 + 1));
        Map<Integer, ParameterContext> parameterMap = new HashMap<>();

        parameterMap.put(1, mockContext);

        assertEquals(StringUtils.repeat("a", 1024 * 8), ModuleUtil.buildParametersLog(parameterMap));
    }
}
