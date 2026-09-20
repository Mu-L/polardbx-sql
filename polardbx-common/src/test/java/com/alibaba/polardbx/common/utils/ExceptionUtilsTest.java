package com.alibaba.polardbx.common.utils;

import org.junit.Test;

import static com.alibaba.polardbx.common.utils.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExceptionUtilsTest {

    /**
     * 测试用例1: 当消息为空时返回null
     */
    @Test
    public void testTransformErrorCodeWithEmptyMessage() {
        assertNull(ExceptionUtils.transformErrorCode(4500, ""));
        assertNull(ExceptionUtils.transformErrorCode(4500, null));
    }

    /**
     * 测试用例2: 当错误码为4500且消息包含"语法错误"时，返回正确的转换后的错误码和SQL状态
     */
    @Test
    public void testTransformErrorCodeForSyntaxError() {
        Pair<Integer, String> result = ExceptionUtils.transformErrorCode(4500, "syntax error");
        assertNotNull(result);
        assertEquals(new Pair<>(1064, "42000"), result);
    }

    /**
     * 测试用例3: 当错误码为4006且消息包含"表不存在"时，返回正确的转换后的错误码和SQL状态
     */
    @Test
    public void testTransformErrorCodeForTableNotExist() {
        Pair<Integer, String> result = ExceptionUtils.transformErrorCode(4006, "ERR_TABLE_NOT_EXIST");
        assertNotNull(result);
        assertEquals(new Pair<>(1146, "42S02"), result);
    }

    /**
     * 测试用例4: 当错误码为4518且消息匹配正则表达式时，返回正确的转换后的错误码和SQL状态
     */
    @Test
    public void testTransformErrorCodeForColumnNotFound() {
        Pair<Integer, String> result =
            ExceptionUtils.transformErrorCode(4518, "[TDDL-4518][ERR_VALIDATE] : Column 'name' not found in any table");
        assertNotNull(result);
        assertEquals(new Pair<>(1054, "42S22"), result);
    }

    /**
     * 测试用例5: 当没有匹配的转换规则时，返回null
     */
    @Test
    public void testTransformErrorCodeNoMatch() {
        assertNull(ExceptionUtils.transformErrorCode(9999, "unknown error"));
    }
}