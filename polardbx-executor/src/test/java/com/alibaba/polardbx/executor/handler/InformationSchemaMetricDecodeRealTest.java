package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaMetricHandler;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Unit test for {@link InformationSchemaMetricHandler#decodeReal(String)}.
 * <p>
 * Note: tests under com.alibaba.polardbx.executor.handler.subhandler are excluded from the CI
 * unit-test coverage gate, so this test lives in the parent package to make the decodeReal
 * change counted as covered.
 */
public class InformationSchemaMetricDecodeRealTest {

    @Test
    public void testDecodeRealTypicalMetricString() {
        Map<String, String> result =
            InformationSchemaMetricHandler.decodeReal("SQL_SELECT:10,SQL_INSERT:5,SQL_UPDATE:3");

        Assert.assertEquals(3, result.size());
        Assert.assertEquals("10", result.get("SQL_SELECT"));
        Assert.assertEquals("5", result.get("SQL_INSERT"));
        Assert.assertEquals("3", result.get("SQL_UPDATE"));
    }

    @Test
    public void testDecodeRealValueContainingColon() {
        Map<String, String> result = InformationSchemaMetricHandler.decodeReal("KEY_WITH_COLON:a:b");

        Assert.assertEquals(1, result.size());
        Assert.assertEquals("a:b", result.get("KEY_WITH_COLON"));
    }

    @Test
    public void testDecodeRealConsecutiveSeparators() {
        Map<String, String> result = InformationSchemaMetricHandler.decodeReal("A:1,,B:2,");

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("1", result.get("A"));
        Assert.assertEquals("2", result.get("B"));
    }
}
