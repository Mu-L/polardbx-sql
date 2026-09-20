package com.alibaba.polardbx.optimizer.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLSyntaxErrorException;

/**
 * Regression test for AONE-58446178:
 * JSON_EXTRACT(x, '$[*].xx') returns null instead of the expected array of values.
 * <p>
 * Root cause: In JsonDocProcessor.extract(), when an ArrayLocation pathLeg with isAsterisk=true
 * is processed, the code returns the entire JSONArray directly (line 110). The subsequent
 * Member pathLeg then receives a JSONArray instead of a JSONObject and returns null (line 82-83).
 * <p>
 * Expected MySQL behavior: '$[*].key' should iterate over each element of the array,
 * apply the '.key' lookup to each element, and return a new array of matched values.
 */
public class JsonExtractWildcardTest {

    private static Object extract(String jsonDoc, String pathExpr) throws SQLSyntaxErrorException {
        MySQLLexer lexer = new MySQLLexer(pathExpr);
        JsonPathExprParser parser = new JsonPathExprParser(lexer);
        JsonPathExprStatement jsonPathExpr = parser.parse();
        return JsonDocProcessor.extract(jsonDoc, jsonPathExpr);
    }

    /**
     * Main regression case from AONE-58446178:
     * JSON_EXTRACT('[{"amount":0.6,"deductAmount":37.94,"name":"手续费","sort":0,"type":1}]',
     * '$[*].deductAmount')
     * Expected: [37.94]
     * Actual (buggy): null
     */
    @Test
    public void testExtractWildcardArraySingleElement() throws SQLSyntaxErrorException {
        String jsonDoc = "[{\"amount\":0.6,\"deductAmount\":37.94,\"name\":\"手续费\",\"sort\":0,\"type\":1}]";
        Object result = extract(jsonDoc, "$[*].deductAmount");

        Assert.assertNotNull("JSON_EXTRACT with '$[*].key' should not return null", result);
        Assert.assertTrue("Result should be a JSONArray", result instanceof JSONArray);
        JSONArray arr = (JSONArray) result;
        Assert.assertEquals("Result array should contain exactly 1 element", 1, arr.size());
        Assert.assertEquals("Extracted value should be 37.94", 37.94, arr.getDouble(0), 0.0001);
    }

    /**
     * Multi-element array: '$[*].key' should return all matching values.
     * Input: [{"v":1},{"v":2},{"v":3}]
     * Path: $[*].v
     * Expected: [1, 2, 3]
     */
    @Test
    public void testExtractWildcardArrayMultipleElements() throws SQLSyntaxErrorException {
        String jsonDoc = "[{\"v\":1},{\"v\":2},{\"v\":3}]";
        Object result = extract(jsonDoc, "$[*].v");

        Assert.assertNotNull("JSON_EXTRACT with '$[*].key' on multi-element array should not return null", result);
        Assert.assertTrue("Result should be a JSONArray", result instanceof JSONArray);
        JSONArray arr = (JSONArray) result;
        Assert.assertEquals("Result array should contain 3 elements", 3, arr.size());
        Assert.assertEquals(1, arr.getIntValue(0));
        Assert.assertEquals(2, arr.getIntValue(1));
        Assert.assertEquals(3, arr.getIntValue(2));
    }

    /**
     * String values: '$[*].name' on array of objects with string fields.
     * Input: [{"name":"Alice"},{"name":"Bob"}]
     * Path: $[*].name
     * Expected: ["Alice","Bob"]
     */
    @Test
    public void testExtractWildcardArrayStringValues() throws SQLSyntaxErrorException {
        String jsonDoc = "[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]";
        Object result = extract(jsonDoc, "$[*].name");

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be a JSONArray", result instanceof JSONArray);
        JSONArray arr = (JSONArray) result;
        Assert.assertEquals(2, arr.size());
        Assert.assertEquals("Alice", arr.getString(0));
        Assert.assertEquals("Bob", arr.getString(1));
    }

    /**
     * The original bug report: combined with another function (e.g., HYPERLOGLOG),
     * the JSON_EXTRACT part should still return [37.94].
     * This test verifies the core extraction logic in isolation.
     */
    @Test
    public void testExtractWildcardReturnsNonNullForValidPath() throws SQLSyntaxErrorException {
        // Simplified version of the original failing query's JSON_EXTRACT part
        String jsonDoc = "[{\"amount\":0.6,\"deductAmount\":37.94,\"name\":\"手续费\",\"sort\":0,\"type\":1}]";
        Object result = extract(jsonDoc, "$[*].deductAmount");

        // This is the exact assertion that reproduces the bug: result was null before the fix
        String resultStr = JSON.toJSONString(result);
        Assert.assertEquals("[37.94]", resultStr);
    }
}
