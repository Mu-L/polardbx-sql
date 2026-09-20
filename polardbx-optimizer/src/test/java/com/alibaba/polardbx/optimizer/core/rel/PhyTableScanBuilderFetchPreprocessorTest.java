package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/**
 * Regression test for PhyTableScanBuilder.FetchPreprocessor#preProcessFetch /
 * resolveDynamicParamValue: a lone dynamic-param LIMIT fetch must be re-validated for
 * sign on every physical SQL build (i.e. every re-execution of a cached plan), covering
 * the BigInteger/BigDecimal fallback branches of resolveDynamicParamValue that
 * CBOUtil#validateNonNegativeFetch (build-time only) does not re-check.
 */
public class PhyTableScanBuilderFetchPreprocessorTest {

    private Object newFetchPreprocessor(Map<Integer, ParameterContext> params) throws Exception {
        Class<?> clazz = Class.forName(
            "com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder$FetchPreprocessor");
        Constructor<?> constructor = clazz.getDeclaredConstructor(Map.class, Map.class, boolean.class);
        constructor.setAccessible(true);
        return constructor.newInstance(params, null, false);
    }

    private void preProcessFetch(Object preprocessor, SqlSelect sqlSelect) throws Exception {
        Method method = preprocessor.getClass().getDeclaredMethod("preProcessFetch", SqlSelect.class);
        method.setAccessible(true);
        try {
            method.invoke(preprocessor, sqlSelect);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    private SqlSelect selectWithFetch(SqlDynamicParam fetch) {
        return new SqlSelect(SqlParserPos.ZERO, null, null, null, null, null, null, null, null, null, fetch);
    }

    @Test
    public void testNegativeBigIntegerFetchRejectedOnRebind() throws Exception {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setObject1,
            new Object[] {1, new BigInteger("-18446744073709551615")}));
        Object preprocessor = newFetchPreprocessor(params);
        SqlDynamicParam fetch = new SqlDynamicParam(0, SqlTypeName.BIGINT, SqlParserPos.ZERO, null);

        try {
            preProcessFetch(preprocessor, selectWithFetch(fetch));
            Assert.fail("Expected TddlRuntimeException for negative BigInteger fetch param");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("get rex"));
        }
    }

    @Test
    public void testNegativeBigDecimalFetchRejectedOnRebind() throws Exception {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, new BigDecimal("-1")}));
        Object preprocessor = newFetchPreprocessor(params);
        SqlDynamicParam fetch = new SqlDynamicParam(0, SqlTypeName.BIGINT, SqlParserPos.ZERO, null);

        try {
            preProcessFetch(preprocessor, selectWithFetch(fetch));
            Assert.fail("Expected TddlRuntimeException for negative BigDecimal fetch param");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("get rex"));
        }
    }

    @Test
    public void testPositiveDynamicParamFetchAccepted() throws Exception {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 10L}));
        Object preprocessor = newFetchPreprocessor(params);
        SqlDynamicParam fetch = new SqlDynamicParam(0, SqlTypeName.BIGINT, SqlParserPos.ZERO, null);

        // Should not throw.
        preProcessFetch(preprocessor, selectWithFetch(fetch));
    }
}
