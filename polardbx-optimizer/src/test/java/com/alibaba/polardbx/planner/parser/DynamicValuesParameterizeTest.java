package com.alibaba.polardbx.planner.parser;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.PreparedParamRef;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamicValuesParameterizeTest {

    @Before
    public void setUp() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(false);
    }

    @After
    public void tearDown() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(false);
    }

    private static ExecutionContext contextWithFoldEnabled() {
        DynamicConfig.getInstance().setEnableDynamicValuesOptimization(true);
        return new ExecutionContext();
    }

    @Test
    public void dynamicValuesOptimizationIsDisabledByDefault() {
        assertFalse(DynamicConfig.getInstance().isEnableDynamicValuesOptimization());
    }

    @Test
    public void unfoldedParameterizationIsKeptByDefault() {
        ExecutionContext executionContext = new ExecutionContext();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(parameterized.getSql(), 2,
            StringUtils.countMatches(parameterized.getSql(), "CAST(? AS bigint)"));
        assertEquals(2, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 2), parameterized.getParameters());
    }

    @Test
    public void switchCanEnableDynamicValuesOptimization() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint))) as v(a)"),
            null,
            executionContext,
            false);

        assertTrue(DynamicConfig.getInstance().isEnableDynamicValuesOptimization());
        assertEquals(parameterized.getSql(), 1,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW(CAST(? AS bigint))"));
        assertEquals(1, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 2), parameterized.getParameters().get(0));
    }

    @Test
    public void foldTypedValuesIntoColumnParameters() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values "
                + "row(cast(1 as bigint), cast('2026-08-18 08:00:00.000' as timestamp(3))), "
                + "row(cast('2' as bigint), cast(null as timestamp(3)))"
                + ") as v(id, cutoff_time)"),
            null,
            executionContext,
            false);

        assertTrue(parameterized.getSql(), parameterized.getSql()
            .contains("VALUES ROW(CAST(? AS bigint), CAST(? AS timestamp(3)))"));
        assertTrue(parameterized.getSql(), parameterized.getSql().contains("AS v (id, cutoff_time)"));
        assertEquals(parameterized.getSql(), 1, StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, "2"), parameterized.getParameters().get(0));
        assertEquals(Arrays.asList("2026-08-18 08:00:00.000", null), parameterized.getParameters().get(1));
    }

    @Test
    public void foldAllowsMixedLiteralJavaTypesWithSameCastTarget() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast('2' as bigint))) as v(id)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 1,
            StringUtils.countMatches(parameterized.getSql(), "CAST(? AS bigint)"));
        assertEquals(1, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, "2"), parameterized.getParameters().get(0));
    }

    @Test
    public void foldRejectsDifferentCastTargets() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(
                "select * from (values row(cast(1 as bigint)), row(cast(2 as decimal(10, 2)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 2, StringUtils.countMatches(parameterized.getSql(), "CAST(? AS"));
        assertEquals(2, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 2), parameterized.getParameters());
    }

    @Test
    public void foldAcceptsIdenticalDecimalPrecisionAndScale() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as decimal(10, 2))), "
                + "row(cast(2 as decimal(10, 2)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 1,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW(CAST(? AS decimal(10, 2))"));
        assertEquals(1, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 2), parameterized.getParameters().get(0));
    }

    @Test
    public void foldRejectsDifferentDecimalScale() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as decimal(10, 2))), "
                + "row(cast(2 as decimal(10, 3)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0, StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
    }

    @Test
    public void foldRejectsDifferentDecimalPrecision() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as decimal(10, 2))), "
                + "row(cast(2 as decimal(12, 2)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0, StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
    }

    @Test
    public void foldRejectsUnsignedDifference() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as decimal(10, 2))), "
                + "row(cast(2 as decimal(10, 2) unsigned))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0, StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
    }

    @Test
    public void foldRejectsIntegerDisplayWidthDifference() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        // int and int(11) are semantically the same type, but the argument lists differ so folding
        // conservatively falls back to per-row parameterization. Results stay correct either way.
        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as int)), row(cast(2 as int(11)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0, StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
    }

    @Test
    public void foldAcceptsMixedCaseCastTargets() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as DECIMAL(10, 2))), "
                + "row(cast(2 as decimal(10, 2)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 1, StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(1, parameterized.getParameters().size());
    }

    @Test
    public void foldRejectsUnlistedCastTargetTypes() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized json = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast('{\"a\":1}' as json)), "
                + "row(cast('{\"b\":2}' as json))) as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(json.getSql(), 0, StringUtils.countMatches(json.getSql(), "VALUES ROW("));
        assertEquals(2, json.getParameters().size());

        SqlParameterized enumeration = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast('x' as enum('x', 'y'))), "
                + "row(cast('y' as enum('x', 'y')))) as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(enumeration.getSql(), 0, StringUtils.countMatches(enumeration.getSql(), "VALUES ROW("));
        assertEquals(2, enumeration.getParameters().size());

        SqlParameterized bit = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bit)), row(cast(0 as bit))) as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(bit.getSql(), 0, StringUtils.countMatches(bit.getSql(), "VALUES ROW("));
        assertEquals(2, bit.getParameters().size());
    }

    @Test
    public void foldIntegerTypeMatrix() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        for (String type : new String[] {"tinyint", "smallint", "mediumint", "int", "bigint"}) {
            assertFolds(executionContext, "row(cast(1 as " + type + ")), row(cast(2 as " + type + "))");
            assertFolds(executionContext,
                "row(cast(1 as " + type + "(4))), row(cast(2 as " + type + "(4)))");
            assertUnfolded(executionContext,
                "row(cast(1 as " + type + "(4))), row(cast(2 as " + type + "(6)))");
            assertUnfolded(executionContext,
                "row(cast(1 as " + type + ")), row(cast(2 as " + type + " unsigned))");
            assertUnfolded(executionContext,
                "row(cast(1 as " + type + " zerofill)), row(cast(2 as " + type + "))");
        }
    }

    @Test
    public void foldFloatAndDoubleMatrix() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        assertFolds(executionContext, "row(cast(1.5 as float)), row(cast(2.5 as float))");
        assertFolds(executionContext, "row(cast(1.5 as double)), row(cast(2.5 as double))");
        // FLOAT(p) bit precision is part of the type definition and must match exactly.
        assertFolds(executionContext, "row(cast(1.5 as float(10))), row(cast(2.5 as float(10)))");
        assertUnfolded(executionContext, "row(cast(1.5 as float(10))), row(cast(2.5 as float(24)))");
        assertUnfolded(executionContext, "row(cast(1.5 as float)), row(cast(2.5 as double))");
    }

    @Test
    public void foldTemporalTypeMatrix() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        assertFolds(executionContext,
            "row(cast('2026-01-01' as date)), row(cast('2026-01-02' as date))");
        assertFolds(executionContext,
            "row(cast('2026-01-01 00:00:00' as datetime)), row(cast('2026-01-02 00:00:00' as datetime))");
        assertFolds(executionContext,
            "row(cast('08:00:00' as time)), row(cast('09:00:00' as time))");
        // Fractional-seconds precision is part of the type definition and must match exactly.
        assertFolds(executionContext,
            "row(cast('2026-01-01 00:00:00' as datetime(3))), row(cast('2026-01-02 00:00:00' as datetime(3)))");
        assertUnfolded(executionContext,
            "row(cast('2026-01-01 00:00:00' as datetime(3))), row(cast('2026-01-02 00:00:00' as datetime(6)))");
        assertUnfolded(executionContext,
            "row(cast('2026-01-01 00:00:00' as datetime)), row(cast('2026-01-02 00:00:00' as timestamp))");
        assertUnfolded(executionContext,
            "row(cast('2026-01-01' as date)), row(cast('08:00:00' as time))");
    }

    @Test
    public void foldCharacterTypeMatrix() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        assertFolds(executionContext, "row(cast('a' as char(10))), row(cast('b' as char(10)))");
        assertUnfolded(executionContext, "row(cast('a' as char(10))), row(cast('b' as char(20)))");
        assertUnfolded(executionContext, "row(cast('a' as char(10))), row(cast('b' as varchar(10)))");
        assertFolds(executionContext, "row(cast('a' as varchar(10))), row(cast('b' as varchar(10)))");
        assertUnfolded(executionContext, "row(cast('a' as varchar(10))), row(cast('b' as varchar(20)))");
        // Charset and collation are part of the type definition and must match exactly.
        assertUnfolded(executionContext,
            "row(cast('a' as varchar(10) character set utf8mb4)), row(cast('b' as varchar(10) character set utf8))");
        assertUnfolded(executionContext,
            "row(cast('a' as varchar(10) collate utf8mb4_bin)), row(cast('b' as varchar(10) collate utf8mb4_general_ci))");
    }

    @Test
    public void foldBinaryAndLobTypeMatrix() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        assertFolds(executionContext, "row(cast('a' as binary(4))), row(cast('b' as binary(4)))");
        assertUnfolded(executionContext, "row(cast('a' as binary(4))), row(cast('b' as binary(8)))");
        assertUnfolded(executionContext, "row(cast('a' as binary(4))), row(cast('b' as varbinary(4)))");
        assertFolds(executionContext, "row(cast('a' as varbinary(10))), row(cast('b' as varbinary(10)))");
        for (String type : new String[] {
            "tinytext", "text", "mediumtext", "longtext",
            "tinyblob", "blob", "mediumblob", "longblob"}) {
            assertFolds(executionContext, "row(cast('a' as " + type + ")), row(cast('b' as " + type + "))");
        }
        assertUnfolded(executionContext, "row(cast('a' as text)), row(cast('b' as mediumtext))");
    }

    @Test
    public void foldRejectsTypeNameAliases() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        // The whitelist compares raw type names; aliases are conservatively treated as distinct
        // targets and fall back to per-row parameterization. Results stay correct either way.
        assertUnfolded(executionContext, "row(cast(1 as int)), row(cast(2 as integer))");
        assertUnfolded(executionContext, "row(cast(1 as decimal(10, 2))), row(cast(2 as numeric(10, 2)))");
    }

    @Test
    public void foldRejectsUnlistedYearAndSetTypes() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        assertUnfolded(executionContext, "row(cast(2020 as year)), row(cast(2021 as year))");
        assertUnfolded(executionContext, "row(cast('a' as set('a', 'b'))), row(cast('b' as set('a', 'b')))");
    }

    @Test
    public void foldPreservesReservedWordAlias() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint))) "
                + "as `order` (a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 1,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertTrue(parameterized.getSql(), parameterized.getSql().contains("`order`"));
    }

    private void assertFolds(ExecutionContext executionContext, String valuesRows) {
        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values " + valuesRows + ") as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(parameterized.getSql(), 1,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(parameterized.getSql(), 1, parameterized.getParameters().size());
    }

    private void assertUnfolded(ExecutionContext executionContext, String valuesRows) {
        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values " + valuesRows + ") as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(parameterized.getSql(), 0,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(parameterized.getSql(), 2, parameterized.getParameters().size());
    }

    @Test
    public void foldAcceptsAliasLessValues() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint)), "
                + "row(cast(3 as bigint)))"),
            null,
            executionContext,
            false);
        assertEquals(parameterized.getSql(), 1,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        // The only " AS" is the one inside CAST(? AS bigint); no dangling alias clause is printed.
        assertEquals(parameterized.getSql(), 1, StringUtils.countMatches(parameterized.getSql(), " AS"));
        assertTrue(parameterized.getSql(), parameterized.getSql().trim().endsWith("))"));
        assertEquals(1, parameterized.getParameters().size());
    }

    @Test
    public void foldRejectsOrderByAndLimitValues() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized orderBy = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint)) "
                + "order by column_0) as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(orderBy.getSql(), 2,
            StringUtils.countMatches(orderBy.getSql(), "CAST(? AS bigint)"));
        assertEquals(2, orderBy.getParameters().size());
        assertEquals(Arrays.asList(1, 2), orderBy.getParameters());

        SqlParameterized limit = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint)) "
                + "limit 1) as v(a)"),
            null,
            executionContext,
            false);
        assertEquals(limit.getSql(), 2,
            StringUtils.countMatches(limit.getSql(), "CAST(? AS bigint)"));
        assertEquals(3, limit.getParameters().size());
        assertEquals(Arrays.asList(1, 2, 1), limit.getParameters());
    }

    @Test
    public void foldRejectsExpressionInnerValues() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 + 1 as bigint)), row(cast(2 as bigint))) as v(a)"),
            null,
            executionContext,
            false);

        assertTrue(parameterized.getSql(), parameterized.getSql().contains("? + ?"));
        assertEquals(parameterized.getSql(), 2, StringUtils.countMatches(parameterized.getSql(), "CAST("));
        assertEquals(3, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 1, 2), parameterized.getParameters());
    }

    @Test
    public void serverPrepareModeDoesNotFoldValues() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(1 as bigint)), row(cast(2 as bigint))) as v(a)"),
            null,
            executionContext,
            true);

        assertEquals(parameterized.getSql(), 2,
            StringUtils.countMatches(parameterized.getSql(), "CAST(? AS bigint)"));
        assertEquals(2, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 2), parameterized.getParameters());
    }

    @Test
    public void foldRejectsMixedCastAndTryCast() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(
                "select * from (values row(cast(1 as bigint)), row(try_cast(2 as bigint))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertTrue(parameterized.getSql(), parameterized.getSql().contains("CAST(? AS bigint)"));
        assertTrue(parameterized.getSql(), parameterized.getSql().contains("TRY_CAST(? AS bigint)"));
        assertEquals(2, parameterized.getParameters().size());
        assertEquals(Arrays.asList(1, 2), parameterized.getParameters());
    }

    @Test
    public void foldRejectsCharsetPrefixedLiterals() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(_utf8mb4'a' as char(10))), "
                + "row(cast('b' as char(10)))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
    }

    @Test
    public void foldRejectsQuestionMarkParameters() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(? as bigint)), row(cast(? as bigint))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 2,
            StringUtils.countMatches(parameterized.getSql(), "CAST(? AS bigint)"));
        assertEquals(2, parameterized.getParameters().size());
        assertTrue(parameterized.getParameters().get(0)
            instanceof PreparedParamRef);
        assertTrue(parameterized.getParameters().get(1)
            instanceof PreparedParamRef);
    }

    @Test
    public void foldRejectsCastTargetsWithDifferentLiteralArguments() {
        ExecutionContext executionContext = contextWithFoldEnabled();

        SqlParameterized parameterized = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast('a b' as enum('a b', 'c'))), "
                + "row(cast('ab' as enum('ab', 'c')))) as v(a)"),
            null,
            executionContext,
            false);

        assertEquals(parameterized.getSql(), 0,
            StringUtils.countMatches(parameterized.getSql(), "VALUES ROW("));
        assertEquals(2, parameterized.getParameters().size());
    }

    @Test
    public void foldNormalizesOversizedBigIntegerLiteralsConsistently() {
        ExecutionContext executionContext = contextWithFoldEnabled();
        String hugeLiteral = "123456789012345678901234567890";

        SqlParameterized folded = SqlParameterizeUtils.parameterize(
            ByteString.from("select * from (values row(cast(" + hugeLiteral + " as decimal(40, 0))), "
                + "row(cast(1 as decimal(40, 0)))) as v(a)"),
            null,
            executionContext,
            false);
        SqlParameterized unfolded = SqlParameterizeUtils.parameterize(
            ByteString.from("select cast(" + hugeLiteral + " as decimal(40, 0))"),
            null,
            new ExecutionContext(),
            false);

        assertTrue(folded.getSql(), folded.getSql().contains("VALUES ROW("));
        Object foldedValue = ((java.util.List<?>) folded.getParameters().get(0)).get(0);
        Object unfoldedValue = unfolded.getParameters().get(0);
        assertEquals(unfoldedValue.getClass(), foldedValue.getClass());
        assertEquals(java.math.BigDecimal.class, foldedValue.getClass());
    }
}
