/*
 * Copyright [2013-2021] Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.fastchecker;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FastCheckerTest {

    private static final List<ColumnMeta> SINGLE_LONG_PK = Collections.singletonList(longColumn("id"));

    @Test
    public void testCalculateDownSampleRateFromLargePartitionTrace() {
        double rate = FastChecker.calculateDownSampleRate(32_768L, 0.2735049F, 182_812_087L);

        Assert.assertEquals(0.065536D, rate, 0.0000001D);
        Assert.assertEquals(1D, FastChecker.calculateDownSampleRate(32_768L, 0.001F, 1L), 0D);
    }

    @Test
    public void testBatchBoundsAreIndependentOfSampleOrder() {
        final int sampleCount = 324;
        final int requestedBatchCount = 191;
        List<Long> ascending = sequence(sampleCount);
        List<Long> descending = new ArrayList<>(ascending);
        Collections.reverse(descending);
        List<Long> shuffled = new ArrayList<>(ascending);
        Collections.shuffle(shuffled, new Random(20260728L));

        List<Long> ascendingBounds = selectLongBounds(ascending, requestedBatchCount);
        Assert.assertEquals(ascendingBounds, selectLongBounds(descending, requestedBatchCount));
        Assert.assertEquals(ascendingBounds, selectLongBounds(shuffled, requestedBatchCount));
        Assert.assertEquals(requestedBatchCount - 1, ascendingBounds.size());
        for (int i = 1; i < requestedBatchCount; i++) {
            long expectedBound = ((long) i * sampleCount + requestedBatchCount - 1) / requestedBatchCount;
            Assert.assertEquals(expectedBound, ascendingBounds.get(i - 1).longValue());
        }
    }

    @Test
    public void testBatchCountAndTailRemainEven() {
        List<Long> nonDivisibleBounds = selectLongBounds(sequence(328), 110);
        Assert.assertEquals(109, nonDivisibleBounds.size());
        Assert.assertEquals(326L, nonDivisibleBounds.get(108).longValue());
        Assert.assertEquals(3L, largestSamplePartition(nonDivisibleBounds, 328));

        Pair<List<Long>, Integer> reducedSelection =
            selectLongBoundsWithDistinct(Arrays.asList(3L, 1L, 2L), 10);
        Assert.assertEquals(Arrays.asList(1L, 2L, 3L), reducedSelection.getKey());
        Assert.assertEquals(3, reducedSelection.getValue().intValue());
        Assert.assertEquals(Collections.singletonList(7L),
            selectLongBounds(Collections.singletonList(7L), 10));
        Assert.assertTrue(selectLongBounds(Collections.emptyList(), 10).isEmpty());
    }

    @Test
    public void testCompositePrimaryKeySortDuplicateAndParameterPairing() {
        List<List<Object>> values = Arrays.asList(
            row(2L, 1L),
            row(1L, 2L),
            row(1L, 1L),
            row(1L, 2L));
        List<Map<Integer, ParameterContext>> parameters = Arrays.asList(
            parameters(2L, 1L),
            parameters(1L, 2L),
            parameters(1L, 1L),
            parameters(101L, 202L));
        List<ColumnMeta> columnMetas = Arrays.asList(longColumn("pk1"), longColumn("pk2"));

        Pair<List<Map<Integer, ParameterContext>>, Integer> selection =
            FastChecker.selectBatchBounds(values, parameters, columnMetas, 10);
        List<Map<Integer, ParameterContext>> bounds = selection.getKey();

        Assert.assertEquals(3, bounds.size());
        Assert.assertEquals(3, selection.getValue().intValue());
        Assert.assertSame(parameters.get(2), bounds.get(0));
        Assert.assertSame(parameters.get(1), bounds.get(1));
        Assert.assertSame(parameters.get(0), bounds.get(2));
        Assert.assertEquals(Arrays.asList(1L, 1L), parameterValues(bounds.get(0)));
        Assert.assertEquals(Arrays.asList(1L, 2L), parameterValues(bounds.get(1)));
        Assert.assertEquals(Arrays.asList(2L, 1L), parameterValues(bounds.get(2)));
    }

    @Test
    public void testBatchSplitSummaryLogLevel() {
        List<String> logEvents = new ArrayList<>();
        Logger recordingLogger = recordingLogger(logEvents);
        List<Long> normalValues = sequence(328);

        List<Map<Integer, ParameterContext>> normalBounds = FastChecker.selectAndLogBatchBounds(recordingLogger,
            "trace", "group", "table", true, 1_000L, 100L, 32_768L, sampleRows(normalValues),
            sampleParameters(normalValues), SINGLE_LONG_PK, 110L);

        Assert.assertEquals(109, normalBounds.size());
        Assert.assertEquals(1, logEvents.size());
        Assert.assertTrue(logEvents.get(0).startsWith("info:"));
        Assert.assertTrue(logEvents.get(0).contains("keptSampleRows=328"));
        Assert.assertTrue(logEvents.get(0).contains("distinctSampleRows=328"));
        Assert.assertTrue(logEvents.get(0).contains("requestedBatchNum=110"));
        Assert.assertTrue(logEvents.get(0).contains("actualBatchNum=110"));
        Assert.assertTrue(logEvents.get(0).contains("outcome=SUCCESS"));

        logEvents.clear();
        List<Long> reducedValues = Arrays.asList(3L, 1L, 2L, 2L);
        List<Map<Integer, ParameterContext>> reducedBounds = FastChecker.selectAndLogBatchBounds(recordingLogger,
            "trace", "group", "table", false, 1_000L, 100L, 32_768L, sampleRows(reducedValues),
            sampleParameters(reducedValues), SINGLE_LONG_PK, 10L);

        Assert.assertEquals(3, reducedBounds.size());
        Assert.assertEquals(1, logEvents.size());
        Assert.assertTrue(logEvents.get(0).startsWith("warn:"));
        Assert.assertTrue(logEvents.get(0).contains("[dst]"));
        Assert.assertTrue(logEvents.get(0).contains("keptSampleRows=4"));
        Assert.assertTrue(logEvents.get(0).contains("distinctSampleRows=3"));
        Assert.assertTrue(logEvents.get(0).contains("requestedBatchNum=10"));
        Assert.assertTrue(logEvents.get(0).contains("actualBatchNum=4"));
        Assert.assertTrue(logEvents.get(0).contains("outcome=REDUCED_BATCHES"));

        logEvents.clear();
        FastChecker.selectAndLogBatchBounds(recordingLogger, "trace", "group", "table", true,
            1_000L, 100L, 32_768L, Collections.emptyList(), Collections.emptyList(), SINGLE_LONG_PK, 10L);
        Assert.assertEquals(1, logEvents.size());
        Assert.assertTrue(logEvents.get(0).startsWith("warn:"));
        Assert.assertTrue(logEvents.get(0).contains("outcome=EMPTY_SAMPLE"));

        logEvents.clear();
        List<Long> singleBatchValues = Collections.singletonList(1L);
        FastChecker.selectAndLogBatchBounds(recordingLogger, "trace", "group", "table", true,
            1_000L, 100L, 32_768L, sampleRows(singleBatchValues), sampleParameters(singleBatchValues),
            SINGLE_LONG_PK, 1L);
        Assert.assertEquals(1, logEvents.size());
        Assert.assertTrue(logEvents.get(0).startsWith("info:"));
        Assert.assertTrue(logEvents.get(0).contains("outcome=SINGLE_BATCH"));

        logEvents.clear();
        FastChecker.selectAndLogBatchBounds(recordingLogger, "trace", "group", "table", true,
            1_000L, 100L, 32_768L, sampleRows(singleBatchValues), sampleParameters(singleBatchValues),
            Collections.emptyList(), 10L);
        Assert.assertEquals(1, logEvents.size());
        Assert.assertTrue(logEvents.get(0).startsWith("warn:"));
        Assert.assertTrue(logEvents.get(0).contains("outcome=INVALID_METADATA"));
    }

    @Test
    public void testBatchHashProgressContainsOnlyCurrentBounds() {
        List<String> logEvents = new ArrayList<>();
        List<Map<Integer, ParameterContext>> batchBounds = Arrays.asList(
            parameters(10L), parameters(20L), parameters(30L), parameters(40L));

        FastChecker.logBatchHashProgress(recordingLogger(logEvents), "trace", "group", "table", true,
            0, 5, batchBounds, 101L);
        FastChecker.logBatchHashProgress(recordingLogger(logEvents), "trace", "group", "table", true,
            2, 5, batchBounds, 103L);
        FastChecker.logBatchHashProgress(recordingLogger(logEvents), "trace", "group", "table", true,
            4, 5, batchBounds, 105L);

        Assert.assertEquals(
            "info:[trace] FastChecker fetched hashcheck result for group[table][src], batch 1/5, "
                + "bound [group.table.{(null), (10)}], hash value[101]",
            logEvents.get(0));
        Assert.assertEquals(
            "info:[trace] FastChecker fetched hashcheck result for group[table][src], batch 3/5, "
                + "bound [group.table.{(20), (30)}], hash value[103]",
            logEvents.get(1));
        Assert.assertEquals(
            "info:[trace] FastChecker fetched hashcheck result for group[table][src], batch 5/5, "
                + "bound [group.table.{(40), (null)}], hash value[105]",
            logEvents.get(2));
    }

    private static List<Long> selectLongBounds(List<Long> values, long requestedBatchCount) {
        return selectLongBoundsWithDistinct(values, requestedBatchCount).getKey();
    }

    private static Pair<List<Long>, Integer> selectLongBoundsWithDistinct(List<Long> values,
                                                                          long requestedBatchCount) {
        Pair<List<Map<Integer, ParameterContext>>, Integer> selection =
            FastChecker.selectBatchBounds(sampleRows(values), sampleParameters(values), SINGLE_LONG_PK,
                requestedBatchCount);
        List<Map<Integer, ParameterContext>> bounds = selection.getKey();
        List<Long> result = new ArrayList<>(bounds.size());
        for (Map<Integer, ParameterContext> bound : bounds) {
            result.add((Long) bound.get(1).getArgs()[1]);
        }
        return Pair.of(result, selection.getValue());
    }

    private static List<List<Object>> sampleRows(List<Long> values) {
        List<List<Object>> rows = new ArrayList<>(values.size());
        for (Long value : values) {
            rows.add(row(value));
        }
        return rows;
    }

    private static List<Map<Integer, ParameterContext>> sampleParameters(List<Long> values) {
        List<Map<Integer, ParameterContext>> result = new ArrayList<>(values.size());
        for (Long value : values) {
            result.add(parameters(value));
        }
        return result;
    }

    private static List<Long> sequence(int size) {
        List<Long> values = new ArrayList<>(size);
        for (long value = 1; value <= size; value++) {
            values.add(value);
        }
        return values;
    }

    private static long largestSamplePartition(List<Long> bounds, long sampleCount) {
        long previousBound = 0L;
        long largestPartition = 0L;
        for (Long bound : bounds) {
            largestPartition = Math.max(largestPartition, bound - previousBound);
            previousBound = bound;
        }
        return Math.max(largestPartition, sampleCount - previousBound);
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private static Map<Integer, ParameterContext> parameters(Object... values) {
        Map<Integer, ParameterContext> parameters = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i++) {
            int parameterIndex = i + 1;
            parameters.put(parameterIndex,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {parameterIndex, values[i]}));
        }
        return parameters;
    }

    private static List<Long> parameterValues(Map<Integer, ParameterContext> parameters) {
        List<Long> values = new ArrayList<>(parameters.size());
        for (int i = 1; i <= parameters.size(); i++) {
            values.add((Long) parameters.get(i).getArgs()[1]);
        }
        return values;
    }

    private static Logger recordingLogger(List<String> logEvents) {
        return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class[] {Logger.class},
            (proxy, method, args) -> {
                if (("info".equals(method.getName()) || "warn".equals(method.getName()))
                    && args != null && args.length == 1 && args[0] instanceof String) {
                    logEvents.add(method.getName() + ":" + args[0]);
                }
                return method.getReturnType() == boolean.class ? false : null;
            });
    }

    private static ColumnMeta longColumn(String name) {
        return new ColumnMeta("test_table", name, null, new Field("test_table", name, DataTypes.LongType));
    }
}
