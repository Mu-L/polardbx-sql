package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ParallelHashJoinExec} covering the passNothing behaviour switch
 * controlled by
 * {@link com.alibaba.polardbx.common.properties.ConnectionProperties#ENABLE_PASS_NOTHING_CONSUME_PROBE}.
 * <p>
 * Name-semantic reminder (see ConnectionProperties javadoc): {@code true} means KEEP the
 * passNothing fast-exit optimization (probe NOT consumed); {@code false} means DISABLE the
 * optimization and let the normal join path consume probe data naturally (empty hash table
 * yields no output but probe is fully pulled). See Aone #79665023.
 */
public class ParallelHashJoinPassNothingTest extends BaseExecTest {

    /**
     * Counts how many times {@link #nextChunk()} is invoked so tests can prove whether the
     * probe side was consumed by the Join executor.
     */
    static class CountingMockExec extends MockExec {
        int nextChunkCallCount = 0;

        CountingMockExec(List<DataType> columnTypes, List<Chunk> chunks) {
            super(columnTypes, chunks);
        }

        @Override
        public Chunk nextChunk() {
            nextChunkCallCount++;
            return super.nextChunk();
        }
    }

    private Map<String, Object> connectionMap;

    @Before
    public void before() {
        connectionMap = new HashMap<>();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1024);
        context.setParamManager(new ParamManager(connectionMap));
    }

    private void setPassNothingConsumeProbe(boolean value) {
        connectionMap.put(ConnectionParams.ENABLE_PASS_NOTHING_CONSUME_PROBE.getName(), value);
        context.setParamManager(new ParamManager(connectionMap));
    }

    private CountingMockExec probeWithChunks() {
        // Three chunks on the probe side; picked to exercise the drain loop across multiple pulls.
        return new CountingMockExec(
            Collections.singletonList(DataTypes.IntegerType),
            java.util.Arrays.asList(
                new Chunk(IntegerBlock.of(1, 2, 3)),
                new Chunk(IntegerBlock.of(4, 5, 6)),
                new Chunk(IntegerBlock.of(7, 8, 9))
            ));
    }

    private MockExec emptyBuild() {
        return MockExec.builder(DataTypes.IntegerType).build();
    }

    private MockExec nonEmptyBuild() {
        return MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(2, 5, 8)))
            .build();
    }

    /**
     * UC-1: default (param=true), INNER JOIN, empty build → passNothing fast-exit;
     * the probe side must NOT be consumed at all.
     */
    @Test
    public void testPassNothing_DefaultTrue_InnerEmpty_ProbeNotConsumed() {
        setPassNothingConsumeProbe(true);

        CountingMockExec probe = probeWithChunks();
        MockExec build = emptyBuild();

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            new EquiJoinKey(0, 0, DataTypes.IntegerType, false));

        ParallelHashJoinExec exec = new ParallelHashJoinExec(
            new Synchronizer(JoinRelType.INNER, false, 1, false, 1),
            probe, build, JoinRelType.INNER, false,
            joinKeys, null, null, false, context, 0, 1, false);

        SingleExecTest test = new SingleExecTest.Builder(exec, build).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.emptyList(), false);
        Assert.assertEquals(
            "probe.nextChunk() should NOT be called when passNothing optimization is enabled",
            0, probe.nextChunkCallCount);
    }

    /**
     * UC-2: param=false, INNER JOIN, empty build → normal join path (no passNothing);
     * the probe side must be fully consumed through the normal hash-join loop
     * (empty hash table → no matches → no output, but probe chunks are all pulled).
     */
    @Test
    public void testDrainProbe_ParamFalse_InnerEmpty_ProbeFullyDrained() {
        setPassNothingConsumeProbe(false);

        CountingMockExec probe = probeWithChunks();
        int probeChunkCount = probe.getChunks().size();
        MockExec build = emptyBuild();

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            new EquiJoinKey(0, 0, DataTypes.IntegerType, false));

        ParallelHashJoinExec exec = new ParallelHashJoinExec(
            new Synchronizer(JoinRelType.INNER, false, 1, false, 1),
            probe, build, JoinRelType.INNER, false,
            joinKeys, null, null, false, context, 0, 1, false);

        SingleExecTest test = new SingleExecTest.Builder(exec, build).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.emptyList(), false);
        Assert.assertTrue(
            "probe.nextChunk() should be called at least once per probe chunk + EOF "
                + "when passNothing optimization is disabled (normal path consumes probe); actual="
                + probe.nextChunkCallCount,
            probe.nextChunkCallCount >= probeChunkCount + 1);
    }

    /**
     * UC-3 regression: param=false with a non-empty build → behave as a normal INNER JOIN.
     * Ensures the new switch path does not alter result correctness when build is non-empty.
     */
    @Test
    public void testDrainProbe_ParamFalse_InnerNonEmpty_BehavesAsNormalJoin() {
        setPassNothingConsumeProbe(false);

        MockExec probe = MockExec.builder(DataTypes.IntegerType)
            .withChunk(new Chunk(IntegerBlock.of(1, 2, 3, 4, 5)))
            .build();
        MockExec build = nonEmptyBuild();

        List<EquiJoinKey> joinKeys = Collections.singletonList(
            new EquiJoinKey(0, 0, DataTypes.IntegerType, false));

        ParallelHashJoinExec exec = new ParallelHashJoinExec(
            new Synchronizer(JoinRelType.INNER, false, 1, false, 1),
            probe, build, JoinRelType.INNER, false,
            joinKeys, null, null, false, context, 0, 1, false);

        SingleExecTest test = new SingleExecTest.Builder(exec, build).build();
        test.exec();

        // Build side contains {2, 5, 8}; probe contains {1..5}; expected join output row count is 2.
        int totalRows = 0;
        for (Chunk c : test.result()) {
            totalRows += c.getPositionCount();
        }
        Assert.assertEquals(
            "INNER JOIN with non-empty build must still produce the normal join result under param=false",
            2, totalRows);
    }
}
