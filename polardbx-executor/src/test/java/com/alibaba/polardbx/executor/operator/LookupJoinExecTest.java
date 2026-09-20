package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for LookupJoinExec class
 */
public class LookupJoinExecTest {

    private static final Logger logger = LoggerFactory.getLogger(LookupJoinExecTest.class);

    private boolean originalEnableGsiLookupOptimize;
    private float originalGsiLookupOptimizeThreshold;

    @Mock
    private ExecutionContext mockContext;

    @Mock
    private ParamManager mockParamManager;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Save original configuration values
        originalEnableGsiLookupOptimize = DynamicConfig.getInstance().enableGsiLookupOptimize();
        originalGsiLookupOptimizeThreshold = DynamicConfig.getInstance().getGsiLookupOptimizeThreshold();

        // Setup mock ExecutionContext
        when(mockContext.getParamManager()).thenReturn(mockParamManager);
    }

    @After
    public void tearDown() {
        // Restore original configuration values
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.ENABLE_GSI_LOOKUP_OPTIMIZE,
            String.valueOf(originalEnableGsiLookupOptimize));
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD,
            String.valueOf(originalGsiLookupOptimizeThreshold));
    }

    // Tests for isLookupSizeTooMuch method
    @Test
    public void testIsLookupSizeTooMuch_WhenInnerIsOpen() {
        // Given: innerIsOpen = true
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(100);

        // When: calling isLookupSizeTooMuch with innerIsOpen = true
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            true, true, mockChunk, 10, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when inner is open", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_WhenOptimizationNotReady() {
        // Given: isAdaptiveLookupOptimizationReady = false
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(100);

        // When: calling isLookupSizeTooMuch with optimization not ready
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, false, mockChunk, 10, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when adaptive optimization is not ready", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_WhenChunkIsNull() {
        // Given: savePopChunk = null
        // When: calling isLookupSizeTooMuch with null chunk
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, null, 10, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when chunk is null", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_WhenOptimizeDisabled() {
        // Given: GSI lookup optimize is disabled
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(100);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(false);

        // When: calling isLookupSizeTooMuch
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 10, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when GSI lookup optimize is disabled", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_WhenShardCountIsZero() {
        // Given: shardCount = 0
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(100);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);

        // When: calling isLookupSizeTooMuch with shardCount = 0
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 0, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when shardCount is 0", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_WhenLookupSizeIsZero() {
        // Given: lookupSize = 0 (from chunk.getPositionCount())
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(0);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);

        // When: calling isLookupSizeTooMuch
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 10, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when lookup size is 0", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_BelowThreshold() {
        // Given: GSI lookup optimize is enabled with threshold 5.0
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(20);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "5.0");

        // When: lookupSize < shardCount * threshold (20 < 5 * 5.0 = 25)
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 5, mockContext);

        // Then: should return false
        Assert.assertFalse("Should return false when lookup size is below threshold", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_AtThreshold() {
        // Given: GSI lookup optimize is enabled with threshold 5.0
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(25);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "5.0");

        // When: lookupSize == shardCount * threshold (25 == 5 * 5.0 = 25)
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 5, mockContext);

        // Then: should return true
        Assert.assertTrue("Should return true when lookup size equals threshold", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_AboveThreshold() {
        // Given: GSI lookup optimize is enabled with threshold 3.0
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(100);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "3.0");

        // When: lookupSize > shardCount * threshold (100 > 10 * 3.0 = 30)
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 10, mockContext);

        // Then: should return true
        Assert.assertTrue("Should return true when lookup size is above threshold", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_WithFloatThreshold() {
        // Given: GSI lookup optimize is enabled with fractional threshold 1.5
        Chunk mockChunk = mock(Chunk.class);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "1.5");

        // Test case 1: lookupSize = 15, shardCount = 10
        // Expected threshold: 10 * 1.5 = 15.0, so 15 >= 15.0 -> true
        when(mockChunk.getPositionCount()).thenReturn(15);
        boolean result1 = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 10, mockContext);
        Assert.assertTrue("Should return true when lookup size equals float threshold", result1);

        // Test case 2: lookupSize = 14, shardCount = 10
        // Expected threshold: 10 * 1.5 = 15.0, so 14 < 15.0 -> false
        when(mockChunk.getPositionCount()).thenReturn(14);
        boolean result2 = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 10, mockContext);
        Assert.assertFalse("Should return false when lookup size is below float threshold", result2);
    }

    @Test
    public void testIsLookupSizeTooMuch_WithLargeNumbers() {
        // Given: GSI lookup optimize is enabled with threshold 2.5
        Chunk mockChunk = mock(Chunk.class);
        when(mockChunk.getPositionCount()).thenReturn(10000);
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "2.5");

        // When: lookupSize > shardCount * threshold (10000 > 1000 * 2.5 = 2500)
        boolean result = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk, 1000, mockContext);

        // Then: should return true
        Assert.assertTrue("Should handle large numbers correctly", result);
    }

    @Test
    public void testIsLookupSizeTooMuch_EdgeCases() {
        // Given: GSI lookup optimize is enabled with threshold 2.0
        when(mockParamManager.getBoolean(ConnectionParams.ENABLE_GSI_LOOKUP_OPTIMIZE)).thenReturn(true);
        DynamicConfig.getInstance().loadValue(logger, ConnectionProperties.GSI_LOOKUP_OPTIMIZE_THRESHOLD, "2.0");

        // Test case 1: Zero shard count
        Chunk mockChunk1 = mock(Chunk.class);
        when(mockChunk1.getPositionCount()).thenReturn(100);
        boolean result1 = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk1, 0, mockContext);
        Assert.assertFalse("Should return false when shardCount is 0 and lookupSize > 0", result1);

        // Test case 2: Zero lookup size
        Chunk mockChunk2 = mock(Chunk.class);
        when(mockChunk2.getPositionCount()).thenReturn(0);
        boolean result2 = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk2, 5, mockContext);
        Assert.assertFalse("Should return false when lookupSize is 0", result2);

        // Test case 3: Both zero
        Chunk mockChunk3 = mock(Chunk.class);
        when(mockChunk3.getPositionCount()).thenReturn(0);
        boolean result3 = LookupJoinExec.isLookupSizeTooMuch(
            false, true, mockChunk3, 0, mockContext);
        Assert.assertFalse("Should return false when both lookupSize and shardCount are 0", result3);
    }
}