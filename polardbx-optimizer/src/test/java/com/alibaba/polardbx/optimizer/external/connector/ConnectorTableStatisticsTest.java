package com.alibaba.polardbx.optimizer.external.connector;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ConnectorTableStatisticsTest {

    @Test
    public void testColumnStatisticsDefaults() {
        ConnectorColumnStatistics stats = new ConnectorColumnStatistics(-1, -1, null, null, -1);
        assertEquals(-1, stats.getNdv());
        assertEquals(-1, stats.getNullFraction(), 0.001);
        assertNull(stats.getMinValue());
        assertNull(stats.getMaxValue());
        assertEquals(-1, stats.getAvgSize());
    }

    @Test
    public void testColumnStatisticsWithValues() {
        ConnectorColumnStatistics stats = new ConnectorColumnStatistics(100, 0.05, "a", "z", 32);
        assertEquals(100, stats.getNdv());
        assertEquals(0.05, stats.getNullFraction(), 0.001);
        assertEquals("a", stats.getMinValue());
        assertEquals("z", stats.getMaxValue());
        assertEquals(32, stats.getAvgSize());
    }

    // --- ConnectorTableStatistics ---

    @Test
    public void testTableStatisticsUnknown() {
        ConnectorTableStatistics stats = new ConnectorTableStatistics(-1, -1, Collections.emptyMap());
        assertEquals(-1, stats.getRowCount());
        assertEquals(-1, stats.getDataSize());
        assertTrue(stats.getColumnStats().isEmpty());
    }

    @Test
    public void testTableStatisticsWithColumns() {
        Map<String, ConnectorColumnStatistics> colStats = new HashMap<>();
        colStats.put("id", new ConnectorColumnStatistics(1000, 0.0, 1L, 9999L, 8));
        ConnectorTableStatistics stats = new ConnectorTableStatistics(5000, 1024 * 1024, colStats);
        assertEquals(5000, stats.getRowCount());
        assertEquals(1024 * 1024, stats.getDataSize());
        assertEquals(1000, stats.getColumnStats().get("id").getNdv());
    }

    @Test
    public void testTableStatisticsColumnStatsUnmodifiable() {
        Map<String, ConnectorColumnStatistics> colStats = new HashMap<>();
        colStats.put("a", new ConnectorColumnStatistics(10, 0.1, null, null, -1));
        ConnectorTableStatistics stats = new ConnectorTableStatistics(100, -1, colStats);
        try {
            stats.getColumnStats().put("b", new ConnectorColumnStatistics(1, 0, null, null, -1));
            fail("Should be unmodifiable");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    // --- ConnectorPartitionInfo ---

    @Test
    public void testPartitionInfo() {
        Map<String, String> values = new HashMap<>();
        values.put("dt", "2024-01-01");
        values.put("region", "us");
        ConnectorPartitionInfo part = new ConnectorPartitionInfo("dt=2024-01-01/region=us", values, 5000, 1024);
        assertEquals("dt=2024-01-01/region=us", part.getName());
        assertEquals("2024-01-01", part.getValues().get("dt"));
        assertEquals(5000, part.getRowCount());
        assertEquals(1024, part.getDataSize());
    }

    @Test
    public void testPartitionInfoUnknownStats() {
        ConnectorPartitionInfo part = new ConnectorPartitionInfo("p0", Collections.emptyMap(), -1, -1);
        assertEquals(-1, part.getRowCount());
        assertEquals(-1, part.getDataSize());
        assertTrue(part.getValues().isEmpty());
    }
}
