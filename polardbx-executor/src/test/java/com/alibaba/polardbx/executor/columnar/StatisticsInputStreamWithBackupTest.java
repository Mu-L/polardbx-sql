package com.alibaba.polardbx.executor.columnar;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.alibaba.polardbx.common.oss.filesystem.InputStreamWithBackup;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

public class StatisticsInputStreamWithBackupTest {

    @Test
    public void testFallbackDetection() throws IOException {
        // Create mock streams
        InputStream masterStream = mock(InputStream.class);
        Function<Integer, InputStream> backupStreamCtor = mock(Function.class);
        InputStream backupStream = mock(InputStream.class);

        // Configure mock to throw an exception on first read, then return backup stream
        when(masterStream.read()).thenThrow(new IOException("Master stream failed"));
        when(backupStreamCtor.apply(anyInt())).thenReturn(backupStream);
        when(backupStream.read()).thenReturn(65, 66, 67, -1); // Return some bytes then EOF

        // Create StatisticsInputStreamWithBackup
        InputStreamWithBackup statsStream = new InputStreamWithBackup(
            masterStream, backupStreamCtor, "test.csv");

        // Verify fallback has not occurred yet
        assertFalse(statsStream.isFallbackOccurred());

        // Manually set fallback occurred (simulating what happens in SimpleCSVFileReader)
        statsStream.setFallbackOccurred();

        // Verify fallback occurred
        assertTrue(statsStream.isFallbackOccurred());
    }

    @Test
    public void testNoFallback() throws IOException {
        // Create mock stream that works correctly
        InputStream masterStream = mock(InputStream.class);
        when(masterStream.read()).thenReturn(65, 66, 67, -1); // Return some bytes then EOF

        // Create StatisticsInputStreamWithBackup
        Function<Integer, InputStream> backupStreamCtor = mock(Function.class);
        InputStreamWithBackup statsStream = new InputStreamWithBackup(
            masterStream, backupStreamCtor, "test.csv");

        // Verify fallback has not occurred
        assertFalse(statsStream.isFallbackOccurred());

        // Read some bytes
        statsStream.read();
        statsStream.read();

        // Verify fallback still has not occurred
        assertFalse(statsStream.isFallbackOccurred());
    }

    @Test
    public void testSetFallbackOccurred() {
        // Create mock streams
        InputStream masterStream = mock(InputStream.class);
        Function<Integer, InputStream> backupStreamCtor = mock(Function.class);

        // Create StatisticsInputStreamWithBackup
        InputStreamWithBackup statsStream = new InputStreamWithBackup(
            masterStream, backupStreamCtor, "test.csv");

        // Verify fallback has not occurred yet
        assertFalse(statsStream.isFallbackOccurred());

        // Set fallback occurred
        statsStream.setFallbackOccurred();

        // Verify fallback occurred
        assertTrue(statsStream.isFallbackOccurred());
    }
}