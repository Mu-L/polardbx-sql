package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class LookupInfoPrefixMatchTest {

    @Test
    public void testPrefixMatchExactMatch() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col1");
        cols.add("col2");
        cols.add("col3");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2", "col3"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertTrue(result);
    }

    @Test
    public void testPrefixMatchUnorderedMatch() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col3");
        cols.add("col1");
        cols.add("col2");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2", "col3", "col4"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertTrue(result);
    }

    @Test
    public void testPrefixMatchSubsetMatch() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col2");
        cols.add("col1");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2", "col3", "col4"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertTrue(result);
    }

    @Test
    public void testPrefixMatchByOrder() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col3");
        cols.add("col1");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2", "col3", "col4"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertFalse(result);
    }

    @Test
    public void testPrefixMatchNoMatch() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col4");
        cols.add("col5");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2", "col3"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertFalse(result);
    }

    @Test
    public void testPrefixMatchEmptyCols() {
        // Arrange
        List<String> cols = new ArrayList<>();

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2", "col3"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertTrue(result);
    }

    @Test
    public void testPrefixMatchNullIndex() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col4");
        cols.add("col5");

        // Act
        boolean result = LookupInfo.prefixMatch(cols, null);

        // Assert
        assertFalse(result);
    }

    @Test
    public void testPrefixMatchEmptyIndexColumns() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col1");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertFalse(result);
    }

    @Test
    public void testPrefixMatchCaseInsensitive() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("COL1");
        cols.add("col2");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "COL2", "col3"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertTrue(result);
    }

    @Test
    public void testPrefixMatchMoreColsThanIndex() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col1");
        cols.add("col2");
        cols.add("col3");
        cols.add("col4");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col1", "col2"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertFalse(result);
    }

    @Test
    public void testPrefixMatchSameSizeDifferentContent() {
        // Arrange
        List<String> cols = new ArrayList<>();
        cols.add("col1");
        cols.add("col2");
        cols.add("col3");

        IndexMeta indexMeta = createMockIndexMeta(new String[] {"col4", "col5", "col6"});

        // Act
        boolean result = LookupInfo.prefixMatch(cols, indexMeta);

        // Assert
        assertFalse(result);
    }

    /**
     * Helper method to create a mock IndexMeta with specified column names
     */
    private IndexMeta createMockIndexMeta(String[] columnNames) {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        for (String columnName : columnNames) {
            ColumnMeta columnMeta = Mockito.mock(ColumnMeta.class);
            when(columnMeta.getName()).thenReturn(columnName);
            columnMetas.add(columnMeta);
        }

        IndexMeta indexMeta = Mockito.mock(IndexMeta.class);
        when(indexMeta.getKeyColumns()).thenReturn(columnMetas);
        return indexMeta;
    }
}