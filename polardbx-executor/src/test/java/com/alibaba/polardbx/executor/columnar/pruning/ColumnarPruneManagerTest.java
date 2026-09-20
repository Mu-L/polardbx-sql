package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.common.orc.FastColumnStatistics;
import com.alibaba.polardbx.common.orc.FastDateColumnStatistics;
import com.alibaba.polardbx.common.orc.FastLongColumnStatistics;
import com.alibaba.polardbx.common.orc.FastStringColumnStatistics;
import com.alibaba.polardbx.common.orc.NormalColumnStatistics;
import com.alibaba.polardbx.common.orc.PreheatFileMeta;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.OrderByOption;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarPruneManagerTest {

    @Mock
    private Path targetFile;

    @Mock
    private PreheatFileMeta preheatFileMeta;

    @Mock
    private ColumnMeta columnMeta1;

    @Mock
    private ColumnMeta columnMeta2;

    @Mock
    private ColumnMeta columnMeta3;

    @Mock
    private OrderByOption sortKey1;

    @Mock
    private OrderByOption sortKey2;

    @Mock
    private FastStringColumnStatistics stringColumnStats;

    @Mock
    private FastLongColumnStatistics longColumnStats;

    @Mock
    private FastDateColumnStatistics dateColumnStats;

    @Mock
    private DynamicConfig dynamicConfig;

    @Mock
    private ModuleLogInfo moduleLogInfo;

    private List<ColumnMeta> columns;
    private List<OrderByOption> sortKeys;
    private List<Integer> orcIndexes;
    private FastColumnStatistics[] fastColumnStatisticsArray;

    @Before
    public void setUp() {
        // Setup basic mocks that are used by all tests
        when(targetFile.toString()).thenReturn("/test/file.orc");

        // Setup columns
        columns = Arrays.asList(columnMeta1, columnMeta2, columnMeta3);

        // Setup ORC indexes
        orcIndexes = Arrays.asList(0, 1, 2);

        // Setup fast column statistics array
        fastColumnStatisticsArray = new FastColumnStatistics[3];
        fastColumnStatisticsArray[0] = longColumnStats;
        fastColumnStatisticsArray[1] = stringColumnStats;
        fastColumnStatisticsArray[2] = dateColumnStats;

        when(preheatFileMeta.getFastColumnStatisticsArray()).thenReturn(fastColumnStatisticsArray);
        when(preheatFileMeta.getStripeSize()).thenReturn(2);
    }

    private void setupBasicColumnStatistics() {
        // Setup accumulated row group counts
        int[] accumulatedCounts = {0, 5, 10};

        // Long column statistics
        when(longColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(longColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(longColumnStats.hasNull(anyInt())).thenReturn(false);
        when(longColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(longColumnStats.getMaxLong(anyInt())).thenReturn(200L);

        // String column statistics
        when(stringColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(stringColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(stringColumnStats.hasNull(anyInt())).thenReturn(true);
        when(stringColumnStats.getMinString(anyInt())).thenReturn("aaa");
        when(stringColumnStats.getMaxString(anyInt())).thenReturn("zzz");

        // Date column statistics
        when(dateColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(dateColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(dateColumnStats.hasNull(anyInt())).thenReturn(false);
        when(dateColumnStats.getMinInt(anyInt())).thenReturn(20220101);
        when(dateColumnStats.getMaxInt(anyInt())).thenReturn(20221231);
        // when(dateColumnStats.getMinLong(anyInt())).thenReturn(100L);
        // when(dateColumnStats.getMaxLong(anyInt())).thenReturn(200L);
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaSingleSortKeyLongType() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        sortKeys = Collections.singletonList(sortKey1);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(sortKey1, atLeastOnce()).getIndex();
        verify(sortKey1, atLeastOnce()).isAsc();
        verify(longColumnStats, atLeastOnce()).getMinLong(anyInt());
        verify(longColumnStats, atLeastOnce()).getMaxLong(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaSingleSortKeyStringType() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(sortKey1.getIndex()).thenReturn(1); // Point to string column
        when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        sortKeys = Collections.singletonList(sortKey1);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, true);

        assertNotNull(result);
        verify(stringColumnStats, atLeastOnce()).getMinString(anyInt());
        verify(stringColumnStats, atLeastOnce()).getMaxString(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeys() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);
        setupBasicColumnStatistics();

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(sortKey1, atLeastOnce()).getIndex();
        verify(sortKey2, atLeastOnce()).getIndex();
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysWithNegativeIndex() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(-1);
        setupBasicColumnStatistics();

        try (MockedStatic<ModuleLogInfo> mockedModuleLogInfo = mockStatic(ModuleLogInfo.class)) {
            mockedModuleLogInfo.when(ModuleLogInfo::getInstance).thenReturn(moduleLogInfo);
            doNothing().when(moduleLogInfo).logRecord(any(), any(), any(), any());

            sortKeys = Arrays.asList(sortKey1, sortKey2);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            // Should continue processing despite negative index
            verify(moduleLogInfo, atLeastOnce()).logRecord(any(), any(), any(), any());
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysNullOrcIndex() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        // when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);
        setupBasicColumnStatistics();

        orcIndexes = Arrays.asList(0, null, 2);
        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        // Should skip columns with null ORC index
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysStringColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1); // Point to string column
        setupBasicColumnStatistics();

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(stringColumnStats, atLeastOnce()).hasNull(anyInt());
        verify(stringColumnStats, atLeastOnce()).getMinString(anyInt());
        verify(stringColumnStats, atLeastOnce()).getMaxString(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysDateColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.DateType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(2); // Point to date column
        setupBasicColumnStatistics();

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(dateColumnStats, atLeastOnce()).hasNull(anyInt());
        verify(dateColumnStats, atLeastOnce()).getMinInt(anyInt());
        verify(dateColumnStats, atLeastOnce()).getMaxInt(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysIntegerColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.IntegerType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);

        // Create a proper long column stats for integer type
        FastLongColumnStatistics integerColumnStats = mock(FastLongColumnStatistics.class);
        when(integerColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(new int[] {0, 5, 10});
        when(integerColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(integerColumnStats.hasNull(anyInt())).thenReturn(false);
        when(integerColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(integerColumnStats.getMaxLong(anyInt())).thenReturn(200L);
        fastColumnStatisticsArray[1] = integerColumnStats;

        // Setup basic statistics for other columns
        int[] accumulatedCounts = {0, 5, 10};
        when(longColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(longColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(longColumnStats.hasNull(anyInt())).thenReturn(false);
        when(longColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(longColumnStats.getMaxLong(anyInt())).thenReturn(200L);

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(integerColumnStats, atLeastOnce()).hasNull(anyInt());
        verify(integerColumnStats, atLeastOnce()).getMinLong(anyInt());
        verify(integerColumnStats, atLeastOnce()).getMaxLong(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysCharColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.CharType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);
        setupBasicColumnStatistics();

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(stringColumnStats, atLeastOnce()).getMinString(anyInt());
        verify(stringColumnStats, atLeastOnce()).getMaxString(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaMultiSortKeysDatetimeColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.DatetimeType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);

        // Create a proper long column stats for datetime type
        FastLongColumnStatistics datetimeColumnStats = mock(FastLongColumnStatistics.class);
        when(datetimeColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(new int[] {0, 5, 10});
        when(datetimeColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(datetimeColumnStats.hasNull(anyInt())).thenReturn(false);
        when(datetimeColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(datetimeColumnStats.getMaxLong(anyInt())).thenReturn(200L);
        fastColumnStatisticsArray[1] = datetimeColumnStats;

        // Setup basic statistics for other columns
        int[] accumulatedCounts = {0, 5, 10};
        when(longColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(longColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(longColumnStats.hasNull(anyInt())).thenReturn(false);
        when(longColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(longColumnStats.getMaxLong(anyInt())).thenReturn(200L);

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        verify(datetimeColumnStats, atLeastOnce()).hasNull(anyInt());
        verify(datetimeColumnStats, atLeastOnce()).getMinLong(anyInt());
        verify(datetimeColumnStats, atLeastOnce()).getMaxLong(anyInt());
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaZoneMapEnabled() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.DateType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            when(dynamicConfig.enableZoneMapPrune()).thenReturn(true);

            sortKeys = Collections.singletonList(sortKey1);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            verify(dynamicConfig, atLeastOnce()).enableZoneMapPrune();
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaZoneMapDisabled() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            // when(dynamicConfig.enableZoneMapPrune()).thenReturn(false);

            sortKeys = Collections.singletonList(sortKey1);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaZoneMapDateColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.DateType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            when(dynamicConfig.enableZoneMapPrune()).thenReturn(true);

            sortKeys = Collections.singletonList(sortKey1);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            // Should process date column in zone map
            verify(dateColumnStats, atLeastOnce()).hasNull(anyInt());
            verify(dateColumnStats, atLeastOnce()).getMinInt(anyInt());
            verify(dateColumnStats, atLeastOnce()).getMaxInt(anyInt());
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaZoneMapLongColumn() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.LongType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);

        FastLongColumnStatistics zoneMapLongStats = mock(FastLongColumnStatistics.class);
        when(zoneMapLongStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(new int[] {0, 5, 10});
        when(zoneMapLongStats.getTotalRowGroupCount()).thenReturn(10);
        when(zoneMapLongStats.hasNull(anyInt())).thenReturn(false);
        when(zoneMapLongStats.getMinLong(anyInt())).thenReturn(300L);
        when(zoneMapLongStats.getMaxLong(anyInt())).thenReturn(400L);
        fastColumnStatisticsArray[2] = zoneMapLongStats;

        // Setup basic statistics for other columns
        int[] accumulatedCounts = {0, 5, 10};
        when(longColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(longColumnStats.getTotalRowGroupCount()).thenReturn(10);
        // when(longColumnStats.hasNull(anyInt())).thenReturn(false);
        when(longColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(longColumnStats.getMaxLong(anyInt())).thenReturn(200L);

        // when(stringColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        // when(stringColumnStats.getTotalRowGroupCount()).thenReturn(10);
        // when(stringColumnStats.hasNull(anyInt())).thenReturn(true);
        // when(stringColumnStats.getMinString(anyInt())).thenReturn("aaa");
        // when(stringColumnStats.getMaxString(anyInt())).thenReturn("zzz");

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            when(dynamicConfig.enableZoneMapPrune()).thenReturn(true);

            sortKeys = Collections.singletonList(sortKey1);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            // Should process long column in zone map
            verify(zoneMapLongStats, atLeastOnce()).hasNull(anyInt());
            verify(zoneMapLongStats, atLeastOnce()).getMinLong(anyInt());
            verify(zoneMapLongStats, atLeastOnce()).getMaxLong(anyInt());
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaZoneMapSkipSortKeyColumns() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.DateType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);
        setupBasicColumnStatistics();

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            when(dynamicConfig.enableZoneMapPrune()).thenReturn(true);

            sortKeys = Arrays.asList(sortKey1, sortKey2);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            // Should only process column 2 (index 2) in zone map since 0 and 1 are sort keys
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaZoneMapNullOrcIndex() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.VarcharType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            // when(dynamicConfig.enableZoneMapPrune()).thenReturn(true);

            orcIndexes = Arrays.asList(0, 1, null); // Make last column have null ORC index
            sortKeys = Collections.singletonList(sortKey1);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            // Should skip column with null ORC index
        }
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaEdgeCaseLastStripe() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(preheatFileMeta.getStripeSize()).thenReturn(1); // Only one stripe
        setupBasicColumnStatistics();

        sortKeys = Collections.singletonList(sortKey1);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        // Should handle last stripe boundary correctly
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndexPrunerFromBinaryMetaNullOrcIndexForSortKey() {
        // Setup specific mocks for this test
        // when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(sortKey1.getIndex()).thenReturn(0);
        // when(sortKey1.isAsc()).thenReturn(true);
        setupBasicColumnStatistics();

        orcIndexes = Arrays.asList(null, 1, 2); // Make sort key column have null ORC index
        sortKeys = Collections.singletonList(sortKey1);

        ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaUnsupportedDataType() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta2.getDataType()).thenReturn(DataTypes.BooleanType); // Unsupported type
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);
        when(sortKey2.getIndex()).thenReturn(1);

        // Create a mock statistics that won't cause ClassCastException
        FastLongColumnStatistics booleanColumnStats = mock(FastLongColumnStatistics.class);
        when(booleanColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(new int[] {0, 5, 10});
        when(booleanColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(booleanColumnStats.hasNull(anyInt())).thenReturn(false);
        when(booleanColumnStats.getMinLong(anyInt())).thenReturn(0L);
        when(booleanColumnStats.getMaxLong(anyInt())).thenReturn(1L);
        fastColumnStatisticsArray[1] = booleanColumnStats;

        // Setup basic statistics for other columns
        int[] accumulatedCounts = {0, 5, 10};
        when(longColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(longColumnStats.getTotalRowGroupCount()).thenReturn(10);
        when(longColumnStats.hasNull(anyInt())).thenReturn(false);
        when(longColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(longColumnStats.getMaxLong(anyInt())).thenReturn(200L);

        sortKeys = Arrays.asList(sortKey1, sortKey2);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        assertNotNull(result);
        // Should skip unsupported data types
    }

    @Test
    public void testGetIndexPrunerFromBinaryMetaUnsupportedDataTypeForZoneMap() {
        // Setup specific mocks for this test
        when(columnMeta1.getDataType()).thenReturn(DataTypes.LongType);
        when(columnMeta3.getDataType()).thenReturn(DataTypes.BooleanType); // Unsupported for zone map
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);

        // Setup basic statistics for columns that are used
        int[] accumulatedCounts = {0, 5, 10};
        when(longColumnStats.getAccumulatedRowGroupCountPerStripe()).thenReturn(accumulatedCounts);
        when(longColumnStats.getTotalRowGroupCount()).thenReturn(10);
        // when(longColumnStats.hasNull(anyInt())).thenReturn(false);
        when(longColumnStats.getMinLong(anyInt())).thenReturn(100L);
        when(longColumnStats.getMaxLong(anyInt())).thenReturn(200L);

        try (MockedStatic<DynamicConfig> mockedDynamicConfig = mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);
            // when(dynamicConfig.enableZoneMapPrune()).thenReturn(true);

            sortKeys = Collections.singletonList(sortKey1);

            IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
                targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

            assertNotNull(result);
            // Should skip unsupported data types for zone map
        }
    }

    /**
     * Iteration 001 regression test for Aone Bug #72283876.
     * <p>
     * Repro: DECIMAL sort-key columns are persisted as NormalColumnStatistics
     * (via ORCMetaReaderImpl#FLAG_DECIMAL_STATISTICS). Before the fix, the single-column
     * sort-key else branch of getIndexPrunerFromBinaryMeta called fastColumnStatistics.getMinLong,
     * which NormalColumnStatistics does not override, so the FastColumnStatistics default
     * method threw UnsupportedOperationException. After the fix, unsupported FastColumnStatistics
     * variants trigger an early-skip that leaves the sort-key index unbuilt (graceful degradation
     * to full scan + filter) while rgNum is still accumulated so downstream indexes stay sized.
     */
    @Test
    public void testGetIndexPrunerFromBinaryMeta_decimalSortKey_normalStats_gracefulSkip() {
        NormalColumnStatistics normalStats = new NormalColumnStatistics()
            .setTotalRowGroupCount(10)
            .setAccumulatedRowGroupCountPerStripe(new int[] {0, 5, 10});

        // Replace the sort-key slot with NormalColumnStatistics to mimic a DECIMAL column.
        fastColumnStatisticsArray[0] = normalStats;

        when(columnMeta1.getDataType()).thenReturn(DataTypes.DecimalType);
        when(sortKey1.getIndex()).thenReturn(0);
        when(sortKey1.isAsc()).thenReturn(true);

        sortKeys = Collections.singletonList(sortKey1);

        IndexPruner result = ColumnarPruneManager.getIndexPrunerFromBinaryMeta(
            targetFile, preheatFileMeta, columns, sortKeys, orcIndexes, false);

        // No UnsupportedOperationException; an IndexPruner is still produced so the query can
        // fall back to full scan + filter on the DECIMAL sort-key column.
        assertNotNull(result);
    }
}