package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ColumnarVersionChainPrunerTest {

    private ColumnarVersionChainPruner pruner;

    @Mock
    private Connection mockConnection;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // Mock the InstConfUtil to return an empty string to avoid NPE
        try (MockedStatic<InstConfUtil> instConfUtilMock = mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {

            instConfUtilMock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER))
                .thenReturn("");

            // Mock MetaDbUtil to return a mock connection
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            pruner = new ColumnarVersionChainPruner();
        }
    }

    @Test
    public void testConstructor() {
        // Test that the constructor creates an instance without throwing exceptions
        assertNotNull(pruner);
        assertNotNull(pruner.getPrunerMap());
    }

    @Test
    public void testCheckPruneStringValidWithNullString() {
        // Test that null string is valid
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid(null);
            // No exception should be thrown
        }
    }

    @Test
    public void testCheckPruneStringValidWithEmptyString() {
        // Test that empty string is valid
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid("");
            // No exception should be thrown
        }
    }

    @Test
    public void testCheckPruneStringValidWithWhitespaceString() {
        // Test that whitespace-only string is valid
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid("   ");
            // No exception should be thrown
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCheckPruneStringValidWithInvalidFormatMissingColon() {
        // Test that string without colon throws exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid("schema.1");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCheckPruneStringValidWithInvalidFormatMissingDot() {
        // Test that string without dot throws exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid("schema1:7");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCheckPruneStringValidWithInvalidFormatEmptySchema() {
        // Test that string with empty schema throws exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid(".1:7");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCheckPruneStringValidWithInvalidFormatEmptyTable() {
        // Test that string with empty table throws exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid("schema.:7");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCheckPruneStringValidWithInvalidTimeFormat() {
        // Test that string with invalid time format throws exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            ColumnarVersionChainPruner.checkPruneStringValid("schema.1:abc");
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCheckPruneStringValidWithTableNotFound() throws SQLException {
        // Test that string with non-existent table throws exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(anyString(), anyLong())).thenReturn(new ArrayList<>());
                })) {

                ColumnarVersionChainPruner.checkPruneStringValid("schema.1:7");
            }
        }
    }

    @Test
    public void testCheckPruneStringValidWithValidFormat() throws SQLException {
        // Test that string with valid format does not throw exception
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            List<ColumnarTableMappingRecord> mockRecords = new ArrayList<>();
            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableId = 1L;
            mockRecords.add(record);

            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(anyString(), anyLong())).thenReturn(mockRecords);
                })) {

                ColumnarVersionChainPruner.checkPruneStringValid("schema.1:7");
                // No exception should be thrown
            }
        }
    }

    @Test
    public void testReloadWithSameString() {
        // Test that reloading with the same string doesn't change anything
        String originalString = "testSchema.1:7";

        // Mock the InstConfUtil to return our test string
        try (MockedStatic<InstConfUtil> instConfUtilMock = mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {

            instConfUtilMock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER))
                .thenReturn(originalString);

            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            // Mock the ColumnarTableMappingAccessor to return mock records
            List<ColumnarTableMappingRecord> mockRecords = new ArrayList<>();
            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableId = 1L;
            mockRecords.add(record);

            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(anyString(), anyLong())).thenReturn(mockRecords);
                })) {

                ColumnarVersionChainPruner newPruner = new ColumnarVersionChainPruner();
                newPruner.reload(originalString);

                // The map should have the same content
                assertEquals(1, newPruner.getPrunerMap().size());
            }
        }
    }

    @Test
    public void testReloadWithDifferentString() throws SQLException {
        // Test that reloading with a different string updates the map
        String originalString = "testSchema.1:7";
        String newString = "testSchema2.2:14";

        // Mock the dependencies
        try (MockedStatic<InstConfUtil> instConfUtilMock = mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {

            instConfUtilMock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER))
                .thenReturn(originalString);

            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            List<ColumnarTableMappingRecord> mockRecords = new ArrayList<>();
            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableId = 1L;
            mockRecords.add(record);

            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(anyString(), anyLong())).thenReturn(mockRecords);
                })) {

                ColumnarVersionChainPruner newPruner = new ColumnarVersionChainPruner();
                newPruner.reload(newString);

                // The map should be updated
                assertNotSame(originalString, newString);
                assertEquals(1, newPruner.getPrunerMap().size());
            }
        }
    }

    @Test
    public void testReloadWithEmptyString() throws SQLException {
        // Test that reloading with an empty string clears the map
        String originalString = "testSchema.1:7";
        String newString = "";

        // Mock the dependencies
        try (MockedStatic<InstConfUtil> instConfUtilMock = mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {

            instConfUtilMock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER))
                .thenReturn(originalString);

            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            List<ColumnarTableMappingRecord> mockRecords = new ArrayList<>();
            ColumnarTableMappingRecord record = new ColumnarTableMappingRecord();
            record.tableId = 1L;
            mockRecords.add(record);

            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(anyString(), anyLong())).thenReturn(mockRecords);
                })) {

                ColumnarVersionChainPruner newPruner = new ColumnarVersionChainPruner();
                // Initially the map should have one entry
                assertEquals(1, newPruner.getPrunerMap().size());

                // After reloading with empty string, the map should be empty
                newPruner.reload(newString);
                assertEquals(0, newPruner.getPrunerMap().size());
            }
        }
    }

    @Test
    public void testReloadWithMultipleEntries() throws SQLException {
        // Test that reloading with multiple entries works correctly
        String originalString = "testSchema.1:7";
        String newString = "schema1.1:7,schema2.2:14";

        // Mock the dependencies
        try (MockedStatic<InstConfUtil> instConfUtilMock = mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {

            instConfUtilMock.when(() -> InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER))
                .thenReturn(originalString);

            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);

            List<ColumnarTableMappingRecord> mockRecords1 = new ArrayList<>();
            ColumnarTableMappingRecord record1 = new ColumnarTableMappingRecord();
            record1.tableId = 1L;
            mockRecords1.add(record1);

            List<ColumnarTableMappingRecord> mockRecords2 = new ArrayList<>();
            ColumnarTableMappingRecord record2 = new ColumnarTableMappingRecord();
            record2.tableId = 2L;
            mockRecords2.add(record2);

            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(eq("schema1"), anyLong())).thenReturn(mockRecords1);
                    when(mock.querySchemaTableId(eq("schema2"), anyLong())).thenReturn(mockRecords2);
                })) {

                ColumnarVersionChainPruner newPruner = new ColumnarVersionChainPruner();
                newPruner.reload(newString);

                // The map should have two entries
                assertEquals(2, newPruner.getPrunerMap().size());
                assertTrue(newPruner.getPrunerMap().containsKey(new Pair<>("schema1", 1L)));
                assertTrue(newPruner.getPrunerMap().containsKey(new Pair<>("schema2", 2L)));
                assertEquals(Integer.valueOf(7), newPruner.getPrunerMap().get(new Pair<>("schema1", 1L)));
                assertEquals(Integer.valueOf(14), newPruner.getPrunerMap().get(new Pair<>("schema2", 2L)));
            }
        }
    }
}