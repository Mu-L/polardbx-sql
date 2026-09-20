package com.alibaba.polardbx.gms.metadb.accessor;

import static com.alibaba.polardbx.common.utils.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import com.alibaba.polardbx.gms.metadb.table.LoadWeightAccessor;
import com.alibaba.polardbx.gms.metadb.table.LoadWeightRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class LoadWeightAccessorTest {

    @Test
    public void testQueryReturnsCorrectLoadWeightRecords() throws Exception {
        try (MockedStatic<MetaDbUtil> mockedStatic = Mockito.mockStatic(MetaDbUtil.class)) {
            // Step 1: Mock Connection
            Connection mockConnection = mock(Connection.class);

            // Step 2: Create a mock ResultSet
            ResultSet mockResultSet = mock(ResultSet.class);
            when(mockResultSet.getLong("id")).thenReturn(1L);
            when(mockResultSet.getString("inst_id")).thenReturn("inst_1");
            when(mockResultSet.getString("node")).thenReturn("node_1");
            when(mockResultSet.getString("load_weight")).thenReturn("100");

            // Step 3: Create a real LoadWeightRecord and verify fill method
            LoadWeightRecord record = new LoadWeightRecord(1,  "inst_1", "node_1", "100");
            record.fill(mockResultSet); // pre-fill for assertion

            // Step 4: Mock MetaDbUtil to return our list of records
            List expectedList = Arrays.asList(record);

            when(MetaDbUtil.getConnection()).thenReturn(mockConnection);
            when(MetaDbUtil.query(anyString(), any(), any(Connection.class))).thenReturn(expectedList);

            // Step 5: Create accessor with mocked connection
            LoadWeightAccessor accessor = new LoadWeightAccessor();
            accessor.setConnection(mockConnection); // assuming there's a setConnection method

            // Step 6: Call the query method
            List<LoadWeightRecord> result = accessor.query();

            // Step 7: Verify results
            assertNotNull(result);
            assertEquals(1, result.size());

            LoadWeightRecord returnedRecord = result.get(0);
            assertEquals(record.id, returnedRecord.id);
            assertEquals(record.instId, returnedRecord.instId);
            assertEquals(record.node, returnedRecord.node);
            assertEquals(record.loadWeight, returnedRecord.loadWeight);

//        // Step 8: (Optional) Verify that MetaDbUtil was called correctly
//        verify(MetaDbUtil, times(1)).query(eq(LoadWeightAccessor.QUERY_RECORD),
//            eq(LoadWeightRecord.class), eq(mockConnection));
        }
    }
}