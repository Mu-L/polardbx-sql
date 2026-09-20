package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class BinlogStreamAccessorTest {

    @Test
    public void testPurgeStream() throws SQLException {
        int expectedRows = 1;
        try (MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class);) {
            metaDbUtil.when(() -> MetaDbUtil.delete(anyString(), Mockito.anyMap(), Mockito.any(Connection.class)))
                .thenReturn(expectedRows);
            BinlogStreamAccessor binlogStreamAccessor = new BinlogStreamAccessor();
            binlogStreamAccessor.setConnection(mock(Connection.class));
            int result = binlogStreamAccessor.purgeStream("s1");
            assertEquals(expectedRows, result);
        }
    }

    @Test
    public void testPurgeStreamFailure() throws SQLException {
        try (MockedStatic<MetaDbUtil> metaDbUtil = Mockito.mockStatic(MetaDbUtil.class);) {
            metaDbUtil.when(() -> MetaDbUtil.delete(anyString(), Mockito.anyMap(), Mockito.any(Connection.class)))
                .thenThrow(new RuntimeException());
            try {
                BinlogStreamAccessor binlogStreamAccessor = new BinlogStreamAccessor();
                binlogStreamAccessor.setConnection(mock(Connection.class));
                int result = binlogStreamAccessor.purgeStream("s1");
                Assert.fail();
            } catch (Exception e) {
            }
        }
    }
}
