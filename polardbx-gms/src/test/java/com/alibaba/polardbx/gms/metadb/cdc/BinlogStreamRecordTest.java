package com.alibaba.polardbx.gms.metadb.cdc;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.mockito.Mockito.when;

public class BinlogStreamRecordTest {

    @Test
    public void testFill() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.getInt("status")).thenReturn(1);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnName(1)).thenReturn("status");
        BinlogStreamRecord binlogStreamRecord = new BinlogStreamRecord();
        binlogStreamRecord.fill(resultSet);
        Assert.assertEquals(1, binlogStreamRecord.getStatus());
    }
}
