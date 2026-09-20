package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalSecretAccessorTest {

    @Test
    public void testSelectByNameFound() {
        Connection conn = mock(Connection.class);
        ExternalSecretRecord record = mock(ExternalSecretRecord.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.singletonList(record));

            ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
            ExternalSecretRecord result = accessor.selectByName("MySecret");
            assertEquals(record, result);
        }
    }

    @Test
    public void testSelectByNameNotFound() {
        Connection conn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

            ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
            assertNull(accessor.selectByName("nonexistent"));
        }
    }

    @Test
    public void testSelectAll() {
        Connection conn = mock(Connection.class);
        ExternalSecretRecord record = mock(ExternalSecretRecord.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.singletonList(record));

            ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
            List<ExternalSecretRecord> results = accessor.selectAll();
            assertEquals(1, results.size());
        }
    }

    @Test
    public void testInsert() {
        Connection conn = mock(Connection.class);
        byte[] kv = new byte[] {1, 2, 3};

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<DdlMetaLogUtil> dmlMock = mockStatic(DdlMetaLogUtil.class)) {
            ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
            accessor.insert("MySecret", "type1", kv);
        }
    }

    @Test
    public void testUpdateEncryptedKv() {
        Connection conn = mock(Connection.class);
        byte[] kv = new byte[] {4, 5, 6};

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<DdlMetaLogUtil> dmlMock = mockStatic(DdlMetaLogUtil.class)) {
            ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
            accessor.updateEncryptedKv("MySecret", kv);
        }
    }

    @Test
    public void testDeleteByName() {
        Connection conn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<DdlMetaLogUtil> dmlMock = mockStatic(DdlMetaLogUtil.class)) {
            ExternalSecretAccessor accessor = new ExternalSecretAccessor(conn);
            accessor.deleteByName("MySecret");
        }
    }

    @Test
    public void testSetConnection() {
        ExternalSecretAccessor accessor = new ExternalSecretAccessor();
        Connection conn = mock(Connection.class);
        accessor.setConnection(conn);
        // connection is set; selectByName would use it
    }
}
