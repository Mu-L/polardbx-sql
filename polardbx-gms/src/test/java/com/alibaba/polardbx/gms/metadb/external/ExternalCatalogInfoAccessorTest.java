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

public class ExternalCatalogInfoAccessorTest {

    @Test
    public void testSelectByNameFound() {
        Connection conn = mock(Connection.class);
        ExternalCatalogInfoRecord record = mock(ExternalCatalogInfoRecord.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.singletonList(record));

            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(conn);
            ExternalCatalogInfoRecord result = accessor.selectByName("MyCatalog");
            assertEquals(record, result);
        }
    }

    @Test
    public void testSelectByNameNotFound() {
        Connection conn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.emptyList());

            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(conn);
            assertNull(accessor.selectByName("nonexistent"));
        }
    }

    @Test
    public void testSelectAll() {
        Connection conn = mock(Connection.class);
        ExternalCatalogInfoRecord record = mock(ExternalCatalogInfoRecord.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.singletonList(record));

            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(conn);
            List<ExternalCatalogInfoRecord> results = accessor.selectAll();
            assertEquals(1, results.size());
        }
    }

    @Test
    public void testSelectBySecretName() {
        Connection conn = mock(Connection.class);
        ExternalCatalogInfoRecord record = mock(ExternalCatalogInfoRecord.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class)) {
            mdbMock.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                .thenReturn(Collections.singletonList(record));

            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(conn);
            List<ExternalCatalogInfoRecord> results = accessor.selectBySecretName("MySecret");
            assertEquals(1, results.size());
        }
    }

    @Test
    public void testInsert() {
        Connection conn = mock(Connection.class);
        byte[] props = new byte[] {1, 2, 3};

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<DdlMetaLogUtil> dmlMock = mockStatic(DdlMetaLogUtil.class)) {
            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(conn);
            accessor.insert("MyCatalog", "oss", props, "MySecret", "comment");
        }
    }

    @Test
    public void testDeleteByName() {
        Connection conn = mock(Connection.class);

        try (MockedStatic<MetaDbUtil> mdbMock = mockStatic(MetaDbUtil.class);
            MockedStatic<DdlMetaLogUtil> dmlMock = mockStatic(DdlMetaLogUtil.class)) {
            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor(conn);
            accessor.deleteByName("MyCatalog");
        }
    }

    @Test
    public void testSetConnection() {
        ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor();
        accessor.setConnection(mock(Connection.class));
    }
}
