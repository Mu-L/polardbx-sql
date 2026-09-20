package com.alibaba.polardbx.gms.privilege;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PolarPrivUtilIncrementalTest {

    private PolarDbPriv mockDbPriv() {
        PolarDbPriv dbPriv = mock(PolarDbPriv.class);
        when(dbPriv.getUserName()).thenReturn("user1");
        when(dbPriv.getHost()).thenReturn("%");
        when(dbPriv.getCatalogName()).thenReturn("cat1");
        when(dbPriv.getDbName()).thenReturn("db1");
        return dbPriv;
    }

    private PolarTbPriv mockTbPriv() {
        PolarTbPriv tbPriv = mock(PolarTbPriv.class);
        when(tbPriv.getUserName()).thenReturn("user1");
        when(tbPriv.getHost()).thenReturn("%");
        when(tbPriv.getCatalogName()).thenReturn("cat1");
        when(tbPriv.getDbName()).thenReturn("db1");
        when(tbPriv.getTbName()).thenReturn("tb1");
        return tbPriv;
    }

    @Test
    public void testGetCheckDbPrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getCheckDbPrivSql(mockDbPriv());
        assertTrue(sql.contains("catalog_name='cat1'"));
        assertTrue(sql.contains("db_name='db1'"));
    }

    @Test
    public void testGetInsertDbPrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getInsertDbPrivSql(mockDbPriv());
        assertTrue(sql.contains("catalog_name"));
        assertTrue(sql.contains("'cat1'"));
    }

    @Test
    public void testGetDeleteDbPrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getDeleteDbPrivSql(mockDbPriv());
        assertTrue(sql.contains("catalog_name='cat1'"));
    }

    @Test
    public void testGetUpdateDbPrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getUpdateDbPrivSql(mockDbPriv());
        assertTrue(sql.contains("catalog_name='cat1'"));
    }

    @Test
    public void testGetCheckTablePrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getCheckTablePrivSql(mockTbPriv());
        assertTrue(sql.contains("catalog_name='cat1'"));
        assertTrue(sql.contains("table_name='tb1'"));
    }

    @Test
    public void testGetInsertTablePrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getInsertTablePrivSql(mockTbPriv());
        assertTrue(sql.contains("catalog_name"));
        assertTrue(sql.contains("'cat1'"));
    }

    @Test
    public void testGetDeleteTablePrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getDeleteTablePrivSql(mockTbPriv());
        assertTrue(sql.contains("catalog_name='cat1'"));
    }

    @Test
    public void testGetUpdateTablePrivSqlContainsCatalogName() {
        String sql = PolarPrivUtil.getUpdateTablePrivSql(mockTbPriv());
        assertTrue(sql.contains("catalog_name='cat1'"));
    }

    @Test
    public void testCatalogNameConstant() {
        assertTrue("catalog_name".equals(PolarPrivUtil.CATALOG_NAME));
    }
}
