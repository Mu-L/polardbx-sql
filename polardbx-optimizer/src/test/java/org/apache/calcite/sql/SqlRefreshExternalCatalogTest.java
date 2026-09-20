package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SqlRefreshExternalCatalogTest {

    @Test
    public void testCatalogRefreshIsAdminDalKind() {
        SqlRefreshExternalCatalog node =
            new SqlRefreshExternalCatalog(SqlParserPos.ZERO, "cat1", null, null);

        assertThat(node, instanceOf(SqlDal.class));
        assertEquals(SqlKind.REFRESH_EXTERNAL_CATALOG, node.getKind());
        assertFalse(SqlKind.LOGICAL_SHOW_QUERY.contains(node.getKind()));
        assertTrue(SqlKind.OTHER_ADMIN_QUERY.contains(node.getKind()));
        assertTrue(SqlKind.DAL.contains(node.getKind()));
        assertFalse(SqlKind.DDL.contains(node.getKind()));
        assertFalse(SqlKind.DDL_SUPPORTED_BY_NEW_ENGINE.contains(node.getKind()));
        assertEquals("cat1", node.getCatalogName());
        assertNull(((SqlDal) node).getDbName());
        assertNull(((SqlDal) node).getTableName());
    }

    @Test
    public void testCatalogRefreshUnparse() {
        SqlRefreshExternalCatalog node =
            new SqlRefreshExternalCatalog(SqlParserPos.ZERO, "cat1", null, null);

        assertEquals("REFRESH EXTERNAL CATALOG cat1", node.toString());
    }

    @Test
    public void testTableRefreshUnparseKeepsTableForm() {
        SqlRefreshExternalCatalog node =
            new SqlRefreshExternalCatalog(SqlParserPos.ZERO, "cat1", "db1", "t1");

        assertEquals("REFRESH EXTERNAL TABLE cat1.db1.t1", node.toString());
        assertEquals("db1", node.getExternalDbName());
        assertEquals("t1", node.getExternalTableName());
        assertNull(((SqlDal) node).getDbName());
        assertNull(((SqlDal) node).getTableName());
    }

    @Test
    public void testOperatorTypeDerivationExposesResultColumn() {
        SqlRefreshExternalCatalog node =
            new SqlRefreshExternalCatalog(SqlParserPos.ZERO, "cat1", null, null);

        assertThat(node.getOperator().getName(), containsString("REFRESH"));
        assertEquals(SqlKind.REFRESH_EXTERNAL_CATALOG, node.getOperator().getKind());
    }
}
