package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExternalNameNormalizerTest {

    @After
    public void teardown() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    private static SQLExpr expr(String text) {
        return SQLUtils.toMySqlExpr(text);
    }

    private static void registerCatalog(String name) {
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo(name, "mock", new HashMap<>(), null, null));
    }

    // ---- normalize(String) ----

    @Test
    public void testNormalizeStripsBacktick() {
        assertEquals("cat", ExternalNameNormalizer.normalize("`cat`"));
    }

    @Test
    public void testNormalizeStripsDoubleQuote() {
        assertEquals("cat", ExternalNameNormalizer.normalize("\"cat\""));
    }

    @Test
    public void testNormalizeKeepsPlainName() {
        assertEquals("cat", ExternalNameNormalizer.normalize("cat"));
    }

    @Test
    public void testNormalizeKeepsWildcard() {
        assertEquals("*", ExternalNameNormalizer.normalize("*"));
    }

    @Test
    public void testNormalizeNullSafe() {
        assertNull(ExternalNameNormalizer.normalize((String) null));
    }

    @Test
    public void testNormalizeUnescapesDoubledBacktick() {
        assertEquals("a`b", ExternalNameNormalizer.normalize("`a``b`"));
    }

    // ---- parseThreePartName ----

    @Test
    public void testParseThreePartQuoted() {
        ExternalNameNormalizer.ThreePartName name =
            ExternalNameNormalizer.parseThreePartName(expr("`cat`.`db`.`t`"));
        assertNotNull(name);
        assertEquals("cat", name.getCatalogName());
        assertEquals("db", name.getDbName());
        assertEquals("t", name.getTableName());
        assertFalse(name.isCatalogWildcard());
    }

    @Test
    public void testParseThreePartPreservesCase() {
        ExternalNameNormalizer.ThreePartName name =
            ExternalNameNormalizer.parseThreePartName(expr("`Cat`.`Db`.`T`"));
        assertNotNull(name);
        assertEquals("Cat", name.getCatalogName());
        assertEquals("Db", name.getDbName());
        assertEquals("T", name.getTableName());
    }

    @Test
    public void testParseThreePartUnquoted() {
        ExternalNameNormalizer.ThreePartName name =
            ExternalNameNormalizer.parseThreePartName(expr("cat.db.t"));
        assertNotNull(name);
        assertEquals("cat", name.getCatalogName());
        assertEquals("db", name.getDbName());
        assertEquals("t", name.getTableName());
    }

    @Test
    public void testParseTwoPartReturnsNull() {
        assertNull(ExternalNameNormalizer.parseThreePartName(expr("db.t")));
    }

    @Test
    public void testParseOnePartReturnsNull() {
        assertNull(ExternalNameNormalizer.parseThreePartName(expr("t")));
    }

    @Test
    public void testParseNullReturnsNull() {
        assertNull(ExternalNameNormalizer.parseThreePartName(null));
    }

    @Test
    public void testParseCatalogWildcard() {
        ExternalNameNormalizer.ThreePartName name =
            ExternalNameNormalizer.parseThreePartName(expr("*.*.t"));
        assertNotNull(name);
        assertTrue(name.isCatalogWildcard());
        assertEquals("*", name.getDbName());
        assertEquals("t", name.getTableName());
    }

    // ---- resolveExternalTable ----

    @Test
    public void testResolveQuotedRegisteredCatalog() {
        registerCatalog("hive");
        ExternalNameNormalizer.ThreePartName name =
            ExternalNameNormalizer.resolveExternalTable(expr("`hive`.`dwd`.`t1`"));
        assertNotNull("backtick-quoted catalog must resolve", name);
        assertEquals("hive", name.getCatalogName());
        assertEquals("dwd", name.getDbName());
        assertEquals("t1", name.getTableName());
    }

    @Test
    public void testResolveMixedCaseRegisteredCatalog() {
        registerCatalog("hive");
        assertNotNull(ExternalNameNormalizer.resolveExternalTable(expr("`HIVE`.`dwd`.`t1`")));
    }

    @Test
    public void testResolveUnregisteredCatalogReturnsNull() {
        registerCatalog("hive");
        assertNull(ExternalNameNormalizer.resolveExternalTable(expr("`other`.`dwd`.`t1`")));
    }

    @Test
    public void testResolveWildcardReturnsNull() {
        registerCatalog("hive");
        assertNull(ExternalNameNormalizer.resolveExternalTable(expr("*.*.t")));
    }

    @Test
    public void testResolveTwoPartReturnsNull() {
        registerCatalog("hive");
        assertNull(ExternalNameNormalizer.resolveExternalTable(expr("hive.t")));
    }
}
