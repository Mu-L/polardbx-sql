package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExternalNameValidatorTest {

    @Before
    public void setup() {
        // isExternalSchema requires the first segment to be a registered catalog
        ExternalCatalogManager.getInstance().register(
            new ExternalCatalogInfo("_test_dummy", "mock", new HashMap<>(), null, null));
    }

    @After
    public void teardown() {
        ExternalCatalogManager.getInstance().invalidateAll();
    }

    @Test
    public void testValidCatalogName() {
        ExternalNameValidator.validateCatalogName("hive_warehouse");
        ExternalNameValidator.validateCatalogName("my-catalog");
        ExternalNameValidator.validateCatalogName("a");
    }

    @Test
    public void testCatalogNameMaxLength30() {
        String name30 = "abcdefghijklmnopqrstuvwxyz1234";
        assertEquals(30, name30.length());
        ExternalNameValidator.validateCatalogName(name30);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameEmpty() {
        ExternalNameValidator.validateCatalogName("");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameNull() {
        ExternalNameValidator.validateCatalogName(null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameContainsDollar() {
        ExternalNameValidator.validateCatalogName("my$catalog");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameContainsDoubleDollar() {
        ExternalNameValidator.validateCatalogName("my$$catalog");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameExceedsMaxLength() {
        ExternalNameValidator.validateCatalogName("abcdefghijklmnopqrstuvwxyz12345");
    }

    @Test
    public void testValidExternalDbName() {
        ExternalNameValidator.validateExternalDbName("dwd");
        ExternalNameValidator.validateExternalDbName("my_database");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalDbNameEmpty() {
        ExternalNameValidator.validateExternalDbName("");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalDbNameContainsDollar() {
        ExternalNameValidator.validateExternalDbName("db$name");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalDbNameExceedsMaxLength() {
        ExternalNameValidator.validateExternalDbName("abcdefghijklmnopqrstuvwxyz12345");
    }

    @Test
    public void testRejectDDLIfExternalSchemaWithDollarDollar() {
        try {
            ExternalNameValidator.rejectDDLIfExternalSchema("_test_dummy$$dwd");
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("DDL is not allowed"));
            assertTrue(e.getMessage().contains("_test_dummy$$dwd"));
        }
    }

    @Test
    public void testRejectDDLIfExternalSchemaUnregisteredPrefix() {
        // first segment not a registered catalog: treated as a normal schema
        ExternalNameValidator.rejectDDLIfExternalSchema("unknown$$dwd");
    }

    @Test
    public void testRejectDDLIfExternalSchemaNormal() {
        ExternalNameValidator.rejectDDLIfExternalSchema("normal_schema");
    }

    @Test
    public void testRejectDDLIfExternalSchemaNull() {
        ExternalNameValidator.rejectDDLIfExternalSchema(null);
    }

    @Test
    public void testRejectDDLIfExternalSchemaSingleDollar() {
        ExternalNameValidator.rejectDDLIfExternalSchema("my$db");
    }

    @Test
    public void testRejectSPMIfExternalSchemaWithDollarDollar() {
        try {
            ExternalNameValidator.rejectSPMIfExternalSchema("_test_dummy$$dwd");
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("SPM is not allowed"));
        }
    }

    @Test
    public void testRejectSPMIfExternalSchemaNormal() {
        ExternalNameValidator.rejectSPMIfExternalSchema("normal");
    }

    @Test
    public void testRejectSPMIfExternalSchemaNull() {
        ExternalNameValidator.rejectSPMIfExternalSchema(null);
    }

    @Test
    public void testRejectPossibleExternalCatalogWithDollarDollar() {
        try {
            ExternalNameValidator.rejectPossibleExternalCatalog("_test_dummy$$db");
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("cannot contain '$$'"));
        }
    }

    @Test
    public void testRejectPossibleExternalCatalogUnregisteredPrefix() {
        // no catalog named "my" registered: legacy $$ db names stay creatable
        ExternalNameValidator.rejectPossibleExternalCatalog("my$$db");
    }

    @Test
    public void testRejectPossibleExternalCatalogNormal() {
        ExternalNameValidator.rejectPossibleExternalCatalog("normal_db");
    }

    @Test
    public void testRejectPossibleExternalCatalogNull() {
        ExternalNameValidator.rejectPossibleExternalCatalog(null);
    }

    @Test
    public void testRejectPossibleExternalCatalogSingleDollar() {
        ExternalNameValidator.rejectPossibleExternalCatalog("my$db");
    }

    @Test
    public void testCheckSchemaConflictWithConflict() throws SQLException {
        Connection conn = Mockito.mock(Connection.class);
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);

        Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
        Mockito.when(ps.executeQuery()).thenReturn(rs);
        Mockito.when(rs.next()).thenReturn(true, false);
        Mockito.when(rs.getString("schema_name")).thenReturn("legacy$$db");

        try {
            ExternalNameValidator.checkLegacyPossibleExternalCatalog(conn);
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Cannot create external catalog"));
            assertTrue(e.getMessage().contains("legacy$$db"));
            assertTrue(e.getMessage().contains("uses the reserved separator"));
        }

        Mockito.verify(ps).close();
        Mockito.verify(rs).close();
    }

    @Test
    public void testCheckSchemaConflictWithoutConflict() throws SQLException {
        Connection conn = Mockito.mock(Connection.class);
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        ResultSet rs = Mockito.mock(ResultSet.class);

        Mockito.when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
        Mockito.when(ps.executeQuery()).thenReturn(rs);
        Mockito.when(rs.next()).thenReturn(false);

        ExternalNameValidator.checkLegacyPossibleExternalCatalog(conn);

        Mockito.verify(ps).close();
        Mockito.verify(rs).close();
    }

    @Test
    public void testCheckLegacyPossibleExternalCatalogWithSQLException() throws SQLException {
        Connection conn = Mockito.mock(Connection.class);
        SQLException sqlException = new SQLException("connection closed");
        Mockito.when(conn.prepareStatement(Mockito.anyString())).thenThrow(sqlException);

        try {
            ExternalNameValidator.checkLegacyPossibleExternalCatalog(conn);
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to query the system table"));
            assertTrue(e.getMessage().contains("connection closed"));
        }
    }

    // ---- H2: newline bypasses the old ".*[$'].*" + matches() check ----

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameDollarAfterNewline() {
        ExternalNameValidator.validateCatalogName("a\n$b");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameSingleQuoteAfterNewline() {
        ExternalNameValidator.validateCatalogName("a\n'b");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameCarriageReturn() {
        ExternalNameValidator.validateCatalogName("a\rb");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalDbNameDollarAfterNewline() {
        ExternalNameValidator.validateExternalDbName("d\n$b");
    }

    // ---- H1 backstop: quoted / dotted / spaced names must not reach MetaDB ----

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameWithBacktick() {
        ExternalNameValidator.validateCatalogName("`c1`");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameWithDoubleQuote() {
        ExternalNameValidator.validateCatalogName("\"c1\"");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameWithDot() {
        ExternalNameValidator.validateCatalogName("a.b");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testCatalogNameWithSpace() {
        ExternalNameValidator.validateCatalogName("a b");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalDbNameWithBacktick() {
        ExternalNameValidator.validateExternalDbName("`dwd`");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalDbNameWithDot() {
        ExternalNameValidator.validateExternalDbName("a.b");
    }

    // ---- secret name gets the same character rules ----

    @Test
    public void testValidSecretName() {
        ExternalNameValidator.validateSecretName("oss_prod");
        ExternalNameValidator.validateSecretName("my-secret-1");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testSecretNameWithBacktick() {
        ExternalNameValidator.validateSecretName("`s1`");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testSecretNameWithNewline() {
        ExternalNameValidator.validateSecretName("s\n1");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testSecretNameEmpty() {
        ExternalNameValidator.validateSecretName("");
    }

    // ---- external table name: characters only, no length bound ----

    @Test
    public void testValidExternalTableName() {
        ExternalNameValidator.validateExternalTableName("orders");
        ExternalNameValidator.validateExternalTableName("fact-orders_2024");
    }

    @Test
    public void testExternalTableNameAllowsMoreThan30Chars() {
        ExternalNameValidator.validateExternalTableName("abcdefghijklmnopqrstuvwxyz1234567890");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalTableNameWithSingleQuote() {
        ExternalNameValidator.validateExternalTableName("a'b");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalTableNameWithBacktick() {
        ExternalNameValidator.validateExternalTableName("`t1`");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalTableNameWithSpace() {
        ExternalNameValidator.validateExternalTableName("bad name");
    }

    @Test(expected = TddlRuntimeException.class)
    public void testExternalTableNameEmpty() {
        ExternalNameValidator.validateExternalTableName("");
    }

    // ---- pins the "do not echo the rejected name" decision ----

    @Test
    public void testIllegalCharErrorMessageDoesNotEchoRawName() {
        try {
            ExternalNameValidator.validateCatalogName("bad$name");
            fail("Should throw");
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Catalog name is invalid at position 3"));
            assertTrue(e.getMessage().contains("only letters, digits, '_' and '-' are allowed"));
            assertFalse("a rejected name may contain line breaks, so it must not be echoed",
                e.getMessage().contains("bad$name"));
        }
    }
}
