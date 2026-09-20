package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.handler.Privileges;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for UseHandler rejecting external catalog schemas with clear error message.
 */
public class UseHandlerExternalSchemaTest {

    @Test
    public void testUseExternalSchemaRejected() {
        FrontendConnection conn = mock(FrontendConnection.class);
        Privileges privileges = mock(Privileges.class);
        when(conn.getPrivileges()).thenReturn(privileges);
        when(privileges.schemaExists("jdbc_cat$$remote_db")).thenReturn(true);

        try (MockedStatic<ExternalNameValidator> mockedValidator =
            mockStatic(ExternalNameValidator.class)) {
            mockedValidator.when(() -> ExternalNameValidator.isExternalSchema("jdbc_cat$$remote_db"))
                .thenReturn(true);

            ByteString sql = ByteString.from("USE `jdbc_cat$$remote_db`");
            // offset = 4 (after "USE ")
            UseHandler.handle(sql, conn, 4, false);

            verify(conn).writeErrMessage(eq(ErrorCode.ERR_EXTERNAL_TABLE),
                contains("USE is not supported for external catalog schema"));
        }
    }

    @Test
    public void testUseNormalSchemaNotAffected() {
        FrontendConnection conn = mock(FrontendConnection.class);
        Privileges privileges = mock(Privileges.class);
        when(conn.getPrivileges()).thenReturn(privileges);
        when(privileges.schemaExists("internal_db")).thenReturn(true);

        try (MockedStatic<ExternalNameValidator> mockedValidator =
            mockStatic(ExternalNameValidator.class)) {
            mockedValidator.when(() -> ExternalNameValidator.isExternalSchema("internal_db"))
                .thenReturn(false);

            ByteString sql = ByteString.from("USE internal_db");
            UseHandler.handle(sql, conn, 4, false);

            // Should NOT get ERR_EXTERNAL_TABLE error
            verify(conn, never()).writeErrMessage(eq(ErrorCode.ERR_EXTERNAL_TABLE), any());
        }
    }

    @Test
    public void testUseWithoutCatalogRegistered() {
        // Schema contains $$ but no external catalog is registered → isExternalSchema returns false
        FrontendConnection conn = mock(FrontendConnection.class);
        Privileges privileges = mock(Privileges.class);
        when(conn.getPrivileges()).thenReturn(privileges);
        when(privileges.schemaExists("ab$$cd")).thenReturn(false);

        try (MockedStatic<ExternalNameValidator> mockedValidator =
            mockStatic(ExternalNameValidator.class)) {
            // ExternalCatalogManager is empty, so isExternalSchema returns false
            mockedValidator.when(() -> ExternalNameValidator.isExternalSchema("ab$$cd"))
                .thenReturn(false);

            ByteString sql = ByteString.from("USE `ab$$cd`");
            UseHandler.handle(sql, conn, 4, false);

            // Should get Unknown database error (schemaExists returned false)
            verify(conn).writeErrMessage(eq(ErrorCode.ER_BAD_DB_ERROR),
                contains("Unknown database"));
            verify(conn, never()).writeErrMessage(eq(ErrorCode.ERR_EXTERNAL_TABLE), any());
        }
    }
}
