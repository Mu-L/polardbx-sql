package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalExternalInsert;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalExternalCatalogDdl;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalSecretDdl;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommandHandlerFactoryMyImpExternalTest {

    private CommandHandlerFactoryMyImp newFactory(String... handlerFields) throws Exception {
        CommandHandlerFactoryMyImp factory = mock(CommandHandlerFactoryMyImp.class, Mockito.CALLS_REAL_METHODS);
        for (String fieldName : handlerFields) {
            Field field = CommandHandlerFactoryMyImp.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(factory, mock(field.getType()));
        }
        return factory;
    }

    private LogicalExternalCatalogDdl newCatalogDdl(DdlType ddlType) {
        LogicalExternalCatalogDdl ddl = mock(LogicalExternalCatalogDdl.class);
        when(ddl.getDdlType()).thenReturn(ddlType);
        return ddl;
    }

    private LogicalSecretDdl newSecretDdl(DdlType ddlType) {
        LogicalSecretDdl ddl = mock(LogicalSecretDdl.class);
        when(ddl.getDdlType()).thenReturn(ddlType);
        return ddl;
    }

    @Test
    public void testExternalCatalogDdlHandlers() throws Exception {
        CommandHandlerFactoryMyImp factory = newFactory(
            "LOGICAL_CREATE_EXTERNAL_CATALOG_HANDLER",
            "LOGICAL_DROP_EXTERNAL_CATALOG_HANDLER",
            "LOGICAL_ALTER_EXTERNAL_CATALOG_HANDLER");
        ExecutionContext ec = mock(ExecutionContext.class);
        factory.getCommandHandler(newCatalogDdl(DdlType.CREATE_EXTERNAL_CATALOG), ec);
        factory.getCommandHandler(newCatalogDdl(DdlType.DROP_EXTERNAL_CATALOG), ec);
        factory.getCommandHandler(newCatalogDdl(DdlType.ALTER_EXTERNAL_CATALOG), ec);
    }

    @Test
    public void testExternalCatalogDdlUnknownType() throws Exception {
        CommandHandlerFactoryMyImp factory = newFactory();
        try {
            factory.getCommandHandler(newCatalogDdl(DdlType.CREATE_TABLE), mock(ExecutionContext.class));
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testSecretDdlHandlers() throws Exception {
        CommandHandlerFactoryMyImp factory = newFactory(
            "LOGICAL_CREATE_SECRET_HANDLER",
            "LOGICAL_DROP_SECRET_HANDLER",
            "LOGICAL_ALTER_SECRET_HANDLER");
        ExecutionContext ec = mock(ExecutionContext.class);
        factory.getCommandHandler(newSecretDdl(DdlType.CREATE_SECRET), ec);
        factory.getCommandHandler(newSecretDdl(DdlType.DROP_SECRET), ec);
        factory.getCommandHandler(newSecretDdl(DdlType.ALTER_SECRET), ec);
        factory.getCommandHandler(newSecretDdl(DdlType.CREATE_TABLE), ec);
    }

    @Test
    public void testExternalInsertHandler() throws Exception {
        CommandHandlerFactoryMyImp factory = newFactory("LOGICAL_EXTERNAL_INSERT_HANDLER");
        factory.getCommandHandler(mock(LogicalExternalInsert.class), mock(ExecutionContext.class));
    }
}
