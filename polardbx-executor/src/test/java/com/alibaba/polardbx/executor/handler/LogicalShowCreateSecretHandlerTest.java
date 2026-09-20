package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.secret.ExternalCredentialEncryptor;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateSecretStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import org.apache.calcite.sql.SqlShowCreateSecret;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogicalShowCreateSecretHandlerTest {

    @After
    public void cleanup() {
        SecretManager.getInstance().invalidateAll();
    }

    @Test
    public void testShowCreateSecretEscapesSqlStringLiterals() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("type", "mock");
        props.put("user", "owner's user");
        props.put("password", "secret");
        SecretManager.getInstance().register("sec_ret", "mock", ExternalCredentialEncryptor.encryptMap(props));

        LogicalShow logicalShow = Mockito.mock(LogicalShow.class);
        Mockito.when(logicalShow.getNativeSqlNode())
            .thenReturn(new SqlShowCreateSecret(SqlParserPos.ZERO, "sec_ret"));

        LogicalShowCreateSecretHandler handler = new LogicalShowCreateSecretHandler(Mockito.mock(IRepository.class));
        Cursor cursor = handler.handle(logicalShow, new ExecutionContext());
        String createStatement = cursor.next().getString(1);

        assertTrue(createStatement.contains("'user'='owner''s user'"));
        assertTrue(createStatement.contains("'password'='******'"));
    }

    @Test
    public void testShowCreateSecretEscapesBackslashes() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("type", "mock");
        props.put("url", "C:\\Users\\admin\\data");
        props.put("note", "path\\to\\file's name");
        SecretManager.getInstance().register("sec_ret2", "mock", ExternalCredentialEncryptor.encryptMap(props));

        LogicalShow logicalShow = Mockito.mock(LogicalShow.class);
        Mockito.when(logicalShow.getNativeSqlNode())
            .thenReturn(new SqlShowCreateSecret(SqlParserPos.ZERO, "sec_ret2"));

        LogicalShowCreateSecretHandler handler = new LogicalShowCreateSecretHandler(Mockito.mock(IRepository.class));
        Cursor cursor = handler.handle(logicalShow, new ExecutionContext());
        String createStatement = cursor.next().getString(1);

        assertTrue(createStatement.contains("'url'='C:\\\\Users\\\\admin\\\\data'"));
        assertTrue(createStatement.contains("'note'='path\\\\to\\\\file''s name'"));
    }

    @Test
    public void testShowCreateSecretOutputCanBeReparsed() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("type", "mock");
        props.put("url", "C:\\Users\\admin\\data");
        props.put("note", "path\\to\\owner's file");
        SecretManager.getInstance().register("sec_ret3", "mock", ExternalCredentialEncryptor.encryptMap(props));

        LogicalShow logicalShow = Mockito.mock(LogicalShow.class);
        Mockito.when(logicalShow.getNativeSqlNode())
            .thenReturn(new SqlShowCreateSecret(SqlParserPos.ZERO, "sec_ret3"));

        LogicalShowCreateSecretHandler handler = new LogicalShowCreateSecretHandler(Mockito.mock(IRepository.class));
        String createStatement = handler.handle(logicalShow, new ExecutionContext()).next().getString(1);

        MySqlCreateSecretStatement parsed = (MySqlCreateSecretStatement)
            new MySqlStatementParser(createStatement).parseStatement();
        assertEquals("sec_ret3", parsed.getName().getSimpleName());
        assertEquals("mock", parsed.getProperties().get("type"));
        assertEquals("C:\\Users\\admin\\data", parsed.getProperties().get("url"));
        assertEquals("path\\to\\owner's file", parsed.getProperties().get("note"));
    }
}
