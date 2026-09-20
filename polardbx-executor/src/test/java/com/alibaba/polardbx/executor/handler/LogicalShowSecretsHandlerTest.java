package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import org.apache.calcite.sql.SqlShow;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalShowSecretsHandlerTest {

    @Test
    public void testHandle() {
        SecretManager mockSm = mock(SecretManager.class);
        SecretManager.SecretInfo info =
            new SecretManager.SecretInfo("secret1", "type1", new HashMap<>());

        try (MockedStatic<SecretManager> smMock = mockStatic(SecretManager.class)) {
            smMock.when(SecretManager::getInstance).thenReturn(mockSm);
            when(mockSm.list()).thenReturn(Collections.singletonList(info));

            LogicalShow plan = mock(LogicalShow.class);
            SqlShow showNode = mock(SqlShow.class);
            when(plan.getNativeSqlNode()).thenReturn(showNode);

            ExecutionContext ec = mock(ExecutionContext.class);
            IRepository repo = mock(IRepository.class);
            LogicalShowSecretsHandler handler = new LogicalShowSecretsHandler(repo);
            Cursor cursor = handler.handle(plan, ec);
            assertNotNull(cursor);
        }
    }
}
