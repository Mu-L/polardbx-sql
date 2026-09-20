package com.alibaba.polardbx.log;

import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.audit.ConnectionInfo;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.biv.MockConnection;
import com.alibaba.polardbx.optimizer.biv.MockFastDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.AuditPrivilege;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class AuditLogTest {

    @Test
    public void testAuditLog() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            ServerConnection c = mock(ServerConnection.class);
            Map<String, Object> map = new HashMap<>();
            when(c.getConnectionVariables()).thenReturn(map);
            ExecutionContext ec = new ExecutionContext();
            when(c.getExecutionContext()).thenReturn(ec);

            MockFastDataSource mockFastDataSource = new MockFastDataSource();
            Connection connection = new MockConnection(mockFastDataSource);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(connection);

            ConnectionInfo info = new ConnectionInfo("testInstance", "user1", "127.0.0.1", 3306, "test", "test");
            when(c.getConnectionInfo()).thenReturn(info);

            AuditPrivilege.polarAudit(c, "testAudit", AuditAction.CREATE);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
