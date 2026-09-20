package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Test that XARecoverTask properly resets firstRecover flag
 * even when the node is not the leader.
 * <p>
 * Bug: When a non-leader node runs XARecoverTask, firstRecover stays true
 * because setFirstRecover(false) is called AFTER the hasLeadership check.
 * When the node later becomes leader via ALTER SYSTEM CHANGE LEADER,
 * the first recovery round skips the 2-round dangling detection.
 */
public class XARecoverTaskTest {

    @Test
    public void testFirstRecoverResetOnNonLeader() {
        String schema = "test_schema_red";

        TransactionExecutor executor = Mockito.mock(TransactionExecutor.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(executor.getAsyncQueue().getSchema()).thenReturn(schema);

        TransactionManager txMgr = Mockito.mock(TransactionManager.class);
        Mockito.when(txMgr.isFirstRecover()).thenReturn(true);

        try (MockedStatic<ExecUtils> execUtilsMock = Mockito.mockStatic(ExecUtils.class);
            MockedStatic<TransactionManager> txMgrMock = Mockito.mockStatic(TransactionManager.class)) {

            execUtilsMock.when(() -> ExecUtils.hasLeadership(schema)).thenReturn(false);
            txMgrMock.when(() -> TransactionManager.getInstance(schema)).thenReturn(txMgr);

            XARecoverTask task = new XARecoverTask(executor, false);
            task.run();

            Mockito.verify(txMgr, Mockito.atLeastOnce()).setFirstRecover(false);
        }
    }
}
