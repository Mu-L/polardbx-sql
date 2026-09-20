package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.TransactionManager;
import org.junit.Test;

import java.sql.SQLException;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.COLUMNAR_RO_EXPLICIT_TRANSACTION;
import static com.alibaba.polardbx.common.type.TransactionType.TSO_MPP;
import static org.junit.Assert.*;

public class ColumnarExplicitTransactionTest {

    private ColumnarExplicitTransaction trx =
        new ColumnarExplicitTransaction(new ExecutionContext(), new TransactionManager());

    @Test
    public void testAPI() throws SQLException {
        assertTrue(trx.snapshotSeqIsEmpty());

        trx.rollback();
        trx.commit();
        trx.tryClose();

        assertEquals(COLUMNAR_RO_EXPLICIT_TRANSACTION, trx.getTransactionClass());
        assertEquals(TSO_MPP, trx.getType());

        MockUtils.assertThrows(NotSupportException.class,
            "ERR-CODE: [PXC-4998][ERR_NOT_SUPPORT] Access DN in Columnar explicit transaction is allowed not support yet! ",
            () -> {
                trx.getConnection("", "", null, null, new ExecutionContext());
            });
        MockUtils.assertThrows(NotSupportException.class,
            "ERR-CODE: [PXC-4998][ERR_NOT_SUPPORT] Access DN in Columnar explicit transaction is allowed not support yet! ",
            trx::getConnectionHolder);
        MockUtils.assertThrows(NotSupportException.class,
            "ERR-CODE: [PXC-4998][ERR_NOT_SUPPORT] Access DN in Columnar explicit transaction is allowed not support yet! ",
            trx::kill);
        MockUtils.assertThrows(NotSupportException.class,
            "ERR-CODE: [PXC-4998][ERR_NOT_SUPPORT] Access DN in Columnar explicit transaction is allowed not support yet! ",
            () -> {
                trx.tryClose(null, "");
            });
        MockUtils.assertThrows(TddlRuntimeException.class,
            "ERR-CODE: [PXC-1305][ERR_UNKNOWN_SAVEPOINT] SAVEPOINT  does not exist ", () -> {
                trx.savepoint("");
            });
        MockUtils.assertThrows(TddlRuntimeException.class,
            "ERR-CODE: [PXC-1305][ERR_UNKNOWN_SAVEPOINT] SAVEPOINT  does not exist ", () -> {
                trx.rollbackTo("");
            });
        MockUtils.assertThrows(TddlRuntimeException.class,
            "ERR-CODE: [PXC-1305][ERR_UNKNOWN_SAVEPOINT] SAVEPOINT  does not exist ", () -> {
                trx.release("");
            });

        trx.close();
    }
}