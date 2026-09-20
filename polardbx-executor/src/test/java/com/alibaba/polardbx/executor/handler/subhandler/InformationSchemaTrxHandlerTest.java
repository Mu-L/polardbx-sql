package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

public class InformationSchemaTrxHandlerTest {
    @Test
    public void calTime() {
        TrxLookupSet trxLookupSet = new TrxLookupSet();
        trxLookupSet.updateTransaction(100L, 100L, "sql", System.currentTimeMillis(), false);

        Timestamp timestamp = InformationSchemaInnodbTrxHandler.calTrxStartTime(trxLookupSet, 100L,
            new Timestamp(System.currentTimeMillis()));
        Assert.assertTrue(timestamp.getTime() > 0);

        // not exists
        timestamp = InformationSchemaInnodbTrxHandler.calTrxStartTime(trxLookupSet, 101L,
            new Timestamp(System.currentTimeMillis()));
        Assert.assertTrue(timestamp.getTime() > 0);

        // bad time
        trxLookupSet.updateTransaction(100L, 100L, "sql", -1L, false);
        timestamp = InformationSchemaInnodbTrxHandler.calTrxStartTime(trxLookupSet, 101L,
            new Timestamp(System.currentTimeMillis()));
        Assert.assertTrue(timestamp.getTime() > 0);
    }
}
