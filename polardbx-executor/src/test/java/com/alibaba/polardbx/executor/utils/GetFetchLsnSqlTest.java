package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GetFetchLsnSqlTest {

    @After
    public void tearDown() {
        StorageInfoManager.setSupportGetCidx(false);
    }

    @Test
    public void testGetFetchLsnSqlWhenSupportGetCidx() {
        StorageInfoManager.setSupportGetCidx(true);
        assertEquals("call dbms_consensus.get_cidx()", ExecUtils.getFetchLsnSql());
    }

    @Test
    public void testGetFetchLsnSqlWhenNotSupportGetCidx() {
        StorageInfoManager.setSupportGetCidx(false);
        assertEquals("SELECT LAST_APPLY_INDEX FROM information_schema.ALISQL_CLUSTER_LOCAL",
            ExecUtils.getFetchLsnSql());
    }
}
