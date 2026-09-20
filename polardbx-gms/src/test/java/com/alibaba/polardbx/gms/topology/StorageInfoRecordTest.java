package com.alibaba.polardbx.gms.topology;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StorageInfoRecordTest {
    @Test
    public void test() {
        assertTrue(StorageInfoRecord.isXcluster(StorageInfoRecord.STORAGE_TYPE_XCLUSTER));
        assertTrue(StorageInfoRecord.isXcluster(StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER));
        assertTrue(StorageInfoRecord.isXcluster(StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER));
    }
}
