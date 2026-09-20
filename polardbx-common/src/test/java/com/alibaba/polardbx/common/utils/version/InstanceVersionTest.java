package com.alibaba.polardbx.common.utils.version;

import com.alibaba.polardbx.common.MergedStorageInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;

public class InstanceVersionTest {

    @After
    public void tearDown() {
        InstanceVersion.setSupportsVectorIndexes(false);
    }

    @Test
    public void testSupportsVectorIndexesCanBeUpdated() {
        InstanceVersion.setSupportsVectorIndexes(false);
        Assert.assertFalse(InstanceVersion.supportsVectorIndexes());

        InstanceVersion.setSupportsVectorIndexes(true);
        Assert.assertTrue(InstanceVersion.supportsVectorIndexes());
    }

    @Test
    public void testMergedStorageInfoExposesVectorIndexSupport() throws Exception {
        Constructor<?> constructor = MergedStorageInfo.class.getDeclaredConstructors()[0];
        Object[] arguments = new Object[constructor.getParameterCount()];
        Arrays.fill(arguments, false);
        arguments[arguments.length - 1] = true;

        MergedStorageInfo storageInfo = (MergedStorageInfo) constructor.newInstance(arguments);

        Assert.assertTrue(storageInfo.isSupportsVectorIndexes());
    }
}
