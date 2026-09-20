package com.alibaba.polardbx.common.constants;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static com.alibaba.polardbx.common.constants.ServerVariables.ENABLE_POLARX_SYNC_POINT;

public class ServerVariablesTest {
    @Test
    public void testMODIFIABLE_SYNC_POINT_PARAM() {
        Set<String> expected = ImmutableSet.of(
            ConnectionProperties.ENABLE_SYNC_POINT,
            ConnectionProperties.SYNC_POINT_TASK_INTERVAL);

        Set<String> actual = ServerVariables.MODIFIABLE_SYNC_POINT_PARAM;

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testVariables() {
        Assert.assertTrue(ServerVariables.variables.contains(ENABLE_POLARX_SYNC_POINT));
        Assert.assertTrue(ServerVariables.variables.contains("polarx_sync_point_timeout"));

        Assert.assertTrue(ServerVariables.mysqlGlobalVariables.contains(ENABLE_POLARX_SYNC_POINT));
        Assert.assertTrue(ServerVariables.mysqlGlobalVariables.contains("polarx_sync_point_timeout"));

        Assert.assertTrue(ServerVariables.mysqlDynamicVariables.contains(ENABLE_POLARX_SYNC_POINT));
        Assert.assertTrue(ServerVariables.mysqlDynamicVariables.contains("polarx_sync_point_timeout"));
    }

    @Test
    public void testVidxHnswEfSearchRegistration() {
        String var = "vidx_hnsw_ef_search";

        Assert.assertTrue("vidx_hnsw_ef_search should be in variables",
            ServerVariables.variables.contains(var));
        Assert.assertTrue("vidx_hnsw_ef_search should be in writableVariables",
            ServerVariables.writableVariables.contains(var));
        Assert.assertTrue("vidx_hnsw_ef_search should be in mysqlBothVariables",
            ServerVariables.mysqlBothVariables.contains(var));
        Assert.assertTrue("vidx_hnsw_ef_search should be in mysqlDynamicVariables",
            ServerVariables.mysqlDynamicVariables.contains(var));

        // Should NOT be in extraVariables (it must be forwarded to DN)
        Assert.assertFalse("vidx_hnsw_ef_search should NOT be in extraVariables",
            ServerVariables.extraVariables.contains(var));
        // Should NOT be in readonlyVariables (it must be writable)
        Assert.assertFalse("vidx_hnsw_ef_search should NOT be in readonlyVariables",
            ServerVariables.readonlyVariables.contains(var));
        // Should NOT be banned
        Assert.assertFalse("vidx_hnsw_ef_search should NOT be in bannedVariables",
            ServerVariables.bannedVariables.contains(var));
        Assert.assertFalse("vidx_hnsw_ef_search should NOT be in XbannedVariables",
            ServerVariables.XbannedVariables.contains(var));
    }

    @Test
    public void testEnableJavaUdfIsGlobalBanned() {
        Assert.assertTrue("ENABLE_JAVA_UDF should be globally banned",
            ServerVariables.isGlobalBanned("enable_java_udf"));
        Assert.assertTrue("ENABLE_JAVA_UDF should be globally banned (case insensitive)",
            ServerVariables.isGlobalBanned("ENABLE_JAVA_UDF"));
    }
}
