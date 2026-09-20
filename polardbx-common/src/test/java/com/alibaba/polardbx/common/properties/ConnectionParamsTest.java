package com.alibaba.polardbx.common.properties;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConnectionParamsTest {
    @Test
    public void testENABLE_XA_TSO() {
        // Assert that ENABLE_XA_TSO is an instance of BooleanConfigParam
        assertTrue("ENABLE_XA_TSO should be an instance of BooleanConfigParam",
            ConnectionParams.ENABLE_XA_TSO instanceof BooleanConfigParam);

        // Assert that the default value of ENABLE_XA_TSO is true
        assertTrue("ENABLE_XA_TSO default value should be true",
            Boolean.parseBoolean(ConnectionParams.ENABLE_XA_TSO.getDefault()));

        // Assert that ENABLE_XA_TSO is editable
        assertTrue("ENABLE_XA_TSO should be editable", ConnectionParams.ENABLE_XA_TSO.isMutable());
    }

    @Test
    public void testENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION() {
        assertTrue("ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION should be an instance of BooleanConfigParam",
            ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION instanceof BooleanConfigParam);

        assertTrue("ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION default value should be true",
            Boolean.parseBoolean(ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION.getDefault()));

        assertTrue("ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION should be editable",
            ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION.isMutable());
    }

    @Test
    public void testENABLE_AUTO_COMMIT_TSO() {
        assertTrue("ENABLE_AUTO_COMMIT_TSO should be an instance of BooleanConfigParam",
            ConnectionParams.ENABLE_AUTO_COMMIT_TSO instanceof BooleanConfigParam);

        assertTrue("ENABLE_AUTO_COMMIT_TSO default value should be true",
            Boolean.parseBoolean(ConnectionParams.ENABLE_AUTO_COMMIT_TSO.getDefault()));

        assertTrue("ENABLE_AUTO_COMMIT_TSO should be editable",
            ConnectionParams.ENABLE_AUTO_COMMIT_TSO.isMutable());
    }

    @Test
    public void testDmlPartitionLocalUkDupCheck() {
        assertFalse(Boolean.parseBoolean(ConnectionParams.DML_PARTITION_LOCAL_UK_DUP_CHECK.getDefault()));
        assertFalse(ConnectionParams.DML_PARTITION_LOCAL_UK_DUP_CHECK.isMutable());
        assertTrue(ConnectionProperties.DML_PARTITION_LOCAL_UK_DUP_CHECK.equals(
            ConnectionParams.DML_PARTITION_LOCAL_UK_DUP_CHECK.getName()));
    }

    @Test
    public void testDmlPartitionLocalPkDupCheck() {
        assertFalse(Boolean.parseBoolean(ConnectionParams.DML_PARTITION_LOCAL_PK_DUP_CHECK.getDefault()));
        assertFalse(ConnectionParams.DML_PARTITION_LOCAL_PK_DUP_CHECK.isMutable());
        assertTrue(ConnectionProperties.DML_PARTITION_LOCAL_PK_DUP_CHECK.equals(
            ConnectionParams.DML_PARTITION_LOCAL_PK_DUP_CHECK.getName()));
    }

    @Test
    public void testGSI_LOOKUP_OPTIMIZE_THRESHOLD() {
        // Assert that GSI_LOOKUP_OPTIMIZE_THRESHOLD is an instance of FloatConfigParam
        assertTrue("GSI_LOOKUP_OPTIMIZE_THRESHOLD should be an instance of FloatConfigParam",
            ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD instanceof FloatConfigParam);

        // Assert that the default value of GSI_LOOKUP_OPTIMIZE_THRESHOLD is 30.0f
        float defaultValue = Float.parseFloat(ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.getDefault());
        assertTrue("GSI_LOOKUP_OPTIMIZE_THRESHOLD default value should be 10.0f",
            Math.abs(defaultValue - 10.0f) < 0.001f);

        // Assert that GSI_LOOKUP_OPTIMIZE_THRESHOLD is editable
        assertTrue("GSI_LOOKUP_OPTIMIZE_THRESHOLD should be editable",
            ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.isMutable());

        // Assert that the parameter name is correct
        assertTrue("GSI_LOOKUP_OPTIMIZE_THRESHOLD parameter name should be correct",
            "GSI_LOOKUP_OPTIMIZE_THRESHOLD".equals(ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.getName()));

        // Test parameter validation by trying to validate valid values
        try {
            ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.validateValue("0.0");
            ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.validateValue("2.0");
            ConnectionParams.GSI_LOOKUP_OPTIMIZE_THRESHOLD.validateValue("1000.0");
        } catch (IllegalArgumentException e) {
            assertFalse("GSI_LOOKUP_OPTIMIZE_THRESHOLD should accept valid values", true);
        }
    }
}
