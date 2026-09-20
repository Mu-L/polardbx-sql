package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TDD tests verifying connector exceptions are properly wrapped with
 * TddlRuntimeException(ERR_EXTERNAL_TABLE) in the optimizer module.
 */
public class ConnectorExceptionWrappingTest {

    // ========================================================================
    // ExternalSchemaManager.getTable: connector exception → ERR_EXTERNAL_TABLE
    // ========================================================================

    @Test
    public void testGetTableWrapsConnectorRuntimeException() {
        // A ConnectorMetadata that throws RuntimeException on getTable
        ConnectorMetadata failingMetadata = new ConnectorMetadata() {
            @Override
            public List<String> listDatabases() {
                return Collections.emptyList();
            }

            @Override
            public List<String> listTables(String db) {
                return Collections.emptyList();
            }

            @Override
            public Optional<ConnectorTable> getTable(String db, String table) {
                throw new RuntimeException("JDBC connection timeout");
            }
        };

        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return "test_failing_runtime";
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> catalogProps, SecretBundle secret) {
                return failingMetadata;
            }
        });
        try {
            ExternalSchemaManager sm = new ExternalSchemaManager("testcat", "mydb", "test_failing_runtime",
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            try {
                sm.getTable("orders");
                fail("Should throw TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                    e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
                assertTrue("Should contain original message",
                    e.getMessage().contains("JDBC connection timeout"));
            }
        } finally {
            ConnectorRegistry.getInstance().unregister("test_failing_runtime");
        }
    }

    @Test
    public void testGetTablePassesThroughTddlRuntimeException() {
        // A ConnectorMetadata that throws TddlRuntimeException directly
        ConnectorMetadata failingMetadata = new ConnectorMetadata() {
            @Override
            public List<String> listDatabases() {
                return Collections.emptyList();
            }

            @Override
            public List<String> listTables(String db) {
                return Collections.emptyList();
            }

            @Override
            public Optional<ConnectorTable> getTable(String db, String table) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE,
                    "Secret expired");
            }
        };

        ConnectorRegistry.getInstance().register(new ConnectorDescriptor() {
            @Override
            public String type() {
                return "test_failing_tddl";
            }

            @Override
            public ConnectorMetadata createMetadata(Map<String, String> catalogProps, SecretBundle secret) {
                return failingMetadata;
            }
        });
        try {
            ExternalSchemaManager sm = new ExternalSchemaManager("testcat", "mydb", "test_failing_tddl",
                new HashMap<>(), SecretBundle.EMPTY, null, -1);

            try {
                sm.getTable("orders");
                fail("Should throw TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                assertTrue(e.getMessage().contains("Secret expired"));
            }
        } finally {
            ConnectorRegistry.getInstance().unregister("test_failing_tddl");
        }
    }

    // ========================================================================
    // TableSource.fromJson: deserialization error → ERR_EXTERNAL_TABLE
    // ========================================================================

    @Test
    public void testFromJsonInvalidClassWrapsAsERR_EXTERNAL_TABLE() {
        Map<String, Object> json = new HashMap<>();
        json.put("class", "com.nonexistent.FakeTableSource");

        try {
            TableSource.fromJson(json, null);
            fail("Should throw TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            assertTrue("Should use ERR_EXTERNAL_TABLE, got: " + e.getErrorCodeType(),
                e.getErrorCodeType() == ErrorCode.ERR_EXTERNAL_TABLE);
            assertTrue("Should mention the class name",
                e.getMessage().contains("FakeTableSource"));
        }
    }
}
