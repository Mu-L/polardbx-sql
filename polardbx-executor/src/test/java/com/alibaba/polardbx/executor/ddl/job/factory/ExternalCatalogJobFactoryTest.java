package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExternalCatalogJobFactoryTest {

    private Map<String, String> kv() {
        Map<String, String> map = new HashMap<>();
        map.put("username", "u");
        map.put("password", "p");
        return map;
    }

    @Test
    public void testCreateSecretJobFactory() {
        new CreateSecretJobFactory("S1", "mysql", kv()).create();
    }

    @Test
    public void testCreateSecretJobFactoryInvalidName() {
        try {
            new CreateSecretJobFactory("", "mysql", kv()).create();
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testAlterSecretJobFactory() {
        new AlterSecretJobFactory("S1", kv()).create();
    }

    @Test
    public void testAlterSecretJobFactoryInvalidName() {
        try {
            new AlterSecretJobFactory("", kv()).create();
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testDropSecretJobFactory() {
        new DropSecretJobFactory("S1", true).create();
        new DropSecretJobFactory("s1", false).create();
    }

    @Test
    public void testCreateExternalCatalogJobFactory() {
        new CreateExternalCatalogJobFactory("C1", "mock", kv(), "sec1", "comment").create();
        new CreateExternalCatalogJobFactory("c1", "mock", kv(), null, null).create();
        new CreateExternalCatalogJobFactory("c1", "mock", kv(), "", null).create();
    }

    @Test
    public void testCreateExternalCatalogJobFactoryInvalidName() {
        try {
            new CreateExternalCatalogJobFactory("", "mock", kv(), null, null).create();
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
        try {
            new CreateExternalCatalogJobFactory("*", "mock", kv(), null, null).create();
        } catch (TddlRuntimeException e) {
            // expected, just cover the branch
        }
    }

    @Test
    public void testAlterExternalCatalogJobFactory() {
        new AlterExternalCatalogJobFactory("C1", "sec1", "{\"k\":\"v\"}", "comment").create();
    }

    @Test
    public void testDropExternalCatalogJobFactory() {
        new DropExternalCatalogJobFactory("C1", true).create();
        new DropExternalCatalogJobFactory("c1", false).create();
    }
}
