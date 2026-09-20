package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaMetricHandler;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * AONE-85434536: information_schema.metric JDK 21 compatibility.
 * InformationSchemaMetricHandler.decodeReal() relies on jdk.nashorn.internal.runtime.QuotedStringTokenizer,
 * whose module jdk.scripting.nashorn was removed since JDK 15, causing NoClassDefFoundError at runtime.
 */
public class InformationSchemaMetricTest extends AutoReadBaseTestCase {

    private static final String HANDLER_CLASS_NAME =
        "com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaMetricHandler";

    /**
     * Load InformationSchemaMetricHandler with a class loader whose module/class universe has no
     * jdk.nashorn classes (equivalent to a JDK 15+ / JDK 21 runtime), then invoke decodeReal()
     * on a real-format metric string. Decoding must succeed using only standard JDK APIs.
     */
    @Test
    public void testDecodeRealWithoutNashornModule() throws Throwable {
        NashornAbsentClassLoader loader = new NashornAbsentClassLoader(getClass().getClassLoader());
        Class<?> handlerClass = Class.forName(HANDLER_CLASS_NAME, true, loader);
        Assert.assertNotSame(
            "the handler must be defined by the nashorn-free loader for this simulation to be valid",
            InformationSchemaMetricHandler.class, handlerClass);

        Method decodeReal = handlerClass.getMethod("decodeReal", String.class);
        @SuppressWarnings("unchecked")
        Map<String, String> metrics;
        try {
            metrics = (Map<String, String>) decodeReal.invoke(null, "TABLE_NUM:5,PLAN_CACHE_SIZE:12,");
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }

        Assert.assertEquals("5", metrics.get("TABLE_NUM"));
        Assert.assertEquals("12", metrics.get("PLAN_CACHE_SIZE"));
        Assert.assertEquals(2, metrics.size());
    }

    /**
     * End-to-end check of the full information_schema.metric path:
     * SQL -> VirtualViewHandler -> InformationSchemaMetricHandler -> MetricSyncAllAction -> decodeReal().
     */
    @Test
    public void testSelectInformationSchemaMetric() throws SQLException {
        String sql = "select * from information_schema.metric";
        Set<String> realMetricKeys = new HashSet<>();
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (rs.next()) {
                String type = rs.getString("TYPE");
                String name = rs.getString("NAME");
                if ("real".equalsIgnoreCase(type) && name != null) {
                    realMetricKeys.add(name.toUpperCase());
                }
            }
        }
        Assert.assertTrue(
            "information_schema.metric should return real metric rows, got keys: " + realMetricKeys,
            realMetricKeys.contains("TABLE_NUM") && realMetricKeys.contains("PLAN_CACHE_SIZE"));
    }

    /**
     * Class loader emulating a JDK 15+ runtime: any jdk.nashorn.* class is absent,
     * and InformationSchemaMetricHandler itself is re-defined here so that its symbolic
     * references are resolved through this loader.
     */
    private static class NashornAbsentClassLoader extends ClassLoader {

        NashornAbsentClassLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    if (name.startsWith("jdk.nashorn.")) {
                        throw new ClassNotFoundException(
                            "jdk.scripting.nashorn has been removed since JDK 15: " + name);
                    }
                    if (HANDLER_CLASS_NAME.equals(name)) {
                        loaded = findClass(name);
                    } else {
                        loaded = getParent().loadClass(name);
                    }
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            String resource = name.replace('.', '/') + ".class";
            try (InputStream in = getParent().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new ClassNotFoundException(name);
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int n;
                while ((n = in.read(buffer)) != -1) {
                    out.write(buffer, 0, n);
                }
                byte[] bytes = out.toByteArray();
                return defineClass(name, bytes, 0, bytes.length);
            } catch (IOException e) {
                throw new ClassNotFoundException(name, e);
            }
        }
    }
}
