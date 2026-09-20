package com.alibaba.polardbx.executor.external;

import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.external.mock.MockConnectorRuntime;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.secret.SecretTypeRegistry;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectorRuntimeManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorRuntimeManager.class);

    private static volatile ConnectorRuntimeManager instance;

    // Matches jar names like connector-jdbc-1.0.0-SNAPSHOT-20260731091548.jar:
    // <artifact>-<numeric.version>[-SNAPSHOT]-<14-digit-timestamp>.jar
    private static final Pattern VERSIONED_JAR =
        Pattern.compile("^(.+?)-(\\d+(?:\\.\\d+)*)(-SNAPSHOT)?-(\\d{14})\\.jar$");

    private final Path connectorsDir;
    private final List<URLClassLoader> classLoaders = new ArrayList<>();

    public ConnectorRuntimeManager() {
        this.connectorsDir = Paths.get(System.getProperty("connectors.dir",
            Paths.get(System.getProperty("user.dir"), "connectors").toString()));
    }

    public ConnectorRuntimeManager(Path connectorsDir) {
        this.connectorsDir = connectorsDir;
    }

    public static ConnectorRuntimeManager getInstance() {
        return instance;
    }

    public static void setInstance(ConnectorRuntimeManager manager) {
        instance = manager;
    }

    /**
     * Initial load at CN startup. Deliberately shares the {@link #reload()} code
     * path: at startup the registries are empty and there are no old classloaders,
     * so the swap, evict and close steps are no-ops.
     */
    public synchronized void init() {
        reload();
    }

    /**
     * List plugin jars, keeping only the newest build per connector artifact.
     * Only jars matching {@code <artifact>-<version>[-SNAPSHOT]-<14-digit-timestamp>.jar}
     * participate: they are grouped by artifact name and within a group the
     * winner is chosen by version first (numeric segment compare, release >
     * SNAPSHOT of the same version), then by build timestamp. Jars not matching
     * the pattern are always loaded as-is. Overwriting a jar in place is
     * unreliable because the JVM's JarFile URL cache may serve stale content,
     * so builds produce uniquely named jars and stale siblings are skipped here.
     */
    private List<File> listPluginJars() {
        File dir = connectorsDir.toFile();
        if (!dir.exists() || !dir.isDirectory()) {
            return Collections.emptyList();
        }
        File[] jars = dir.listFiles((d, name) -> name.endsWith(".jar"));
        if (jars == null) {
            return Collections.emptyList();
        }
        // listFiles() order is OS-dependent; sort so that load order, dedup
        // decisions and warn logs are deterministic across machines
        Arrays.sort(jars, Comparator.comparing(File::getName));

        List<File> result = new ArrayList<>();
        Map<String, List<VersionedJar>> groups = new LinkedHashMap<>();
        for (File jar : jars) {
            Matcher m = VERSIONED_JAR.matcher(jar.getName());
            if (m.matches()) {
                groups.computeIfAbsent(m.group(1), k -> new ArrayList<>()).add(new VersionedJar(jar, m));
            } else {
                result.add(jar);
            }
        }
        for (List<VersionedJar> group : groups.values()) {
            VersionedJar winner = Collections.max(group);
            result.add(winner.file);
        }
        return result;
    }

    /**
     * A jar name matched by {@link #VERSIONED_JAR}, ordered by version first,
     * then release-over-SNAPSHOT, then build timestamp.
     */
    private static class VersionedJar implements Comparable<VersionedJar> {
        final File file;
        final String version;
        final boolean snapshot;
        final String timestamp;

        VersionedJar(File file, Matcher m) {
            this.file = file;
            this.version = m.group(2);
            this.snapshot = m.group(3) != null;
            this.timestamp = m.group(4);
        }

        @Override
        public int compareTo(VersionedJar o) {
            int cmp = compareVersion(version, o.version);
            if (cmp != 0) {
                return cmp;
            }
            if (snapshot != o.snapshot) {
                return snapshot ? -1 : 1;
            }
            return timestamp.compareTo(o.timestamp);
        }

        private int compareVersion(String v1, String v2) {
            String[] p1 = v1.split("\\.");
            String[] p2 = v2.split("\\.");
            int len = Math.max(p1.length, p2.length);
            for (int i = 0; i < len; i++) {
                int cmp = compareNumeric(i < p1.length ? p1[i] : "0", i < p2.length ? p2[i] : "0");
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        /**
         * Compare two digit-only strings numerically without parsing, so segments
         * longer than Long.MAX_VALUE cannot throw and abort connector loading.
         */
        private int compareNumeric(String n1, String n2) {
            String s1 = stripLeadingZeros(n1);
            String s2 = stripLeadingZeros(n2);
            if (s1.length() != s2.length()) {
                return s1.length() < s2.length() ? -1 : 1;
            }
            return s1.compareTo(s2);
        }

        private String stripLeadingZeros(String s) {
            int i = 0;
            while (i < s.length() - 1 && s.charAt(i) == '0') {
                i++;
            }
            return s.substring(i);
        }
    }

    /**
     * Build a fresh connector set from the built-in mock connector, the classpath
     * and the plugin jar directory. Each plugin jar gets its own classloader,
     * appended to {@code newClassLoaders} so the caller can close them whether the
     * reload commits or aborts.
     */
    private Map<String, ConnectorDescriptor> loadAll(List<URLClassLoader> newClassLoaders) {
        Map<String, ConnectorDescriptor> staging = new ConcurrentHashMap<>();

        MockConnectorRuntime mockRuntime = new MockConnectorRuntime();
        staging.put(mockRuntime.type().toLowerCase(), mockRuntime);

        try {
            for (ConnectorDescriptor factory : ServiceLoader.load(ConnectorDescriptor.class)) {
                putRuntime(staging, factory, "classpath");
            }
        } catch (Throwable e) {
            LOGGER.warn("Failed to load connectors from classpath: " + e.getMessage(), e);
        }

        for (File jar : listPluginJars()) {
            try {
                URLClassLoader classLoader = new URLClassLoader(
                    new URL[] {jar.toURI().toURL()},
                    getClass().getClassLoader()
                );
                newClassLoaders.add(classLoader);
                int before = staging.size();
                for (ConnectorDescriptor factory : ServiceLoader.load(ConnectorDescriptor.class, classLoader)) {
                    putRuntime(staging, factory, "jar: " + jar.getName());
                }
                if (staging.size() == before) {
                    LOGGER.warn("Connector jar loaded but no ConnectorRuntime found: " + jar.getName()
                        + ". Possible SPI interface incompatibility (check ConnectorDescriptor version match).");
                }
            } catch (Throwable e) {
                LOGGER.warn("Failed to load connector jar: " + jar.getName(), e);
            }
        }
        return staging;
    }

    /**
     * Collect secret type definitions from the staged connectors. Calls into
     * plugin code, so it runs before the registry swap where a throw is harmless.
     */
    private Map<String, PropertyDefinition> collectSecretDefinitions(
        Map<String, ConnectorDescriptor> staging) {
        Map<String, PropertyDefinition> defs = new ConcurrentHashMap<>();
        for (ConnectorDescriptor factory : staging.values()) {
            for (PropertyDefinition def : factory.secretDefinitions()) {
                defs.put(def.getType().toLowerCase(), def);
            }
        }
        return defs;
    }

    /**
     * Reload connector plugins: stage a fresh connector set behind new classloaders,
     * atomically swap the registries, evict external schemas built from the old
     * generation, then close the old classloaders. In-flight queries touching an
     * external catalog may fail during reload (accepted trade-off).
     *
     * <p>What is reclaimed, and what is not:
     * closing a {@link URLClassLoader} releases the jar file handle but does not
     * unload classes — that only happens once the loader itself becomes unreachable.
     * Per-catalog resources (clients, pools, connections) live in
     * {@link com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata}
     * instances and are released by the eviction step, and descriptors are required
     * to be STATELESS (see
     * {@link com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor}),
     * so a compliant plugin leaves nothing behind that keeps the old loader alive.
     *
     * <p>The residual leak is a plugin class that publishes itself into a JVM-global
     * registry owned by a parent classloader — the classic case being a bundled JDBC
     * driver whose static initializer calls {@code DriverManager.registerDriver}.
     * Such a reference pins the old loader, and therefore its metaspace, for the rest
     * of the process lifetime; no hook on this side can undo it. The cost is
     * proportional to the number of reloads, which is why it is tolerable only as
     * long as reload stays a low-frequency operation.
     *
     * <p>Consequently: adding a connector jar that no catalog uses yet is safe, since
     * there is no old generation to displace. Replacing a jar that is already in use
     * should go through a rolling CN restart, which both avoids failing in-flight
     * queries and actually reclaims the metaspace.
     */
    public synchronized void reload() {
        LOGGER.info("Loading connector plugins...");

        List<URLClassLoader> newClassLoaders = new ArrayList<>();
        Map<String, ConnectorDescriptor> staging;
        Map<String, PropertyDefinition> newSecretDefs;
        try {
            staging = loadAll(newClassLoaders);
            newSecretDefs = collectSecretDefinitions(staging);
        } catch (RuntimeException | Error t) {
            // Nothing has been published yet, so these loaders are unreachable
            closeAll(newClassLoaders, "new connector classloader after reload failure");
            throw t;
        }

        // Swap SecretTypeRegistry first so new type defs are visible before the
        // connectors that need them. Both calls only publish an already-built map.
        SecretTypeRegistry.getInstance().replaceAll(newSecretDefs);
        ConnectorRegistry.getInstance().replaceAll(staging);

        // Best-effort: the swap already succeeded, so a failure while closing old
        // metadata must not abort the reload — doing so would leave the registries
        // pointing at connectors whose classloader is closed below, with no way back.
        // Runs after the swap so a concurrent re-bootstrap picks the new generation.
        try {
            OptimizerContext.evictAllExternalSchemas();
        } catch (RuntimeException | Error t) {
            LOGGER.warn("Failed to evict external schemas during reload; "
                + "metadata from the previous generation may linger until next access", t);
        }

        closeAll(classLoaders, "old connector classloader");
        classLoaders.clear();
        classLoaders.addAll(newClassLoaders);

        LOGGER.info("Connector reload complete: " + staging.size() + " connectors loaded ("
            + newClassLoaders.size() + " jar(s)).");
    }

    private static void closeAll(List<URLClassLoader> loaders, String what) {
        for (URLClassLoader cl : loaders) {
            try {
                cl.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close " + what, e);
            }
        }
    }

    private void putRuntime(Map<String, ConnectorDescriptor> staging, ConnectorDescriptor factory, String source) {
        if (!(factory instanceof ConnectorRuntime)) {
            LOGGER.warn("Skipping connector '" + factory.type() + "' from " + source
                + ": runtime loading requires ConnectorRuntime");
            return;
        }
        staging.put(factory.type().toLowerCase(), factory);
        LOGGER.info("Registered connector from " + source + ": " + factory.type());
    }

    public synchronized void shutdown() {
        closeAll(classLoaders, "connector classloader");
        classLoaders.clear();
    }
}
