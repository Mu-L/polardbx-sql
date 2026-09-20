package com.alibaba.polardbx.executor.external;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectorRuntimeManagerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testInitWithEmptyDirectory() throws IOException {
        File emptyDir = tempFolder.newFolder("connectors");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(emptyDir.toPath());
        manager.init();
        // Should not throw; no bundles to load
    }

    @Test
    public void testInitWithNonExistentDirectory() {
        Path nonExistent = Paths.get(tempFolder.getRoot().getAbsolutePath(), "does_not_exist");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(nonExistent);
        manager.init();
        // Should not throw; just logs info and returns
    }

    @Test
    public void testShutdownWithNoInit() {
        Path anyPath = Paths.get(tempFolder.getRoot().getAbsolutePath(), "connectors");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(anyPath);
        manager.shutdown();
        // Should not throw; no classloaders to close
    }

    @Test
    public void testShutdownAfterInit() throws IOException {
        File emptyDir = tempFolder.newFolder("connectors");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(emptyDir.toPath());
        manager.init();
        manager.shutdown();
        // Should not throw
    }

    @Test
    public void testInitWithNonJarFiles() throws IOException {
        File connectorsDir = tempFolder.newFolder("connectors");
        new File(connectorsDir, "readme.txt").createNewFile();
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(connectorsDir.toPath());
        manager.init();
        // Should not throw; non-jar files are ignored
    }

    @Test
    public void testReloadThenShutdownNoException() throws IOException {
        File emptyDir = tempFolder.newFolder("connectors_reload");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(emptyDir.toPath());
        manager.init();
        manager.reload();
        manager.shutdown();
        // Should not throw — reload closes old classloaders itself; shutdown closes the rest
    }

    @Test
    public void testDoubleReloadNoException() throws IOException {
        File emptyDir = tempFolder.newFolder("connectors_double_reload");
        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(emptyDir.toPath());
        manager.init();
        manager.reload();
        manager.reload();
        manager.shutdown();
    }

    @Test
    public void testReloadSkipsMalformedServiceJar() throws IOException {
        File connectorsDir = tempFolder.newFolder("connectors_bad_service");
        createMalformedServiceJar(new File(connectorsDir, "bad-connector-sources.jar"));

        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(connectorsDir.toPath());
        manager.init();
        manager.reload();
        manager.shutdown();
    }

    @Test
    public void testReloadClosesOldClassLoadersImmediately() throws Exception {
        File connectorsDir = tempFolder.newFolder("connectors_close_now");
        createMalformedServiceJar(new File(connectorsDir, "c1.jar"));

        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(connectorsDir.toPath());
        manager.init();

        List<URLClassLoader> oldLoaders = snapshotClassLoaders(manager);
        assertEquals(1, oldLoaders.size());
        String servicePath =
            "META-INF/services/com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor";
        try (InputStream in = oldLoaders.get(0).getResourceAsStream(servicePath)) {
            assertNotNull("resource must be readable before reload", in);
        }

        manager.reload();

        // Old loader must be closed by reload itself, not deferred to a later reload
        try (InputStream in = oldLoaders.get(0).getResourceAsStream(servicePath)) {
            assertNull("old classloader must be closed right after reload", in);
        }

        // New generation must be functional
        List<URLClassLoader> newLoaders = snapshotClassLoaders(manager);
        assertEquals(1, newLoaders.size());
        try (InputStream in = newLoaders.get(0).getResourceAsStream(servicePath)) {
            assertNotNull("new classloader must serve resources", in);
        }

        manager.shutdown();
    }

    @Test
    public void testInitLoadsOnlyNewestVersionAndTimestampPerArtifact() throws Exception {
        File connectorsDir = tempFolder.newFolder("connectors_stale");
        // Same version: newest timestamp wins
        createMalformedServiceJar(new File(connectorsDir, "c-1.0.0-SNAPSHOT-20260101000000.jar"));
        createMalformedServiceJar(new File(connectorsDir, "c-1.0.0-SNAPSHOT-20260102000000.jar"));
        // Higher version wins even with an older timestamp
        createMalformedServiceJar(new File(connectorsDir, "c-1.0.1-SNAPSHOT-20250101000000.jar"));
        // Non-matching name is always loaded
        createMalformedServiceJar(new File(connectorsDir, "plain.jar"));

        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(connectorsDir.toPath());
        manager.init();

        List<URLClassLoader> loaders = snapshotClassLoaders(manager);
        List<String> urls = new ArrayList<>();
        for (URLClassLoader cl : loaders) {
            urls.add(cl.getURLs()[0].toString());
        }
        assertEquals(2, loaders.size());
        assertTrue(urls.stream().anyMatch(u -> u.endsWith("c-1.0.1-SNAPSHOT-20250101000000.jar")));
        assertTrue(urls.stream().anyMatch(u -> u.endsWith("plain.jar")));
        assertTrue(urls.stream().noneMatch(u -> u.endsWith("c-1.0.0-SNAPSHOT-20260101000000.jar")));
        assertTrue(urls.stream().noneMatch(u -> u.endsWith("c-1.0.0-SNAPSHOT-20260102000000.jar")));

        manager.shutdown();
    }

    @Test
    public void testInitReleaseBeatsNewerSnapshotOfSameVersion() throws Exception {
        File connectorsDir = tempFolder.newFolder("connectors_release_wins");
        createMalformedServiceJar(new File(connectorsDir, "d-1.0.0-20260101000000.jar"));
        createMalformedServiceJar(new File(connectorsDir, "d-1.0.0-SNAPSHOT-20260202000000.jar"));

        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(connectorsDir.toPath());
        manager.init();

        List<URLClassLoader> loaders = snapshotClassLoaders(manager);
        assertEquals(1, loaders.size());
        assertTrue(loaders.get(0).getURLs()[0].toString().endsWith("d-1.0.0-20260101000000.jar"));

        manager.shutdown();
    }

    @Test
    public void testInitToleratesVersionSegmentLargerThanLong() throws Exception {
        File connectorsDir = tempFolder.newFolder("connectors_huge_version");
        createMalformedServiceJar(new File(connectorsDir, "e-1.0.0-20260102000000.jar"));
        createMalformedServiceJar(new File(connectorsDir, "e-99999999999999999999999-20260101000000.jar"));

        ConnectorRuntimeManager manager = new ConnectorRuntimeManager(connectorsDir.toPath());
        manager.init();

        // Must not throw; the numerically larger version wins despite exceeding Long range
        List<URLClassLoader> loaders = snapshotClassLoaders(manager);
        assertEquals(1, loaders.size());
        assertTrue(loaders.get(0).getURLs()[0].toString()
            .endsWith("e-99999999999999999999999-20260101000000.jar"));

        manager.shutdown();
    }

    @SuppressWarnings("unchecked")
    private static List<URLClassLoader> snapshotClassLoaders(ConnectorRuntimeManager manager)
        throws Exception {
        Field f = ConnectorRuntimeManager.class.getDeclaredField("classLoaders");
        f.setAccessible(true);
        return new ArrayList<>((List<URLClassLoader>) f.get(manager));
    }

    private void createMalformedServiceJar(File jar) throws IOException {
        String servicePath = "META-INF/services/com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor";
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar.toPath()))) {
            out.putNextEntry(new JarEntry(servicePath));
            out.write("com.alibaba.polardbx.missing.BadConnector\n".getBytes(StandardCharsets.UTF_8));
            out.closeEntry();
        }
    }
}
