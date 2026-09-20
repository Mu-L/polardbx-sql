package com.alibaba.polardbx.server;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StartupRuntimeConfigTest {

    @Test
    public void testJdk21IsDefault() throws Exception {
        Assert.assertEquals("21|", configure("", "", "pxc-hz123-example"));
    }

    @Test
    public void testExplicitJdkOverrideIsPreserved() throws Exception {
        Assert.assertEquals("11|", configure("11", "", "pxc-hz123-example"));
    }

    @Test
    public void testHhhtDoesNotEnableAutoLoomByDefault() throws Exception {
        Assert.assertEquals("21|", configure("", "", "pxc-ht123-example"));
    }

    @Test
    public void testApDoesNotEnableAutoLoomByDefault() throws Exception {
        Assert.assertEquals("21|", configure("", "", "pxc-sp123-example"));
    }

    @Test
    public void testControlPlaneCanEnableAutoLoom() throws Exception {
        Assert.assertEquals("21|wisp", configure("", "wisp", "pxc-ht123-example"));
    }

    @Test
    public void testControlPlaneCanDisableAutoLoom() throws Exception {
        Assert.assertEquals("21|disabled", configure("", "disabled", "pxc-ht123-example"));
    }

    private String configure(String jdkVersion, String wisp, String instanceId) throws Exception {
        Path startupScript = findStartupScript();
        String command = "function_definition=$(awk '/^function configure_jdk\\(\\)/,/^}/' \"$1\"); "
            + "test -n \"$function_definition\"; eval \"$function_definition\"; "
            + "jdk_ver=\"$2\"; wisp=\"$3\"; instanceId=\"$4\"; "
            + "configure_jdk >/dev/null; printf '%s|%s' \"$jdk_ver\" \"$wisp\"";
        Process process = new ProcessBuilder("bash", "-c", command, "bash", startupScript.toString(), jdkVersion,
            wisp, instanceId).redirectErrorStream(true).start();
        byte[] output = readAllBytes(process);
        int exitCode = process.waitFor();
        Assert.assertEquals("startup runtime config failed: " + new String(output, StandardCharsets.UTF_8), 0,
            exitCode);
        return new String(output, StandardCharsets.UTF_8);
    }

    private Path findStartupScript() {
        Path basedir = Paths.get(System.getProperty("basedir", "."));
        Path modulePath = basedir.resolve("src/main/bin/startup.sh");
        if (Files.exists(modulePath)) {
            return modulePath.toAbsolutePath();
        }
        return basedir.resolve("polardbx-server/src/main/bin/startup.sh").toAbsolutePath();
    }

    private byte[] readAllBytes(Process process) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int length;
        while ((length = process.getInputStream().read(buffer)) >= 0) {
            output.write(buffer, 0, length);
        }
        return output.toByteArray();
    }
}
