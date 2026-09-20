package com.alibaba.polardbx.qatest.cdc.binlog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.Commands;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.net.packet.HandshakePacket;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AONE-85440700 regression test.
 * <p>
 * COM_BINLOG_DUMP carries binlog-pos as a 4-byte unsigned integer (uint32).
 * When a downstream replica requests a position beyond 2GB (>= 2^31), the
 * position must be parsed as an unsigned value and passed to the CDC daemon
 * untouched. Before the fix CN parsed it with a signed readInt() and the
 * daemon received a negative position (value - 2^32).
 * <p>
 * The test drives the real protocol path: raw MySQL handshake against CN ->
 * COM_BINLOG_DUMP with position 3357165078 (0xC81A0996, the real incident
 * position) -> CN parses the packet and calls the daemon /dumper/getTarget
 * endpoint, whose address comes from metadb binlog_node_info. The qatest
 * harness ships no CDC daemon deployment, so the daemon endpoint registered
 * in metadb is an HTTP stub carried by the test JVM. The stub is bound to the
 * wildcard address and registered with a non-loopback address of the test
 * runner host, therefore the case does not depend on CN and the test
 * sharing a loopback interface; it only requires CN to be able to reach the
 * test runner host over the network, the same reachability assumption the
 * harness already makes in the opposite direction (test -> CN JDBC). The stub
 * captures the "pos" value for assertion and is removed from metadb in
 * teardown.
 */
public class BinlogDumpUnsignedPositionTest extends BaseTestCase {

    private static final Log log = LogFactory.getLog(BinlogDumpUnsignedPositionTest.class);

    // Real incident position: 0xC81A0996, larger than Integer.MAX_VALUE
    private static final long EXPECTED_POSITION = 3357165078L;
    private static final String BINLOG_FILE_NAME = "binlog.1000004";
    private static final long SERVER_ID = 65535L;
    private static final String STUB_MARKER = "qatest-binlog-dump-stub";
    private static final long POS_NOT_CAPTURED = Long.MIN_VALUE;

    private HttpServer daemonStub;
    private String stubHost;
    private int stubPort;
    private final AtomicLong capturedPos = new AtomicLong(POS_NOT_CAPTURED);
    private boolean createdBinlogNodeInfoTable;

    @Before
    public void setUpBinlogDumpStub() throws Exception {
        stubHost = resolveCnVisibleAddress();
        daemonStub = HttpServer.create(new InetSocketAddress(0), 0);
        stubPort = daemonStub.getAddress().getPort();
        daemonStub.createContext("/dumper/getTarget", exchange -> {
            try {
                String body = new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8);
                JSONObject json = JSON.parseObject(body);
                capturedPos.set(Long.parseLong(json.getString("pos")));
                byte[] resp = ("{\"code\":200,\"msg\":\"success\",\"data\":\"" + stubHost + ":" + stubPort + "\"}")
                    .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            } finally {
                exchange.close();
            }
        });
        daemonStub.start();

        try (Connection metaConn = getMetaConnection()) {
            createdBinlogNodeInfoTable = ensureBinlogNodeInfoTable(metaConn);
            JdbcUtil.executeUpdate(metaConn,
                "INSERT INTO binlog_node_info (cluster_id, container_id, ip, daemon_port, available_ports,"
                    + " cluster_type, role) VALUES ('" + STUB_MARKER + "', '" + STUB_MARKER + "', '" + stubHost
                    + "', " + stubPort + ", '', 'BINLOG', 'M')");
        }
    }

    /**
     * The binlog_node_info table is normally created by the CDC daemon when
     * it registers itself. CN+DN-only test environments deploy no daemon, so
     * the table may not exist, and both this test's stub registration and
     * CN's daemon lookup would fail against it. Create it with the columns CN
     * reads (BinlogNodeInfoAccessor) when missing, and report whether it was
     * created so teardown can drop it again and leave the metadb as found.
     */
    private static boolean ensureBinlogNodeInfoTable(Connection metaConn) throws SQLException {
        boolean exists;
        try (PreparedStatement ps = metaConn.prepareStatement(
            "SELECT COUNT(*) FROM information_schema.tables"
                + " WHERE table_schema = DATABASE() AND table_name = 'binlog_node_info'");
            ResultSet rs = ps.executeQuery()) {
            exists = rs.next() && rs.getInt(1) > 0;
        }
        if (exists) {
            return false;
        }
        JdbcUtil.executeUpdate(metaConn,
            "CREATE TABLE binlog_node_info ("
                + "id bigint NOT NULL AUTO_INCREMENT, "
                + "gmt_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                + "gmt_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                + "cluster_id varchar(128) NOT NULL, "
                + "container_id varchar(128) DEFAULT NULL, "
                + "ip varchar(32) NOT NULL, "
                + "daemon_port int NOT NULL, "
                + "available_ports varchar(1024) DEFAULT NULL, "
                + "cluster_type varchar(32) DEFAULT NULL, "
                + "role varchar(8) DEFAULT NULL, "
                + "group_name varchar(128) DEFAULT NULL, "
                + "polarx_inst_id varchar(64) DEFAULT NULL, "
                + "PRIMARY KEY (id))");
        return true;
    }

    /**
     * Picks a non-loopback IPv4 of the test runner host that CN can use to
     * reach the embedded daemon stub. No address is hardcoded: the case must
     * also work when CN runs on a different host than the test runner. Lab
     * networks use routable ranges outside RFC1918, so any non-loopback,
     * non-link-local IPv4 on an up physical interface is accepted.
     */
    private static String resolveCnVisibleAddress() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            if (ni.isLoopback() || ni.isVirtual() || !ni.isUp()) {
                continue;
            }
            Enumeration<InetAddress> addresses = ni.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (addr instanceof Inet4Address && !addr.isLoopbackAddress() && !addr.isLinkLocalAddress()) {
                    return addr.getHostAddress();
                }
            }
        }
        throw new IllegalStateException(
            "no non-loopback IPv4 found on the test runner host, cannot register a CN-visible daemon stub");
    }

    @After
    public void tearDownBinlogDumpStub() {
        try (Connection metaConn = getMetaConnection()) {
            JdbcUtil.executeUpdate(metaConn,
                "DELETE FROM binlog_node_info WHERE cluster_id = '" + STUB_MARKER + "'");
            if (createdBinlogNodeInfoTable) {
                JdbcUtil.executeUpdate(metaConn, "DROP TABLE binlog_node_info");
            }
        } catch (Exception e) {
            log.warn("failed to clean binlog_node_info stub record", e);
        }
        if (daemonStub != null) {
            daemonStub.stop(0);
        }
    }

    @Test
    public void testBinlogDumpPositionParsedAsUnsigned() throws Exception {
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        String cnHost = connectionManager.getPolardbxAddress();
        int cnPort = Integer.parseInt(connectionManager.getPolardbxPort());
        String user = connectionManager.getPolardbxUser();
        String password = connectionManager.getPolardbxPassword();

        try (Socket socket = new Socket(cnHost, cnPort)) {
            socket.setSoTimeout(30000);
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // 1. read server handshake
            byte[] handshakeRaw = readPacket(in);
            Assert.assertTrue("expect handshake packet, got error: " + describeError(handshakeRaw),
                handshakeRaw.length > 4 && (handshakeRaw[4] & 0xff) != 0xff);
            HandshakePacket handshake = new HandshakePacket();
            handshake.read(handshakeRaw);
            byte[] seed = new byte[handshake.seed.length + handshake.restOfScrambleBuff.length];
            System.arraycopy(handshake.seed, 0, seed, 0, handshake.seed.length);
            System.arraycopy(handshake.restOfScrambleBuff, 0, seed, handshake.seed.length,
                handshake.restOfScrambleBuff.length);

            // 2. authenticate with mysql_native_password
            out.write(buildAuthPacket((byte) (handshake.packetId + 1), user, password, seed));
            out.flush();
            byte[] authResp = readPacket(in);
            Assert.assertTrue("authentication failed: " + describeError(authResp),
                authResp.length > 4 && (authResp[4] & 0xff) == 0x00);

            // 3. send COM_BINLOG_DUMP with a position beyond 2GB (uint32 range)
            out.write(buildBinlogDumpPacket((byte) 0, EXPECTED_POSITION, SERVER_ID, BINLOG_FILE_NAME));
            out.flush();
        }

        // 4. CN must forward the parsed position to the daemon /dumper/getTarget stub
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (capturedPos.get() == POS_NOT_CAPTURED && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        Assert.assertTrue("daemon stub never received /dumper/getTarget request from CN",
            capturedPos.get() != POS_NOT_CAPTURED);
        Assert.assertEquals(
            "COM_BINLOG_DUMP binlog-pos must be parsed as an unsigned 4-byte integer",
            EXPECTED_POSITION, capturedPos.get());
    }

    private byte[] buildAuthPacket(byte packetId, String user, String password, byte[] seed) throws Exception {
        long clientFlags = Capabilities.CLIENT_LONG_PASSWORD
            | Capabilities.CLIENT_PROTOCOL_41
            | Capabilities.CLIENT_SECURE_CONNECTION
            | Capabilities.CLIENT_PLUGIN_AUTH;
        byte[] scrambled = SecurityUtil.scramble411(password.getBytes(StandardCharsets.UTF_8), seed);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeUB4(body, clientFlags);
        writeUB4(body, 16 * 1024 * 1024);
        body.write(33);
        body.write(new byte[23]);
        body.write(user.getBytes(StandardCharsets.UTF_8));
        body.write(0);
        body.write(scrambled.length);
        body.write(scrambled);
        body.write("mysql_native_password".getBytes(StandardCharsets.UTF_8));
        body.write(0);
        return wrapPacket(packetId, body.toByteArray());
    }

    private byte[] buildBinlogDumpPacket(byte packetId, long position, long serverId, String fileName) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(Commands.COM_BINLOG_DUMP);
        writeUB4(body, position);
        writeUB2(body, 0);
        writeUB4(body, serverId);
        byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
        body.write(fileNameBytes, 0, fileNameBytes.length);
        return wrapPacket(packetId, body.toByteArray());
    }

    private static void writeUB4(ByteArrayOutputStream out, long value) {
        out.write((int) (value & 0xff));
        out.write((int) ((value >> 8) & 0xff));
        out.write((int) ((value >> 16) & 0xff));
        out.write((int) ((value >> 24) & 0xff));
    }

    private static void writeUB2(ByteArrayOutputStream out, int value) {
        out.write(value & 0xff);
        out.write((value >> 8) & 0xff);
    }

    private static byte[] wrapPacket(byte packetId, byte[] body) {
        byte[] packet = new byte[4 + body.length];
        packet[0] = (byte) (body.length & 0xff);
        packet[1] = (byte) ((body.length >> 8) & 0xff);
        packet[2] = (byte) ((body.length >> 16) & 0xff);
        packet[3] = packetId;
        System.arraycopy(body, 0, packet, 4, body.length);
        return packet;
    }

    /**
     * Reads one MySQL packet including the 4-byte header.
     */
    private static byte[] readPacket(InputStream in) throws IOException {
        byte[] header = readN(in, 4);
        int length = (header[0] & 0xff) | ((header[1] & 0xff) << 8) | ((header[2] & 0xff) << 16);
        byte[] payload = readN(in, length);
        byte[] packet = new byte[4 + length];
        System.arraycopy(header, 0, packet, 0, 4);
        System.arraycopy(payload, 0, packet, 4, length);
        return packet;
    }

    private static byte[] readN(InputStream in, int n) throws IOException {
        byte[] data = new byte[n];
        int offset = 0;
        while (offset < n) {
            int read = in.read(data, offset, n - offset);
            if (read < 0) {
                throw new IOException("unexpected end of stream, read " + offset + "/" + n + " bytes");
            }
            offset += read;
        }
        return data;
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int read;
        while ((read = in.read(buf)) >= 0) {
            out.write(buf, 0, read);
        }
        return out.toByteArray();
    }

    private static String describeError(byte[] packet) {
        if (packet.length > 4 && (packet[4] & 0xff) == 0xff) {
            int errCode = (packet[5] & 0xff) | ((packet[6] & 0xff) << 8);
            int messageStart = 9;
            if (packet.length > 7 && packet[7] == '#') {
                messageStart = 13;
            }
            String message = messageStart < packet.length
                ? new String(packet, messageStart, packet.length - messageStart, StandardCharsets.UTF_8)
                : "";
            return "ERR " + errCode + ": " + message;
        }
        return "unexpected packet of length " + packet.length;
    }
}
