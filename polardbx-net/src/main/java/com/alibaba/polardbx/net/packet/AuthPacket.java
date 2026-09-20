/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.net.packet;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.Commands;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.util.BufferUtil;
import com.alibaba.polardbx.net.util.MySQLMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * From client to server during initial handshake.
 *
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 *
 * @author xianmao.hexm 2010-7-15 下午04:35:34
 */
public class AuthPacket extends MySQLPacket {

    private static final byte[] FILLER = new byte[23];

    public long clientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra;                // from FILLER(23)
    public String user;
    public byte[] password;
    public String database;
    public String authMethod;
    public boolean isSsl;

    public static final String ATTR_CLIENT_NAME = "_client_name";
    public static final String ATTR_CLIENT_VERSION = "_client_version";
    private static final long INVALID_LENGTH = -2L;
    private Map<String, String> connectionAttributes = Collections.emptyMap();

    public static boolean checkSsl(byte[] data) {
        if (data.length == QuitPacket.QUIT.length && data[4] == Commands.COM_QUIT) {
            return false;
        }

        MySQLMessage mm = new MySQLMessage(data);
        mm.move(4);
        long clientFlags = mm.readUB4();
        if ((clientFlags & Capabilities.CLIENT_SSL) != 0) {
            // client use ssl
            return true;
        }

        return false;
    }

    public void read(byte[] data) {
        connectionAttributes = Collections.emptyMap();

        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        clientFlags = mm.readUB4();
        maxPacketSize = mm.readUB4();
        charsetIndex = (mm.read() & 0xff);
        // read extra
        int current = mm.position();
        int len = (int) mm.readLength();
        if (len > 0 && len < FILLER.length) {
            byte[] ab = new byte[len];
            System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
            this.extra = ab;
        }
        mm.position(current + FILLER.length);
        user = mm.readStringWithNull();
        password = mm.readBytesWithLength();
        if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && mm.hasRemaining()) {
            database = mm.readStringWithNull();
        }
        if (((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) && mm.hasRemaining()) {
            authMethod = mm.readStringWithNull();
            if (authMethod == null
                && (clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) == 0
                && mm.hasRemaining()) {
                // Connector/J 5.1 writes an empty database placeholder even when the flag is absent.
                final int candidatePosition = mm.position();
                final String candidate = mm.readStringWithNull();
                if (isAuthPluginName(candidate)) {
                    authMethod = candidate;
                } else {
                    mm.position(candidatePosition);
                }
            }
        }
        if (((clientFlags & Capabilities.CLIENT_CONNECT_ATTRS) != 0) && mm.hasRemaining()) {
            readConnectionAttributes(mm);
        }
        if ((clientFlags & Capabilities.CLIENT_SSL) != 0) {
            // client use ssl
            isSsl = true;
        }
    }

    public String getConnectionAttribute(String name) {
        return connectionAttributes.get(name);
    }

    private static boolean isAuthPluginName(String value) {
        if (value == null || !value.endsWith("_password")) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            if (ch != '_' && ch != '-' && !Character.isLetterOrDigit(ch)) {
                return false;
            }
        }
        return true;
    }

    private void readConnectionAttributes(MySQLMessage mm) {
        final long attributesLength = readLengthEncodedInteger(mm, mm.length());
        if (attributesLength < 0 || attributesLength > mm.length() - mm.position()) {
            return;
        }

        final int attributesEnd = mm.position() + (int) attributesLength;
        final Map<String, String> attributes = new LinkedHashMap<>();

        while (mm.position() < attributesEnd) {
            final String name = readLengthEncodedString(mm, attributesEnd);
            final String value = readLengthEncodedString(mm, attributesEnd);

            if (name == null || value == null) {
                return;
            }
            attributes.put(name, value);
        }

        if (mm.position() == attributesEnd) {
            connectionAttributes = attributes;
        }
    }

    private static String readLengthEncodedString(MySQLMessage mm, int end) {
        final long length = readLengthEncodedInteger(mm, end);
        if (length < 0 || length > Integer.MAX_VALUE || length > end - mm.position()) {
            return null;
        }
        return new String(mm.readBytes((int) length), StandardCharsets.UTF_8);
    }

    private static long readLengthEncodedInteger(MySQLMessage mm, int end) {
        if (mm.position() >= end) {
            return INVALID_LENGTH;
        }

        final int first = mm.read() & 0xff;
        switch (first) {
        case 251:
            return MySQLMessage.NULL_LENGTH;
        case 252:
            if (end - mm.position() < 2) {
                return INVALID_LENGTH;
            }
            return mm.readUB2();
        case 253:
            if (end - mm.position() < 3) {
                return INVALID_LENGTH;
            }
            return mm.readUB3();
        case 254:
            if (end - mm.position() < 8) {
                return INVALID_LENGTH;
            }
            return mm.readLong();
        default:
            return first < 251 ? first : INVALID_LENGTH;
        }
    }

    public IPacketOutputProxy write(IPacketOutputProxy proxy) throws IOException {
        // ------------------------
        proxy.packetBegin();

        proxy.writeUB3(getPacketLength());
        proxy.write(packetId);

        proxy.writeUB4(clientFlags);
        proxy.writeUB4(maxPacketSize);
        proxy.write((byte) charsetIndex);
        proxy.write(FILLER);
        if (user == null) {
            proxy.write((byte) 0);
        } else {
            proxy.writeWithNull(user.getBytes());
        }
        if (password == null) {
            proxy.write((byte) 0);
        } else {
            proxy.writeWithLength(password);
        }
        if (database == null) {
            proxy.write((byte) 0);
        } else {
            proxy.writeWithNull(database.getBytes());
        }

        proxy.packetEnd();
        // ------------------------
        return proxy;
    }

    protected int getPacketLength() {
        int size = 32;// 4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtil.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String packetInfo() {
        return "MySQL Authentication Packet";
    }

}
