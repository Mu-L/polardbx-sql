/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.polardbx.net.packet;

import com.alibaba.polardbx.Capabilities;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

@RunWith(MockitoJUnitRunner.class)
public class AuthPacketTest {

    private static final long CLIENT_FLAGS = Capabilities.CLIENT_PROTOCOL_41
        | Capabilities.CLIENT_SECURE_CONNECTION
        | Capabilities.CLIENT_PLUGIN_AUTH
        | Capabilities.CLIENT_CONNECT_ATTRS;

    @InjectMocks
    private AuthPacket target;

    @Test
    public void testReadConnectionAttributes() {
        ByteArrayOutputStream attributes = new ByteArrayOutputStream();
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_NAME);
        writeLengthEncodedString(attributes, "MySQL Connector/J");
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_VERSION);
        writeLengthEncodedString(attributes, "8.0.33");

        target.read(createHandshakeResponse(withLength(attributes.toByteArray())));

        Assert.assertEquals("MySQL Connector/J",
            target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_NAME));
        Assert.assertEquals("8.0.33",
            target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_VERSION));
    }

    @Test
    public void testReadConnectorJHandshakeResponseWithoutDatabase() {
        ByteArrayOutputStream attributes = new ByteArrayOutputStream();
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_VERSION);
        writeLengthEncodedString(attributes, "5.1.40.12");
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_NAME);
        writeLengthEncodedString(attributes, "MySQL Connector Java");

        target.read(createHandshakeResponse(withLength(attributes.toByteArray()), true));

        Assert.assertEquals("MySQL Connector Java",
            target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_NAME));
        Assert.assertEquals("5.1.40.12",
            target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_VERSION));
    }

    @Test
    public void testMalformedConnectionAttributesFailClosed() {
        target.read(createHandshakeResponse(validClientAttributes()));
        Assert.assertNotNull(target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_NAME));

        byte[][] malformedAttributes = new byte[][] {
            new byte[0],
            new byte[] {5, 1},
            new byte[] {2, 5, 'x'},
            new byte[] {(byte) 251},
            new byte[] {(byte) 252, 1},
            new byte[] {(byte) 253, 1, 0},
            new byte[] {(byte) 254, 1, 0, 0, 0, 0, 0, 0},
            new byte[] {(byte) 255}
        };

        for (byte[] attributes : malformedAttributes) {
            target.read(createHandshakeResponse(attributes));
            Assert.assertNull(target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_NAME));
            Assert.assertNull(target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_VERSION));
        }
    }

    @Test
    public void testConnectionAttributesWithTwoByteLengthEncoding() {
        ByteArrayOutputStream attributes = new ByteArrayOutputStream();
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_NAME);
        writeLengthEncodedString(attributes, "MySQL Connector/J");
        writeLengthEncodedString(attributes, "_program_name");
        writeLengthEncodedString(attributes, repeat('x', 260));
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_VERSION);
        writeLengthEncodedString(attributes, "8.0.33");

        target.read(createHandshakeResponse(withLength(attributes.toByteArray())));

        Assert.assertEquals("MySQL Connector/J",
            target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_NAME));
        Assert.assertEquals("8.0.33",
            target.getConnectionAttribute(AuthPacket.ATTR_CLIENT_VERSION));
    }

    @Test
    public void testConnectionAttributesWithThreeAndEightByteLengthEncoding() {
        assertConnectionAttributeWithLengthMarker(253, 3);
        assertConnectionAttributeWithLengthMarker(254, 8);
    }

    private void assertConnectionAttributeWithLengthMarker(int marker, int byteCount) {
        ByteArrayOutputStream attributes = new ByteArrayOutputStream();
        writeLengthEncodedString(attributes, "name");
        writeLengthEncodedString(attributes, "value");

        ByteArrayOutputStream encodedAttributes = new ByteArrayOutputStream();
        encodedAttributes.write(marker);
        writeLittleEndian(encodedAttributes, attributes.size(), byteCount);
        encodedAttributes.write(attributes.toByteArray(), 0, attributes.size());

        target.read(createHandshakeResponse(encodedAttributes.toByteArray()));

        Assert.assertEquals("value", target.getConnectionAttribute("name"));
    }

    private static byte[] validClientAttributes() {
        ByteArrayOutputStream attributes = new ByteArrayOutputStream();
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_NAME);
        writeLengthEncodedString(attributes, "MySQL Connector Java");
        writeLengthEncodedString(attributes, AuthPacket.ATTR_CLIENT_VERSION);
        writeLengthEncodedString(attributes, "5.1.35");
        return withLength(attributes.toByteArray());
    }

    private static byte[] createHandshakeResponse(byte[] attributes) {
        return createHandshakeResponse(attributes, false);
    }

    private static byte[] createHandshakeResponse(byte[] attributes, boolean connectorJDatabasePlaceholder) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        writeLittleEndian(payload, CLIENT_FLAGS, 4);
        writeLittleEndian(payload, 0, 4);
        payload.write(45);
        payload.write(new byte[23], 0, 23);
        writeNullTerminatedString(payload, "test_user");
        payload.write(20);
        payload.write(new byte[20], 0, 20);
        if (connectorJDatabasePlaceholder) {
            payload.write(0);
        }
        writeNullTerminatedString(payload, "mysql_native_password");
        payload.write(attributes, 0, attributes.length);

        byte[] payloadBytes = payload.toByteArray();
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        writeLittleEndian(packet, payloadBytes.length, 3);
        packet.write(1);
        packet.write(payloadBytes, 0, payloadBytes.length);
        return packet.toByteArray();
    }

    private static byte[] withLength(byte[] value) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        writeLengthEncodedInteger(result, value.length);
        result.write(value, 0, value.length);
        return result.toByteArray();
    }

    private static void writeLengthEncodedString(ByteArrayOutputStream output, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeLengthEncodedInteger(output, bytes.length);
        output.write(bytes, 0, bytes.length);
    }

    private static void writeLengthEncodedInteger(ByteArrayOutputStream output, int value) {
        if (value < 251) {
            output.write(value);
        } else {
            output.write(252);
            writeLittleEndian(output, value, 2);
        }
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            result.append(value);
        }
        return result.toString();
    }

    private static void writeNullTerminatedString(ByteArrayOutputStream output, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.write(bytes, 0, bytes.length);
        output.write(0);
    }

    private static void writeLittleEndian(ByteArrayOutputStream output, long value, int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            output.write((byte) (value >>> (i * 8)));
        }
    }
}
