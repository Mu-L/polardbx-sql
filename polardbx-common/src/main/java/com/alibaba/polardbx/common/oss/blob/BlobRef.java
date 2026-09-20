package com.alibaba.polardbx.common.oss.blob;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Encoding for externalized-column Blob references.
 *
 * <p>V0 and V1 are legacy read-only hex formats. V2 addresses one logical value in an immutable Blob Page and has
 * the following binary layout:
 *
 * <pre>
 * ┌────────────┬────────────┬────────────────┬─────────────┬────────────────┐
 * │ version:u8 │ seqId:u32  │ slotAddr:u64   │ rawSize:u32 │ rawMD5:16B     │
 * └────────────┴────────────┴────────────────┴─────────────┴────────────────┘
 * Total: 33 bytes
 * </pre>
 *
 * <p>V2 is stored as exactly 66 characters of canonical lowercase hexadecimal text. The old
 * 90-character single-object V2 hex representation is intentionally not accepted.
 */
public final class BlobRef {

    public static final int VERSION_0 = 0;
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

    public static final int REF_LENGTH_V0 = 21;
    public static final int HEX_LENGTH_V0 = REF_LENGTH_V0 * 2;
    public static final int REF_LENGTH_V1 = 29;
    public static final int HEX_LENGTH_V1 = REF_LENGTH_V1 * 2;

    public static final int REF_LENGTH_V2 = 33;
    public static final int HEX_LENGTH_V2 = REF_LENGTH_V2 * 2;
    public static final int TEXT_LENGTH_V2 = HEX_LENGTH_V2;

    public static final int MD5_LENGTH = 16;

    public static final int VERSION_OFFSET_V2 = 0;
    public static final int VERSION_LENGTH_V2 = Byte.BYTES;
    public static final int SEQ_ID_OFFSET_V2 = VERSION_OFFSET_V2 + VERSION_LENGTH_V2;
    public static final int SEQ_ID_LENGTH_V2 = Integer.BYTES;
    public static final int SLOT_ADDR_OFFSET_V2 = SEQ_ID_OFFSET_V2 + SEQ_ID_LENGTH_V2;
    public static final int SLOT_ADDR_LENGTH_V2 = Long.BYTES;
    public static final int RAW_SIZE_OFFSET_V2 = SLOT_ADDR_OFFSET_V2 + SLOT_ADDR_LENGTH_V2;
    public static final int RAW_SIZE_LENGTH_V2 = Integer.BYTES;
    public static final int MD5_OFFSET_V2 = RAW_SIZE_OFFSET_V2 + RAW_SIZE_LENGTH_V2;
    /**
     * Wire-field maximum; current production readers enforce {@link BlobPageFormat#MAX_LOGICAL_VALUE_BYTES}.
     */
    public static final long MAX_RAW_SIZE_V2 = 0xFFFF_FFFFL;

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private BlobRef() {
    }

    // ==================== Encode ====================

    /**
     * V0 is a legacy read-only format.
     *
     * @deprecated new writes must use V2
     */
    @Deprecated
    public static String encodeHexV0(int seqId, long blobAddr, long size) {
        throw new UnsupportedOperationException("BlobRef V0 is read-only");
    }

    /**
     * V1 is a legacy read-only format.
     *
     * @deprecated new writes must use V2
     */
    @Deprecated
    public static String encodeHexV1(int seqId, long blobAddr, long compressedSize, long uncompressedSize) {
        throw new UnsupportedOperationException("BlobRef V1 is read-only");
    }

    /**
     * Encode one V2 logical-value reference in canonical lowercase hexadecimal text.
     */
    public static String encodeV2(int seqId, long slotAddr, long rawSize, byte[] rawMd5) {
        if (seqId < 0) {
            throw new IllegalArgumentException("seqId must be non-negative: " + seqId);
        }
        if (!BlobObjectId.isValidSlotAddr(slotAddr)) {
            throw new IllegalArgumentException(
                "Invalid Blob Page slot address: " + Long.toUnsignedString(slotAddr));
        }
        if (rawSize < 0 || rawSize > BlobPageFormat.MAX_LOGICAL_VALUE_BYTES) {
            throw new IllegalArgumentException("rawSize exceeds the supported V2 logical value limit: "
                + rawSize);
        }
        if (rawMd5 == null || rawMd5.length != MD5_LENGTH) {
            throw new IllegalArgumentException("rawMd5 must be 16 bytes");
        }

        byte[] raw = new byte[REF_LENGTH_V2];
        raw[VERSION_OFFSET_V2] = (byte) VERSION_2;
        putInt(raw, SEQ_ID_OFFSET_V2, seqId);
        putLong(raw, SLOT_ADDR_OFFSET_V2, slotAddr);
        putInt(raw, RAW_SIZE_OFFSET_V2, (int) rawSize);
        System.arraycopy(rawMd5, 0, raw, MD5_OFFSET_V2, MD5_LENGTH);
        return bytesToHex(raw);
    }

    // ==================== Decode ====================

    /**
     * Return whether the input is a canonical V2 hex reference or a valid legacy V0/V1 hex reference.
     */
    public static boolean isValid(String encoded) {
        try {
            decodeReference(encoded);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Return whether the input is a valid hexadecimal BlobRef.
     */
    public static boolean isValidHex(String encoded) {
        return isValid(encoded);
    }

    public static boolean isVersion2(String encoded) {
        return encoded != null && encoded.length() == TEXT_LENGTH_V2 && isValid(encoded);
    }

    public static int decodeVersion(String encoded) {
        return decodeReference(encoded)[0] & 0xFF;
    }

    public static int decodeSeqId(String encoded) {
        return getInt(decodeReference(encoded), 1);
    }

    /**
     * Decode the V2 full logical slot address.
     */
    public static long decodeSlotAddr(String encoded) {
        byte[] raw = decodeReference(encoded);
        if ((raw[0] & 0xFF) != VERSION_2) {
            throw new IllegalArgumentException("Slot address is only present in BlobRef V2");
        }
        return getLong(raw, 5);
    }

    /**
     * Decode the V0/V1 single-object address retained for legacy reads.
     */
    public static long decodeLegacyBlobAddr(String encoded) {
        byte[] raw = decodeReference(encoded);
        if ((raw[0] & 0xFF) == VERSION_2) {
            throw new IllegalArgumentException("Legacy object address is not present in BlobRef V2");
        }
        return getLong(raw, 5);
    }

    /**
     * For V0 this returns raw size; for V1 it returns compressed size. V2 has no per-value stored size.
     */
    public static long decodeStoredSize(String encoded) {
        byte[] raw = decodeReference(encoded);
        int version = raw[0] & 0xFF;
        if (version == VERSION_2) {
            throw new IllegalArgumentException("BlobRef V2 does not encode a per-value stored size");
        }
        return getLong(raw, 13);
    }

    /**
     * Decode the logical value's original byte length.
     */
    public static long decodeRawSize(String encoded) {
        byte[] raw = decodeReference(encoded);
        int version = raw[0] & 0xFF;
        if (version == VERSION_0) {
            return getLong(raw, 13);
        }
        if (version == VERSION_1) {
            return getLong(raw, 21);
        }
        return getUnsignedInt(raw, RAW_SIZE_OFFSET_V2);
    }

    public static byte[] decodeRawMd5(String encoded) {
        byte[] raw = decodeReference(encoded);
        if ((raw[0] & 0xFF) != VERSION_2) {
            throw new IllegalArgumentException("Raw MD5 is only present in BlobRef V2");
        }
        byte[] md5 = new byte[MD5_LENGTH];
        System.arraycopy(raw, MD5_OFFSET_V2, md5, 0, MD5_LENGTH);
        return md5;
    }

    public static String decodeRawMd5Hex(String encoded) {
        return bytesToHex(decodeRawMd5(encoded));
    }

    // ==================== MD5 utilities ====================

    public static byte[] md5(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            return digest.digest(data == null ? new byte[0] : data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm is not available", e);
        }
    }

    public static String md5Hex(byte[] data) {
        return bytesToHex(md5(data));
    }

    // ==================== Text codecs ====================

    private static byte[] decodeReference(String encoded) {
        if (encoded == null) {
            throw new IllegalArgumentException("BlobRef must not be null");
        }
        if (encoded.length() == TEXT_LENGTH_V2) {
            return decodeV2(encoded);
        }
        if (encoded.length() == HEX_LENGTH_V0) {
            return decodeLegacyHex(encoded, VERSION_0, REF_LENGTH_V0);
        }
        if (encoded.length() == HEX_LENGTH_V1) {
            return decodeLegacyHex(encoded, VERSION_1, REF_LENGTH_V1);
        }
        throw new IllegalArgumentException("Unsupported BlobRef text length: " + encoded.length());
    }

    private static byte[] decodeV2(String encoded) {
        byte[] raw = hexToBytes(encoded);
        if (raw.length != REF_LENGTH_V2 || (raw[0] & 0xFF) != VERSION_2) {
            throw new IllegalArgumentException("Invalid BlobRef V2 version or binary length");
        }
        if (!BlobObjectId.isValidSlotAddr(getLong(raw, 5))) {
            throw new IllegalArgumentException("Invalid BlobRef V2 slot address");
        }
        if (getInt(raw, 1) < 0) {
            throw new IllegalArgumentException("Invalid negative BlobRef V2 seqId");
        }
        if (getUnsignedInt(raw, RAW_SIZE_OFFSET_V2) > BlobPageFormat.MAX_LOGICAL_VALUE_BYTES) {
            throw new IllegalArgumentException("BlobRef V2 raw size exceeds the supported logical value limit");
        }
        if (!bytesToHex(raw).equals(encoded)) {
            throw new IllegalArgumentException("Non-canonical BlobRef V2 hexadecimal text");
        }
        return raw;
    }

    private static byte[] decodeLegacyHex(String encoded, int expectedVersion, int expectedLength) {
        byte[] raw = hexToBytes(encoded);
        if (raw.length != expectedLength || (raw[0] & 0xFF) != expectedVersion) {
            throw new IllegalArgumentException("BlobRef version does not match its legacy hex length");
        }
        return raw;
    }

    private static String bytesToHex(byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xFF;
            hex[i * 2] = HEX_CHARS[value >>> 4];
            hex[i * 2 + 1] = HEX_CHARS[value & 0x0F];
        }
        return new String(hex);
    }

    private static byte[] hexToBytes(String hex) {
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException("BlobRef hex length must be even");
        }
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("Invalid BlobRef hex character at offset " + i);
            }
            bytes[i / 2] = (byte) (high << 4 | low);
        }
        return bytes;
    }

    // ==================== Binary utilities ====================

    private static void putInt(byte[] buffer, int offset, int value) {
        buffer[offset] = (byte) (value >>> 24);
        buffer[offset + 1] = (byte) (value >>> 16);
        buffer[offset + 2] = (byte) (value >>> 8);
        buffer[offset + 3] = (byte) value;
    }

    private static int getInt(byte[] buffer, int offset) {
        return (buffer[offset] & 0xFF) << 24
            | (buffer[offset + 1] & 0xFF) << 16
            | (buffer[offset + 2] & 0xFF) << 8
            | buffer[offset + 3] & 0xFF;
    }

    private static long getUnsignedInt(byte[] buffer, int offset) {
        return getInt(buffer, offset) & 0xFFFF_FFFFL;
    }

    private static void putLong(byte[] buffer, int offset, long value) {
        buffer[offset] = (byte) (value >>> 56);
        buffer[offset + 1] = (byte) (value >>> 48);
        buffer[offset + 2] = (byte) (value >>> 40);
        buffer[offset + 3] = (byte) (value >>> 32);
        buffer[offset + 4] = (byte) (value >>> 24);
        buffer[offset + 5] = (byte) (value >>> 16);
        buffer[offset + 6] = (byte) (value >>> 8);
        buffer[offset + 7] = (byte) value;
    }

    private static long getLong(byte[] buffer, int offset) {
        return (long) (buffer[offset] & 0xFF) << 56
            | (long) (buffer[offset + 1] & 0xFF) << 48
            | (long) (buffer[offset + 2] & 0xFF) << 40
            | (long) (buffer[offset + 3] & 0xFF) << 32
            | (long) (buffer[offset + 4] & 0xFF) << 24
            | (long) (buffer[offset + 5] & 0xFF) << 16
            | (long) (buffer[offset + 6] & 0xFF) << 8
            | (long) (buffer[offset + 7] & 0xFF);
    }
}
