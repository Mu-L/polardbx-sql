package com.alibaba.polardbx.common.oss.blob;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.util.Arrays;

/**
 * Compression codecs used by Blob Page chunks.
 *
 * <p>The numeric codec identifiers are part of the on-disk format. Keep them stable.
 */
public final class BlobCompressionCodec {

    public static final int RAW = 0;
    public static final int ZSTD = 1;

    private static final ThreadLocal<ZstdCompressor> ZSTD_COMPRESSOR =
        ThreadLocal.withInitial(ZstdCompressor::new);
    private static final ThreadLocal<ZstdDecompressor> ZSTD_DECOMPRESSOR =
        ThreadLocal.withInitial(ZstdDecompressor::new);

    private BlobCompressionCodec() {
    }

    /**
     * Compress data with ZSTD when doing so produces a smaller representation.
     *
     * @param raw raw input, never {@code null}
     * @param minCompressionBytes minimum input length at which ZSTD is attempted
     */
    public static Encoded compress(byte[] raw, int minCompressionBytes) {
        if (raw == null) {
            throw new IllegalArgumentException("raw data is null");
        }
        if (minCompressionBytes < 0) {
            throw new IllegalArgumentException("minCompressionBytes is negative: " + minCompressionBytes);
        }
        if (raw.length == 0 || raw.length < minCompressionBytes) {
            return new Encoded(RAW, raw);
        }

        ZstdCompressor compressor = ZSTD_COMPRESSOR.get();
        int maxCompressedLength = compressor.maxCompressedLength(raw.length);
        byte[] output = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(raw, 0, raw.length, output, 0, output.length);
        if (compressedLength >= raw.length) {
            return new Encoded(RAW, raw);
        }
        return new Encoded(ZSTD, Arrays.copyOf(output, compressedLength));
    }

    /**
     * Decode one complete payload and require its declared raw length to match exactly.
     */
    public static byte[] decompress(int codec, byte[] stored, int expectedRawLength) {
        if (stored == null) {
            throw new IllegalArgumentException("stored data is null");
        }
        if (expectedRawLength < 0) {
            throw new IllegalArgumentException("expectedRawLength is negative: " + expectedRawLength);
        }
        switch (codec) {
        case RAW:
            if (stored.length != expectedRawLength) {
                throw new IllegalArgumentException("RAW length mismatch: expected=" + expectedRawLength
                    + ", actual=" + stored.length);
            }
            return stored;
        case ZSTD:
            byte[] raw = new byte[expectedRawLength];
            int actualRawLength;
            try {
                actualRawLength = ZSTD_DECOMPRESSOR.get().decompress(stored, 0, stored.length,
                    raw, 0, raw.length);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Invalid ZSTD payload", e);
            }
            if (actualRawLength != expectedRawLength) {
                throw new IllegalArgumentException("ZSTD length mismatch: expected=" + expectedRawLength
                    + ", actual=" + actualRawLength);
            }
            return raw;
        default:
            throw new IllegalArgumentException("Unknown blob compression codec: " + codec);
        }
    }

    public static boolean isSupported(int codec) {
        return codec == RAW || codec == ZSTD;
    }

    public static final class Encoded {
        private final int codec;
        private final byte[] data;

        private Encoded(int codec, byte[] data) {
            this.codec = codec;
            this.data = data;
        }

        public int getCodec() {
            return codec;
        }

        /**
         * Returns the encoded bytes. The caller must not modify the returned array.
         */
        public byte[] getData() {
            return data;
        }
    }
}
