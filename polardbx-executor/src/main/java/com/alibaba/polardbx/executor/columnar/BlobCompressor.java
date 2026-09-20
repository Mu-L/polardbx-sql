package com.alibaba.polardbx.executor.columnar;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

/**
 * ZSTD compression/decompression utility for externalized column blob data.
 *
 * <p>Thread-safe: uses ThreadLocal compressor/decompressor instances.
 */
public final class BlobCompressor {

    private static final ThreadLocal<ZstdCompressor> COMPRESSOR =
        ThreadLocal.withInitial(ZstdCompressor::new);

    private static final ThreadLocal<ZstdDecompressor> DECOMPRESSOR =
        ThreadLocal.withInitial(ZstdDecompressor::new);

    private BlobCompressor() {
    }

    /**
     * Try to compress the input data using ZSTD.
     *
     * <p>Returns uncompressed data (compressed=false) if:
     * <ul>
     *   <li>input.length < minSize (too small, not worth the CPU)</li>
     *   <li>compressed output >= input length (incompressible data fallback)</li>
     * </ul>
     *
     * @param input raw blob data
     * @param minSize minimum size to attempt compression
     * @return CompressResult with either compressed or original data
     */
    public static CompressResult tryCompress(byte[] input, int minSize) {
        if (input == null || input.length < minSize) {
            return new CompressResult(input, input == null ? 0 : input.length, false);
        }

        ZstdCompressor compressor = COMPRESSOR.get();
        int maxCompressedLength = compressor.maxCompressedLength(input.length);
        byte[] output = new byte[maxCompressedLength];
        int compressedSize = compressor.compress(input, 0, input.length, output, 0, output.length);

        if (compressedSize >= input.length) {
            // Incompressible fallback: compressed is not smaller, use original
            return new CompressResult(input, input.length, false);
        }

        // Copy to tight array
        byte[] compressed = new byte[compressedSize];
        System.arraycopy(output, 0, compressed, 0, compressedSize);
        return new CompressResult(compressed, input.length, true);
    }

    /**
     * Decompress ZSTD-compressed data.
     *
     * <p>The {@code uncompressedSize} parameter is {@code long} to match the BlobRef wire
     * format, but Java arrays are int-indexed. In practice, blob sizes are bounded by
     * the external-column logical value limit, so overflow is impossible
     * under normal operation. A precondition check guards against corruption.
     *
     * @param compressed the compressed bytes
     * @param uncompressedSize the original (uncompressed) size, used to allocate output buffer
     * @return decompressed bytes
     * @throws IllegalArgumentException if uncompressedSize exceeds Integer.MAX_VALUE
     * @throws RuntimeException if decompression fails
     */
    public static byte[] decompress(byte[] compressed, long uncompressedSize) {
        if (uncompressedSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "uncompressedSize exceeds Integer.MAX_VALUE: " + uncompressedSize);
        }
        ZstdDecompressor decompressor = DECOMPRESSOR.get();
        byte[] output = new byte[(int) uncompressedSize];
        int decompressedSize = decompressor.decompress(compressed, 0, compressed.length,
            output, 0, output.length);
        if (decompressedSize != (int) uncompressedSize) {
            throw new RuntimeException(
                "ZSTD decompression size mismatch: expected=" + uncompressedSize
                    + ", actual=" + decompressedSize);
        }
        return output;
    }

    /**
     * Result of a compression attempt.
     */
    public static final class CompressResult {
        /**
         * Output data: either compressed bytes or original bytes (if skipped/fallback).
         */
        public final byte[] data;
        /**
         * Original uncompressed size of the input.
         */
        public final int uncompressedSize;
        /**
         * Whether compression was actually applied (data is smaller than original).
         */
        public final boolean compressed;

        public CompressResult(byte[] data, int uncompressedSize, boolean compressed) {
            this.data = data;
            this.uncompressedSize = uncompressedSize;
            this.compressed = compressed;
        }
    }
}
