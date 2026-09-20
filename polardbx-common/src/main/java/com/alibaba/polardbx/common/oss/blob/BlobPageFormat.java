package com.alibaba.polardbx.common.oss.blob;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Deterministic binary format for a Page containing multiple external-column logical values.
 *
 * <pre>
 * FixedHeader[64] | SlotPresentBitmap[4096] | RawOffsetTable[32768 * 4]
 * | CompactChunkTable[2048 * 12] | ZeroReserved[4032] | payload
 * </pre>
 *
 * <p>The payload always begins at byte {@value #FIXED_PAYLOAD_OFFSET}, so a reader can fetch and parse the complete
 * header and fixed index with one range read, without a preliminary header read. All multi-byte integers use
 * big-endian byte order.
 * Physical offsets in the compact chunk table are absolute offsets from the beginning of the Page object. CRC fields
 * use CRC32C (Castagnoli).
 */
public final class BlobPageFormat {

    /**
     * ASCII "BLPG".
     */
    public static final int MAGIC = 0x424c5047;
    /**
     * Page format version is deliberately independent from the BlobRef protocol generation.
     */
    public static final int PAGE_FORMAT_VERSION = 1;
    public static final int FIXED_HEADER_LENGTH = 64;
    /**
     * One bit for every addressable slot. Bit {@code slotId & 7} is stored LSB-first in its byte.
     */
    public static final int SLOT_PRESENT_BITMAP_BYTES = 4096;
    /**
     * One unsigned raw start offset for every addressable slot, including absent slots.
     */
    public static final int RAW_OFFSET_TABLE_BYTES = 32768 * Integer.BYTES;
    /**
     * Persisted raw-offset width advertised in the fixed header.
     */
    public static final int SLOT_INDEX_ENTRY_SIZE = Integer.BYTES;
    /**
     * storedOffset:u32 | storedLength:u24 | codec:u8 | storedCrc32c:u32.
     */
    public static final int COMPACT_CHUNK_DESCRIPTOR_SIZE = 12;
    public static final int CHUNK_INDEX_ENTRY_SIZE = COMPACT_CHUNK_DESCRIPTOR_SIZE;
    public static final int COMPACT_CHUNK_TABLE_BYTES = 2048 * COMPACT_CHUNK_DESCRIPTOR_SIZE;
    public static final int RESERVED_INDEX_BYTES = 4032;
    public static final int FIXED_SLOT_INDEX_BYTES = SLOT_PRESENT_BITMAP_BYTES + RAW_OFFSET_TABLE_BYTES;
    /**
     * Includes the compact chunk table and the trailing zero-reserved area.
     */
    public static final int FIXED_CHUNK_INDEX_BYTES = COMPACT_CHUNK_TABLE_BYTES + RESERVED_INDEX_BYTES;
    /**
     * Fixed index bytes following the 64-byte header.
     */
    public static final int FIXED_INDEX_LENGTH = FIXED_SLOT_INDEX_BYTES + FIXED_CHUNK_INDEX_BYTES;
    public static final int FIXED_PAYLOAD_OFFSET = FIXED_HEADER_LENGTH + FIXED_INDEX_LENGTH;
    /**
     * Exact {@code [0, payloadOffset)} range used by the one-shot fixed-index reader.
     */
    public static final int FIXED_INDEX_REGION_BYTES = FIXED_PAYLOAD_OFFSET;
    public static final int MAX_ENTRIES = 32768;
    /**
     * Writer target for newly built Pages. A smaller raw chunk bounds read amplification because
     * Page reads fetch exact stored ranges for the chunks that overlap one logical value.
     */
    public static final int DEFAULT_TARGET_CHUNK_RAW_BYTES = 512 * 1024;
    /**
     * Reader-side structural safety cap for a chunk's declared raw length, not a fixed read size.
     * Actual I/O uses each chunk's metadata {@code storedLength}; a Page declaring a raw chunk larger
     * than this cap (for example 2 MiB) is rejected during metadata parsing before any payload read.
     * Keep this at or above {@link #DEFAULT_TARGET_CHUNK_RAW_BYTES} only for explicit format
     * compatibility after measuring the read-amplification impact.
     */
    public static final int MAX_SUPPORTED_CHUNK_RAW_BYTES = 512 * 1024;
    /**
     * Normal raw-payload sealing target for newly allocated Pages. A single logical value may exceed
     * this target and is placed alone in one Page, up to {@link #MAX_LOGICAL_VALUE_BYTES}.
     */
    public static final long DEFAULT_TARGET_PAGE_RAW_BYTES = 128L * 1024 * 1024;
    public static final int MAX_CHUNKS = 2048;
    public static final int MAX_LOGICAL_VALUE_BYTES = 256 * 1024 * 1024;
    /**
     * Maximum stored payload requested by one coalesced value read. GeneralCache 1.0.12 limits one batch load to
     * 2 MiB including the offset within the first 16 KiB cache block, so reserving one complete block keeps
     * {@code offsetInFirstBlock + storedRangeLength} strictly below that ceiling for every physical offset.
     */
    public static final int MAX_COALESCED_STORED_RANGE_BYTES = 2 * 1024 * 1024 - 16 * 1024;

    private static final int SUPPORTED_HEADER_FLAGS = 0;
    private static final long UINT32_MAX = 0xffff_ffffL;
    private static final int MAX_JAVA_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

    private static final int HEADER_MAGIC_OFFSET = 0;
    private static final int HEADER_VERSION_OFFSET = 4;
    private static final int HEADER_FLAGS_OFFSET = 5;
    private static final int HEADER_LENGTH_OFFSET = 6;
    private static final int HEADER_PAGE_ADDR_OFFSET = 8;
    private static final int HEADER_TABLE_ID_OFFSET = 16;
    private static final int HEADER_SEQ_ID_OFFSET = 24;
    private static final int HEADER_ENTRY_COUNT_OFFSET = 28;
    private static final int HEADER_CHUNK_COUNT_OFFSET = 30;
    private static final int HEADER_SLOT_ENTRY_SIZE_OFFSET = 32;
    private static final int HEADER_CHUNK_ENTRY_SIZE_OFFSET = 34;
    private static final int HEADER_SLOT_INDEX_BYTES_OFFSET = 36;
    private static final int HEADER_CHUNK_INDEX_BYTES_OFFSET = 40;
    private static final int HEADER_PAYLOAD_OFFSET = 44;
    private static final int HEADER_PAGE_LENGTH_OFFSET = 48;
    private static final int HEADER_RAW_PAYLOAD_LENGTH_OFFSET = 52;
    private static final int HEADER_METADATA_CRC_OFFSET = 56;
    private static final int HEADER_CRC_OFFSET = 60;

    private static final int SLOT_PRESENT_BITMAP_OFFSET = 0;
    private static final int RAW_OFFSET_TABLE_OFFSET = SLOT_PRESENT_BITMAP_OFFSET + SLOT_PRESENT_BITMAP_BYTES;
    private static final int COMPACT_CHUNK_TABLE_OFFSET = RAW_OFFSET_TABLE_OFFSET + RAW_OFFSET_TABLE_BYTES;
    private static final int RESERVED_INDEX_OFFSET = COMPACT_CHUNK_TABLE_OFFSET + COMPACT_CHUNK_TABLE_BYTES;

    private static final int CHUNK_STORED_OFFSET = 0;
    private static final int CHUNK_STORED_LENGTH_OFFSET = 4;
    private static final int CHUNK_CODEC_OFFSET = 7;
    private static final int CHUNK_CRC_OFFSET = 8;
    private static final int UINT24_MAX = 0x00ff_ffff;

    private static final int[] CRC32C_TABLE = buildCrc32cTable();

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private BlobPageFormat() {
    }

    /**
     * Emergency salvage-read mode (EXT_BLOB_PAGE_SALVAGE_READ): checksum mismatches are logged
     * instead of thrown so intact payload bytes can be rescued from partially corrupted Pages.
     * Only checksum comparisons consult this; structural and identity checks always throw.
     */
    private static boolean salvageChecksumMismatch(String detail) {
        if (DynamicConfig.getInstance().isExtBlobPageSalvageRead()) {
            LOGGER.warn("EXT_BLOB_PAGE_SALVAGE_READ ignoring " + detail);
            return true;
        }
        return false;
    }

    public static Builder builder(long pageObjectAddr, long tableId, int seqId) {
        return new Builder(pageObjectAddr, tableId, seqId);
    }

    /**
     * Compatibility API for callers that already hold the exact fixed-length header. New Page readers should use
     * {@link #parseFixedIndexRegion(byte[])} to parse the complete fixed prefix obtained by one range read.
     */
    public static Header parseHeader(byte[] headerBytes) {
        if (headerBytes == null) {
            throw new IllegalArgumentException("Page header is null");
        }
        if (headerBytes.length != FIXED_HEADER_LENGTH) {
            throw new IllegalArgumentException("Page header must be exactly " + FIXED_HEADER_LENGTH
                + " bytes, actual=" + headerBytes.length);
        }

        int magic = getInt(headerBytes, HEADER_MAGIC_OFFSET);
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid Page magic: 0x" + Integer.toHexString(magic));
        }
        int version = headerBytes[HEADER_VERSION_OFFSET] & 0xff;
        if (version != PAGE_FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported Page format version: " + version);
        }
        int flags = headerBytes[HEADER_FLAGS_OFFSET] & 0xff;
        if ((flags & ~SUPPORTED_HEADER_FLAGS) != 0) {
            throw new IllegalArgumentException("Unsupported Page header flags: " + flags);
        }
        int headerLength = getUnsignedShort(headerBytes, HEADER_LENGTH_OFFSET);
        if (headerLength != FIXED_HEADER_LENGTH) {
            throw new IllegalArgumentException("Invalid Page header length: " + headerLength);
        }

        int declaredHeaderCrc = getInt(headerBytes, HEADER_CRC_OFFSET);
        byte[] crcInput = headerBytes.clone();
        putInt(crcInput, HEADER_CRC_OFFSET, 0);
        int actualHeaderCrc = crc32c(crcInput, 0, crcInput.length);
        if (declaredHeaderCrc != actualHeaderCrc && !salvageChecksumMismatch("Page header CRC32C mismatch")) {
            throw new IllegalArgumentException("Page header CRC32C mismatch");
        }

        long pageObjectAddr = getLong(headerBytes, HEADER_PAGE_ADDR_OFFSET);
        if (!BlobObjectId.isPageObjectAddr(pageObjectAddr)) {
            throw new IllegalArgumentException(
                "Invalid Page object address: " + Long.toUnsignedString(pageObjectAddr));
        }
        long tableId = getLong(headerBytes, HEADER_TABLE_ID_OFFSET);
        int seqId = getInt(headerBytes, HEADER_SEQ_ID_OFFSET);
        if (seqId < 0) {
            throw new IllegalArgumentException("Negative Page seqId: " + seqId);
        }
        int entryCount = getUnsignedShort(headerBytes, HEADER_ENTRY_COUNT_OFFSET);
        int chunkCount = getUnsignedShort(headerBytes, HEADER_CHUNK_COUNT_OFFSET);
        if (entryCount == 0 || entryCount > MAX_ENTRIES) {
            throw new IllegalArgumentException("Invalid Page entry count: " + entryCount);
        }
        if (chunkCount > MAX_CHUNKS) {
            throw new IllegalArgumentException("Invalid Page chunk count: " + chunkCount);
        }

        int slotEntrySize = getUnsignedShort(headerBytes, HEADER_SLOT_ENTRY_SIZE_OFFSET);
        int chunkEntrySize = getUnsignedShort(headerBytes, HEADER_CHUNK_ENTRY_SIZE_OFFSET);
        if (slotEntrySize != SLOT_INDEX_ENTRY_SIZE) {
            throw new IllegalArgumentException("Invalid Page slot-index entry size: " + slotEntrySize);
        }
        if (chunkEntrySize != CHUNK_INDEX_ENTRY_SIZE) {
            throw new IllegalArgumentException("Invalid Page chunk-index entry size: " + chunkEntrySize);
        }

        long slotIndexBytes = getUnsignedInt(headerBytes, HEADER_SLOT_INDEX_BYTES_OFFSET);
        long chunkIndexBytes = getUnsignedInt(headerBytes, HEADER_CHUNK_INDEX_BYTES_OFFSET);
        if (slotIndexBytes != FIXED_SLOT_INDEX_BYTES) {
            throw new IllegalArgumentException("Page slot-index length mismatch: expected="
                + FIXED_SLOT_INDEX_BYTES + ", actual=" + slotIndexBytes);
        }
        if (chunkIndexBytes != FIXED_CHUNK_INDEX_BYTES) {
            throw new IllegalArgumentException("Page chunk-index length mismatch: expected="
                + FIXED_CHUNK_INDEX_BYTES + ", actual=" + chunkIndexBytes);
        }

        long payloadOffset = getUnsignedInt(headerBytes, HEADER_PAYLOAD_OFFSET);
        if (payloadOffset != FIXED_PAYLOAD_OFFSET) {
            throw new IllegalArgumentException("Page payload offset mismatch: expected="
                + FIXED_PAYLOAD_OFFSET + ", actual=" + payloadOffset);
        }
        long pageLength = getUnsignedInt(headerBytes, HEADER_PAGE_LENGTH_OFFSET);
        if (pageLength < payloadOffset) {
            throw new IllegalArgumentException("Page length precedes payload: pageLength=" + pageLength
                + ", payloadOffset=" + payloadOffset);
        }
        long rawPayloadLength = getUnsignedInt(headerBytes, HEADER_RAW_PAYLOAD_LENGTH_OFFSET);
        if (rawPayloadLength == 0 && chunkCount != 0) {
            throw new IllegalArgumentException("Empty Page raw payload has chunks: " + chunkCount);
        }
        if (rawPayloadLength != 0 && chunkCount == 0) {
            throw new IllegalArgumentException("Non-empty Page raw payload has no chunks");
        }
        long expectedChunkCount = rawPayloadLength == 0 ? 0
            : (rawPayloadLength + DEFAULT_TARGET_CHUNK_RAW_BYTES - 1L) / DEFAULT_TARGET_CHUNK_RAW_BYTES;
        if (chunkCount != expectedChunkCount) {
            throw new IllegalArgumentException("Page chunk count mismatch: expected="
                + expectedChunkCount + ", actual=" + chunkCount);
        }

        int metadataCrc32c = getInt(headerBytes, HEADER_METADATA_CRC_OFFSET);
        return new Header(pageObjectAddr, tableId, seqId, entryCount, chunkCount,
            slotIndexBytes, chunkIndexBytes, payloadOffset, pageLength, rawPayloadLength,
            metadataCrc32c, declaredHeaderCrc);
    }

    /**
     * Compatibility API for separately supplied header and fixed-index bytes. New Page readers should use
     * {@link #parseFixedIndexRegion(byte[])} and do not need to read the header first.
     */
    public static PageMetadata parseMetadata(Header header, byte[] metadataBytes) {
        if (header == null) {
            throw new IllegalArgumentException("Page header is null");
        }
        if (metadataBytes == null) {
            throw new IllegalArgumentException("Page metadata is null");
        }
        long declaredMetadataLength = header.slotIndexBytes + header.chunkIndexBytes;
        if (declaredMetadataLength != FIXED_INDEX_LENGTH || metadataBytes.length != FIXED_INDEX_LENGTH) {
            throw new IllegalArgumentException("Page metadata length mismatch: expected="
                + declaredMetadataLength + ", actual=" + metadataBytes.length);
        }
        int actualMetadataCrc = crc32c(metadataBytes, 0, metadataBytes.length);
        if (actualMetadataCrc != header.metadataCrc32c
            && !salvageChecksumMismatch("Page metadata CRC32C mismatch")) {
            throw new IllegalArgumentException("Page metadata CRC32C mismatch");
        }

        requireZeroRange(metadataBytes, RESERVED_INDEX_OFFSET, RESERVED_INDEX_BYTES,
            "Page reserved index area is not zero-filled");

        byte[] presenceBitmap = Arrays.copyOfRange(metadataBytes, SLOT_PRESENT_BITMAP_OFFSET,
            SLOT_PRESENT_BITMAP_OFFSET + SLOT_PRESENT_BITMAP_BYTES);
        int[] rawOffsets = new int[MAX_ENTRIES];
        long previousRawOffset = -1;
        int previousPresentSlotId = -1;
        int presentSlotCount = 0;
        for (int slotId = 0; slotId < MAX_ENTRIES; slotId++) {
            long rawOffset = getUnsignedInt(metadataBytes,
                RAW_OFFSET_TABLE_OFFSET + slotId * SLOT_INDEX_ENTRY_SIZE);
            if (!isSlotPresent(presenceBitmap, slotId)) {
                if (rawOffset != 0) {
                    throw new IllegalArgumentException("Absent Page slot has a non-zero raw offset: slot=" + slotId);
                }
                continue;
            }
            if (rawOffset < previousRawOffset || rawOffset > header.rawPayloadLength) {
                throw new IllegalArgumentException("Page slot raw offsets are invalid at slotId=" + slotId);
            }
            if (previousPresentSlotId >= 0) {
                validateSlotRawLength(previousPresentSlotId, rawOffset - previousRawOffset);
            } else if (rawOffset != 0) {
                throw new IllegalArgumentException("Page first present slot must begin at raw offset 0");
            }
            rawOffsets[slotId] = (int) rawOffset;
            previousRawOffset = rawOffset;
            previousPresentSlotId = slotId;
            presentSlotCount++;
        }
        if (presentSlotCount != header.entryCount) {
            throw new IllegalArgumentException("Page slot bitmap cardinality mismatch: expected="
                + header.entryCount + ", actual=" + presentSlotCount);
        }
        validateSlotRawLength(previousPresentSlotId, header.rawPayloadLength - previousRawOffset);

        List<ChunkIndexEntry> chunks = new ArrayList<>(header.chunkCount);
        long expectedStoredOffset = header.payloadOffset;
        for (int chunkId = 0; chunkId < MAX_CHUNKS; chunkId++) {
            int indexOffset = COMPACT_CHUNK_TABLE_OFFSET + chunkId * COMPACT_CHUNK_DESCRIPTOR_SIZE;
            if (chunkId >= header.chunkCount) {
                requireZeroRange(metadataBytes, indexOffset, COMPACT_CHUNK_DESCRIPTOR_SIZE,
                    "Unused Page chunk descriptor is not zero-filled at chunk=" + chunkId);
                continue;
            }

            long rawOffset = (long) chunkId * DEFAULT_TARGET_CHUNK_RAW_BYTES;
            int rawLength = (int) Math.min(DEFAULT_TARGET_CHUNK_RAW_BYTES,
                header.rawPayloadLength - rawOffset);
            long storedOffset = getUnsignedInt(metadataBytes, indexOffset + CHUNK_STORED_OFFSET);
            int storedLength = getUnsignedMedium(metadataBytes, indexOffset + CHUNK_STORED_LENGTH_OFFSET);
            int codec = metadataBytes[indexOffset + CHUNK_CODEC_OFFSET] & 0xff;
            int storedCrc32c = getInt(metadataBytes, indexOffset + CHUNK_CRC_OFFSET);

            if (rawLength <= 0 || rawLength > MAX_SUPPORTED_CHUNK_RAW_BYTES) {
                throw new IllegalArgumentException("Invalid Page chunk raw length: " + rawLength);
            }
            if (storedLength <= 0) {
                throw new IllegalArgumentException("Invalid Page chunk stored length: " + storedLength);
            }
            if (storedOffset != expectedStoredOffset) {
                throw new IllegalArgumentException("Page chunk stored ranges are not contiguous at chunk=" + chunkId);
            }
            if (!BlobCompressionCodec.isSupported(codec)) {
                throw new IllegalArgumentException("Unknown Page chunk codec: " + codec);
            }
            if (codec == BlobCompressionCodec.RAW && storedLength != rawLength) {
                throw new IllegalArgumentException("Page RAW chunk length mismatch at chunk=" + chunkId);
            }
            if (codec == BlobCompressionCodec.ZSTD && storedLength >= rawLength) {
                throw new IllegalArgumentException("Non-canonical Page ZSTD chunk lengths at chunk=" + chunkId);
            }

            expectedStoredOffset = checkedUnsignedIntAdd(storedOffset, storedLength, "chunk stored range");
            if (expectedStoredOffset > header.pageLength) {
                throw new IllegalArgumentException("Page chunk range exceeds declared bounds at chunk=" + chunkId);
            }
            chunks.add(new ChunkIndexEntry(rawOffset, rawLength, storedOffset, storedLength,
                codec, 0, storedCrc32c));
        }
        if (expectedStoredOffset != header.pageLength) {
            throw new IllegalArgumentException("Page chunks do not cover the stored payload: covered="
                + expectedStoredOffset + ", pageLength=" + header.pageLength);
        }
        return new PageMetadata(header, presenceBitmap, rawOffsets,
            chunks.toArray(new ChunkIndexEntry[0]));
    }

    /**
     * Parse the exact fixed {@code [0, payloadOffset)} prefix returned by a single range read.
     */
    public static PageMetadata parseFixedIndexRegion(byte[] fixedIndexRegion) {
        if (fixedIndexRegion == null || fixedIndexRegion.length != FIXED_INDEX_REGION_BYTES) {
            throw new IllegalArgumentException("Page fixed index region must be exactly "
                + FIXED_INDEX_REGION_BYTES + " bytes, actual="
                + (fixedIndexRegion == null ? -1 : fixedIndexRegion.length));
        }
        Header header = parseHeader(Arrays.copyOf(fixedIndexRegion, FIXED_HEADER_LENGTH));
        return parseMetadata(header,
            Arrays.copyOfRange(fixedIndexRegion, FIXED_HEADER_LENGTH, FIXED_INDEX_REGION_BYTES));
    }

    /**
     * Parse header and metadata from a complete in-memory Page. Chunk CRC and decompression are deferred until read.
     */
    public static PageMetadata parse(byte[] pageData) {
        if (pageData == null) {
            throw new IllegalArgumentException("Page data is null");
        }
        if (pageData.length < FIXED_INDEX_REGION_BYTES) {
            throw new IllegalArgumentException("Page data is truncated: " + pageData.length);
        }
        PageMetadata pageMetadata = parseFixedIndexRegion(
            Arrays.copyOf(pageData, FIXED_INDEX_REGION_BYTES));
        Header header = pageMetadata.header;
        if (header.pageLength != pageData.length) {
            throw new IllegalArgumentException("Page object length mismatch: expected=" + header.pageLength
                + ", actual=" + pageData.length);
        }
        return pageMetadata;
    }

    /**
     * Read one logical value using coalesced contiguous physical chunk ranges. The callback is invoked once per
     * planned range, never once per individual chunk, and no invocation exceeds
     * {@link #MAX_COALESCED_STORED_RANGE_BYTES}.
     */
    public static byte[] readValue(PageMetadata metadata, int slotId, StoredChunkReader reader) throws IOException {
        return readValue(metadata, slotId, reader, MAX_COALESCED_STORED_RANGE_BYTES);
    }

    /**
     * Read one logical value with an explicit coalescing ceiling up to
     * {@link #MAX_COALESCED_STORED_RANGE_BYTES}.
     */
    public static byte[] readValue(PageMetadata metadata, int slotId, StoredChunkReader reader,
                                   int maxStoredRangeBytes) throws IOException {
        if (metadata == null) {
            throw new IllegalArgumentException("Page metadata is null");
        }
        if (reader == null) {
            throw new IllegalArgumentException("Page chunk reader is null");
        }
        SlotIndexEntry slot = metadata.requireSlot(slotId);
        if (slot.rawLength == 0) {
            return new byte[0];
        }

        List<StoredRange> ranges = planValueStoredRanges(metadata, slotId, maxStoredRangeBytes);
        byte[] value = new byte[slot.rawLength];
        long valueStart = slot.rawOffset;
        long valueEnd = valueStart + slot.rawLength;
        int copied = 0;
        for (StoredRange range : ranges) {
            byte[] storedRange = reader.read(range.storedOffset, range.storedLength);
            if (storedRange == null || storedRange.length != range.storedLength) {
                throw new IllegalArgumentException("Short Page stored-range read: expected=" + range.storedLength
                    + ", actual=" + (storedRange == null ? -1 : storedRange.length));
            }
            for (ChunkIndexEntry chunk : range.chunks) {
                int chunkStoredOffset = (int) (chunk.storedOffset - range.storedOffset);
                if (chunkStoredOffset < 0
                    || chunkStoredOffset > storedRange.length - chunk.storedLength) {
                    throw new IllegalArgumentException("Page chunk lies outside its planned stored range");
                }
                if (crc32c(storedRange, chunkStoredOffset, chunk.storedLength) != chunk.storedCrc32c
                    && !salvageChecksumMismatch(
                    "Page chunk CRC32C mismatch at storedOffset=" + chunk.storedOffset)) {
                    throw new IllegalArgumentException("Page chunk CRC32C mismatch at storedOffset="
                        + chunk.storedOffset);
                }
                byte[] storedChunk = chunkStoredOffset == 0 && chunk.storedLength == storedRange.length
                    ? storedRange
                    : Arrays.copyOfRange(storedRange, chunkStoredOffset, chunkStoredOffset + chunk.storedLength);
                byte[] rawChunk = BlobCompressionCodec.decompress(chunk.codec, storedChunk, chunk.rawLength);
                long chunkEnd = chunk.rawOffset + chunk.rawLength;
                long overlapStart = Math.max(valueStart, chunk.rawOffset);
                long overlapEnd = Math.min(valueEnd, chunkEnd);
                if (overlapStart >= overlapEnd) {
                    throw new IllegalArgumentException("Page metadata returned a non-intersecting chunk");
                }
                int sourceOffset = (int) (overlapStart - chunk.rawOffset);
                int targetOffset = (int) (overlapStart - valueStart);
                int copyLength = (int) (overlapEnd - overlapStart);
                System.arraycopy(rawChunk, sourceOffset, value, targetOffset, copyLength);
                copied += copyLength;
            }
        }
        if (copied != slot.rawLength) {
            throw new IllegalArgumentException("Page chunks do not cover slot=" + slotId + ": expected="
                + slot.rawLength + ", copied=" + copied);
        }
        return value;
    }

    /**
     * Plan the contiguous stored ranges needed for one slot. Adjacent chunk payloads are merged until adding the next
     * chunk would exceed {@code maxStoredRangeBytes}. Empty slots return an empty immutable list.
     */
    public static List<StoredRange> planValueStoredRanges(PageMetadata metadata, int slotId) {
        return planValueStoredRanges(metadata, slotId, MAX_COALESCED_STORED_RANGE_BYTES);
    }

    /**
     * Plan the contiguous stored ranges needed for one slot with an explicit GeneralCache-safe payload ceiling.
     */
    public static List<StoredRange> planValueStoredRanges(PageMetadata metadata, int slotId,
                                                          int maxStoredRangeBytes) {
        if (metadata == null) {
            throw new IllegalArgumentException("Page metadata is null");
        }
        if (maxStoredRangeBytes <= 0 || maxStoredRangeBytes > MAX_COALESCED_STORED_RANGE_BYTES) {
            throw new IllegalArgumentException("maxStoredRangeBytes must be in [1, "
                + MAX_COALESCED_STORED_RANGE_BYTES + "]: " + maxStoredRangeBytes);
        }
        SlotIndexEntry slot = metadata.requireSlot(slotId);
        List<ChunkIndexEntry> chunks = metadata.chunksFor(slot);
        if (chunks.isEmpty()) {
            return Collections.emptyList();
        }

        List<StoredRange> ranges = new ArrayList<>();
        List<ChunkIndexEntry> currentChunks = new ArrayList<>();
        long rangeOffset = chunks.get(0).storedOffset;
        int rangeLength = 0;
        for (ChunkIndexEntry chunk : chunks) {
            if (chunk.storedLength > maxStoredRangeBytes) {
                throw new IllegalArgumentException("Page chunk exceeds the stored-range ceiling: "
                    + chunk.storedLength);
            }
            long expectedChunkOffset = rangeOffset + rangeLength;
            if (!currentChunks.isEmpty()
                && (chunk.storedOffset != expectedChunkOffset
                || rangeLength > maxStoredRangeBytes - chunk.storedLength)) {
                ranges.add(new StoredRange(rangeOffset, rangeLength, currentChunks));
                currentChunks = new ArrayList<>();
                rangeOffset = chunk.storedOffset;
                rangeLength = 0;
            }
            if (chunk.storedOffset != rangeOffset + rangeLength) {
                throw new IllegalArgumentException("Page chunks are not physically contiguous for slot=" + slotId);
            }
            currentChunks.add(chunk);
            rangeLength += chunk.storedLength;
        }
        ranges.add(new StoredRange(rangeOffset, rangeLength, currentChunks));
        return Collections.unmodifiableList(ranges);
    }

    /**
     * Compute the on-disk CRC32C value for a byte range.
     */
    public static int crc32c(byte[] data, int offset, int length) {
        if (data == null) {
            throw new IllegalArgumentException("CRC32C data is null");
        }
        if (offset < 0 || length < 0 || offset > data.length - length) {
            throw new IllegalArgumentException("Invalid CRC32C range: offset=" + offset + ", length=" + length
                + ", dataLength=" + data.length);
        }
        int crc = 0xffff_ffff;
        for (int i = offset; i < offset + length; i++) {
            crc = CRC32C_TABLE[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
        }
        return ~crc;
    }

    private static int[] buildCrc32cTable() {
        int[] table = new int[256];
        for (int i = 0; i < table.length; i++) {
            int value = i;
            for (int bit = 0; bit < 8; bit++) {
                value = (value >>> 1) ^ ((value & 1) == 0 ? 0 : 0x82f63b78);
            }
            table[i] = value;
        }
        return table;
    }

    private static long checkedUnsignedIntAdd(long left, long right, String field) {
        long result = left + right;
        if (left < 0 || right < 0 || result > UINT32_MAX) {
            throw new IllegalArgumentException("Page " + field + " overflows uint32");
        }
        return result;
    }

    private static void validateSlotRawLength(int slotId, long rawLength) {
        if (slotId < 0 || rawLength < 0 || rawLength > MAX_LOGICAL_VALUE_BYTES) {
            throw new IllegalArgumentException("Page slot exceeds the supported logical value limit: slot="
                + slotId + ", rawLength=" + rawLength);
        }
    }

    private static void checkUnsignedInt(long value, String field) {
        if (value < 0 || value > UINT32_MAX) {
            throw new IllegalArgumentException(field + " is outside uint32: " + value);
        }
    }

    private static void putUnsignedShort(byte[] target, int offset, int value) {
        if (value < 0 || value > 0xffff) {
            throw new IllegalArgumentException("Value is outside uint16: " + value);
        }
        target[offset] = (byte) (value >>> 8);
        target[offset + 1] = (byte) value;
    }

    private static int getUnsignedShort(byte[] source, int offset) {
        return ((source[offset] & 0xff) << 8) | (source[offset + 1] & 0xff);
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static int getInt(byte[] source, int offset) {
        return ((source[offset] & 0xff) << 24)
            | ((source[offset + 1] & 0xff) << 16)
            | ((source[offset + 2] & 0xff) << 8)
            | (source[offset + 3] & 0xff);
    }

    private static void putUnsignedInt(byte[] target, int offset, long value) {
        checkUnsignedInt(value, "value");
        putInt(target, offset, (int) value);
    }

    private static long getUnsignedInt(byte[] source, int offset) {
        return getInt(source, offset) & UINT32_MAX;
    }

    private static void putUnsignedMedium(byte[] target, int offset, int value) {
        if (value < 0 || value > UINT24_MAX) {
            throw new IllegalArgumentException("Value is outside uint24: " + value);
        }
        target[offset] = (byte) (value >>> 16);
        target[offset + 1] = (byte) (value >>> 8);
        target[offset + 2] = (byte) value;
    }

    private static int getUnsignedMedium(byte[] source, int offset) {
        return (source[offset] & 0xff) << 16
            | (source[offset + 1] & 0xff) << 8
            | source[offset + 2] & 0xff;
    }

    private static void setSlotPresent(byte[] fixedIndex, int slotId) {
        fixedIndex[SLOT_PRESENT_BITMAP_OFFSET + (slotId >>> 3)] |= (byte) (1 << (slotId & 7));
    }

    private static boolean isSlotPresent(byte[] fixedIndex, int slotId) {
        return (fixedIndex[SLOT_PRESENT_BITMAP_OFFSET + (slotId >>> 3)] & (1 << (slotId & 7))) != 0;
    }

    private static void requireZeroRange(byte[] source, int offset, int length, String message) {
        for (int i = offset; i < offset + length; i++) {
            if (source[i] != 0) {
                throw new IllegalArgumentException(message + ", byteOffset=" + i);
            }
        }
    }

    private static void putLong(byte[] target, int offset, long value) {
        target[offset] = (byte) (value >>> 56);
        target[offset + 1] = (byte) (value >>> 48);
        target[offset + 2] = (byte) (value >>> 40);
        target[offset + 3] = (byte) (value >>> 32);
        target[offset + 4] = (byte) (value >>> 24);
        target[offset + 5] = (byte) (value >>> 16);
        target[offset + 6] = (byte) (value >>> 8);
        target[offset + 7] = (byte) value;
    }

    private static long getLong(byte[] source, int offset) {
        return ((long) (source[offset] & 0xff) << 56)
            | ((long) (source[offset + 1] & 0xff) << 48)
            | ((long) (source[offset + 2] & 0xff) << 40)
            | ((long) (source[offset + 3] & 0xff) << 32)
            | ((long) (source[offset + 4] & 0xff) << 24)
            | ((long) (source[offset + 5] & 0xff) << 16)
            | ((long) (source[offset + 6] & 0xff) << 8)
            | (source[offset + 7] & 0xffL);
    }

    @FunctionalInterface
    public interface StoredChunkReader {
        /**
         * Read exactly {@code storedLength} bytes beginning at the absolute Page object offset.
         */
        byte[] read(long storedOffset, int storedLength) throws IOException;
    }

    public static final class Builder {
        private final long pageObjectAddr;
        private final long tableId;
        private final int seqId;
        private final TreeMap<Integer, byte[]> values = new TreeMap<>();
        private final int targetChunkRawBytes = DEFAULT_TARGET_CHUNK_RAW_BYTES;
        private long rawPayloadLength;

        private Builder(long pageObjectAddr, long tableId, int seqId) {
            if (!BlobObjectId.isPageObjectAddr(pageObjectAddr)) {
                throw new IllegalArgumentException(
                    "Invalid Page object address: " + Long.toUnsignedString(pageObjectAddr));
            }
            if (seqId < 0) {
                throw new IllegalArgumentException("Negative Page seqId: " + seqId);
            }
            this.pageObjectAddr = pageObjectAddr;
            this.tableId = tableId;
            this.seqId = seqId;
        }

        /**
         * Add one raw logical value. The caller must not modify {@code rawValue} until {@link #build()} returns.
         */
        public Builder addValue(int slotId, byte[] rawValue) {
            if (slotId < 0 || slotId >= MAX_ENTRIES) {
                throw new IllegalArgumentException("slotId is outside [0, " + MAX_ENTRIES + "): " + slotId);
            }
            if (rawValue == null) {
                throw new IllegalArgumentException("rawValue is null for slotId=" + slotId);
            }
            if (rawValue.length > MAX_LOGICAL_VALUE_BYTES) {
                throw new IllegalArgumentException("logical value exceeds the supported Page limit: "
                    + rawValue.length);
            }
            if (values.containsKey(slotId)) {
                throw new IllegalArgumentException("Duplicate Page slotId: " + slotId);
            }
            if (values.size() == MAX_ENTRIES) {
                throw new IllegalArgumentException("Page exceeds " + MAX_ENTRIES + " slots");
            }
            long newRawPayloadLength = rawPayloadLength + rawValue.length;
            if (newRawPayloadLength > MAX_JAVA_ARRAY_LENGTH) {
                throw new IllegalArgumentException("Page raw payload exceeds Java array limit: "
                    + newRawPayloadLength);
            }
            values.put(slotId, rawValue);
            rawPayloadLength = newRawPayloadLength;
            return this;
        }

        public int getEntryCount() {
            return values.size();
        }

        public long getRawPayloadLength() {
            return rawPayloadLength;
        }

        public BuiltPage build() {
            if (values.isEmpty()) {
                throw new IllegalStateException("Cannot build an empty Blob Page");
            }
            int rawLength = (int) rawPayloadLength;
            int chunkCount = rawLength == 0 ? 0
                : (int) ((rawPayloadLength + targetChunkRawBytes - 1L) / targetChunkRawBytes);
            if (chunkCount > MAX_CHUNKS) {
                throw new IllegalArgumentException("Page exceeds " + MAX_CHUNKS + " chunks: " + chunkCount);
            }

            byte[] fixedIndex = new byte[FIXED_INDEX_LENGTH];
            int rawOffset = 0;
            for (Map.Entry<Integer, byte[]> valueEntry : values.entrySet()) {
                int slotId = valueEntry.getKey();
                byte[] rawValue = valueEntry.getValue();
                setSlotPresent(fixedIndex, slotId);
                putUnsignedInt(fixedIndex, RAW_OFFSET_TABLE_OFFSET + slotId * SLOT_INDEX_ENTRY_SIZE, rawOffset);
                rawOffset += rawValue.length;
            }

            RawChunkCursor sizingCursor = new RawChunkCursor(values.values());
            long storedOffset = FIXED_PAYLOAD_OFFSET;
            for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
                int chunkRawOffset = chunkId * targetChunkRawBytes;
                int chunkRawLength = Math.min(targetChunkRawBytes, rawLength - chunkRawOffset);
                byte[] rawChunk = sizingCursor.nextChunk(chunkRawLength);
                BlobCompressionCodec.Encoded encoded = BlobCompressionCodec.compress(rawChunk, 0);
                byte[] storedChunk = encoded.getData();
                long newStoredOffset = checkedUnsignedIntAdd(storedOffset, storedChunk.length,
                    "builder stored range");
                if (newStoredOffset > MAX_JAVA_ARRAY_LENGTH) {
                    throw new IllegalArgumentException("Page physical length exceeds Java array limit: "
                        + newStoredOffset);
                }

                if (storedChunk.length > UINT24_MAX) {
                    throw new IllegalArgumentException("Page chunk stored length exceeds uint24: "
                        + storedChunk.length);
                }
                int indexOffset = COMPACT_CHUNK_TABLE_OFFSET + chunkId * COMPACT_CHUNK_DESCRIPTOR_SIZE;
                putUnsignedInt(fixedIndex, indexOffset + CHUNK_STORED_OFFSET, storedOffset);
                putUnsignedMedium(fixedIndex, indexOffset + CHUNK_STORED_LENGTH_OFFSET, storedChunk.length);
                fixedIndex[indexOffset + CHUNK_CODEC_OFFSET] = (byte) encoded.getCodec();
                putInt(fixedIndex, indexOffset + CHUNK_CRC_OFFSET,
                    crc32c(storedChunk, 0, storedChunk.length));
                storedOffset = newStoredOffset;
            }

            int pageLength = (int) storedOffset;
            byte[] pageData = new byte[pageLength];
            System.arraycopy(fixedIndex, 0, pageData, FIXED_HEADER_LENGTH, fixedIndex.length);
            RawChunkCursor writingCursor = new RawChunkCursor(values.values());
            int payloadWriteOffset = FIXED_PAYLOAD_OFFSET;
            for (int chunkId = 0; chunkId < chunkCount; chunkId++) {
                int indexOffset = COMPACT_CHUNK_TABLE_OFFSET + chunkId * COMPACT_CHUNK_DESCRIPTOR_SIZE;
                int chunkRawLength = Math.min(targetChunkRawBytes,
                    rawLength - chunkId * targetChunkRawBytes);
                byte[] rawChunk = writingCursor.nextChunk(chunkRawLength);
                BlobCompressionCodec.Encoded encoded = BlobCompressionCodec.compress(rawChunk, 0);
                byte[] storedChunk = encoded.getData();
                int expectedCodec = fixedIndex[indexOffset + CHUNK_CODEC_OFFSET] & 0xff;
                int expectedStoredLength =
                    getUnsignedMedium(fixedIndex, indexOffset + CHUNK_STORED_LENGTH_OFFSET);
                int expectedCrc = getInt(fixedIndex, indexOffset + CHUNK_CRC_OFFSET);
                if (encoded.getCodec() != expectedCodec || storedChunk.length != expectedStoredLength
                    || crc32c(storedChunk, 0, storedChunk.length) != expectedCrc) {
                    throw new IllegalStateException("Non-deterministic Page chunk compression at chunk=" + chunkId);
                }
                System.arraycopy(storedChunk, 0, pageData, payloadWriteOffset, storedChunk.length);
                payloadWriteOffset += storedChunk.length;
            }

            putInt(pageData, HEADER_MAGIC_OFFSET, MAGIC);
            pageData[HEADER_VERSION_OFFSET] = (byte) PAGE_FORMAT_VERSION;
            pageData[HEADER_FLAGS_OFFSET] = 0;
            putUnsignedShort(pageData, HEADER_LENGTH_OFFSET, FIXED_HEADER_LENGTH);
            putLong(pageData, HEADER_PAGE_ADDR_OFFSET, pageObjectAddr);
            putLong(pageData, HEADER_TABLE_ID_OFFSET, tableId);
            putInt(pageData, HEADER_SEQ_ID_OFFSET, seqId);
            putUnsignedShort(pageData, HEADER_ENTRY_COUNT_OFFSET, values.size());
            putUnsignedShort(pageData, HEADER_CHUNK_COUNT_OFFSET, chunkCount);
            putUnsignedShort(pageData, HEADER_SLOT_ENTRY_SIZE_OFFSET, SLOT_INDEX_ENTRY_SIZE);
            putUnsignedShort(pageData, HEADER_CHUNK_ENTRY_SIZE_OFFSET, CHUNK_INDEX_ENTRY_SIZE);
            putUnsignedInt(pageData, HEADER_SLOT_INDEX_BYTES_OFFSET, FIXED_SLOT_INDEX_BYTES);
            putUnsignedInt(pageData, HEADER_CHUNK_INDEX_BYTES_OFFSET, FIXED_CHUNK_INDEX_BYTES);
            putUnsignedInt(pageData, HEADER_PAYLOAD_OFFSET, FIXED_PAYLOAD_OFFSET);
            putUnsignedInt(pageData, HEADER_PAGE_LENGTH_OFFSET, pageLength);
            putUnsignedInt(pageData, HEADER_RAW_PAYLOAD_LENGTH_OFFSET, rawPayloadLength);
            putInt(pageData, HEADER_METADATA_CRC_OFFSET,
                crc32c(pageData, FIXED_HEADER_LENGTH, FIXED_INDEX_LENGTH));
            putInt(pageData, HEADER_CRC_OFFSET, 0);
            putInt(pageData, HEADER_CRC_OFFSET, crc32c(pageData, 0, FIXED_HEADER_LENGTH));

            // Re-parse the built bytes to fail fast if builder and reader invariants diverge.
            parse(pageData);
            return new BuiltPage(pageData);
        }

        /**
         * Streams the logical values into fixed-size raw chunks without allocating a second full
         * raw-payload array. Compression is intentionally run in two deterministic passes: the
         * first sizes/indexes the Page, and the second writes directly into the final Page array.
         */
        private static final class RawChunkCursor {
            private final java.util.Iterator<byte[]> values;
            private byte[] current;
            private int currentOffset;

            private RawChunkCursor(Iterable<byte[]> values) {
                this.values = values.iterator();
            }

            private byte[] nextChunk(int length) {
                byte[] chunk = new byte[length];
                int chunkOffset = 0;
                while (chunkOffset < length) {
                    while (current == null || currentOffset == current.length) {
                        if (!values.hasNext()) {
                            throw new IllegalStateException("Page raw values end before declared payload length");
                        }
                        current = values.next();
                        currentOffset = 0;
                    }
                    int copied = Math.min(length - chunkOffset, current.length - currentOffset);
                    System.arraycopy(current, currentOffset, chunk, chunkOffset, copied);
                    currentOffset += copied;
                    chunkOffset += copied;
                }
                return chunk;
            }
        }
    }

    public static final class BuiltPage {
        private final byte[] pageData;

        private BuiltPage(byte[] pageData) {
            this.pageData = pageData;
        }

        /**
         * Returns the Page object bytes. The caller must treat the returned array as immutable.
         */
        public byte[] getPageData() {
            return pageData;
        }
    }

    public static final class Header {
        private final long pageObjectAddr;
        private final long tableId;
        private final int seqId;
        private final int entryCount;
        private final int chunkCount;
        private final long slotIndexBytes;
        private final long chunkIndexBytes;
        private final long payloadOffset;
        private final long pageLength;
        private final long rawPayloadLength;
        private final int metadataCrc32c;
        private final int headerCrc32c;

        private Header(long pageObjectAddr, long tableId, int seqId, int entryCount, int chunkCount,
                       long slotIndexBytes, long chunkIndexBytes, long payloadOffset, long pageLength,
                       long rawPayloadLength, int metadataCrc32c, int headerCrc32c) {
            this.pageObjectAddr = pageObjectAddr;
            this.tableId = tableId;
            this.seqId = seqId;
            this.entryCount = entryCount;
            this.chunkCount = chunkCount;
            this.slotIndexBytes = slotIndexBytes;
            this.chunkIndexBytes = chunkIndexBytes;
            this.payloadOffset = payloadOffset;
            this.pageLength = pageLength;
            this.rawPayloadLength = rawPayloadLength;
            this.metadataCrc32c = metadataCrc32c;
            this.headerCrc32c = headerCrc32c;
        }

        public long getPageObjectAddr() {
            return pageObjectAddr;
        }

        public long getTableId() {
            return tableId;
        }

        public int getSeqId() {
            return seqId;
        }

        public int getEntryCount() {
            return entryCount;
        }

        public int getChunkCount() {
            return chunkCount;
        }

        public int getMetadataLength() {
            return (int) (slotIndexBytes + chunkIndexBytes);
        }

        public long getPayloadOffset() {
            return payloadOffset;
        }

        public long getPageLength() {
            return pageLength;
        }

        public long getRawPayloadLength() {
            return rawPayloadLength;
        }

    }

    public static final class SlotIndexEntry {
        private final int slotId;
        private final int flags;
        private final long rawOffset;
        private final int rawLength;

        private SlotIndexEntry(int slotId, int flags, long rawOffset, int rawLength) {
            this.slotId = slotId;
            this.flags = flags;
            this.rawOffset = rawOffset;
            this.rawLength = rawLength;
        }

        public int getRawLength() {
            return rawLength;
        }
    }

    public static final class ChunkIndexEntry {
        private final long rawOffset;
        private final int rawLength;
        private final long storedOffset;
        private final int storedLength;
        private final int codec;
        private final int flags;
        private final int storedCrc32c;

        private ChunkIndexEntry(long rawOffset, int rawLength, long storedOffset, int storedLength,
                                int codec, int flags, int storedCrc32c) {
            this.rawOffset = rawOffset;
            this.rawLength = rawLength;
            this.storedOffset = storedOffset;
            this.storedLength = storedLength;
            this.codec = codec;
            this.flags = flags;
            this.storedCrc32c = storedCrc32c;
        }

        public int getCodec() {
            return codec;
        }
    }

    /**
     * One physically contiguous stored range planned for a single callback invocation.
     */
    public static final class StoredRange {
        private final long storedOffset;
        private final int storedLength;
        private final List<ChunkIndexEntry> chunks;

        private StoredRange(long storedOffset, int storedLength, List<ChunkIndexEntry> chunks) {
            this.storedOffset = storedOffset;
            this.storedLength = storedLength;
            this.chunks = Collections.unmodifiableList(new ArrayList<>(chunks));
        }

        public long getStoredOffset() {
            return storedOffset;
        }

        public int getStoredLength() {
            return storedLength;
        }

        public int getChunkCount() {
            return chunks.size();
        }
    }

    public static final class PageMetadata {
        private final Header header;
        private final byte[] presenceBitmap;
        private final int[] rawOffsets;
        private final ChunkIndexEntry[] chunks;
        private final List<ChunkIndexEntry> chunkView;

        private PageMetadata(Header header, byte[] presenceBitmap, int[] rawOffsets,
                             ChunkIndexEntry[] chunks) {
            this.header = header;
            this.presenceBitmap = presenceBitmap;
            this.rawOffsets = rawOffsets;
            this.chunks = chunks;
            this.chunkView = Collections.unmodifiableList(Arrays.asList(chunks));
        }

        public Header getHeader() {
            return header;
        }

        public List<ChunkIndexEntry> getChunks() {
            return chunkView;
        }

        public SlotIndexEntry findSlot(int slotId) {
            if (slotId < 0 || slotId >= MAX_ENTRIES || !isSlotPresent(presenceBitmap, slotId)) {
                return null;
            }
            int nextSlotId = nextPresentSlot(presenceBitmap, slotId + 1);
            long rawOffset = rawOffsets[slotId];
            long rawEnd = nextSlotId < 0 ? header.rawPayloadLength : rawOffsets[nextSlotId];
            return new SlotIndexEntry(slotId, 0, rawOffset, (int) (rawEnd - rawOffset));
        }

        public SlotIndexEntry requireSlot(int slotId) {
            if (slotId < 0 || slotId >= MAX_ENTRIES) {
                throw new IllegalArgumentException("slotId is outside [0, " + MAX_ENTRIES + "): " + slotId);
            }
            SlotIndexEntry slot = findSlot(slotId);
            if (slot == null) {
                throw new IllegalArgumentException("Slot does not exist in Page: " + slotId);
            }
            return slot;
        }

        public List<ChunkIndexEntry> chunksFor(SlotIndexEntry slot) {
            if (slot == null) {
                throw new IllegalArgumentException("Page slot is null");
            }
            if (slot.rawLength == 0) {
                return Collections.emptyList();
            }
            int firstChunkId = (int) (slot.rawOffset / DEFAULT_TARGET_CHUNK_RAW_BYTES);
            long slotEnd = slot.rawOffset + slot.rawLength;
            int lastChunkId = (int) ((slotEnd - 1) / DEFAULT_TARGET_CHUNK_RAW_BYTES);
            if (firstChunkId < 0 || lastChunkId < firstChunkId || lastChunkId >= chunks.length) {
                throw new IllegalArgumentException("Page slot chunk range is outside parsed metadata");
            }
            return chunkView.subList(firstChunkId, lastChunkId + 1);
        }

        /**
         * Conservative retained-heap estimate for cache weighing. It includes the fixed primitive slot tables,
         * the actual chunk descriptor array/objects, the header, and container overhead.
         */
        public long estimateHeapBytes() {
            return 256L
                + 16L + presenceBitmap.length
                + 16L + (long) rawOffsets.length * Integer.BYTES
                + 16L + (long) chunks.length * Long.BYTES
                + (long) chunks.length * 64L;
        }
    }

    private static int nextPresentSlot(byte[] presenceBitmap, int fromSlotId) {
        if (fromSlotId >= MAX_ENTRIES) {
            return -1;
        }
        int byteIndex = Math.max(0, fromSlotId) >>> 3;
        int bitIndex = Math.max(0, fromSlotId) & 7;
        int bits = (presenceBitmap[byteIndex] & 0xff) & (0xff << bitIndex);
        while (true) {
            if (bits != 0) {
                return (byteIndex << 3) + Integer.numberOfTrailingZeros(bits);
            }
            byteIndex++;
            if (byteIndex >= presenceBitmap.length) {
                return -1;
            }
            bits = presenceBitmap[byteIndex] & 0xff;
        }
    }
}
