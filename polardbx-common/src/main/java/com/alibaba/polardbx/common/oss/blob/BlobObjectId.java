package com.alibaba.polardbx.common.oss.blob;

/**
 * Blob Page address encoding/decoding utility.
 *
 * <p>A logical value is addressed by a 64-bit {@code slotAddr}:
 *
 * <pre>
 * ┌──────────────────┬────────────────────────────────┬──────────────────┐
 * │ marker (8 bits)  │ global blobPageId (41 bits)    │ slotId (15 bits) │
 * │   = 0x80         │                                │                  │
 * └──────────────────┴────────────────────────────────┴──────────────────┘
 * </pre>
 *
 * <p>The full {@code slotAddr} identifies one logical value and is used by BlobRef and staging. The Page object
 * identity is {@link #clearSlotBits(long)} and is used by GeneralCache and OSS. Callers must not use a full slot
 * address as a Page object identity, even though the two numeric values are equal when {@code slotId == 0}.
 */
public final class BlobObjectId {

    /**
     * High 8 bits = 0x80 marker for the Page slot address format.
     */
    public static final long MARKER = 0x80L << 56;

    public static final long MARKER_MASK = 0xFFL << 56;

    /**
     * Low 56 bits used by the read-only V0/V1 single-object address format.
     */
    public static final long LEGACY_SEQUENCE_MASK = (1L << 56) - 1;
    public static final long MAX_LEGACY_SEQUENCE = LEGACY_SEQUENCE_MASK;

    public static final int SLOT_BITS = 15;
    public static final int MAX_SLOT_ID = (1 << SLOT_BITS) - 1;
    public static final long SLOT_MASK = MAX_SLOT_ID;

    public static final int BLOB_PAGE_ID_BITS = 41;
    public static final long BLOB_PAGE_ID_MASK = (1L << BLOB_PAGE_ID_BITS) - 1;
    public static final long MAX_BLOB_PAGE_ID = BLOB_PAGE_ID_MASK;

    /**
     * Deprecated naming alias for the 41-bit Page ID mask; this is not the legacy 56-bit sequence mask.
     */
    @Deprecated
    public static final long SEQUENCE_MASK = BLOB_PAGE_ID_MASK;

    /**
     * Deprecated naming alias for the maximum Page ID; this is not the maximum legacy sequence.
     */
    @Deprecated
    public static final long MAX_SEQUENCE = MAX_BLOB_PAGE_ID;

    private BlobObjectId() {
    }

    /**
     * Encode the logical address of one slot in a globally unique Blob Page.
     *
     * @param blobPageId globally allocated Page ID in {@code [1, 2^41)}
     * @param slotId slot in {@code [0, 32767]}
     */
    public static long encode(long blobPageId, int slotId) {
        if (blobPageId <= 0 || blobPageId > MAX_BLOB_PAGE_ID) {
            throw new IllegalArgumentException(
                "Blob Page ID out of range [1, 2^41): " + blobPageId);
        }
        if (slotId < 0 || slotId > MAX_SLOT_ID) {
            throw new IllegalArgumentException(
                "Blob Page slot ID out of range [0, 32767]: " + slotId);
        }
        return MARKER | blobPageId << SLOT_BITS | slotId;
    }

    /**
     * Encode the Page object address for a single-slot Page.
     *
     * @deprecated use {@link #encode(long, int)} with an explicit slot ID
     */
    @Deprecated
    public static long encode(long blobPageId) {
        return encode(blobPageId, 0);
    }

    /**
     * Extract the global Page ID from a slot or Page object address.
     */
    public static long decodeBlobPageId(long slotAddr) {
        validateSlotAddr(slotAddr);
        return slotAddr >>> SLOT_BITS & BLOB_PAGE_ID_MASK;
    }

    /**
     * Extract the logical slot ID from a slot address.
     */
    public static int decodeSlotId(long slotAddr) {
        validateSlotAddr(slotAddr);
        return (int) (slotAddr & SLOT_MASK);
    }

    /**
     * Clear the logical slot bits and return the immutable Page object identity used by OSS and GeneralCache.
     */
    public static long clearSlotBits(long slotAddr) {
        validateSlotAddr(slotAddr);
        return slotAddr & ~SLOT_MASK;
    }

    /**
     * Return the GeneralCache file ID for a Page object.
     *
     * <p>This explicit method rejects a full address with a non-zero slot ID, preventing per-value data from entering
     * the Page cache namespace.
     */
    public static long pageCacheFileId(long pageObjectAddr) {
        validatePageObjectAddr(pageObjectAddr);
        return pageObjectAddr;
    }

    /**
     * Return whether the value is a structurally valid slot or Page object address.
     */
    public static boolean isValidSlotAddr(long slotAddr) {
        return (slotAddr & MARKER_MASK) == MARKER
            && (slotAddr >>> SLOT_BITS & BLOB_PAGE_ID_MASK) != 0;
    }

    /**
     * Return whether the value is a structurally valid Page object address with cleared slot bits.
     */
    public static boolean isPageObjectAddr(long pageObjectAddr) {
        return isValidSlotAddr(pageObjectAddr) && (pageObjectAddr & SLOT_MASK) == 0;
    }

    /**
     * Return whether the value is a valid read-only V0/V1 single-object address.
     */
    public static boolean isValidLegacyBlobAddr(long legacyBlobAddr) {
        return (legacyBlobAddr & MARKER_MASK) == MARKER
            && (legacyBlobAddr & LEGACY_SEQUENCE_MASK) != 0;
    }

    /**
     * Build the OSS object key for an immutable Page. Format: {@code {tableId}/{unsigned pageObjectAddr}}.
     */
    public static String buildPageOSSKey(long tableId, long pageObjectAddr) {
        validatePageObjectAddr(pageObjectAddr);
        return tableId + "/" + Long.toUnsignedString(pageObjectAddr);
    }

    /**
     * Build an OSS key for a read-only V0/V1 single-object address.
     *
     * <p>Legacy address low bits are part of its 56-bit sequence and must never be interpreted as a Page slot ID.
     */
    public static String buildLegacyOSSKey(long tableId, long legacyBlobAddr) {
        if (!isValidLegacyBlobAddr(legacyBlobAddr)) {
            throw new IllegalArgumentException(
                "Invalid legacy Blob object address: " + Long.toUnsignedString(legacyBlobAddr));
        }
        return tableId + "/" + Long.toUnsignedString(legacyBlobAddr);
    }

    /**
     * Compatibility alias for Page-aware callers.
     *
     * @deprecated use {@link #buildPageOSSKey(long, long)} to make the address domain explicit
     */
    @Deprecated
    public static String buildOSSKey(long tableId, long pageObjectAddr) {
        return buildPageOSSKey(tableId, pageObjectAddr);
    }

    private static void validateSlotAddr(long slotAddr) {
        if (!isValidSlotAddr(slotAddr)) {
            throw new IllegalArgumentException(
                "Invalid Blob Page slot address: " + Long.toUnsignedString(slotAddr));
        }
    }

    private static void validatePageObjectAddr(long pageObjectAddr) {
        if (!isPageObjectAddr(pageObjectAddr)) {
            throw new IllegalArgumentException(
                "Invalid Blob Page object address: " + Long.toUnsignedString(pageObjectAddr));
        }
    }
}
