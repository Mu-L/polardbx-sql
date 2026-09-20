package com.alibaba.polardbx.common.orc;

/**
 * Thrown when the BinaryMetadata embedded in an ORC footer violates a known invariant.
 * Caught by {@link ORCMetaReaderImpl#preheat} and used to fall back to the legacy
 * {@code preheatStripe} path. Not part of any public API contract; serves read-side
 * defense only.
 */
public class BinaryMetadataCorruptException extends RuntimeException {

    private final String branch;
    private final String path;
    private final int stripeIdx;
    private final int columnIdx;

    public BinaryMetadataCorruptException(String path, String branch, String detail) {
        this(path, branch, -1, -1, detail);
    }

    public BinaryMetadataCorruptException(String path, String branch, int stripeIdx, int columnIdx, String detail) {
        super(buildMessage(path, branch, stripeIdx, columnIdx, detail));
        this.path = path;
        this.branch = branch;
        this.stripeIdx = stripeIdx;
        this.columnIdx = columnIdx;
    }

    private static String buildMessage(String path, String branch, int stripeIdx, int columnIdx, String detail) {
        StringBuilder sb = new StringBuilder("BinaryMetadataCorruptException: branch=").append(branch);
        if (stripeIdx >= 0) {
            sb.append(", stripeIdx=").append(stripeIdx);
        }
        if (columnIdx >= 0) {
            sb.append(", columnIdx=").append(columnIdx);
        }
        sb.append(", path=").append(path);
        if (detail != null && !detail.isEmpty()) {
            sb.append(", detail=").append(detail);
        }
        return sb.toString();
    }

    public String getBranch() {
        return branch;
    }

    public String getPath() {
        return path;
    }

    public int getStripeIdx() {
        return stripeIdx;
    }

    public int getColumnIdx() {
        return columnIdx;
    }
}
