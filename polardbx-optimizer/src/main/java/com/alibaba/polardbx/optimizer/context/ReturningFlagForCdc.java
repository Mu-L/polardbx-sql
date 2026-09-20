package com.alibaba.polardbx.optimizer.context;

/**
 * Enum representing the returning flag for CDC (Change Data Capture).
 * Used to indicate how CDC should handle the returning data in different DML scenarios.
 */
public enum ReturningFlagForCdc {

    /**
     * No special handling required.
     */
    NO_SPECIAL(0),

    /**
     * Use returning_all for replace. CDC needs to ignore order for fix delete.
     */
    REPLACE_IGNORE_ORDER(1),

    /**
     * Use returning for insert ignore. CDC needs to merge fix delete and prev insert.
     */
    INSERT_IGNORE_MERGE(2);

    private final int value;

    ReturningFlagForCdc(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    public static ReturningFlagForCdc fromValue(int value) {
        for (ReturningFlagForCdc flag : values()) {
            if (flag.value == value) {
                return flag;
            }
        }
        throw new IllegalArgumentException("Unknown ReturningFlagForCdc value: " + value);
    }
}
