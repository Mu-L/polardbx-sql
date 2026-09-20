package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

public enum CTEMode {
    AUTO,
    REUSE,
    INLINE;

    public static CTEMode parse(String value) {
        if (value == null) {
            return INLINE;
        }
        for (CTEMode mode : values()) {
            if (mode.name().equalsIgnoreCase(value.trim())) {
                return mode;
            }
        }
        return INLINE;
    }
}
