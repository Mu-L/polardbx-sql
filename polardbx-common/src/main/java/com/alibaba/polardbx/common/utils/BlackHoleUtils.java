package com.alibaba.polardbx.common.utils;

public class BlackHoleUtils {
    public static String getInsertToDeleteBlackHoleTableName(final String tableName) {
        return "__$_" + tableName;
    }
}
