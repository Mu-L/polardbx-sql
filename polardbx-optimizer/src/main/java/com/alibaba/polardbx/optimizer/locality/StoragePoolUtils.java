package com.alibaba.polardbx.optimizer.locality;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StoragePoolUtils {
    public static String RECYCLE_STORAGE_POOL = "_recycle";
    public static String DEFAULT_STORAGE_POOL = "_default";

    public static String LOCK_PREFIX = "lock_storage_pool";

    public static String FULL_LOCK_NAME = "full_";

    public static String buildStringFromStorageInstList(Collection<String> storageInstList) {
        if (storageInstList == null) {
            return "";
        }
        return String.join(",", new HashSet<>(storageInstList));
    }

    public static List<String> buildStorageInstListFromString(String storageInstListStr) {
        if (com.alibaba.polardbx.druid.util.StringUtils.isEmpty(storageInstListStr)) {
            return new ArrayList<>();
        }
        String[] storageInstList = storageInstListStr.split(",");
        return Arrays.stream(storageInstList).collect(Collectors.toList());
    }

    public static List<String> mergeDnIdList(List<String> dnList1, List<String> dnList2) {
        if (dnList1 == null) {
            return dnList2;
        } else if (dnList2 == null) {
            return dnList1;
        } else {
            Set<String> dnSet = new HashSet<>(dnList1);
            dnSet.addAll(dnList2);
            return new ArrayList<>(dnSet);
        }
    }
}
