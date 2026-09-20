package com.alibaba.polardbx.common.orc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.Set;

public interface PreheatMetaManager extends FileStatusManager {
    PreheatMetaManager INSTANCE = new PreheatMetaManagerImpl();

    static PreheatMetaManager getInstance() {
        return INSTANCE;
    }

    PreheatFileMeta get(Path path, FileSystem fileSystem) throws Throwable;

    default PreheatFileMeta get(Path path, FileSystem fileSystem, Integer sortKeyColumn) throws Throwable {
        return get(path, fileSystem, Collections.singleton(sortKeyColumn));
    }

    PreheatFileMeta get(Path path, FileSystem fileSystem, Set<Integer> sortKeyColumns) throws Throwable;

    void resizeMaximumMemorySize(long maxMemorySize);

    byte[][] getCacheStat();

    Object[] dumpTotalUsage();

    long entries();

    long memorySize();

    long hitCount();

    long missCount();

    long exceedCount();
}
