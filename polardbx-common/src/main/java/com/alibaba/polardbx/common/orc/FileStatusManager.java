package com.alibaba.polardbx.common.orc;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public interface FileStatusManager {
    FileStatus getFileStatus(Path path);
}
