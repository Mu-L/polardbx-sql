package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.executor.fastchecker.CheckerBatch;
import lombok.Data;

/**
 * @author wumu
 */
@Data
public class HashCheckResult {
    // source or target
    public Boolean isSource;
    // 所有未修改的列的 hash 值
    public Long commonHash;
    // 变更列的 hash 值
    public Long originColumnHash;
    // 变更列对应虚拟列的 hash 值
    public Long checkColumnHash;
    // checker batch
    public CheckerBatch checkerBatch;
}
