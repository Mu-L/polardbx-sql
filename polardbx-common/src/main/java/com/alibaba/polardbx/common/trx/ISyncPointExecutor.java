package com.alibaba.polardbx.common.trx;

/**
 * @author yaozhili
 */
public interface ISyncPointExecutor {
    /**
     * @return -1 if failed, or commit TSO if success.
     */
    long execute(long tableId);
}
