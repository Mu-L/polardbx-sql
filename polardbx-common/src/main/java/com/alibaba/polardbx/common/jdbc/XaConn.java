package com.alibaba.polardbx.common.jdbc;

import java.sql.SQLException;

public abstract class XaConn implements IConnection {
    /**
     * Xid 缓存
     * 避免重复构造 {@link com.alibaba.polardbx.transaction.utils.XAUtils.XATransInfo}
     */
    private String xid = null;
    private long seq = -1;
    private String dnId = null;

    @Override
    public String getTrxXid() {
        return xid;
    }

    @Override
    public void setTrxXid(String xid) {
        this.xid = xid;
    }

    @Override
    public void close() throws SQLException {
        this.xid = null;
    }

    @Override
    public long getSeq() {
        return seq;
    }

    @Override
    public void setSeq(long seq) {
        this.seq = seq;
    }

    @Override
    public String getDnId() {
        return dnId;
    }

    @Override
    public void setDnId(String dnId) {
        this.dnId = dnId;
    }

}
