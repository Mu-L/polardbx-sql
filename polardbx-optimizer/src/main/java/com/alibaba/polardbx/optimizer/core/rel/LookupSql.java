package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.BytesSql;

public class LookupSql {

    public LookupSql(BytesSql bytesSql, BytesSql bytesSqlWithoutMget, BytesSql startSql, String orderBy,
                     String selectNode) {
        this.bytesSql = bytesSql;
        this.bytesSqlWithoutMget = bytesSqlWithoutMget;
        this.startSql = startSql;
        this.orderBy = orderBy;
        this.selectNode = selectNode;
    }

    public BytesSql bytesSql;
    public BytesSql bytesSqlWithoutMget;
    public BytesSql startSql;
    public String orderBy;
    public String selectNode;

}
