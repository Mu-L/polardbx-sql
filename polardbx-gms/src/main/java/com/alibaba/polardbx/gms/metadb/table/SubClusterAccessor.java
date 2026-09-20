package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.ArrayList;
import java.util.List;

public class SubClusterAccessor extends AbstractAccessor {

    private static final String QUERY_RECORD = "select id, inst_id, node, sub_cluster, gmt_created from sub_clusters";

    public List<SubClusterRecord> query() {
        try {
            return MetaDbUtil.query(QUERY_RECORD, SubClusterRecord.class, connection);
        } catch (Exception e) {
            // return a empty list to make sure cn can startup gracefully
            return new ArrayList<>();
        }
    }
}
